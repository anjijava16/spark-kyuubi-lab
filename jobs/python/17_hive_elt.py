"""
17_hive_elt.py [landing_dir]

Hive-style ELT, all in SQL, the way many Cloudera/Hive jobs are written:
load raw files untouched, then transform inside the warehouse.

  E+L  external TEXTFILE table over the raw CSV folder (nothing is copied)
  T    1. staging  -> typed, cleaned table          STORED AS ORC
       2. facts    -> partitioned by order_month     STORED AS PARQUET
                      (INSERT OVERWRITE with dynamic partitions)
       3. archive  -> same data                      STORED AS RCFILE
       4. CTAS     -> monthly summary                STORED AS ORC
       5. a VIEW on top, ANALYZE TABLE for statistics,
          SHOW PARTITIONS / DESCRIBE FORMATTED to see what Hive recorded

Every table lives in database "elt" in the Hive Metastore, data under
s3a://warehouse/elt.db/. Rerunnable: tables are dropped first.
"""
import sys

from pyspark.sql import SparkSession

landing = (sys.argv[1] if len(sys.argv) > 1 else "s3a://data/input").rstrip("/")

spark = (SparkSession.builder.appName("17-hive-elt")
         .enableHiveSupport()
         .config("hive.exec.dynamic.partition", "true")
         .config("hive.exec.dynamic.partition.mode", "nonstrict")
         .getOrCreate())
sql = spark.sql


def count(t):
    return sql(f"SELECT count(*) c FROM {t}").first()["c"]


def fmt(t):
    rows = sql(f"DESCRIBE FORMATTED {t}").where("col_name IN ('InputFormat', 'Provider', 'Location')").collect()
    return {r.col_name: r.data_type for r in rows}


sql("CREATE DATABASE IF NOT EXISTS elt COMMENT 'Hive-style ELT demo'")
sql("USE elt")
for t in ("v_monthly_top", "monthly_summary", "orders_archive_rc", "orders_fact", "orders_clean", "raw_orders"):
    sql(f"DROP VIEW IF EXISTS {t}") if t.startswith("v_") else sql(f"DROP TABLE IF EXISTS {t}")

# ---- E + L: external table straight over the CSV folder -----------------
# Only orders.csv matches the schema, so point at a folder holding just it.
raw_dir = f"{landing}/elt_raw_orders"
spark.read.option("header", True).csv(f"{landing}/orders.csv") \
     .write.mode("overwrite").option("header", True).csv(raw_dir)
sql(f"""
    CREATE EXTERNAL TABLE raw_orders (
        order_id STRING, order_date STRING, region STRING, product STRING,
        quantity STRING, unit_price STRING, customer_id STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '{raw_dir}'
    TBLPROPERTIES ('skip.header.line.count' = '1')
""")
n_lines = count("raw_orders")
# Spark's reader may ignore skip.header.line.count, in which case the header
# line shows up as a row; count real data rows the way the next step does.
n_raw = sql("SELECT count(*) c FROM raw_orders WHERE try_cast(order_id AS INT) IS NOT NULL").first()["c"]
print(f"[raw]     raw_orders (external TEXTFILE over {raw_dir}): {n_lines} lines, "
      f"{n_raw} data rows, every column a string")

# ---- T1: typed + cleaned staging, ORC -----------------------------------
sql("""
    CREATE TABLE orders_clean STORED AS ORC AS
    -- try_cast, not cast: Spark 4 runs with ANSI SQL on, where CAST('abc' AS INT)
    -- throws. Raw text can hold anything, so cast defensively and filter.
    SELECT try_cast(order_id AS INT)                     AS order_id,
           try_cast(order_date AS DATE)                  AS order_date,
           upper(trim(region))                           AS region,
           lower(trim(product))                          AS product,
           try_cast(quantity AS INT)                     AS quantity,
           try_cast(unit_price AS DECIMAL(10,2))         AS unit_price,
           try_cast(customer_id AS INT)                  AS customer_id,
           CAST(try_cast(quantity AS INT) * try_cast(unit_price AS DECIMAL(10,2)) AS DECIMAL(12,2)) AS amount
    FROM raw_orders
    WHERE try_cast(order_id AS INT) IS NOT NULL      -- also drops the CSV header line
      AND try_cast(quantity AS INT) > 0
""")
print(f"[clean]   orders_clean (ORC): {count('orders_clean')} rows")

# ---- T2: partitioned fact, Parquet, dynamic partitions ------------------
sql("""
    CREATE TABLE orders_fact (
        order_id INT, order_date DATE, region STRING, product STRING,
        quantity INT, unit_price DECIMAL(10,2), customer_id INT, amount DECIMAL(12,2))
    PARTITIONED BY (order_month STRING)
    STORED AS PARQUET
""")
sql("""
    INSERT OVERWRITE TABLE orders_fact PARTITION (order_month)
    SELECT order_id, order_date, region, product, quantity, unit_price, customer_id, amount,
           date_format(order_date, 'yyyy-MM') AS order_month
    FROM orders_clean
""")
parts = [r[0] for r in sql("SHOW PARTITIONS orders_fact").collect()]
print(f"[fact]    orders_fact (PARQUET, partitioned): {count('orders_fact')} rows in partitions {parts}")

# ---- T3: archive copy, RCFile -------------------------------------------
sql("""
    CREATE TABLE orders_archive_rc (
        order_id INT, order_date DATE, region STRING, product STRING,
        quantity INT, unit_price DECIMAL(10,2), customer_id INT, amount DECIMAL(12,2))
    STORED AS RCFILE
""")
sql("INSERT OVERWRITE TABLE orders_archive_rc SELECT * FROM orders_clean")
print(f"[archive] orders_archive_rc (RCFILE): {count('orders_archive_rc')} rows")

# ---- T4: CTAS summary + T5: view, statistics ----------------------------
sql("""
    CREATE TABLE monthly_summary STORED AS ORC AS
    SELECT order_month, region,
           count(*)                 AS orders,
           sum(quantity)            AS units,
           CAST(sum(amount) AS DECIMAL(14,2)) AS revenue,
           count(DISTINCT customer_id) AS customers
    FROM orders_fact
    GROUP BY order_month, region
""")
sql("""
    CREATE VIEW v_monthly_top AS
    SELECT order_month, region, revenue,
           rank() OVER (PARTITION BY order_month ORDER BY revenue DESC) AS rank_in_month
    FROM monthly_summary
""")
sql("ANALYZE TABLE orders_fact PARTITION (order_month) COMPUTE STATISTICS")
sql("ANALYZE TABLE monthly_summary COMPUTE STATISTICS FOR ALL COLUMNS")

print("\nbest region per month (view v_monthly_top):")
sql("SELECT * FROM v_monthly_top WHERE rank_in_month = 1 ORDER BY order_month").show()

print("what the metastore recorded per table:")
for t in ("raw_orders", "orders_clean", "orders_fact", "orders_archive_rc", "monthly_summary"):
    f = fmt(t)
    print(f"  {t:18} {f.get('InputFormat', f.get('Provider', '?')).split('.')[-1]:32} {f.get('Location', '')}")

# ---- checks ---------------------------------------------------------------
n = count("orders_clean")
assert n == n_raw, f"clean {n} vs raw {n_raw}"
assert count("orders_fact") == n and count("orders_archive_rc") == n
rev_fact = sql("SELECT sum(amount) s FROM orders_fact").first()["s"]
rev_rc = sql("SELECT sum(amount) s FROM orders_archive_rc").first()["s"]
rev_sum = sql("SELECT sum(revenue) s FROM monthly_summary").first()["s"]
print(f"revenue: fact={rev_fact} rcfile={rev_rc} summary={rev_sum}")
assert rev_fact == rev_rc == rev_sum, "revenue differs between tables"
assert len(parts) >= 2, "expected several month partitions"
print("HIVE ELT OK")
spark.stop()
