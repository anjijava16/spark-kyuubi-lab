"""
14_iceberg_demo.py [input_csv]          submit with: --with iceberg

Apache Iceberg end to end, using catalog "ice" = Iceberg tables registered
in the lab's Hive Metastore, data files on s3a://warehouse/iceberg/.

  1. CREATE TABLE ... PARTITIONED BY (months(order_date))   hidden partitioning
  2. append the CSV                                         snapshot 1
  3. MERGE INTO (update 10 orders, insert 5 new)            snapshot 2
  4. DELETE row-level                                       snapshot 3
  5. ALTER TABLE ADD COLUMN + UPDATE                        schema evolution
  6. ALTER TABLE ADD PARTITION FIELD region                 partition evolution
  7. time travel: VERSION AS OF <snapshot 1>                must equal step 2
  8. metadata tables: history, snapshots, files

Rerunnable: the table is dropped and recreated each run.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

src = sys.argv[1] if len(sys.argv) > 1 else "s3a://data/input/orders.csv"
T = "ice.lab.orders"

spark = SparkSession.builder.appName("14-iceberg-demo").getOrCreate()
sql = spark.sql

SCHEMA = StructType([
    StructField("order_id", IntegerType()), StructField("order_date", DateType()),
    StructField("region", StringType()), StructField("product", StringType()),
    StructField("quantity", IntegerType()), StructField("unit_price", DoubleType()),
    StructField("customer_id", IntegerType()),
])
orders = (spark.read.option("header", True).schema(SCHEMA).csv(src)
          .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2)))
n = orders.count()


def count(table=T):
    return sql(f"SELECT count(*) c FROM {table}").first()["c"]


def latest_snapshot():
    return sql(f"SELECT snapshot_id FROM {T}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]


sql("CREATE NAMESPACE IF NOT EXISTS ice.lab")
sql(f"DROP TABLE IF EXISTS {T} PURGE")
sql(f"""
    CREATE TABLE {T} (
        order_id INT, order_date DATE, region STRING, product STRING,
        quantity INT, unit_price DOUBLE, customer_id INT, amount DOUBLE)
    USING iceberg
    PARTITIONED BY (months(order_date))
    TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
""")

# 2. append ---------------------------------------------------------------
orders.writeTo(T).append()
snap1 = latest_snapshot()
print(f"[append]  rows={count()} snapshot={snap1}")
assert count() == n

# 3. MERGE: bump quantity on 10 existing orders, add 5 new ones -----------
changes = (orders.orderBy("order_id").limit(10)
           .withColumn("quantity", F.col("quantity") + 1)
           .unionByName(orders.orderBy("order_id").limit(5)
                        .withColumn("order_id", F.col("order_id") + 1000)))
changes = changes.withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
changes.createOrReplaceTempView("changes")
sql(f"""
    MERGE INTO {T} t USING changes s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"[merge]   rows={count()} (expected {n + 5})")
assert count() == n + 5

# 4. row-level DELETE -----------------------------------------------------
to_delete = sql(f"SELECT count(*) c FROM {T} WHERE product = 'mouse' AND quantity <= 2").first()["c"]
sql(f"DELETE FROM {T} WHERE product = 'mouse' AND quantity <= 2")
print(f"[delete]  removed {to_delete} rows, rows={count()}")
assert count() == n + 5 - to_delete

# 5. schema evolution: new column, old files untouched --------------------
sql(f"ALTER TABLE {T} ADD COLUMN channel STRING")
sql(f"UPDATE {T} SET channel = CASE WHEN region IN ('North', 'East') THEN 'web' ELSE 'store' END")
print("[schema]  columns now:", [r.col_name for r in sql(f"DESCRIBE {T}").collect()
                                if r.col_name and not r.col_name.startswith("#")][:9])

# 6. partition evolution: new writes also split by region -----------------
sql(f"ALTER TABLE {T} ADD PARTITION FIELD region")
sql(f"INSERT INTO {T} VALUES (9999, DATE '2026-10-15', 'North', 'dock', 1, 149.0, 1, 149.0, 'web')")

# 7. time travel back to the first append --------------------------------
then = sql(f"SELECT count(*) c FROM {T} VERSION AS OF {snap1}").first()["c"]
print(f"[travel]  rows at snapshot {snap1} = {then}; rows now = {count()}")
assert then == n

# 8. metadata tables ------------------------------------------------------
print("history:")
sql(f"SELECT made_current_at, snapshot_id, is_current_ancestor FROM {T}.history").show(truncate=False)
print("snapshots (operation per commit):")
sql(f"SELECT committed_at, operation, summary['added-records'] added, "
    f"summary['deleted-records'] deleted FROM {T}.snapshots ORDER BY committed_at").show(truncate=False)
print("data files per partition:")
sql(f"SELECT partition, count(*) files, sum(record_count) records FROM {T}.files "
    f"GROUP BY partition ORDER BY 1").show(truncate=False)

loc_row = sql(f"DESCRIBE TABLE EXTENDED {T}").where("col_name = 'Location'").first()
loc = loc_row["data_type"] if loc_row else "?"
print(f"table location: {loc}")
assert loc == "?" or loc.startswith("s3a://warehouse/"), f"unexpected location {loc}"
print("ICEBERG DEMO OK")
spark.stop()
