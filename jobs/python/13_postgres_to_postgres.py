"""
13_postgres_to_postgres.py

Postgres -> Spark -> Postgres, two ways:

  daily_sales    full refresh: mode("overwrite") replaces the table
  customer_ltv   upsert: Spark has no JDBC "merge", so the standard pattern is
                 write to a staging table, then run INSERT ... ON CONFLICT
                 DO UPDATE inside Postgres, then drop the staging table.

The upsert runs twice on purpose; the second run must change nothing,
which is what "idempotent" means and what reruns in production need.

Needs: orders (11_file_to_postgres.py), customer_ltv (created by pg-init).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("13-postgres-to-postgres").getOrCreate()
c = spark.conf
URL, USER, PASSWORD = (c.get("spark.lab.pg.url"), c.get("spark.lab.pg.user"),
                       c.get("spark.lab.pg.password"))
PG = {"url": URL, "user": USER, "password": PASSWORD, "driver": "org.postgresql.Driver"}


def pg_exec(sql):
    """Run one statement in Postgres from the driver (DDL, upsert, ...)."""
    conn = spark._jvm.java.sql.DriverManager.getConnection(URL, USER, PASSWORD)
    try:
        return conn.createStatement().executeUpdate(sql)
    finally:
        conn.close()


def pg_query(sql):
    return spark.read.format("jdbc").options(**PG).option("query", sql).load()


orders = spark.read.format("jdbc").options(**PG).option("dbtable", "orders").load()

# ---- 1. full refresh ------------------------------------------------------
daily = (orders.groupBy("order_date", "region")
         .agg(F.count("*").alias("orders"), F.sum("quantity").alias("units"),
              F.round(F.sum("amount"), 2).alias("revenue"))
         .orderBy("order_date", "region"))
daily.write.format("jdbc").options(**PG).option("dbtable", "daily_sales") \
     .option("truncate", "true").mode("overwrite").save()
print(f"daily_sales refreshed: {daily.count()} rows")

# ---- 2. upsert via staging table ---------------------------------------
ltv = (orders.groupBy("customer_id")
       .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue"),
            F.max("order_date").alias("last_order")))

UPSERT = """
    INSERT INTO customer_ltv AS t (customer_id, orders, revenue, last_order, updated_at)
    SELECT customer_id, orders, revenue, last_order, now() FROM customer_ltv_stage
    ON CONFLICT (customer_id) DO UPDATE
       SET orders = EXCLUDED.orders, revenue = EXCLUDED.revenue,
           last_order = EXCLUDED.last_order, updated_at = now()
     WHERE (t.orders, t.revenue, t.last_order)
           IS DISTINCT FROM (EXCLUDED.orders, EXCLUDED.revenue, EXCLUDED.last_order)
"""

for attempt in (1, 2):
    ltv.write.format("jdbc").options(**PG).option("dbtable", "customer_ltv_stage") \
       .mode("overwrite").save()
    changed = pg_exec(UPSERT)
    pg_exec("DROP TABLE IF EXISTS customer_ltv_stage")
    print(f"upsert run {attempt}: {changed} rows inserted or changed")
    if attempt == 2:
        assert changed == 0, "second identical upsert should change nothing"

check = pg_query("SELECT count(*) n FROM customer_ltv").first()["n"]
expected = ltv.count()
print(f"customer_ltv rows={check} (customers with orders={expected})")
assert check >= expected

pg_query("""SELECT l.customer_id, c.full_name, c.tier, l.orders, l.revenue
            FROM customer_ltv l JOIN customers c USING (customer_id)
            ORDER BY l.revenue DESC LIMIT 5""").show(truncate=False)
print("POSTGRES TO POSTGRES OK")
spark.stop()
