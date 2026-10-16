"""
12_postgres_to_files.py [output_root]

Postgres -> files. Reads two Postgres tables and writes the joined result
to RustFS as Parquet (partitioned), ORC and a single CSV.

Three ways to read over JDBC, side by side:
  1. dbtable  = whole table                        (customers, small)
  2. partitioned read: partitionColumn/lowerBound/upperBound/numPartitions
     -> Spark opens N connections, each reading a range of order_id
  3. query    = SQL that runs inside Postgres; only the result comes back
     (push the GROUP BY down instead of shipping every row)

Needs: customers (seeded by pg-init) and orders (11_file_to_postgres.py).
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

root = (sys.argv[1] if len(sys.argv) > 1 else "s3a://data/output/pg_export").rstrip("/")

spark = SparkSession.builder.appName("12-postgres-to-files").getOrCreate()
c = spark.conf
PG = {"url": c.get("spark.lab.pg.url"), "user": c.get("spark.lab.pg.user"),
      "password": c.get("spark.lab.pg.password"), "driver": "org.postgresql.Driver"}


def jdbc():
    return spark.read.format("jdbc").options(**PG)


# 1. small table, read whole
customers = jdbc().option("dbtable", "customers").load()

# 2. bigger table, read in parallel ranges of order_id
bounds = jdbc().option("query", "SELECT min(order_id) lo, max(order_id) hi FROM orders").load().first()
orders = (jdbc().option("dbtable", "orders")
          .option("partitionColumn", "order_id")
          .option("lowerBound", bounds["lo"]).option("upperBound", bounds["hi"])
          .option("numPartitions", 4)
          .option("fetchsize", 1000)
          .load())
print(f"orders read with {orders.rdd.getNumPartitions()} parallel JDBC partitions")

# 3. aggregation pushed down to Postgres
by_tier = jdbc().option("query", """
    SELECT c.tier, count(*) AS orders, round(sum(o.amount)::numeric, 2) AS revenue
    FROM orders o JOIN customers c USING (customer_id)
    GROUP BY c.tier ORDER BY revenue DESC
""").load()
print("revenue by customer tier (computed inside Postgres):")
by_tier.show()

customer_orders = (orders.join(customers.select("customer_id", "full_name", "tier"), "customer_id")
                   .select("order_id", "order_date", "region", "product", "quantity",
                           "unit_price", "amount", "customer_id", "full_name", "tier"))
n = customer_orders.count()

outputs = {
    "parquet": lambda p: customer_orders.write.mode("overwrite").partitionBy("region").parquet(p),
    "orc": lambda p: customer_orders.write.mode("overwrite").orc(p),
    "csv": lambda p: customer_orders.coalesce(1).write.mode("overwrite").option("header", True).csv(p),
}
readers = {
    "parquet": lambda p: spark.read.parquet(p),
    "orc": lambda p: spark.read.orc(p),
    "csv": lambda p: spark.read.option("header", True).csv(p),
}
for fmt, write in outputs.items():
    path = f"{root}/customer_orders_{fmt}"
    write(path)
    back = readers[fmt](path).count()
    print(f"  {fmt:8} {back:4} rows -> {path}")
    assert back == n, f"{fmt}: wrote {n}, read back {back}"

by_tier.write.mode("overwrite").parquet(f"{root}/revenue_by_tier")
print("POSTGRES TO FILES OK")
spark.stop()
