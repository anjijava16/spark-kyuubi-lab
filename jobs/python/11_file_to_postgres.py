"""
11_file_to_postgres.py [parquet_path] [table]

Files -> Postgres. Reads the Parquet written by 10_format_conversions.py
(falls back to the raw CSV if that hasn't run), adds a computed column,
and loads it into Postgres table shop.public.orders over JDBC.

Shows the JDBC write options that matter in practice:
  * mode("overwrite") + truncate=true  -> keep the table, replace the rows
  * batchsize                          -> rows per INSERT batch
  * numPartitions                      -> parallel connections (keep small!)
then reads the row count back from Postgres to prove the load.

Connection settings come from spark.lab.pg.* (see spark-defaults.conf).
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

src = sys.argv[1] if len(sys.argv) > 1 else "s3a://data/output/formats/parquet"
table = sys.argv[2] if len(sys.argv) > 2 else "orders"

spark = SparkSession.builder.appName("11-file-to-postgres").getOrCreate()
conf = spark.conf
PG = {
    "url": conf.get("spark.lab.pg.url"),
    "user": conf.get("spark.lab.pg.user"),
    "password": conf.get("spark.lab.pg.password"),
    "driver": "org.postgresql.Driver",
}

try:
    orders = spark.read.parquet(src)
    print(f"reading Parquet {src}")
except Exception as e:  # 10_format_conversions not run yet
    print(f"{src} not readable ({e.__class__.__name__}); falling back to the CSV")
    schema = StructType([
        StructField("order_id", IntegerType()), StructField("order_date", DateType()),
        StructField("region", StringType()), StructField("product", StringType()),
        StructField("quantity", IntegerType()), StructField("unit_price", DoubleType()),
        StructField("customer_id", IntegerType()),
    ])
    orders = spark.read.option("header", True).schema(schema).csv("s3a://data/input/orders.csv")

orders = (orders
          .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
          .withColumn("loaded_at", F.current_timestamp()))
n = orders.count()

(orders.repartition(2)                 # 2 partitions -> 2 parallel connections
       .write.format("jdbc").options(**PG)
       .option("dbtable", table)
       .option("truncate", "true")     # overwrite = TRUNCATE + INSERT, table kept
       .option("batchsize", 500)
       .mode("overwrite")
       .save())
print(f"wrote {n} rows to postgres table {table}")

# Read back with a query that runs inside Postgres (only the result comes back)
check = (spark.read.format("jdbc").options(**PG)
         .option("query", f"SELECT count(*) AS n, round(sum(amount)::numeric, 2) AS revenue FROM {table}")
         .load().first())
print(f"postgres says: rows={check['n']} revenue={check['revenue']}")
assert check["n"] == n, f"postgres has {check['n']} rows, expected {n}"
print("FILE TO POSTGRES OK")
spark.stop()
