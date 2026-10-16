"""
03_hive_etl.py [csv_path]

CSV on RustFS -> managed Hive table demo.orders (Parquet).

What this proves:
  * Spark talks to the Hive Metastore (CREATE DATABASE, saveAsTable)
  * The metastore records the table location on s3a://warehouse/...
  * Executors write Parquet files there
  * Any later job (or the Kyuubi SQL shell) can find the table by name
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

src = sys.argv[1] if len(sys.argv) > 1 else "s3a://data/input/orders.csv"

spark = (SparkSession.builder
         .appName("03-hive-etl")
         .enableHiveSupport()
         .getOrCreate())

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", DateType()),
    StructField("region", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("customer_id", IntegerType()),
])

orders = (spark.read.option("header", True).schema(schema).csv(src)
          .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2)))

spark.sql("CREATE DATABASE IF NOT EXISTS demo COMMENT 'kyuubi lab'")
orders.write.mode("overwrite").format("parquet").saveAsTable("demo.orders")

print("Databases:", [r.namespace for r in spark.sql("SHOW DATABASES").collect()])
print("Tables in demo:", [r.tableName for r in spark.sql("SHOW TABLES IN demo").collect()])

loc = (spark.sql("DESCRIBE FORMATTED demo.orders")
       .where("col_name = 'Location'").first()["data_type"])
print(f"demo.orders location: {loc}")

spark.sql("""
    SELECT region, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
    FROM demo.orders GROUP BY region ORDER BY revenue DESC
""").show(truncate=False)

n_src = orders.count()
n_tbl = spark.table("demo.orders").count()
assert n_src == n_tbl, f"table has {n_tbl} rows, source had {n_src}"
assert loc.startswith("s3a://warehouse/"), f"unexpected location {loc}"
print("HIVE ETL OK")
spark.stop()
