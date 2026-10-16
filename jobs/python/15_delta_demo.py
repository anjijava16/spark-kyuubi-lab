"""
15_delta_demo.py [input_csv]            submit with: --with delta

Delta Lake end to end. The table lives at s3a://warehouse/lake/delta/orders
and is also registered in the Hive Metastore as default.delta_orders, so the
Kyuubi SQL shell can query it by name.

  1. write the CSV as a Delta table (change data feed enabled)  version 0
  2. MERGE INTO: update 10 orders, insert 5                      version 1
  3. DELETE + UPDATE                                             versions 2, 3
  4. schema evolution: append with a new column (mergeSchema)    version 4
  5. time travel: VERSION AS OF 0 must equal the original
  6. change data feed: what changed since version 1
  7. DESCRIBE HISTORY, OPTIMIZE ... ZORDER BY

Rerunnable: the path is overwritten with overwriteSchema on every run.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

src = sys.argv[1] if len(sys.argv) > 1 else "s3a://data/input/orders.csv"
spark = SparkSession.builder.appName("15-delta-demo").getOrCreate()
sql = spark.sql
root = spark.conf.get("spark.lab.lake.root", "s3a://warehouse/lake").rstrip("/")
path = f"{root}/delta/orders"
D = f"delta.`{path}`"

SCHEMA = StructType([
    StructField("order_id", IntegerType()), StructField("order_date", DateType()),
    StructField("region", StringType()), StructField("product", StringType()),
    StructField("quantity", IntegerType()), StructField("unit_price", DoubleType()),
    StructField("customer_id", IntegerType()),
])
orders = (spark.read.option("header", True).schema(SCHEMA).csv(src)
          .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2)))
n = orders.count()


def count(ref=D):
    return sql(f"SELECT count(*) c FROM {ref}").first()["c"]


def version():
    return sql(f"DESCRIBE HISTORY {D} LIMIT 1").first()["version"]


# 1. initial write --------------------------------------------------------
(orders.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .partitionBy("region")
       .save(path))
v_initial = version()
# record every row-level change from here on (read back in step 6)
sql(f"ALTER TABLE {D} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
sql(f"CREATE TABLE IF NOT EXISTS default.delta_orders USING DELTA LOCATION '{path}'")
print(f"[write]   rows={count()} version={v_initial} (also as default.delta_orders)")
assert count() == n

# 2. MERGE ----------------------------------------------------------------
changes = (orders.orderBy("order_id").limit(10).withColumn("quantity", F.col("quantity") + 1)
           .unionByName(orders.orderBy("order_id").limit(5).withColumn("order_id", F.col("order_id") + 1000))
           .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2)))
changes.createOrReplaceTempView("changes")
sql(f"""
    MERGE INTO {D} t USING changes s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
v_merge = version()
print(f"[merge]   rows={count()} version={v_merge}")
assert count() == n + 5

# 3. DELETE + UPDATE --------------------------------------------------------
sql(f"DELETE FROM {D} WHERE product = 'mouse' AND quantity <= 2")
sql(f"UPDATE {D} SET unit_price = unit_price * 0.9, amount = round(amount * 0.9, 2) WHERE product = 'laptop'")
print(f"[dml]     rows={count()} version={version()}")

# 4. schema evolution -----------------------------------------------------
extra = (orders.limit(3).withColumn("order_id", F.col("order_id") + 5000)
         .withColumn("channel", F.lit("mobile")))
extra.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
cols = spark.read.format("delta").load(path).columns
print(f"[schema]  columns now: {cols}")
assert "channel" in cols

# 5. time travel ------------------------------------------------------------
then = sql(f"SELECT count(*) c FROM {D} VERSION AS OF {v_initial}").first()["c"]
print(f"[travel]  rows at version {v_initial} = {then}; rows now = {count()}")
assert then == n

# 6. change data feed since the merge -------------------------------------
cdf = (spark.read.format("delta").option("readChangeFeed", "true")
       .option("startingVersion", v_merge).load(path))
print("changes since the MERGE, by type:")
cdf.groupBy("_commit_version", "_change_type").count().orderBy("_commit_version", "_change_type").show()

# 7. history + compaction ---------------------------------------------------
sql(f"OPTIMIZE {D} ZORDER BY (customer_id)").select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
sql(f"DESCRIBE HISTORY {D}").select("version", "operation", "operationMetrics.numOutputRows").show(truncate=False)

by_name = count("default.delta_orders")
assert by_name == count(), "metastore-registered name should see the same table"
print("DELTA DEMO OK")
spark.stop()
