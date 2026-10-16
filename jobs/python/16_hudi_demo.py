"""
16_hudi_demo.py [input_csv]             submit with: --with hudi

Apache Hudi end to end, Copy-on-Write table at
s3a://warehouse/lake/hudi/orders_cow, keyed by order_id, partitioned by
region.

  1. insert the CSV                      commit 1
  2. upsert: 10 updated + 5 new orders   commit 2  (newest updated_at wins)
  3. delete 3 orders by key              commit 3
  4. snapshot query (latest state)
  5. incremental query: only rows changed after commit 1
  6. time travel: the table as of commit 1
  7. Hudi's own columns: _hoodie_commit_time, _hoodie_record_key, ...

Rerunnable: the first write uses mode("overwrite"), which recreates the table.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

src = sys.argv[1] if len(sys.argv) > 1 else "s3a://data/input/orders.csv"
spark = SparkSession.builder.appName("16-hudi-demo").getOrCreate()
root = spark.conf.get("spark.lab.lake.root", "s3a://warehouse/lake").rstrip("/")
path = f"{root}/hudi/orders_cow"

SCHEMA = StructType([
    StructField("order_id", IntegerType()), StructField("order_date", DateType()),
    StructField("region", StringType()), StructField("product", StringType()),
    StructField("quantity", IntegerType()), StructField("unit_price", DoubleType()),
    StructField("customer_id", IntegerType()),
])
orders = (spark.read.option("header", True).schema(SCHEMA).csv(src)
          .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
          .withColumn("updated_at", F.lit("2026-10-15 09:00:00").cast("timestamp")))
n = orders.count()

HUDI = {
    "hoodie.table.name": "orders_cow",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.partitionpath.field": "region",
    # when two versions of a key meet, the larger updated_at wins
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "false",
    # the embedded timeline server is an optimisation for big tables; off
    # keeps executors from needing one more port on the driver
    "hoodie.embed.timeline.server": "false",
}


def write(df, operation, mode="append"):
    (df.write.format("hudi").options(**HUDI)
       .option("hoodie.datasource.write.operation", operation)
       .mode(mode).save(path))


def snapshot():
    return spark.read.format("hudi").load(path)


def commits():
    return [r[0] for r in snapshot().select("_hoodie_commit_time").distinct()
            .orderBy("_hoodie_commit_time").collect()]


# 1. insert ----------------------------------------------------------------
write(orders, "insert", mode="overwrite")
c1 = commits()[-1]
print(f"[insert]  rows={snapshot().count()} commit={c1}")
assert snapshot().count() == n

# 2. upsert ----------------------------------------------------------------
upd = (orders.orderBy("order_id").limit(10).withColumn("quantity", F.col("quantity") + 1)
       .unionByName(orders.orderBy("order_id").limit(5).withColumn("order_id", F.col("order_id") + 1000))
       .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
       .withColumn("updated_at", F.lit("2026-10-15 12:00:00").cast("timestamp")))
write(upd, "upsert")
print(f"[upsert]  rows={snapshot().count()} (expected {n + 5})")
assert snapshot().count() == n + 5

# 3. delete by key ---------------------------------------------------------
victims = orders.orderBy(F.desc("order_id")).limit(3)
write(victims, "delete")
print(f"[delete]  rows={snapshot().count()} (expected {n + 2})")
assert snapshot().count() == n + 2

# 4/7. snapshot query with Hudi's metadata columns -------------------------
snapshot().select("_hoodie_commit_time", "_hoodie_record_key", "_hoodie_partition_path",
                  "order_id", "quantity", "updated_at") \
          .orderBy("order_id").show(5, truncate=False)

# 5. incremental: what changed after the first commit ----------------------
inc = (spark.read.format("hudi")
       .option("hoodie.datasource.query.type", "incremental")
       .option("hoodie.datasource.read.begin.instanttime", c1)
       .load(path))
print(f"[incr]    rows changed after commit {c1}: {inc.count()} (10 updated + 5 new)")
assert inc.count() == 15

# 6. time travel ------------------------------------------------------------
then = spark.read.format("hudi").option("as.of.instant", c1).load(path).count()
print(f"[travel]  rows as of {c1} = {then}")
assert then == n

print("rows per partition now:")
snapshot().groupBy("_hoodie_partition_path").count().orderBy(1).show(truncate=False)
print("HUDI DEMO OK")
spark.stop()
