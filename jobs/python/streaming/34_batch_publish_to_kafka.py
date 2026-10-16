"""
34_batch_publish_to_kafka.py [--run-id ID] [--source hive|csv]    submit with: --with kafka

Spark as a Kafka producer in a plain BATCH job (no stream), then Spark as a
batch consumer reading the same topic back.

  demo.orders (Hive) or orders.csv ──► df.write.format("kafka") ──► orders.backfill
  orders.backfill ──► spark.read.format("kafka") (startingOffsets/endingOffsets) ──► check

Typical uses: backfilling a topic from a table, replaying history into a
new consumer, or publishing the output of a nightly job.
"""
import argparse
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).strftime("b%m%d%H%M%S"))
p.add_argument("--source", choices=["hive", "csv"], default="hive")
p.add_argument("--topic", default="orders.backfill")
args = p.parse_args()

spark = SparkSession.builder.appName("34-batch-publish-to-kafka").getOrCreate()
bs = spark.conf.get("spark.lab.kafka.bootstrap", "kafka:9092")

if args.source == "hive" and spark.catalog.tableExists("demo.orders"):
    orders = spark.table("demo.orders")
    print("source: Hive table demo.orders")
else:
    orders = (spark.read.option("header", True).option("inferSchema", True)
              .csv("s3a://data/input/orders.csv"))
    print("source: s3a://data/input/orders.csv")

msgs = (orders.withColumn("run_id", F.lit(args.run_id))
        .withColumn("published_at", F.current_timestamp())
        .select(F.col("order_id").cast("string").alias("key"),
                F.to_json(F.struct("*")).alias("value"),
                # Kafka headers ride along with each message
                F.array(F.struct(F.lit("source").alias("key"),
                                 F.lit(args.source).cast("binary").alias("value"))).alias("headers")))
n = msgs.count()
(msgs.write.format("kafka")
     .option("kafka.bootstrap.servers", bs)
     .option("topic", args.topic)
     .option("includeHeaders", "true")
     .save())
print(f"published {n} messages to {args.topic} with run_id={args.run_id}")

# ---- batch consumer: read the whole topic, keep this run's messages ------
back = (spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bs)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("includeHeaders", "true")
        .load())
mine = back.where(F.get_json_object(F.col("value").cast("string"), "$.run_id") == args.run_id)
got = mine.count()
print(f"topic holds {back.count()} messages; {got} from this run")
mine.groupBy("partition").agg(F.count("*").alias("messages"), F.min("offset").alias("first_offset"),
                              F.max("offset").alias("last_offset")).orderBy("partition").show()
mine.select(F.col("key").cast("string"), F.col("value").cast("string").substr(1, 90).alias("value"),
            F.expr("headers[0].key").alias("header")).show(3, truncate=False)
assert got == n, f"published {n} but read back {got}"
print("BATCH PUBLISH TO KAFKA OK")
spark.stop()
