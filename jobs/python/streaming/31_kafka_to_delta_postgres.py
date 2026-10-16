"""
31_kafka_to_delta_postgres.py     submit with: --with kafka,delta --py-files .../stream_common.py

Kafka -> Spark -> two sinks, each micro-batch, via foreachBatch:

  orders.events ──► Delta  s3a://warehouse/lake/delta/kafka_order_events
                    (append-only event table; MERGE on event_id, so a replayed
                     Kafka message is stored once - Kafka delivery is
                     at-least-once, the table is exactly-once)
                └─► Postgres kafka_order_status
                    (current status per order; upsert that never replaces a
                     newer status with an older one)

This is the classic "stream into the lake, serve from a database" shape.
Same run-mode flags as every job in this folder.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

import stream_common as sc

args = sc.parse_args("Kafka -> Delta + Postgres")
spark = SparkSession.builder.appName("31-kafka-to-delta-postgres").getOrCreate()
spark.streams.addListener(sc.ProgressPrinter())

lake = sc.conf(spark, "spark.lab.lake.root", "s3a://warehouse/lake").rstrip("/")
path = f"{lake}/delta/kafka_order_events"
T = f"delta.`{path}`"
ckpt = sc.checkpoint(spark, "31_kafka_to_delta_postgres", args.reset)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {T} (
        event_id STRING, order_id STRING, customer_id INT, event_type STRING,
        event_time TIMESTAMP, amount DOUBLE, city STRING, carrier STRING, run_id STRING,
        kafka_partition INT, kafka_offset BIGINT, ingested_at TIMESTAMP)
    USING DELTA
""")
spark.sql(f"CREATE TABLE IF NOT EXISTS default.kafka_order_events USING DELTA LOCATION '{path}'")

events = (sc.parse_events(sc.kafka_stream(spark, "orders.events", args))
          .withColumnRenamed("partition", "kafka_partition")
          .withColumnRenamed("offset", "kafka_offset")
          .drop("kafka_key", "kafka_ts")
          .withColumn("ingested_at", F.current_timestamp()))


def upsert(batch, batch_id):
    s = batch.sparkSession
    batch = batch.dropDuplicates(["event_id"]).persist()
    n = batch.count()
    if n == 0:
        return
    # 1. Delta: insert unseen events only
    batch.createOrReplaceTempView("incoming")
    s.sql(f"MERGE INTO {T} t USING incoming s ON t.event_id = s.event_id "
          f"WHEN NOT MATCHED THEN INSERT *")

    # 2. Postgres: current status of every order touched, from all its events
    touched = batch.select("order_id").distinct()
    history = s.table("default.kafka_order_events").join(touched, "order_id")
    newest = Window.partitionBy("order_id").orderBy(
        F.desc("event_time"), F.desc(sc.status_rank()))
    status = (history.withColumn("rn", F.row_number().over(newest)).where("rn = 1")
              .join(history.groupBy("order_id").agg(F.count("*").alias("events_seen")), "order_id")
              .select("order_id", "run_id", "customer_id", F.col("event_type").alias("status"),
                      F.col("event_time").alias("status_time"), "amount", "city", "events_seen"))
    changed = sc.pg_upsert(s, status, "kafka_order_status", ["order_id"],
                           ["run_id", "customer_id", "status", "status_time", "amount", "city", "events_seen"],
                           guard="t.status_time <= EXCLUDED.status_time")
    print(f"  batch {batch_id}: {n} events -> delta; {status.count()} orders -> postgres ({changed} changed)")
    batch.unpersist()


q = (events.writeStream.queryName("kafka_to_delta_postgres")
     .foreachBatch(upsert)
     .option("checkpointLocation", ckpt)
     .trigger(**sc.trigger(args))
     .start())
sc.run(spark, [q], args)

spark.sql("SELECT event_type, count(*) n FROM default.kafka_order_events GROUP BY 1 ORDER BY 1").show()
print("KAFKA TO DELTA POSTGRES OK")
spark.stop()
