"""
30_kafka_consumer_basic.py        submit with: --with kafka --py-files .../stream_common.py

Kafka producer -> Spark consumer, the simplest useful version.

  orders.events (Kafka)
      │  readStream.format("kafka")      key, value (bytes), partition, offset, timestamp
      │  from_json(value, EVENT_SCHEMA)  one column per field
      ▼
  s3a://data/stream/bronze/kafka_events/   Parquet, partitioned by event_date
  + a per-batch summary in the driver log (events by type, Kafka partition range)

This "bronze" copy of the raw stream is the usual first step: keep every
event as received, with its Kafka partition/offset for traceability, then
build cleaner tables from it.

Run modes (see stream_common.py): always-on by default, --run-seconds N,
or --available-now.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import stream_common as sc

args = sc.parse_args("Kafka -> Parquet bronze")
spark = SparkSession.builder.appName("30-kafka-consumer-basic").getOrCreate()
spark.streams.addListener(sc.ProgressPrinter())

out = f"{sc.conf(spark, 'spark.lab.stream.root', 's3a://data/stream').rstrip('/')}/bronze/kafka_events"
ckpt = sc.checkpoint(spark, "30_kafka_consumer_basic", args.reset)

events = (sc.parse_events(sc.kafka_stream(spark, "orders.events", args))
          .withColumn("event_date", F.to_date("event_time"))
          .withColumn("ingested_at", F.current_timestamp()))


def summarize(batch, batch_id):
    """Write the batch, then print what arrived (runs once per micro-batch)."""
    batch = batch.persist()        # several actions below: read the source once
    n = batch.count()
    if n == 0:
        batch.unpersist()
        return
    batch.write.mode("append").partitionBy("event_date").parquet(out)
    kinds = {r["event_type"]: r["count"] for r in batch.groupBy("event_type").count().collect()}
    offs = batch.groupBy("partition").agg(F.min("offset").alias("from"), F.max("offset").alias("to")) \
                .orderBy("partition").collect()
    ranges = ", ".join(f"p{r['partition']}:{r['from']}-{r['to']}" for r in offs)
    print(f"  batch {batch_id}: {n} events {dict(sorted(kinds.items()))}  offsets {ranges}")
    batch.unpersist()


q = (events.writeStream.queryName("kafka_to_bronze")
     .foreachBatch(summarize)
     .option("checkpointLocation", ckpt)
     .trigger(**sc.trigger(args))
     .start())
sc.run(spark, [q], args)

try:
    print(f"bronze now holds {spark.read.parquet(out).count()} events at {out}")
except Exception:  # nothing written yet (empty topic)
    print(f"bronze is still empty: {out}")
print("KAFKA CONSUMER BASIC OK")
spark.stop()
