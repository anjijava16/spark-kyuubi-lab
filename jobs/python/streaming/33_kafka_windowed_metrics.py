"""
33_kafka_windowed_metrics.py      submit with: --with kafka --py-files .../stream_common.py

Event-time windows with a watermark: live order metrics per city.

  orders.events ──► window(event_time, 5 min) x city ──► orders.metrics (Kafka, compacted topic)
                                                    └─► Postgres order_metrics_live (upsert)

Concepts this shows:
  * event time vs processing time: windows use when the event HAPPENED
    (event_time), not when Spark saw it
  * watermark: how long to wait for late events before a window is final.
    The demo uses a generous --watermark of 2 hours because the producer's
    finite mode spreads event times over the last hour; in a live system
    you'd use minutes
  * output mode "update": each micro-batch emits only the windows that
    changed, which is what you want for a dashboard. Kafka gets
    key = "<window start>|<city>" on a compacted topic, so consumers can
    always rebuild the latest value per key
  * one aggregation, two sinks (Kafka + Postgres via foreachBatch)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import stream_common as sc

args = sc.parse_args("windowed metrics",
                     lambda ap: (ap.add_argument("--window", default="5 minutes"),
                                 ap.add_argument("--watermark", default="2 hours")))
spark = SparkSession.builder.appName("33-kafka-windowed-metrics").getOrCreate()
spark.streams.addListener(sc.ProgressPrinter())


def flag(t):
    return F.sum(F.when(F.col("event_type") == t, 1).otherwise(0))


metrics = (sc.parse_events(sc.kafka_stream(spark, "orders.events", args))
           .withWatermark("event_time", args.watermark)
           .groupBy(F.window("event_time", args.window), "city")
           .agg(F.count("*").alias("events"),
                flag("CREATED").alias("orders_created"),
                flag("PAID").alias("orders_paid"),
                flag("DELIVERED").alias("orders_delivered"),
                flag("CANCELLED").alias("orders_cancelled"),
                F.round(F.sum(F.when(F.col("event_type") == "PAID", F.col("amount"))), 2).alias("revenue_paid"))
           .select(F.col("window.start").alias("window_start"),
                   F.col("window.end").alias("window_end"), "city", "events", "orders_created",
                   "orders_paid", "orders_delivered", "orders_cancelled", "revenue_paid")
           .withColumn("metric_key", F.concat_ws("|", F.date_format("window_start", "yyyy-MM-dd'T'HH:mm"), "city")))

q1 = (sc.to_kafka(metrics, "metric_key").writeStream.queryName("metrics_to_kafka")
      .outputMode("update").format("kafka")
      .option("kafka.bootstrap.servers", sc.bootstrap(spark)).option("topic", "orders.metrics")
      .option("checkpointLocation", sc.checkpoint(spark, "33_metrics_kafka", args.reset))
      .trigger(**sc.trigger(args)).start())


def to_postgres(batch, batch_id):
    rows = batch.drop("metric_key").na.fill(0, ["revenue_paid"])
    n = rows.count()
    if n:
        sc.pg_upsert(batch.sparkSession, rows, "order_metrics_live", ["window_start", "city"],
                     ["window_end", "events", "orders_created", "orders_paid",
                      "orders_delivered", "orders_cancelled", "revenue_paid"])
        print(f"  batch {batch_id}: {n} windows updated in postgres")


q2 = (metrics.writeStream.queryName("metrics_to_postgres").outputMode("update")
      .foreachBatch(to_postgres)
      .option("checkpointLocation", sc.checkpoint(spark, "33_metrics_pg", args.reset))
      .trigger(**sc.trigger(args)).start())
sc.run(spark, [q1, q2], args)

(spark.read.format("jdbc").options(**sc.pg_options(spark))
 .option("query", "SELECT window_start, city, events, orders_created, revenue_paid "
                  "FROM order_metrics_live ORDER BY window_start DESC, city LIMIT 10")
 .load().show(truncate=False))
print("KAFKA WINDOWED METRICS OK")
spark.stop()
