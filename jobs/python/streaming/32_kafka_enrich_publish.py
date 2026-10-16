"""
32_kafka_enrich_publish.py        submit with: --with kafka --py-files .../stream_common.py

Spark as a Kafka PRODUCER, inside a stream: Kafka in, Kafka out.

  orders.events ──► join customers (from Postgres) ──► orders.enriched
                                                  └─► orders.alerts   (only the interesting ones)

  * stream-static join: every event gets the customer's name and tier.
    The customers table is read from Postgres once and cached; in a long
    running job you'd refresh it on a schedule.
  * two output queries from one input, each with its own checkpoint:
    the Kafka sink is a plain .writeStream.format("kafka") with
    key + value columns. Key = order_id keeps each order's events in one
    partition, in order, for whoever consumes downstream.
  * alerts: CANCELLED orders, and CREATED orders worth >= 700 from
    gold-tier customers (so someone can call them).

Read the results with: python client/producers/topic_tail.py orders.enriched
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import stream_common as sc

args = sc.parse_args("Kafka -> enrich -> Kafka")
spark = SparkSession.builder.appName("32-kafka-enrich-publish").getOrCreate()
spark.streams.addListener(sc.ProgressPrinter())

customers = (spark.read.format("jdbc").options(**sc.pg_options(spark))
             .option("dbtable", "customers").load()
             .select("customer_id", "full_name", "tier", F.col("region").alias("customer_region"))
             .cache())
print(f"loaded {customers.count()} customers from Postgres for the join")

events = sc.parse_events(sc.kafka_stream(spark, "orders.events", args)).drop("kafka_key")
enriched = (events.join(F.broadcast(customers), "customer_id", "left")
            .withColumn("enriched_at", F.current_timestamp())
            .select("event_id", "order_id", "event_type", "event_time", "amount", "city", "carrier",
                    "customer_id", "full_name", "tier", "customer_region", "run_id",
                    F.col("partition").alias("source_partition"),
                    F.col("offset").alias("source_offset"), "enriched_at"))

alerts = (enriched
          .where((F.col("event_type") == "CANCELLED") |
                 ((F.col("event_type") == "CREATED") & (F.col("amount") >= 700) & (F.col("tier") == "gold")))
          .withColumn("alert", F.when(F.col("event_type") == "CANCELLED", "order_cancelled")
                                .otherwise("high_value_gold_order")))

bs = sc.bootstrap(spark)
q1 = (sc.to_kafka(enriched, "order_id").writeStream.queryName("publish_enriched")
      .format("kafka").option("kafka.bootstrap.servers", bs).option("topic", "orders.enriched")
      .option("checkpointLocation", sc.checkpoint(spark, "32_enriched", args.reset))
      .trigger(**sc.trigger(args)).start())
q2 = (sc.to_kafka(alerts, "order_id").writeStream.queryName("publish_alerts")
      .format("kafka").option("kafka.bootstrap.servers", bs).option("topic", "orders.alerts")
      .option("checkpointLocation", sc.checkpoint(spark, "32_alerts", args.reset))
      .trigger(**sc.trigger(args)).start())
sc.run(spark, [q1, q2], args)
print("KAFKA ENRICH PUBLISH OK")
spark.stop()
