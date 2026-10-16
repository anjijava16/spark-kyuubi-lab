#!/bin/bash
# Creates the lab's Kafka topics. Safe to run again (--if-not-exists).
#
#   orders.events     raw order events from the Python producer   (Kafka -> Spark)
#   orders.enriched   events joined with customer data by Spark    (Spark -> Kafka)
#   orders.metrics    per-minute, per-city aggregates from Spark   (Spark -> Kafka)
#   orders.alerts     high-value / cancelled orders flagged by Spark
#   orders.backfill   historical orders published by a Spark batch job
set -euo pipefail
BS=kafka:9092
T=/opt/kafka/bin/kafka-topics.sh

for topic in orders.events orders.enriched orders.metrics orders.alerts orders.backfill; do
  "$T" --bootstrap-server "$BS" --create --if-not-exists --topic "$topic" \
       --partitions 3 --replication-factor 1
done
# metrics are keyed by window+city; keep only the newest value per key
/opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BS" --alter --entity-type topics \
    --entity-name orders.metrics --add-config cleanup.policy=compact,min.cleanable.dirty.ratio=0.1

echo ">> topics:"
"$T" --bootstrap-server "$BS" --describe | grep -E "^Topic:" | grep -v __consumer
