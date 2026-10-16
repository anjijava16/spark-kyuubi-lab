#!/usr/bin/env python3
"""
topic_tail.py <topic> [--last N] [--follow]

Print messages from a Kafka topic: what Spark published, for example.

  docker compose --profile tools run --rm kyuubi-client \\
      python client/producers/topic_tail.py orders.enriched --last 5
  ... python client/producers/topic_tail.py orders.metrics --follow
"""
import argparse
import os

from kafka import KafkaConsumer, TopicPartition

ap = argparse.ArgumentParser()
ap.add_argument("topic")
ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
ap.add_argument("--last", type=int, default=10, help="messages per partition to show")
ap.add_argument("--follow", action="store_true", help="keep printing new messages")
a = ap.parse_args()

c = KafkaConsumer(bootstrap_servers=a.bootstrap, enable_auto_commit=False,
                  consumer_timeout_ms=None if a.follow else 3000)
parts = [TopicPartition(a.topic, p) for p in sorted(c.partitions_for_topic(a.topic) or [])]
if not parts:
    raise SystemExit(f"topic {a.topic} not found")
c.assign(parts)
ends = c.end_offsets(parts)
begins = c.beginning_offsets(parts)
total = sum(ends[p] - begins[p] for p in parts)
print(f"{a.topic}: {len(parts)} partitions, {total} messages retained")
for p in parts:
    c.seek(p, max(begins[p], ends[p] - a.last))
for msg in c:
    key = msg.key.decode() if msg.key else None
    print(f"[p{msg.partition} @{msg.offset}] {key} -> {msg.value.decode()[:300]}")
