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

# --follow: wait forever for new messages (kafka-python's "forever" is inf,
# not None); otherwise stop after 3 s with nothing new
c = KafkaConsumer(bootstrap_servers=a.bootstrap, enable_auto_commit=False,
                  consumer_timeout_ms=float("inf") if a.follow else 3000)
parts = [TopicPartition(a.topic, p) for p in sorted(c.partitions_for_topic(a.topic) or [])]
if not parts:
    raise SystemExit(f"topic {a.topic} not found")
c.assign(parts)
ends = c.end_offsets(parts)
begins = c.beginning_offsets(parts)
total = sum(ends[p] - begins[p] for p in parts)
print(f"{a.topic}: {len(parts)} partitions, {total} messages retained")
if total == 0:
    hint = {
        "orders.events": "run a producer: make produce",
        "orders.enriched": "start job 32: make stream-start (and a producer: make produce)",
        "orders.alerts": "start job 32: make stream-start (and a producer: make produce)",
        "orders.metrics": "start job 33: make stream-start (and a producer: make produce)",
        "orders.backfill": "run job 34: make test-streaming ARGS='--only kafka-batch-publish'",
    }.get(a.topic, "nothing has been published to it yet")
    print(f"  (empty so far - {hint})")
if a.follow:
    print("  following; Ctrl-C to stop")
for p in parts:
    c.seek(p, max(begins[p], ends[p] - a.last))
for msg in c:
    key = msg.key.decode() if msg.key else None
    print(f"[p{msg.partition} @{msg.offset}] {key} -> {msg.value.decode()[:300]}")