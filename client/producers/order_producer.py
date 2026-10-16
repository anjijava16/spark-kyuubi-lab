#!/usr/bin/env python3
"""
order_producer.py - an "outside application" that emits order events.

It plays the part of an order service: each order goes through
CREATED -> PAID -> PACKED -> SHIPPED -> OUT_FOR_DELIVERY -> DELIVERED
(or gets CANCELLED), and every step is one JSON event.

Two sinks:
  --sink kafka   publish to a Kafka topic, keyed by order_id (default)
  --sink s3      drop JSON-lines files into RustFS for file-based streams

Two modes:
  --orders N     emit N complete order lifecycles as fast as possible, with
                 event times spread over the last hour, then exit (tests)
  --forever      keep emitting live events at --rate events/second, with
                 event_time = now, until Ctrl-C (always-on demos)

Every event carries --run-id, so a check can count exactly what one run
produced even though topics and tables accumulate across runs.

Examples (inside the lab, via the kyuubi-client container):
  docker compose --profile tools run --rm kyuubi-client \\
      python client/producers/order_producer.py --orders 40 --run-id demo1
  docker compose --profile tools run --rm kyuubi-client \\
      python client/producers/order_producer.py --forever --rate 5
From your Mac: --bootstrap localhost:9094 (and pip install kafka-python boto3).
"""
import argparse
import datetime as dt
import io
import json
import os
import random
import signal
import sys
import time
import uuid

CITIES = ["Philadelphia", "Pittsburgh", "Newark", "Baltimore", "Wilmington"]
CARRIERS = ["UPS", "FedEx", "USPS"]
STEPS = ["CREATED", "PAID", "PACKED", "SHIPPED", "OUT_FOR_DELIVERY", "DELIVERED"]


def lifecycle(rnd, run_id, n, start):
    """All events of one order, with realistic gaps between steps."""
    oid = f"{run_id}-ORD-{n:05d}"
    cust, city = rnd.randint(1, 30), rnd.choice(CITIES)
    amount = round(rnd.uniform(15, 900), 2)
    carrier = rnd.choice(CARRIERS)
    cancel_at = rnd.choice([1, 2]) if rnd.random() < 0.08 else None
    t, events = start, []
    for i, step in enumerate(STEPS):
        if cancel_at is not None and i == cancel_at:
            step = "CANCELLED"
        events.append({
            "event_id": f"{oid}-{i}", "order_id": oid, "customer_id": cust,
            "event_type": step, "event_time": t, "amount": amount, "city": city,
            "carrier": carrier if i >= 3 and step != "CANCELLED" else None,
            "run_id": run_id,
        })
        if step == "CANCELLED":
            break
        t += dt.timedelta(minutes=rnd.randint(1, 12))
    return events


def to_json(e):
    e = dict(e)
    e["event_time"] = e["event_time"].strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return json.dumps(e)


class KafkaSink:
    def __init__(self, bootstrap, topic):
        from kafka import KafkaProducer
        self.topic = topic
        self.p = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=20,
                               key_serializer=str.encode, value_serializer=str.encode)

    def send(self, events):
        for e in events:
            # key = order_id: all events of one order land in one partition, in order
            self.p.send(self.topic, key=e["order_id"], value=to_json(e))

    def close(self):
        self.p.flush()
        self.p.close()


class S3Sink:
    """Writes each batch of events as one JSON-lines file (one 'file drop')."""
    def __init__(self, endpoint, bucket, prefix):
        import boto3
        from botocore.config import Config
        self.bucket, self.prefix = bucket, prefix.strip("/")
        self.s3 = boto3.client(
            "s3", endpoint_url=endpoint, region_name="us-east-1",
            aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"),
            aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin123"),
            config=Config(s3={"addressing_style": "path"},
                          request_checksum_calculation="when_required",
                          response_checksum_validation="when_required"))

    def send(self, events):
        if not events:
            return
        body = "\n".join(to_json(e) for e in events) + "\n"
        key = f"{self.prefix}/drop-{dt.datetime.now(dt.timezone.utc).replace(tzinfo=None):%Y%m%dT%H%M%S%f}-{uuid.uuid4().hex[:6]}.json"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=io.BytesIO(body.encode()))
        print(f"  dropped s3://{self.bucket}/{key} ({len(events)} events)")

    def close(self):
        pass


def main():
    ap = argparse.ArgumentParser(description="emit order lifecycle events")
    ap.add_argument("--sink", choices=["kafka", "s3"], default="kafka")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="orders.events")
    ap.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT", "http://rustfs:9000"))
    ap.add_argument("--s3-path", default="data/stream/landing",
                    help="bucket/prefix for --sink s3 (default data/stream/landing)")
    ap.add_argument("--orders", type=int, default=40, help="finite mode: number of orders")
    ap.add_argument("--files", type=int, default=4, help="--sink s3 finite mode: number of files")
    ap.add_argument("--forever", action="store_true", help="live mode until Ctrl-C")
    ap.add_argument("--rate", type=float, default=5.0, help="live mode: events per second")
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).strftime("r%m%d%H%M%S"))
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    if a.sink == "kafka":
        sink = KafkaSink(a.bootstrap, a.topic)
        where = f"kafka {a.bootstrap} topic {a.topic}"
    else:
        bucket, _, prefix = a.s3_path.partition("/")
        sink = S3Sink(a.s3_endpoint, bucket, prefix)
        where = f"s3://{a.s3_path}/"
    rnd = random.Random(a.seed)
    print(f"run_id={a.run_id} -> {where}")

    if not a.forever:
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).replace(microsecond=0)
        events = []
        for n in range(1, a.orders + 1):
            start = now - dt.timedelta(minutes=60) + dt.timedelta(seconds=rnd.randint(0, 3000))
            events += lifecycle(rnd, a.run_id, n, start)
        events.sort(key=lambda e: e["event_time"])
        if a.sink == "s3":                      # split into a few file drops
            size = -(-len(events) // max(a.files, 1))
            for i in range(0, len(events), size):
                sink.send(events[i:i + size])
        else:
            sink.send(events)
        sink.close()
        kinds = {}
        for e in events:
            kinds[e["event_type"]] = kinds.get(e["event_type"], 0) + 1
        print(f"sent {len(events)} events for {a.orders} orders  {dict(sorted(kinds.items()))}")
        print(f"PRODUCER OK run_id={a.run_id} events={len(events)}")
        return

    # ---- live mode: interleave many open orders, emit their next step ----
    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))
    open_orders, n, sent = [], 0, 0
    pending_s3 = []
    print(f"live: ~{a.rate} events/s, Ctrl-C to stop")
    while not stop["now"]:
        if len(open_orders) < 20 or rnd.random() < 0.3:
            n += 1
            evs = lifecycle(rnd, a.run_id, n, dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).replace(microsecond=0))
            open_orders.append(evs)
        order = rnd.choice(open_orders)
        e = order.pop(0)
        e["event_time"] = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).replace(microsecond=0)
        if not order:
            open_orders.remove(order)
        if a.sink == "kafka":
            sink.send([e])
        else:
            pending_s3.append(e)
            if len(pending_s3) >= max(int(a.rate * 5), 1):      # one file every ~5 s
                sink.send(pending_s3)
                pending_s3 = []
        sent += 1
        if sent % 50 == 0:
            print(f"  {sent} events sent, {len(open_orders)} orders in flight")
        time.sleep(1.0 / a.rate)
    sink.send(pending_s3) if a.sink == "s3" else None
    sink.close()
    print(f"stopped after {sent} events")


if __name__ == "__main__":
    sys.exit(main())
