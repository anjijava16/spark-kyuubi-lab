"""
20_generate_order_events.py --wave N [--orders 60] [--reset] [--out PATH]

Simulates order lifecycle events arriving over a day, in three waves.
Each wave drops new JSON files under <out>/wave=N/, which is exactly what
a streaming file source sees as "new data arrived".

Every order follows a lifecycle, with realistic gaps between steps:

    CREATED -> PAID -> PACKED -> SHIPPED -> OUT_FOR_DELIVERY -> DELIVERED
         \\-> CANCELLED (before or after payment, a few percent of orders)

Wave 1: everything before 12:00   Wave 2: before 20:00   Wave 3: the rest

Two deliberately awkward things happen in wave 3, because real event
streams do them too:
  * a DUPLICATE: an event from wave 1 is sent again with the same event_id
  * a LATE event: one order's PACKED event (which happened in wave-2 time)
    only arrives in wave 3, after that order already SHIPPED
The tracking job must not double count the duplicate, and must not move
that order's status backwards to PACKED.

The whole timeline is generated from a fixed seed, so every run produces
the same events and the report job can check exact results.
"""
import argparse
import datetime as dt
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

p = argparse.ArgumentParser()
p.add_argument("--wave", type=int, required=True, choices=[1, 2, 3])
p.add_argument("--orders", type=int, default=60)
p.add_argument("--out", default="s3a://data/stream/order_events")
p.add_argument("--reset", action="store_true", help="delete all earlier waves first")
args = p.parse_args()

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("amount", DoubleType()),
    StructField("city", StringType()),
    StructField("carrier", StringType()),
])

BASE = dt.datetime(2026, 10, 15, 8, 0, 0)
WAVE_END = {1: BASE + dt.timedelta(hours=4), 2: BASE + dt.timedelta(hours=12), 3: None}
CITIES = ["Philadelphia", "Pittsburgh", "Newark", "Baltimore", "Wilmington"]
CARRIERS = ["UPS", "FedEx", "USPS"]


def timeline(n_orders):
    """All events for all orders, deterministic."""
    rnd = random.Random(2026)
    events = []
    for i in range(1, n_orders + 1):
        oid = f"ORD-{i:04d}"
        cust = rnd.randint(1, 30)
        amount = round(rnd.uniform(15, 900), 2)
        city = rnd.choice(CITIES)
        carrier = rnd.choice(CARRIERS)
        t = BASE + dt.timedelta(minutes=4 * i + rnd.randint(0, 3))
        steps = [("CREATED", t, None)]
        roll = rnd.random()
        if roll < 0.06:                                    # cancelled before paying
            steps.append(("CANCELLED", t + dt.timedelta(minutes=rnd.randint(10, 60)), None))
        else:
            t += dt.timedelta(minutes=rnd.randint(2, 30)); steps.append(("PAID", t, None))
            if roll < 0.10:                                # cancelled after paying
                steps.append(("CANCELLED", t + dt.timedelta(minutes=rnd.randint(20, 120)), None))
            else:
                t += dt.timedelta(minutes=rnd.randint(30, 180)); steps.append(("PACKED", t, None))
                t += dt.timedelta(minutes=rnd.randint(60, 300)); steps.append(("SHIPPED", t, carrier))
                t += dt.timedelta(hours=rnd.uniform(6, 30)); steps.append(("OUT_FOR_DELIVERY", t, carrier))
                t += dt.timedelta(hours=rnd.uniform(1, 6)); steps.append(("DELIVERED", t, carrier))
        for k, (etype, when, carr) in enumerate(steps):
            events.append({
                "event_id": f"{oid}-{k}", "order_id": oid, "customer_id": cust,
                "event_type": etype, "event_time": when.replace(microsecond=0),
                "amount": amount, "city": city, "carrier": carr,
            })
    return events


def wave_of(e):
    if e["event_time"] < WAVE_END[1]:
        return 1
    if e["event_time"] < WAVE_END[2]:
        return 2
    return 3


events = timeline(args.orders)
waves = {1: [], 2: [], 3: []}
for e in events:
    waves[wave_of(e)].append(e)

# the awkward cases, all landing in wave 3
late = next(e for e in waves[2] if e["event_type"] == "PACKED"
            and any(x["order_id"] == e["order_id"] and x["event_type"] == "SHIPPED" for x in waves[2]))
waves[2].remove(late)
waves[3].append(late)
waves[3].append(dict(waves[1][0]))           # exact duplicate (same event_id)

spark = SparkSession.builder.appName(f"20-order-events-wave-{args.wave}").getOrCreate()
jvm = spark._jvm
root = jvm.org.apache.hadoop.fs.Path(args.out)
fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
if args.reset and fs.exists(root):
    fs.delete(root, True)
    print(f"reset: deleted {args.out}")

rows = waves[args.wave]
target = f"{args.out}/wave={args.wave}"
# 3 files per wave so the stream gets several micro-batches
spark.createDataFrame(rows, EVENT_SCHEMA).repartition(3).write.mode("overwrite").json(target)

kinds = {}
for e in rows:
    kinds[e["event_type"]] = kinds.get(e["event_type"], 0) + 1
print(f"wave {args.wave}: wrote {len(rows)} events for "
      f"{len({e['order_id'] for e in rows})} orders -> {target}")
print("  by type:", dict(sorted(kinds.items())))
if args.wave == 3:
    print(f"  includes duplicate event_id {waves[1][0]['event_id']} and late "
          f"{late['event_type']} for {late['order_id']} (event time {late['event_time']})")
print("ORDER EVENTS WAVE OK")
spark.stop()
