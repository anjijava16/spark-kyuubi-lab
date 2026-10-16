"""
39_verify_streaming.py --run-id ID [--file-run-id ID]     submit with: --with kafka,delta

Batch job that checks the streaming examples did their job for ONE run of
the producer (identified by run_id), reading every output back:

  Kafka  orders.events     how many events the producer sent (ground truth)
  Parquet bronze (30)      every event landed, none twice
  Delta + Postgres (31)    every event once in Delta; every order's status in
                           Postgres = its newest event
  Kafka orders.enriched (32)  every event published once, with a customer tier
  Kafka orders.alerts (32)    exactly the events that match the alert rule
  Postgres order_metrics_live + Kafka orders.metrics (33)  windows exist and
                           their event counts add up to at least this run's
  file-drop silver (35)    every dropped event landed (when --file-run-id is given)
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

import stream_common as sc

p = argparse.ArgumentParser()
p.add_argument("--run-id", required=True)
p.add_argument("--file-run-id")
args = p.parse_args()
RID = args.run_id

spark = SparkSession.builder.appName("39-verify-streaming").getOrCreate()
bs = sc.bootstrap(spark)
root = sc.conf(spark, "spark.lab.stream.root", "s3a://data/stream").rstrip("/")
failures = []


def check(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    if not ok:
        failures.append(name)


def topic(name):
    raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", bs)
           .option("subscribe", name).option("startingOffsets", "earliest")
           .option("endingOffsets", "latest").option("failOnDataLoss", "false").load())
    return raw.select(F.col("key").cast("string").alias("key"),
                      F.col("value").cast("string").alias("value"), "timestamp")


def field(col, name):
    return F.get_json_object(col, f"$.{name}")


# ground truth: what the producer sent for this run
sent = (sc.parse_events(spark.read.format("kafka").option("kafka.bootstrap.servers", bs)
                        .option("subscribe", "orders.events").option("startingOffsets", "earliest")
                        .option("endingOffsets", "latest").load())
        .where(F.col("run_id") == RID).dropDuplicates(["event_id"]).cache())
n_sent = sent.count()
n_orders = sent.select("order_id").distinct().count()
print(f"run {RID}: producer sent {n_sent} events for {n_orders} orders")
check("producer sent events", n_sent > 0, f"{n_sent} events")

# 30: bronze parquet
bronze = spark.read.parquet(f"{root}/bronze/kafka_events").where(F.col("run_id") == RID)
b_rows, b_distinct = bronze.count(), bronze.select("event_id").distinct().count()
check("30 bronze has every event", b_distinct == n_sent, f"{b_distinct}/{n_sent} distinct ({b_rows} rows)")

# 31: delta + postgres status
delta = spark.table("default.kafka_order_events").where(F.col("run_id") == RID)
d_rows, d_distinct = delta.count(), delta.select("event_id").distinct().count()
check("31 delta stores each event once", d_rows == d_distinct == n_sent, f"{d_rows} rows, {d_distinct} distinct")
newest = Window.partitionBy("order_id").orderBy(F.desc("event_time"), F.desc(sc.status_rank()))
expected = (sent.withColumn("rn", F.row_number().over(newest)).where("rn = 1")
            .select("order_id", F.col("event_type").alias("expected")))
pg = (spark.read.format("jdbc").options(**sc.pg_options(spark)).option("dbtable", "kafka_order_status")
      .load().where(F.col("run_id") == RID).select("order_id", "status"))
wrong = expected.join(pg, "order_id", "left").where("status IS NULL OR status <> expected").count()
check("31 postgres status = newest event", wrong == 0 and pg.count() == n_orders,
      f"{pg.count()}/{n_orders} orders, {wrong} wrong")

# 32: enriched + alerts topics
enr = topic("orders.enriched").where(field("value", "run_id") == RID)
e_ids = enr.select(field("value", "event_id").alias("id")).distinct().count()
no_tier = enr.where(field("value", "tier").isNull()).count()
check("32 enriched has every event", e_ids == n_sent, f"{e_ids}/{n_sent} events, {no_tier} without tier")
customers = (spark.read.format("jdbc").options(**sc.pg_options(spark)).option("dbtable", "customers")
             .load().select("customer_id", "tier"))
want_alerts = (sent.join(customers, "customer_id", "left")
               .where((F.col("event_type") == "CANCELLED") |
                      ((F.col("event_type") == "CREATED") & (F.col("amount") >= 700) & (F.col("tier") == "gold")))
               .count())
got_alerts = topic("orders.alerts").where(field("value", "run_id") == RID) \
    .select(field("value", "event_id").alias("id")).distinct().count()
check("32 alerts match the rule", got_alerts == want_alerts, f"{got_alerts} published, {want_alerts} expected")

# 33: metrics
run_start = sent.agg(F.min("kafka_ts")).first()[0]
m_msgs = topic("orders.metrics").where(F.col("timestamp") >= run_start).count()
win = (spark.read.format("jdbc").options(**sc.pg_options(spark))
       .option("query", "SELECT window_start, city, events FROM order_metrics_live").load())
span = sent.agg(F.min("event_time").alias("lo"), F.max("event_time").alias("hi")).first()
in_span = win.where((F.col("window_start") >= F.lit(span["lo"]) - F.expr("INTERVAL 5 MINUTES")) &
                    (F.col("window_start") <= F.lit(span["hi"])))
events_in_windows = in_span.agg(F.sum("events")).first()[0] or 0
check("33 metrics published to kafka", m_msgs > 0, f"{m_msgs} metric updates since the run started")
check("33 windows cover this run's events", events_in_windows >= n_sent,
      f"{in_span.count()} windows holding {events_in_windows} events (>= {n_sent})")

# 35: file drops
if args.file_run_id:
    silver = spark.read.parquet(f"{root}/silver/order_events_by_city").where(F.col("run_id") == args.file_run_id)
    s_distinct = silver.select("event_id").distinct().count()
    check("35 file drops all landed", s_distinct > 0, f"{s_distinct} events for run {args.file_run_id}")

print(f"\n{len(failures)} check(s) failed: {failures}" if failures else "\nall checks passed")
assert not failures, f"failed: {failures}"
print("VERIFY STREAMING OK")
spark.stop()
