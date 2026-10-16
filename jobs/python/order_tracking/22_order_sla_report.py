"""
22_order_sla_report.py [--source PATH] [--out PATH]     submit with: --with delta,iceberg

The report side of the order-tracking project, plus the checks that the
pipeline did its job.

Report (written to Parquet and to Postgres table order_sla):
  * delivery time per order = DELIVERED time - CREATED time, from the
    Iceberg event log
  * per carrier and city: delivered orders, average / p90 hours, share
    delivered within 24h, cancellations

Checks, against the raw event files as ground truth:
  * the Iceberg log holds each distinct event exactly once (duplicates dropped)
  * every order's status in Delta and in Postgres equals the status of its
    latest event (so the late PACKED event did not win)
  * Delta change data feed: show one order's status history

Also shows Iceberg time travel: how many events the log held after its
first commit vs now.
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

p = argparse.ArgumentParser()
p.add_argument("--source", default="s3a://data/stream/order_events")
p.add_argument("--out", default="s3a://data/output/order_tracking")
args = p.parse_args()

spark = SparkSession.builder.appName("22-order-sla-report").getOrCreate()
sql = spark.sql
c = spark.conf
PG = {"url": c.get("spark.lab.pg.url"), "user": c.get("spark.lab.pg.user"),
      "password": c.get("spark.lab.pg.password"), "driver": "org.postgresql.Driver"}
LAKE = c.get("spark.lab.lake.root", "s3a://warehouse/lake").rstrip("/")
STATUS_PATH = f"{LAKE}/delta/order_status"
EVENTS = "ice.tracking.order_events"
RANK = {"CREATED": 1, "PAID": 2, "PACKED": 3, "SHIPPED": 4,
        "OUT_FOR_DELIVERY": 5, "DELIVERED": 6, "CANCELLED": 7}
rank_col = F.create_map(*[x for k, v in RANK.items() for x in (F.lit(k), F.lit(v))])

log = spark.table(EVENTS)
status = spark.table("default.order_status")
pg_status = spark.read.format("jdbc").options(**PG).option("dbtable", "order_tracking").load()

# ---- SLA report ------------------------------------------------------------
created = log.where("event_type = 'CREATED'").groupBy("order_id").agg(F.min("event_time").alias("created_at"))
delivered = log.where("event_type = 'DELIVERED'").groupBy("order_id").agg(F.min("event_time").alias("delivered_at"))
per_order = (status.select("order_id", "status", "carrier", "city")
             .join(created, "order_id", "left").join(delivered, "order_id", "left")
             .withColumn("hours_to_deliver",
                         F.round((F.unix_timestamp("delivered_at") - F.unix_timestamp("created_at")) / 3600, 2)))

sla = (per_order.groupBy(F.coalesce("carrier", F.lit("(not shipped)")).alias("carrier"), "city")
       .agg(F.count("*").alias("orders"),
            F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled"),
            F.round(F.avg("hours_to_deliver"), 1).alias("avg_hours"),
            F.round(F.percentile_approx("hours_to_deliver", 0.9), 1).alias("p90_hours"),
            F.round(F.avg(F.when(F.col("hours_to_deliver").isNotNull(),
                                 (F.col("hours_to_deliver") <= 24).cast("int"))) * 100, 1)
             .alias("pct_within_24h"))
       .orderBy("carrier", "city"))
print("delivery SLA by carrier and city:")
sla.show(50, truncate=False)

sla.write.mode("overwrite").parquet(f"{args.out}/sla_by_carrier_city")
per_order.write.mode("overwrite").parquet(f"{args.out}/order_delivery_times")
sla.write.format("jdbc").options(**PG).option("dbtable", "order_sla").option("truncate", "true") \
   .mode("overwrite").save()
print(f"report written to {args.out}/ and postgres table order_sla")

# ---- checks against the raw files ------------------------------------------
raw_schema = StructType([
    StructField("event_id", StringType()), StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()), StructField("event_type", StringType()),
    StructField("event_time", TimestampType()), StructField("amount", DoubleType()),
    StructField("city", StringType()), StructField("carrier", StringType()),
    StructField("wave", IntegerType()),
])
raw = spark.read.schema(raw_schema).json(args.source)
raw_rows, raw_distinct = raw.count(), raw.select("event_id").distinct().count()
log_rows, log_distinct = log.count(), log.select("event_id").distinct().count()
print(f"raw files: {raw_rows} events ({raw_distinct} distinct) | iceberg log: {log_rows} ({log_distinct} distinct)")
assert log_rows == log_distinct == raw_distinct, "event log should hold each distinct event exactly once"

newest = Window.partitionBy("order_id").orderBy(F.desc("event_time"), F.desc(rank_col[F.col("event_type")]))
expected = (raw.dropDuplicates(["event_id"])
            .withColumn("rn", F.row_number().over(newest)).where("rn = 1")
            .select("order_id", F.col("event_type").alias("expected_status")))

for name, df in (("delta", status), ("postgres", pg_status)):
    diff = (expected.join(df.select("order_id", "status"), "order_id", "full_outer")
            .where("status IS NULL OR expected_status IS NULL OR status <> expected_status"))
    bad = diff.count()
    print(f"{name}: {df.count()} orders, {bad} with a wrong or missing status")
    if bad:
        diff.show(20, truncate=False)
    assert bad == 0, f"{name} status does not match the event log"

# the late event: an order whose PACKED arrived after it had shipped
late = (raw.where("wave = 3 AND event_type = 'PACKED'").select("order_id").first())
if late:
    oid = late["order_id"]
    print(f"late PACKED event for {oid}; its status stayed:",
          status.where(F.col("order_id") == oid).first()["status"])
    print(f"status history of {oid} from the Delta change data feed:")
    (spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 0)
     .load(STATUS_PATH).where(F.col("order_id") == oid)
     .where("_change_type IN ('insert', 'update_postimage')")
     .select("_commit_version", "status", "status_time", "events_seen")
     .orderBy("_commit_version").show(truncate=False))

# Iceberg time travel: the log after its first commit vs now
first_snap = sql(f"SELECT snapshot_id FROM {EVENTS}.snapshots ORDER BY committed_at LIMIT 1").first()[0]
then = sql(f"SELECT count(*) c FROM {EVENTS} VERSION AS OF {first_snap}").first()["c"]
print(f"iceberg log: {then} events after the first commit, {log_rows} now "
      f"({sql(f'SELECT count(*) c FROM {EVENTS}.snapshots').first()['c']} snapshots)")
print("ORDER SLA REPORT OK")
spark.stop()
