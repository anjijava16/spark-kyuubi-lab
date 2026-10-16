"""
21_track_orders_stream.py [--reset] [--source PATH]     submit with: --with delta,iceberg

The order-tracking pipeline. A Structured Streaming job reads order events
as they land in s3a://data/stream/order_events/ and keeps three stores up
to date, each with a different job:

  Iceberg  ice.tracking.order_events   the full event log, append-only.
           MERGE ... WHEN NOT MATCHED INSERT on event_id, so a re-sent
           event is stored once.
  Delta    default.order_status        one row per order: its current
           status. Rebuilt from the event log for every order touched in
           the micro-batch, so a late event can't move a status backwards.
           Change data feed on, so you can see every status transition.
  Postgres order_tracking (shop DB)    what an app or API would read.
           Upserted from the Delta rows; never overwritten by older data.

Trigger is availableNow: process every file that has arrived, in
micro-batches (one file each), then stop. That makes a streaming job
behave like a batch for Kyuubi, so it can run on a schedule. Swap in
.trigger(processingTime="30 seconds") to run it continuously.

The checkpoint remembers which files are done, so running this again
only picks up new files. --reset wipes the checkpoint and all three
stores to start over.
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

p = argparse.ArgumentParser()
p.add_argument("--source", default="s3a://data/stream/order_events")
p.add_argument("--checkpoint", default="s3a://data/stream/_checkpoints/order_tracking")
p.add_argument("--reset", action="store_true")
args = p.parse_args()

spark = SparkSession.builder.appName("21-track-orders-stream").getOrCreate()
sql = spark.sql
c = spark.conf
URL, USER, PASSWORD = c.get("spark.lab.pg.url"), c.get("spark.lab.pg.user"), c.get("spark.lab.pg.password")
PG = {"url": URL, "user": USER, "password": PASSWORD, "driver": "org.postgresql.Driver"}
LAKE = c.get("spark.lab.lake.root", "s3a://warehouse/lake").rstrip("/")
STATUS_PATH = f"{LAKE}/delta/order_status"
STATUS = f"delta.`{STATUS_PATH}`"
EVENTS = "ice.tracking.order_events"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()), StructField("event_type", StringType()),
    StructField("event_time", TimestampType()), StructField("amount", DoubleType()),
    StructField("city", StringType()), StructField("carrier", StringType()),
    StructField("wave", IntegerType()),          # from the wave=N folder name
])
# tie-breaker when two events share a timestamp: the later lifecycle step wins
STATUS_RANK = F.create_map(*[x for kv in {
    "CREATED": 1, "PAID": 2, "PACKED": 3, "SHIPPED": 4,
    "OUT_FOR_DELIVERY": 5, "DELIVERED": 6, "CANCELLED": 7}.items()
    for x in (F.lit(kv[0]), F.lit(kv[1]))])


def pg_exec(stmt):
    conn = spark._jvm.java.sql.DriverManager.getConnection(URL, USER, PASSWORD)
    try:
        return conn.createStatement().executeUpdate(stmt)
    finally:
        conn.close()


def hdfs_delete(path):
    jp = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jp.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(jp):
        fs.delete(jp, True)


# ---- setup -----------------------------------------------------------------
if args.reset:
    print("reset: dropping checkpoint, Iceberg log, Delta status, Postgres rows")
    hdfs_delete(args.checkpoint)
    sql(f"DROP TABLE IF EXISTS {EVENTS} PURGE")
    sql("DROP TABLE IF EXISTS default.order_status")
    hdfs_delete(STATUS_PATH)
    pg_exec("TRUNCATE order_tracking")

sql("CREATE NAMESPACE IF NOT EXISTS ice.tracking")
sql(f"""
    CREATE TABLE IF NOT EXISTS {EVENTS} (
        event_id STRING, order_id STRING, customer_id INT, event_type STRING,
        event_time TIMESTAMP, amount DOUBLE, city STRING, carrier STRING,
        ingested_at TIMESTAMP)
    USING iceberg PARTITIONED BY (days(event_time))
    TBLPROPERTIES ('format-version' = '2')
""")
sql(f"""
    CREATE TABLE IF NOT EXISTS {STATUS} (
        order_id STRING, customer_id INT, status STRING, status_time TIMESTAMP,
        created_at TIMESTAMP, amount DOUBLE, city STRING, carrier STRING,
        events_seen BIGINT, updated_at TIMESTAMP)
    USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
sql(f"CREATE TABLE IF NOT EXISTS default.order_status USING DELTA LOCATION '{STATUS_PATH}'")

PG_UPSERT = """
    INSERT INTO order_tracking AS t
           (order_id, customer_id, status, status_time, amount, city, carrier, events_seen, updated_at)
    SELECT order_id, customer_id, status, status_time, amount, city, carrier, events_seen, now()
      FROM order_tracking_stage
    ON CONFLICT (order_id) DO UPDATE
       SET customer_id = EXCLUDED.customer_id, status = EXCLUDED.status,
           status_time = EXCLUDED.status_time, amount = EXCLUDED.amount,
           city = EXCLUDED.city, carrier = EXCLUDED.carrier,
           events_seen = EXCLUDED.events_seen, updated_at = now()
     WHERE t.status_time <= EXCLUDED.status_time
"""

stats = []


def process(batch, batch_id):
    """Runs once per micro-batch with that batch's events as a normal DataFrame."""
    s = batch.sparkSession
    events = (batch.drop("wave").dropDuplicates(["event_id"])
              .withColumn("ingested_at", F.current_timestamp()))
    n_in = events.count()
    if n_in == 0:
        return
    before = s.table(EVENTS).count()

    # 1. event log: insert only events we haven't stored yet
    events.createOrReplaceTempView("incoming_events")
    s.sql(f"""MERGE INTO {EVENTS} t USING incoming_events s ON t.event_id = s.event_id
              WHEN NOT MATCHED THEN INSERT *""")
    added = s.table(EVENTS).count() - before

    # 2. current status for every order this batch touched, from ALL its events
    touched = events.select("order_id").distinct()
    history = s.table(EVENTS).join(touched, "order_id")
    latest_first = Window.partitionBy("order_id").orderBy(
        F.desc("event_time"), F.desc(STATUS_RANK[F.col("event_type")]))
    latest = (history.withColumn("rn", F.row_number().over(latest_first)).where("rn = 1")
              .select("order_id", "customer_id", F.col("event_type").alias("status"),
                      F.col("event_time").alias("status_time"), "amount", "city"))
    per_order = history.groupBy("order_id").agg(
        F.count("*").alias("events_seen"),
        F.min("event_time").alias("created_at"),
        F.max("carrier").alias("carrier"))
    current = (latest.join(per_order, "order_id")
               .withColumn("updated_at", F.current_timestamp())
               .select("order_id", "customer_id", "status", "status_time", "created_at",
                       "amount", "city", "carrier", "events_seen", "updated_at"))
    current.createOrReplaceTempView("current_status")
    s.sql(f"""MERGE INTO {STATUS} t USING current_status s ON t.order_id = s.order_id
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *""")

    # 3. serving table in Postgres
    (current.drop("created_at", "updated_at")
            .write.format("jdbc").options(**PG).option("dbtable", "order_tracking_stage")
            .mode("overwrite").save())
    upserted = pg_exec(PG_UPSERT)
    pg_exec("DROP TABLE IF EXISTS order_tracking_stage")

    mix = {r["status"]: r["count"] for r in current.groupBy("status").count().collect()}
    stats.append((batch_id, n_in, added, current.count(), upserted))
    print(f"  batch {batch_id}: {n_in} events in, {added} new in log, "
          f"{current.count()} orders updated, {upserted} PG rows changed  {mix}")


events_stream = (spark.readStream.schema(EVENT_SCHEMA)
                 .option("maxFilesPerTrigger", 1)     # one file per micro-batch
                 .json(args.source))

query = (events_stream.writeStream
         .foreachBatch(process)
         .option("checkpointLocation", args.checkpoint)
         .trigger(availableNow=True)
         .queryName("order_tracking")
         .start())
query.awaitTermination()

print(f"\nprocessed {len(stats)} micro-batch(es) this run")
log_n = spark.table(EVENTS).count()
status_n = spark.table("default.order_status").count()
pg_n = (spark.read.format("jdbc").options(**PG)
        .option("query", "SELECT count(*) n FROM order_tracking").load().first()["n"])
print(f"event log: {log_n} events | order_status: {status_n} orders | postgres: {pg_n} orders")
spark.table("default.order_status").groupBy("status").count().orderBy("status").show()
assert status_n == pg_n, "Delta and Postgres disagree on the number of orders"
print("ORDER TRACKING STREAM OK")
spark.stop()
