"""
stream_common.py - shared bits for the jobs in jobs/python/streaming/.

Shipped to each job with --py-files s3a://jobs/python/streaming/stream_common.py
(a single .py works with --py-files, not only zips).

Every streaming job accepts the same run-mode flags:

  (no flag)            always-on: micro-batch every --trigger-seconds (10),
                       runs until you kill the Kyuubi batch
  --run-seconds N      same trigger, but stops by itself after N seconds
  --available-now      process everything available right now, then stop
                       (what the tests and scheduled runs use)

and --reset to delete the job's checkpoint (start over from --starting-offsets).
"""
import argparse
import json

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

# What client/producers/order_producer.py sends (JSON value, key = order_id)
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("amount", DoubleType()),
    StructField("city", StringType()),
    StructField("carrier", StringType()),
    StructField("run_id", StringType()),
])

RANKS = {"CREATED": 1, "PAID": 2, "PACKED": 3, "SHIPPED": 4,
         "OUT_FOR_DELIVERY": 5, "DELIVERED": 6, "CANCELLED": 7}


def status_rank(col="event_type"):
    """Lifecycle position of an event type; breaks ties between equal timestamps.
    (A function, not a module constant: Column objects need an active SparkSession.)"""
    m = F.create_map(*[x for k, v in RANKS.items() for x in (F.lit(k), F.lit(v))])
    return m[F.col(col)]


def parse_args(description, extra=None):
    ap = argparse.ArgumentParser(description=description)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--available-now", action="store_true",
                      help="drain what's available, then stop")
    mode.add_argument("--run-seconds", type=int, default=0,
                      help="always-on trigger, stop after N seconds")
    ap.add_argument("--trigger-seconds", type=int, default=10)
    ap.add_argument("--starting-offsets", default="earliest",
                    help="Kafka: earliest | latest (only used without a checkpoint)")
    ap.add_argument("--max-offsets", type=int, default=500,
                    help="Kafka: max records per micro-batch")
    ap.add_argument("--reset", action="store_true", help="delete this job's checkpoint first")
    if extra:
        extra(ap)
    return ap.parse_args()


def conf(spark, key, default=None):
    return spark.conf.get(key, default)


def bootstrap(spark):
    return conf(spark, "spark.lab.kafka.bootstrap", "kafka:9092")


def checkpoint(spark, name, reset=False):
    root = conf(spark, "spark.lab.stream.root", "s3a://data/stream").rstrip("/")
    path = f"{root}/_checkpoints/{name}"
    if reset:
        delete_path(spark, path)
        print(f"reset: deleted checkpoint {path}")
    return path


def delete_path(spark, path):
    jp = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jp.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(jp):
        fs.delete(jp, True)


def kafka_stream(spark, topic, args):
    """Kafka -> streaming DataFrame with key/value/partition/offset/timestamp columns."""
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap(spark))
            .option("subscribe", topic)
            .option("startingOffsets", args.starting_offsets)
            .option("maxOffsetsPerTrigger", args.max_offsets)
            # topics are recreated when the Kafka container is recreated;
            # don't die because an old checkpoint points past the new end
            .option("failOnDataLoss", "false")
            .load())


def parse_events(kafka_df):
    """Kafka rows -> one column per event field, plus Kafka metadata."""
    return (kafka_df
            .select(F.col("key").cast("string").alias("kafka_key"),
                    F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                    "partition", "offset", F.col("timestamp").alias("kafka_ts"))
            .select("e.*", "kafka_key", "partition", "offset", "kafka_ts")
            .where(F.col("event_id").isNotNull()))          # drop unparseable messages


def to_kafka(df, key_col):
    """Any DataFrame -> the (key, value) shape the Kafka sink wants, value = JSON."""
    return df.select(F.col(key_col).cast("string").alias("key"),
                     F.to_json(F.struct(*df.columns)).alias("value"))


def trigger(args):
    if args.available_now:
        return {"availableNow": True}
    return {"processingTime": f"{args.trigger_seconds} seconds"}


class ProgressPrinter(StreamingQueryListener):
    """One log line per micro-batch, so the Kyuubi batch log shows progress."""
    def onQueryStarted(self, event):
        print(f"[stream] started {event.name} id={event.id}")

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        if p.get("numInputRows", 0) or p.get("batchId", 0) == 0:
            print(f"[stream] {p.get('name')} batch={p.get('batchId')} rows={p.get('numInputRows')} "
                  f"in/s={round(p.get('inputRowsPerSecond') or 0, 1)} "
                  f"processed/s={round(p.get('processedRowsPerSecond') or 0, 1)}")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[stream] terminated id={event.id} exception={event.exception}")


def run(spark, queries, args):
    """Wait according to the run mode, then report what each query did."""
    if args.available_now:
        for q in queries:
            q.awaitTermination()
    elif args.run_seconds:
        spark.streams.awaitAnyTermination(args.run_seconds)
        for q in queries:
            q.stop()
    else:
        print("always-on: running until the Kyuubi batch is killed "
              "(kyuubi_submit.py kill <batch-id>)")
        spark.streams.awaitAnyTermination()
    for q in queries:
        if q.exception():
            raise RuntimeError(f"query {q.name} failed: {q.exception()}")
        last = q.lastProgress or {}
        print(f"[stream] {q.name}: last batch {last.get('batchId')} "
              f"rows={last.get('numInputRows')}")


def pg_options(spark):
    return {"url": conf(spark, "spark.lab.pg.url"), "user": conf(spark, "spark.lab.pg.user"),
            "password": conf(spark, "spark.lab.pg.password"), "driver": "org.postgresql.Driver"}


def pg_exec(spark, sql):
    o = pg_options(spark)
    conn = spark._jvm.java.sql.DriverManager.getConnection(o["url"], o["user"], o["password"])
    try:
        return conn.createStatement().executeUpdate(sql)
    finally:
        conn.close()


def pg_upsert(spark, df, table, key_cols, update_cols, guard=None):
    """Write df to <table>_stage, then INSERT ... ON CONFLICT DO UPDATE into table."""
    stage = f"{table}_stage"
    df.write.format("jdbc").options(**pg_options(spark)).option("dbtable", stage) \
      .mode("overwrite").save()
    cols = ", ".join(df.columns)
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    where = f" WHERE {guard}" if guard else ""
    n = pg_exec(spark, f"""
        INSERT INTO {table} AS t ({cols}, updated_at)
        SELECT {cols}, now() FROM {stage}
        ON CONFLICT ({", ".join(key_cols)}) DO UPDATE SET {sets}, updated_at = now(){where}""")
    pg_exec(spark, f"DROP TABLE IF EXISTS {stage}")
    return n
