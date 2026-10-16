"""
35_file_drop_stream.py            submit with: --py-files .../stream_common.py   (no Kafka needed)

Always-on stream over FILE EVENTS: watch a folder in object storage and
process every new file that lands in it.

  s3a://data/stream/landing/*.json  (new files = new events)
      │  readStream.json(...)  with maxFilesPerTrigger, cleanSource=archive
      ▼
  s3a://data/stream/silver/order_events_by_city/   Parquet, partitioned by event_date + city
  + per-batch counts in the driver log

Drop files with the producer:
  python client/producers/order_producer.py --sink s3 --orders 30 --files 3
  python client/producers/order_producer.py --sink s3 --forever --rate 5   (a file every ~5 s)
or just upload JSON-lines files to data/stream/landing/ in the RustFS console.

cleanSource=archive moves processed files to landing_archive/, so the
landing folder only ever holds unprocessed work. Same run-mode flags as
the Kafka jobs.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import stream_common as sc

args = sc.parse_args("file drops -> Parquet",
                     lambda ap: ap.add_argument("--files-per-trigger", type=int, default=2))
spark = SparkSession.builder.appName("35-file-drop-stream").getOrCreate()
spark.streams.addListener(sc.ProgressPrinter())

root = sc.conf(spark, "spark.lab.stream.root", "s3a://data/stream").rstrip("/")
landing, archive = f"{root}/landing", f"{root}/landing_archive"
out = f"{root}/silver/order_events_by_city"

drops = (spark.readStream.schema(sc.EVENT_SCHEMA)
         .option("maxFilesPerTrigger", args.files_per_trigger)
         .option("cleanSource", "archive")
         .option("sourceArchiveDir", archive)
         .json(landing)
         # hidden _metadata column of file sources: path, name, size, mtime
         .withColumn("source_file", F.col("_metadata.file_name"))
         .withColumn("event_date", F.to_date("event_time")))


def write(batch, batch_id):
    batch = batch.persist()        # several actions below: read the files once
    n = batch.count()
    if n == 0:
        batch.unpersist()
        return
    batch.write.mode("append").partitionBy("event_date", "city").parquet(out)
    files = batch.select("source_file").distinct().count()
    by_city = {r["city"]: r["count"] for r in batch.groupBy("city").count().collect()}
    print(f"  batch {batch_id}: {n} events from {files} file(s) {dict(sorted(by_city.items()))}")
    batch.unpersist()


q = (drops.writeStream.queryName("file_drops")
     .foreachBatch(write)
     .option("checkpointLocation", sc.checkpoint(spark, "35_file_drop_stream", args.reset))
     .trigger(**sc.trigger(args))
     .start())
sc.run(spark, [q], args)
print("FILE DROP STREAM OK")
spark.stop()
