"""
job_catalog.py - every example job in the lab, in one place.

Used by:
  run_tests.py           runs them through Kyuubi and checks the results
  gen_payloads.py        writes All_execution_payload.md / .json (CLI, REST
                         payload, curl and kyuubi-ctl for each job)

Fields per entry:
  name, group, note      identity and one-line description
  kind                   "batch" (a Kyuubi batch; default) or "local" (a
                         command run in the client container, e.g. a producer)
  resource               .py/.jar path; s3a://... when remote=True
  class_name             main class for jars (Python gets PythonRunner)
  args, conf, py_files   what the batch request carries
  with_formats           connectors: delta, iceberg, hudi, kafka (see kyuubi_submit.py)
  after                  tests whose output this one reads (skipped if they failed)
  marker                 line the job prints only after its own checks pass
  expect                 final batch state (default FINISHED)
  needs_jar / needs_kafka  skipped with a hint if the jar isn't built / Kafka isn't installed
  cmd                    for kind="local": the command
  always_on              for streaming jobs: args for the always-on variant
"{run_id}" in args/cmd is replaced by a fresh id per test run.
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
JOBS = os.path.normpath(os.path.join(HERE, "..", "jobs"))
SPARK_VERSION = os.getenv("SPARK_VERSION", "4.1.3")
JAR = "s3a://jobs/jars/spark-kyuubi-jobs.jar"
LOCAL_JAR = os.path.join(JOBS, "jars", "spark-kyuubi-jobs.jar")

# Each case: name, group, resource, and the final batch state we expect.
# "remote" means Kyuubi's spark-submit reads the file from RustFS (or the
# Kyuubi host). remote=False means this client uploads the file itself.
# "marker" is a line the job prints only when its own checks passed; the
# test fails if the batch FINISHED but the marker is missing.
# "with" adds a table-format profile (see LAKEHOUSE_PROFILES in kyuubi_submit.py).
# "after" lists tests whose output this one reads; it is skipped if they failed.
PY = "s3a://jobs/python"
OT = f"{PY}/order_tracking"
ST = f"{PY}/streaming"
COMMON = f"{ST}/stream_common.py"
CASES = [
    # ---- core: Kyuubi -> Spark -> storage / metastore ----------------------
    dict(name="jar-sparkpi", group="core",
         note="example JAR that ships with Spark, path on the Kyuubi host",
         resource=f"/opt/spark/examples/jars/spark-examples_2.13-{SPARK_VERSION}.jar",
         class_name="org.apache.spark.examples.SparkPi",
         args=["20"], remote=True, marker="Pi is roughly"),
    dict(name="py-smoke", group="core",
         note="PySpark from RustFS, checks executors respond",
         resource=f"{PY}/01_smoke_test.py", remote=True, marker="SMOKE TEST OK"),
    dict(name="py-upload", group="core",
         note="same script, uploaded by the client instead of read from RustFS",
         resource=os.path.join(JOBS, "python", "01_smoke_test.py"), remote=False,
         marker="SMOKE TEST OK"),
    dict(name="py-wordcount", group="core",
         note="reads and writes RustFS through s3a",
         resource=f"{PY}/02_wordcount_s3.py",
         args=["s3a://data/input/sample.txt", "s3a://data/output/wordcount_py"],
         remote=True, marker="WORDCOUNT OK"),
    dict(name="py-hive-etl", group="core",
         note="CSV on RustFS -> Hive table demo.orders (metastore + warehouse)",
         resource=f"{PY}/03_hive_etl.py",
         args=["s3a://data/input/orders.csv"], remote=True, marker="HIVE ETL OK"),
    dict(name="py-pyfiles", group="core",
         note="--py-files zip from RustFS, imported on executors",
         resource=f"{PY}/04_pyfiles_demo.py",
         py_files=["s3a://jobs/python/libs/text_utils.zip"],
         args=["s3a://data/input/sample.txt"], remote=True, marker="PYFILES OK"),
    dict(name="py-conf-args", group="core",
         note="per-job Spark conf and app args reach the driver",
         resource=f"{PY}/06_conf_and_args.py",
         conf={"spark.executor.memory": "768m", "spark.app.custom.team": "data-eng"},
         args=["--run-date", "2026-10-15", "--env", "lab"], remote=True,
         marker="CONF AND ARGS OK"),
    dict(name="java-wordcount", group="core",
         note="Java class from the built jar",
         resource=JAR, class_name="com.example.JavaWordCount",
         args=["s3a://data/input/sample.txt", "s3a://data/output/wordcount_java"],
         remote=True, needs_jar=True, marker="JAVA WORDCOUNT OK"),
    dict(name="scala-sales-report", group="core",
         note="Scala class reading demo.orders, writing demo.sales_summary",
         resource=JAR, class_name="com.example.ScalaSalesReport",
         remote=True, needs_jar=True, after=["py-hive-etl"], marker="SCALA SALES REPORT OK"),
    dict(name="py-expected-failure", group="core",
         note="job raises on purpose; Kyuubi must report ERROR, not FINISHED",
         resource=f"{PY}/05_expected_failure.py",
         remote=True, expect="ERROR"),

    # ---- file formats ------------------------------------------------------
    dict(name="formats", group="formats",
         note="CSV->Parquet->ORC->JSON->Avro->CSV, round trip must be identical",
         resource=f"{PY}/10_format_conversions.py", remote=True,
         marker="FORMAT CONVERSIONS OK"),

    # ---- Postgres ----------------------------------------------------------
    dict(name="pg-file-to-pg", group="postgres",
         note="Parquet on RustFS -> Postgres orders (JDBC write)",
         resource=f"{PY}/11_file_to_postgres.py", remote=True,
         marker="FILE TO POSTGRES OK"),
    dict(name="pg-pg-to-files", group="postgres",
         note="Postgres (parallel + pushdown reads) -> Parquet/ORC/CSV",
         resource=f"{PY}/12_postgres_to_files.py", remote=True,
         after=["pg-file-to-pg"], marker="POSTGRES TO FILES OK"),
    dict(name="pg-pg-to-pg", group="postgres",
         note="Postgres -> Postgres: full refresh + idempotent upsert",
         resource=f"{PY}/13_postgres_to_postgres.py", remote=True,
         after=["pg-file-to-pg"], marker="POSTGRES TO POSTGRES OK"),

    # ---- ai: pgvector + jev inside Postgres, driven by Spark ---------------
    dict(name="pgvector-embeddings", group="ai",
         note="Spark embeds products -> pgvector (HNSW) in Postgres, checks Spark == lab_embed(), semantic search",
         resource=f"{PY}/18_pgvector_embeddings.py", remote=True,
         marker="PGVECTOR EMBEDDINGS OK"),
    dict(name="jev-tickets-to-lake", group="ai",
         note="one JDBC query: jev_choice/jev_prob/jev_score_norm + nearest KB article -> Parquet, Hive, Postgres",
         resource=f"{PY}/19_jev_tickets_to_lake.py", remote=True,
         after=["pgvector-embeddings"], marker="JEV TICKETS TO LAKE OK"),

    # ---- lakehouse table formats -------------------------------------------
    dict(name="iceberg", group="lakehouse",
         note="Iceberg in Hive Metastore: MERGE, DELETE, schema+partition evolution, time travel",
         resource=f"{PY}/14_iceberg_demo.py", remote=True, with_formats="iceberg",
         marker="ICEBERG DEMO OK"),
    dict(name="delta", group="lakehouse",
         note="Delta: MERGE, DML, mergeSchema, time travel, change data feed, OPTIMIZE",
         resource=f"{PY}/15_delta_demo.py", remote=True, with_formats="delta",
         marker="DELTA DEMO OK"),
    dict(name="hudi", group="lakehouse",
         note="Hudi COW: insert, upsert, delete, incremental query, time travel",
         resource=f"{PY}/16_hudi_demo.py", remote=True, with_formats="hudi",
         marker="HUDI DEMO OK"),

    # ---- order tracking project (runs in this order) ----------------------
    dict(name="orders-wave1", group="orders",
         note="generate wave 1 of order events (resets earlier runs)",
         resource=f"{OT}/20_generate_order_events.py", remote=True,
         args=["--wave", "1", "--reset"], marker="ORDER EVENTS WAVE OK"),
    dict(name="orders-stream1", group="orders",
         note="stream wave 1 -> Iceberg log, Delta status, Postgres (fresh start)",
         resource=f"{OT}/21_track_orders_stream.py", remote=True,
         with_formats="delta,iceberg", args=["--reset"], after=["orders-wave1"],
         marker="ORDER TRACKING STREAM OK"),
    dict(name="orders-wave2", group="orders", note="generate wave 2",
         resource=f"{OT}/20_generate_order_events.py", remote=True,
         args=["--wave", "2"], after=["orders-stream1"], marker="ORDER EVENTS WAVE OK"),
    dict(name="orders-stream2", group="orders",
         note="stream again: only wave 2 files are new (checkpoint)",
         resource=f"{OT}/21_track_orders_stream.py", remote=True,
         with_formats="delta,iceberg", after=["orders-wave2"],
         marker="ORDER TRACKING STREAM OK"),
    dict(name="orders-wave3", group="orders",
         note="generate wave 3 (with a duplicate and a late event)",
         resource=f"{OT}/20_generate_order_events.py", remote=True,
         args=["--wave", "3"], after=["orders-stream2"], marker="ORDER EVENTS WAVE OK"),
    dict(name="orders-stream3", group="orders", note="stream wave 3",
         resource=f"{OT}/21_track_orders_stream.py", remote=True,
         with_formats="delta,iceberg", after=["orders-wave3"],
         marker="ORDER TRACKING STREAM OK"),
    dict(name="orders-report", group="orders",
         note="SLA report + check Iceberg, Delta and Postgres all agree",
         resource=f"{OT}/22_order_sla_report.py", remote=True,
         with_formats="delta,iceberg", after=["orders-stream3"],
         marker="ORDER SLA REPORT OK"),

    # ---- Hive-style ELT --------------------------------------------------
    dict(name="hive-elt", group="hive",
         note="Hive SQL ELT: external TEXTFILE -> ORC -> partitioned PARQUET -> RCFILE, CTAS, view, ANALYZE",
         resource=f"{PY}/17_hive_elt.py", remote=True, marker="HIVE ELT OK"),

    # ---- streaming: Kafka + file events -----------------------------------
    dict(name="kafka-produce", group="streaming", kind="local", needs_kafka=True,
         note="Python producer app sends 40 order lifecycles to Kafka orders.events",
         cmd=["python", "client/producers/order_producer.py", "--orders", "40", "--run-id", "{run_id}"],
         marker="PRODUCER OK"),
    dict(name="kafka-consumer-basic", group="streaming", needs_kafka=True,
         note="Kafka -> Spark consumer -> Parquet bronze (offsets + per-batch summary)",
         resource=f"{ST}/30_kafka_consumer_basic.py", remote=True, with_formats="kafka",
         py_files=[COMMON], args=["--available-now"], always_on=[],
         after=["kafka-produce"], marker="KAFKA CONSUMER BASIC OK"),
    dict(name="kafka-to-delta-pg", group="streaming", needs_kafka=True,
         note="Kafka -> Delta event table (exactly once) + Postgres order status (upsert)",
         resource=f"{ST}/31_kafka_to_delta_postgres.py", remote=True, with_formats="kafka,delta",
         py_files=[COMMON], args=["--available-now"], always_on=[],
         after=["kafka-produce"], marker="KAFKA TO DELTA POSTGRES OK"),
    dict(name="kafka-enrich-publish", group="streaming", needs_kafka=True,
         note="Kafka -> join customers (Postgres) -> Spark publishes orders.enriched + orders.alerts",
         resource=f"{ST}/32_kafka_enrich_publish.py", remote=True, with_formats="kafka",
         py_files=[COMMON], args=["--available-now"], always_on=[],
         after=["kafka-produce"], marker="KAFKA ENRICH PUBLISH OK"),
    dict(name="kafka-windowed-metrics", group="streaming", needs_kafka=True,
         note="event-time windows + watermark -> Spark publishes orders.metrics + Postgres upsert",
         resource=f"{ST}/33_kafka_windowed_metrics.py", remote=True, with_formats="kafka",
         py_files=[COMMON], args=["--available-now"], always_on=[],
         after=["kafka-produce"], marker="KAFKA WINDOWED METRICS OK"),
    dict(name="kafka-batch-publish", group="streaming", needs_kafka=True,
         note="Spark BATCH producer: Hive demo.orders -> orders.backfill, then batch read-back",
         resource=f"{ST}/34_batch_publish_to_kafka.py", remote=True, with_formats="kafka",
         args=["--run-id", "{run_id}b"], marker="BATCH PUBLISH TO KAFKA OK"),
    dict(name="file-drop-produce", group="streaming", kind="local",
         note="producer drops 3 JSON files of order events into s3a://data/stream/landing/",
         cmd=["python", "client/producers/order_producer.py", "--sink", "s3", "--orders", "20",
              "--files", "3", "--run-id", "{run_id}f"],
         marker="PRODUCER OK"),
    dict(name="file-drop-stream", group="streaming",
         note="file-event stream: new files in landing/ -> Parquet silver, processed files archived",
         resource=f"{ST}/35_file_drop_stream.py", remote=True,
         py_files=[COMMON], args=["--available-now"], always_on=[],
         after=["file-drop-produce"], marker="FILE DROP STREAM OK"),
    dict(name="verify-streaming", group="streaming", needs_kafka=True,
         note="batch check: every output of jobs 30-35 holds exactly this run's events",
         resource=f"{ST}/39_verify_streaming.py", remote=True, with_formats="kafka,delta",
         py_files=[COMMON], args=["--run-id", "{run_id}", "--file-run-id", "{run_id}f"],
         after=["kafka-consumer-basic", "kafka-to-delta-pg", "kafka-enrich-publish",
                "kafka-windowed-metrics", "file-drop-stream"],
         marker="VERIFY STREAMING OK"),
]

GROUPS = ["core", "formats", "hive", "postgres", "ai", "lakehouse", "orders", "streaming"]

# Examples that are documented (and in the payload file) but not run by the
# test suite, because they run until stopped.
MANUAL = [
    dict(name="live-producer", group="streaming", kind="local", needs_kafka=True,
         note="always-on producer: ~5 events/s into orders.events until Ctrl-C",
         cmd=["python", "client/producers/order_producer.py", "--forever", "--rate", "5"]),
    dict(name="live-file-dropper", group="streaming", kind="local",
         note="always-on producer: a JSON file into data/stream/landing/ every ~5 s",
         cmd=["python", "client/producers/order_producer.py", "--sink", "s3", "--forever", "--rate", "5"]),
    dict(name="tail-enriched", group="streaming", kind="local", needs_kafka=True,
         note="Python consumer: print the newest messages Spark published to orders.enriched",
         cmd=["python", "client/producers/topic_tail.py", "orders.enriched", "--last", "5"]),
    dict(name="tail-metrics-follow", group="streaming", kind="local", needs_kafka=True,
         note="Python consumer: follow orders.metrics live while job 33 runs always-on",
         cmd=["python", "client/producers/topic_tail.py", "orders.metrics", "--follow"]),
]
