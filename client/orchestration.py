"""
orchestration.py - what the Airflow DAGs and Jupyter notebooks run.

Plain data (standard library only) so gen_payloads.py can put it into
All_execution_payload.md/.json next to the job payloads. gen_payloads.py also
checks it against the real files: every DAG id must exist in airflow/dags/,
every notebook in notebooks/, and every job name in job_catalog.py.

Each DAG task that runs a job sends exactly the catalog payload for that job
(see the job sections), with name "airflow-<job>" and run_id "af<yyyymmddHHMMSS>".
Notebooks send it with name "nb-<job>" and run_id "nb<mmddHHMMSS>".
"""
from job_catalog import CASES, GROUPS

SUITE_JOBS = {g: [c["name"] for c in CASES if c["group"] == g] for g in GROUPS}

DAGS = [
    dict(dag_id="kyuubi_tests_all", file="kyuubi_test_suites.py",
         what="Every catalog job, group by group (core, formats, hive, postgres, lakehouse, "
              "orders, streaming), then a pass/fail report. The Airflow version of `make test`.",
         jobs=[n for g in GROUPS for n in SUITE_JOBS[g]], uses=["kyuubi-rest"],
         conf=None),
] + [
    dict(dag_id=f"kyuubi_tests_{g}", file="kyuubi_test_suites.py",
         what=f"The `{g}` test group only, with the same dependencies as `make test-{g}`.",
         jobs=SUITE_JOBS[g], uses=["kyuubi-rest"] + (["kafka"] if g == "streaming" else []),
         conf=None)
    for g in GROUPS
] + [
    dict(dag_id="kyuubi_run_any_job", file="kyuubi_run_any_job.py",
         what="Pick any catalog job in the trigger form; optional args (replace), extra_args "
              "(append) and Spark conf.",
         jobs=["<any>"], uses=["kyuubi-rest"],
         conf={"job": "iceberg", "args": None, "extra_args": [], "conf": {"spark.executor.memory": "1g"}}),
    dict(dag_id="lakehouse_daily_elt", file="lakehouse_daily_elt.py",
         what="Daily pipeline (02:00, created paused): S3 sensor -> Hive ELT (TEXTFILE/ORC/Parquet/RCFILE) "
              "-> formats -> Postgres file/table jobs -> Iceberg, Delta, Hudi -> SQL quality checks over "
              "Kyuubi JDBC -> top region per month.",
         jobs=["hive-elt", "formats", "pg-file-to-pg", "pg-pg-to-files", "pg-pg-to-pg",
               "iceberg", "delta", "hudi"], uses=["rustfs", "kyuubi-rest", "kyuubi-jdbc"],
         conf=None),
    dict(dag_id="kafka_streaming_pipeline", file="kafka_streaming_pipeline.py",
         what="Producer -> Kafka -> four Structured Streaming jobs (--available-now) -> Kafka/Delta/Postgres, "
              "Spark batch backfill to Kafka, file-event stream, verify job, then Python checks of the "
              "topics and Postgres. Skips itself when Kafka isn't installed.",
         jobs=["kafka-produce", "kafka-consumer-basic", "kafka-to-delta-pg", "kafka-enrich-publish",
               "kafka-windowed-metrics", "kafka-batch-publish", "file-drop-produce", "file-drop-stream",
               "verify-streaming"], uses=["kafka", "kyuubi-rest", "postgres", "rustfs"],
         conf={"orders": 40}),
    dict(dag_id="kafka_live_streams", file="kafka_live_streams.py",
         what="Always-on streams: starts jobs 31/32/33 without --available-now (1 core each), waits until "
              "RUNNING, feeds them with the producer for N minutes, checks Postgres and Kafka, then kills "
              "the three batches (always, even after a failure).",
         jobs=["kafka-to-delta-pg", "kafka-enrich-publish", "kafka-windowed-metrics"],
         uses=["kafka", "kyuubi-rest", "postgres"], conf={"minutes": 2, "rate": 5}),
    dict(dag_id="kyuubi_sql_reporting", file="kyuubi_sql_reporting.py",
         what="SQL only, over Kyuubi JDBC: branch on whether demo.orders exists (build it with a batch if "
              "not), CTAS into Parquet/ORC + a view, quality checks, copy into Iceberg and Delta, upsert "
              "the report into Postgres shop.report_region_month.",
         jobs=["py-hive-etl"], uses=["kyuubi-jdbc", "kyuubi-rest", "postgres"], conf=None),
    dict(dag_id="file_event_pipeline", file="file_event_pipeline.py",
         what="JSON files dropped into s3a://data/stream/landing -> sensor -> file-source stream -> "
              "Parquet silver -> SQL on the folder -> second run processes nothing (checkpoint).",
         jobs=["file-drop-produce", "file-drop-stream"], uses=["rustfs", "kyuubi-rest", "kyuubi-jdbc"],
         conf={"orders": 20, "files": 3}),
    dict(dag_id="postgres_ai_pipeline", file="postgres_ai_pipeline.py",
         what="pgvector + jev: check the extensions, Spark embeds products into pgvector, semantic search "
              "in Postgres, one JDBC query routes tickets with jev_choice/jev_prob/jev_score_norm + nearest "
              "KB article into Parquet/Hive/Postgres, Kyuubi SQL report, escalation list.",
         jobs=["pgvector-embeddings", "jev-tickets-to-lake"], uses=["postgres", "kyuubi-rest", "kyuubi-jdbc"],
         conf={"search": "warm clothes for cold winter"}),
    dict(dag_id="order_tracking_project", file="order_tracking_project.py",
         what="Order tracking project: three waves, each streamed into Iceberg + Delta + Postgres, "
              "SLA report, then SQL on the Delta status table and the Iceberg event log.",
         jobs=SUITE_JOBS["orders"], uses=["kyuubi-rest", "kyuubi-jdbc", "postgres"], conf=None),
]

NOTEBOOKS = [
    dict(file="00_start_here.ipynb", what="service check, the job catalog, a REST payload, the first job",
         jobs=["py-smoke"], uses=["kyuubi-rest"]),
    dict(file="01_kyuubi_rest_basics.ipynb",
         what="raw requests: JSON batch, multipart upload, polling, localLog, list, kill, expected failure",
         jobs=["py-upload", "py-expected-failure"], uses=["kyuubi-rest"]),
    dict(file="02_kyuubi_sql_jdbc.ipynb",
         what="PyHive over Kyuubi JDBC: CTAS Parquet/ORC, DESCRIBE FORMATTED, EXPLAIN, SET, views",
         jobs=["py-hive-etl"], uses=["kyuubi-jdbc"]),
    dict(file="03_file_formats_and_hive_elt.ipynb",
         what="format round trip, SQL on parquet/orc/json/avro folders, Hive ELT tables incl. RCFILE",
         jobs=["formats", "hive-elt"], uses=["kyuubi-rest", "kyuubi-jdbc", "rustfs"]),
    dict(file="04_postgres_pipelines.ipynb",
         what="file -> Postgres, Postgres -> Parquet/ORC/CSV, Postgres -> Postgres upsert (idempotency check)",
         jobs=["pg-file-to-pg", "pg-pg-to-files", "pg-pg-to-pg"], uses=["kyuubi-rest", "kyuubi-jdbc", "postgres", "rustfs"]),
    dict(file="05_lakehouse_iceberg_delta_hudi.ipynb",
         what="Iceberg snapshots + time travel, Delta history + time travel + change data feed, Hudi timeline",
         jobs=["iceberg", "delta", "hudi"], uses=["kyuubi-rest", "kyuubi-jdbc", "rustfs"]),
    dict(file="06_kafka_produce_consume.ipynb",
         what="Python producer -> orders.events, Spark consumer -> Parquet bronze, Kafka -> Delta + Postgres",
         jobs=["kafka-produce", "kafka-consumer-basic", "kafka-to-delta-pg"], uses=["kafka", "kyuubi-rest", "kyuubi-jdbc", "postgres"]),
    dict(file="07_spark_publishes_to_kafka.ipynb",
         what="enrich + alerts, windowed metrics, batch backfill to Kafka, then the verify job",
         jobs=["kafka-enrich-publish", "kafka-windowed-metrics", "kafka-batch-publish", "file-drop-stream",
               "kafka-consumer-basic", "kafka-to-delta-pg", "verify-streaming"], uses=["kafka", "kyuubi-rest", "postgres", "rustfs"]),
    dict(file="08_file_event_streaming.ipynb",
         what="file drops -> file-source stream -> Parquet; rerun is a no-op; live mode with kill",
         jobs=["file-drop-stream"], uses=["rustfs", "kyuubi-rest", "kyuubi-jdbc"]),
    dict(file="09_order_tracking_project.ipynb",
         what="the three-wave order tracking project and its Iceberg/Delta/Postgres results",
         jobs=SUITE_JOBS["orders"], uses=["kyuubi-rest", "kyuubi-jdbc", "postgres"]),
    dict(file="10_always_on_streams.ipynb",
         what="start jobs 31/32/33 always-on, feed them, watch Postgres/Kafka, kill them",
         jobs=["kafka-to-delta-pg", "kafka-enrich-publish", "kafka-windowed-metrics"], uses=["kafka", "kyuubi-rest", "postgres"]),
    dict(file="11_airflow_from_notebook.ipynb",
         what="list DAGs, trigger kyuubi_run_any_job / kyuubi_sql_reporting / kafka_streaming_pipeline over Airflow REST",
         jobs=["py-wordcount"], uses=["airflow", "postgres"]),
    dict(file="12_end_to_end_all.ipynb",
         what="every catalog job, group by group, with a pass/fail table and chart",
         jobs=[c["name"] for c in CASES], uses=["kyuubi-rest"]),
    dict(file="13_pgvector_and_jev.ipynb",
         what="jev(), jev_prob(), jev_choice(), jev_score() on people/tickets/products; Spark-written pgvector "
              "embeddings and semantic search; jev + pgvector routing landed in the lake",
         jobs=["pgvector-embeddings", "jev-tickets-to-lake"], uses=["postgres", "kyuubi-rest", "kyuubi-jdbc"]),
]
