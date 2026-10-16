"""
file_event_pipeline - Structured Streaming on file events, driven by Airflow.

A "source system" drops JSON files into s3a://data/stream/landing/. A sensor
waits for new files, then the file-source stream (job 35, --available-now)
processes only the files it hasn't seen (its checkpoint remembers them) and
writes Parquet partitioned by date + city. Then plain SQL reads the result.

  drop_files       producer writes `files` JSON files (simulates the upstream system)
  wait_for_files   sensor: landing/ has files for this run
  file_stream      Kyuubi batch: 35_file_drop_stream.py --available-now
  silver_by_city   Kyuubi SQL straight on the Parquet folder
  rerun_is_noop    run the stream again: nothing new, so nothing is reprocessed
  report

Doesn't need Kafka.
"""
from airflow.sdk import DAG, Param, task

from kyuubi_ops import KyuubiBatchOperator, KyuubiSqlOperator
from lab_common import DEFAULT_ARGS, START, TAGS, job, report, s3_client

SILVER = "s3a://data/stream/silver/order_events_by_city"

with DAG(
    dag_id="file_event_pipeline",
    description="JSON files land in RustFS -> sensor -> Spark file-source stream -> Parquet -> SQL",
    doc_md=__doc__,
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS, max_active_runs=1,
    params={"orders": Param(20, type="integer", minimum=1), "files": Param(3, type="integer", minimum=1)},
    tags=TAGS + ["streaming", "file-events", "parquet"],
) as dag:

    drop = job("file-drop-produce", task_id="drop_files",
               extra_args=["--orders", "{{ params.orders }}", "--files", "{{ params.files }}"])

    @task.sensor(poke_interval=10, timeout=300, mode="poke")
    def wait_for_files(dag_run=None):
        """New = written to landing/ after this DAG run started."""
        resp = s3_client().list_objects_v2(Bucket="data", Prefix="stream/landing/")
        new = [o["Key"] for o in resp.get("Contents", [])
               if o["Key"].endswith(".json") and o["LastModified"] >= dag_run.start_date]
        print(f"{len(new)} new file(s) in s3a://data/stream/landing/:", new[:10])
        return bool(new)

    stream = job("file-drop-stream", task_id="file_stream")

    silver = KyuubiSqlOperator(
        task_id="silver_by_city",
        sql=f"""SELECT city, event_type, count(*) AS events
                FROM parquet.`{SILVER}`
                GROUP BY city, event_type ORDER BY city, events DESC""",
        checks=[(f"SELECT count(*) FROM parquet.`{SILVER}`", ">0")],
        show=50,
    )

    # the same stream again: the checkpoint already lists every file, so the
    # batch finishes without processing anything (that's the point of checkpoints)
    rerun = KyuubiBatchOperator(task_id="rerun_is_noop", job="file-drop-stream")

    drop >> wait_for_files() >> stream >> silver >> rerun >> report()
