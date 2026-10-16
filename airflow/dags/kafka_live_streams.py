"""
kafka_live_streams - always-on Structured Streaming, started and stopped by
Airflow. Unlike kafka_streaming_pipeline (--available-now), these streams keep
running until someone kills the Kyuubi batch, so the DAG does the full life
cycle:

  kafka_ready      short-circuit if Kafka isn't installed
  start/*          submit jobs 31, 32, 33 without waiting (always-on, 1 core each)
  wait_running     sensor: all three batches RUNNING in Kyuubi
  live_producer    the Python producer sends events for `minutes` at `rate`/s
  check_outputs    Postgres + Kafka got data while the streams were live
  stop_streams     kills the three batches - runs even if something failed
                   (trigger_rule=all_done), so no stream is left behind
  report

Spark worker has 4 cores: 3 streams x 1 core. Don't run other jobs meanwhile.
"""
import os
import subprocess
import sys
import time

from airflow.sdk import DAG, Param, TaskGroup, task
from airflow.sdk.exceptions import AirflowSkipException

from lab_common import DEFAULT_ARGS, LAB_RUN_ID, START, TAGS, report

STREAMS = {"to_delta_pg": "kafka-to-delta-pg",
           "enrich_publish": "kafka-enrich-publish",
           "windowed": "kafka-windowed-metrics"}

with DAG(
    dag_id="kafka_live_streams",
    description="start always-on Kafka streams via Kyuubi, feed them live, verify, stop",
    doc_md=__doc__,
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS, max_active_runs=1, max_active_tasks=4,
    params={
        "minutes": Param(2, type="integer", minimum=1, maximum=60, description="how long to feed the streams"),
        "rate": Param(5, type="number", minimum=0.5, maximum=200, description="events per second"),
    },
    tags=TAGS + ["kafka", "streaming", "always-on"],
) as dag:

    @task.short_circuit
    def kafka_ready():
        import lab_runner
        return lab_runner.kafka_reachable()

    def make_starter(task_id, name):
        @task(task_id=task_id)
        def start(lab_run: str):
            import kyuubi_lab
            import lab_runner
            c = lab_runner.case(name)
            return kyuubi_lab.submit_no_wait(
                name, lab_run, extra_args=c.get("always_on") or [],
                conf={"spark.cores.max": "1"}, batch_name=f"airflow-live-{name}")
        return start(LAB_RUN_ID)

    @task.sensor(poke_interval=10, timeout=600, mode="poke")
    def wait_running(batch_ids: dict):
        import kyuubi_lab
        states = {k: kyuubi_lab.state(b) for k, b in batch_ids.items()}
        print(states)
        dead = {k: s for k, s in states.items() if s in ("ERROR", "FINISHED", "CANCELED", "CANCELLED")}
        if dead:
            raise RuntimeError(f"streams ended before going live: {dead}")
        return all(s == "RUNNING" for s in states.values())

    @task
    def collect(to_delta_pg: str, enrich_publish: str, windowed: str):
        return {"to_delta_pg": to_delta_pg, "enrich_publish": enrich_publish, "windowed": windowed}

    @task
    def live_producer(lab_run: str, params=None):
        secs = int(params["minutes"]) * 60
        cmd = [sys.executable, "client/producers/order_producer.py", "--forever",
               "--rate", str(params["rate"]), "--run-id", lab_run]
        print(f"$ {' '.join(cmd)}   (for {secs}s)")
        p = subprocess.Popen(cmd, cwd=os.getenv("LAB_ROOT", "/opt/lab"))
        time.sleep(secs)
        p.terminate()
        p.wait(30)
        print("producer stopped")

    @task
    def check_outputs(lab_run: str):
        import kyuubi_lab
        time.sleep(30)          # one more trigger interval so the last events land
        pg = kyuubi_lab.pg("SELECT count(*) AS n FROM kafka_order_status")
        enriched = kyuubi_lab.topic("orders.enriched", last=20, run_id=lab_run)
        metrics = kyuubi_lab.topic("orders.metrics", last=20)
        result = {"pg_status_rows": int(pg["n"][0]), "enriched_this_run": len(enriched),
                  "metrics_recent": len(metrics)}
        print(result)
        if not result["pg_status_rows"] or not result["enriched_this_run"]:
            raise ValueError(f"live streams produced nothing: {result}")
        return result

    @task(trigger_rule="all_done")
    def stop_streams(ti=None):
        """Reads the batch ids straight from the start/* tasks, so streams are
        stopped even if a task in between failed."""
        import kyuubi_lab
        batch_ids = {k: ti.xcom_pull(task_ids=f"start.{k}") for k in STREAMS}
        batch_ids = {k: b for k, b in batch_ids.items() if b}
        if not batch_ids:
            raise AirflowSkipException("no streams were started")
        for k, b in batch_ids.items():
            try:
                kyuubi_lab.kill(b)
                print(f"killed {k} ({b})")
            except SystemExit as e:
                print(f"{k} ({b}): {e}")

    gate = kafka_ready()
    with TaskGroup("start") as start:
        started = {tid: make_starter(tid, name) for tid, name in STREAMS.items()}
    ids = collect(**started)
    gate >> start
    live = wait_running(ids)
    feed = live_producer(LAB_RUN_ID)
    check = check_outputs(LAB_RUN_ID)
    stop = stop_streams()
    start >> stop
    live >> feed >> check >> stop >> report()
