"""
lab_common.py - small helpers shared by the lab DAGs (not a DAG itself).

  job("delta")            a KyuubiBatchOperator for a catalog job, task_id = job name
  report()                last task of a test DAG: prints a pass/fail table for the
                          run and fails the DAG run if anything failed
  s3_has(bucket, prefix)  True if RustFS has at least one object under the prefix
  LAB_RUN_ID              templated id shared by every task in one DAG run
"""
import os
from datetime import datetime, timedelta

from airflow.sdk import get_current_context, task
from airflow.exceptions import AirflowException

from kyuubi_ops import LAB_RUN_ID, KyuubiBatchOperator  # noqa: F401  (re-exported)

START = datetime(2026, 1, 1)
DEFAULT_ARGS = {
    "owner": "lab",
    "retries": 0,                       # a failed Spark job should show up as failed
    "execution_timeout": timedelta(minutes=40),
}
TAGS = ["kyuubi", "spark"]


def job(name, task_id=None, **kw):
    return KyuubiBatchOperator(task_id=task_id or name, job=name, **kw)


def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://rustfs:9000"),
        aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"),
        aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin123"),
        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))


def s3_keys(bucket, prefix):
    resp = s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
    return [o["Key"] for o in resp.get("Contents", [])]


def s3_has(bucket, prefix):
    return bool(s3_keys(bucket, prefix))


BAD = {"failed", "upstream_failed"}


@task(trigger_rule="all_done")
def report():
    """Pass/fail table for this DAG run. Fails if any task failed, so the run
    turns red even though this task runs regardless (trigger_rule=all_done)."""
    ctx = get_current_context()
    ti, dag_run = ctx["ti"], ctx["dag_run"]
    task_ids = [t for t in ctx["dag"].task_ids if t != ti.task_id]
    states = ti.get_task_states(dag_id=ti.dag_id, task_ids=task_ids,
                                run_ids=[dag_run.run_id]).get(dag_run.run_id, {})
    width = max([len(t) for t in task_ids] + [10])
    print(f"\n{'task':{width}}  state")
    print(f"{'-' * width}  {'-' * 16}")
    for t in task_ids:
        print(f"{t:{width}}  {states.get(t)}")
    counts = {}
    for s in states.values():
        counts[str(s)] = counts.get(str(s), 0) + 1
    print("\ntotals:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    bad = [t for t, s in states.items() if str(s) in BAD]
    if bad:
        raise AirflowException(f"{len(bad)} task(s) failed: {', '.join(sorted(bad))}")
    return counts
