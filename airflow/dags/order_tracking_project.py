"""
order_tracking_project - the real-time order tracking project as an Airflow
pipeline. Three waves of order events arrive; after each wave the tracking
stream picks up only the new files (checkpoint) and updates:
  Iceberg  ice.tracking.order_events   append-only event log
  Delta    order_status                current status per order (MERGE)
  Postgres order_tracking              serving table for apps
The SLA report at the end checks that all three stores agree.

  wave1 -> stream1 -> wave2 -> stream2 -> wave3 (duplicate + late event) -> stream3
  -> sla_report -> current_status (SQL on Delta) -> late_orders (SQL on Iceberg) -> report
"""
from airflow.sdk import DAG, TaskGroup

from kyuubi_ops import KyuubiSqlOperator
from lab_common import DEFAULT_ARGS, START, TAGS, job, report

with DAG(
    dag_id="order_tracking_project",
    description="3 waves of order events -> stream -> Iceberg + Delta + Postgres -> SLA report",
    doc_md=__doc__,
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS, max_active_runs=1,
    tags=TAGS + ["project", "streaming", "iceberg", "delta", "postgres"],
) as dag:
    previous = None
    for n in (1, 2, 3):
        with TaskGroup(f"wave{n}") as g:
            job(f"orders-wave{n}", task_id="generate") >> job(f"orders-stream{n}", task_id="stream")
        if previous:
            previous >> g
        previous = g

    sla = job("orders-report", task_id="sla_report")

    current_status = KyuubiSqlOperator(
        task_id="current_status", lake=True,
        sql="""SELECT status, count(*) AS orders
               FROM delta.`s3a://warehouse/lake/delta/order_status`
               GROUP BY status ORDER BY orders DESC""",
        checks=[("SELECT count(*) FROM delta.`s3a://warehouse/lake/delta/order_status`", ">0")],
    )
    event_log = KyuubiSqlOperator(
        task_id="event_log", lake=True,
        sql=["SELECT event_type, count(*) AS events FROM ice.tracking.order_events "
             "GROUP BY event_type ORDER BY events DESC",
             "SELECT snapshot_id, operation, summary['added-records'] AS added "
             "FROM ice.tracking.order_events.snapshots ORDER BY committed_at"],
        checks=[("SELECT count(*) FROM ice.tracking.order_events.snapshots", ">=3")],
    )

    previous >> sla >> [current_status, event_log] >> report()
