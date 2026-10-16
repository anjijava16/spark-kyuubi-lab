"""
lakehouse_daily_elt - a "real" daily pipeline: every step is a Kyuubi batch
or Kyuubi SQL, Airflow only orchestrates.

  wait_for_landing      sensor: s3a://data/input/orders.csv is in RustFS
  hive_elt              Hive SQL ELT: TEXTFILE -> ORC -> partitioned PARQUET -> RCFILE
  formats               CSV -> Parquet -> ORC -> JSON -> Avro -> CSV
  postgres/*            file -> Postgres, then Postgres -> files and Postgres -> Postgres
  lakehouse/*           Iceberg, Delta, Hudi demos (each one batch)
  quality_hive          SQL checks on the Hive tables over Kyuubi JDBC
  quality_lake          SQL checks on Iceberg + Delta (session with both enabled)
  best_region           the published result: top region per month
  report                pass/fail table

Scheduled 02:00 daily but created paused - unpause it in the UI if you want
it to run by itself, or trigger it by hand.
"""
from airflow.sdk import DAG, TaskGroup, task

from kyuubi_ops import KyuubiSqlOperator
from lab_common import DEFAULT_ARGS, START, TAGS, job, report, s3_has

with DAG(
    dag_id="lakehouse_daily_elt",
    description="Hive ELT + formats + Postgres + Iceberg/Delta/Hudi, then SQL quality checks",
    doc_md=__doc__,
    schedule="0 2 * * *", start_date=START, catchup=False, is_paused_upon_creation=True,
    default_args=DEFAULT_ARGS, max_active_runs=1, max_active_tasks=2,
    tags=TAGS + ["pipeline", "hive", "postgres", "iceberg", "delta", "hudi", "orc", "parquet", "rcfile"],
) as dag:

    @task.sensor(poke_interval=20, timeout=600, mode="reschedule")
    def wait_for_landing():
        """The upstream 'system' drops orders.csv into the data bucket (make upload)."""
        return s3_has("data", "input/orders.csv")

    hive_elt = job("hive-elt", task_id="hive_elt")
    formats = job("formats")

    with TaskGroup("postgres") as postgres:
        to_pg = job("pg-file-to-pg", task_id="file_to_pg")
        to_pg >> [job("pg-pg-to-files", task_id="pg_to_files"),
                  job("pg-pg-to-pg", task_id="pg_to_pg")]

    with TaskGroup("lakehouse") as lakehouse:
        job("iceberg") >> job("delta") >> job("hudi")

    quality_hive = KyuubiSqlOperator(
        task_id="quality_hive",
        sql=["SHOW TABLES IN elt"],
        checks=[
            ("SELECT count(*) FROM elt.orders_fact", ">0"),
            ("SELECT count(*) FROM elt.monthly_summary", ">0"),
            ("SELECT (SELECT count(*) FROM elt.orders_fact) - (SELECT count(*) FROM elt.orders_archive_rc)", "==0"),
            ("SELECT count(*) FROM elt.orders_fact WHERE amount IS NULL OR amount < 0", "==0"),
        ],
    )
    quality_lake = KyuubiSqlOperator(
        task_id="quality_lake", lake=True,
        sql=["SELECT 'iceberg' AS fmt, count(*) AS n FROM ice.lab.orders "
             "UNION ALL SELECT 'delta', count(*) FROM default.delta_orders"],
        checks=[
            ("SELECT count(*) FROM ice.lab.orders", ">0"),
            ("SELECT count(*) FROM default.delta_orders", ">0"),
            ("SELECT count(*) FROM ice.lab.orders.snapshots", ">=2"),
        ],
    )
    best_region = KyuubiSqlOperator(
        task_id="best_region",
        sql="SELECT order_month, region, revenue FROM elt.v_monthly_top "
            "WHERE rank_in_month = 1 ORDER BY order_month",
    )

    landing = wait_for_landing()
    landing >> hive_elt >> quality_hive >> best_region
    landing >> formats >> postgres
    hive_elt >> lakehouse >> quality_lake
    [best_region, postgres, quality_lake] >> report()
