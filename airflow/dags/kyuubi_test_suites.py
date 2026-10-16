"""
Test-suite DAGs generated from client/job_catalog.py - the same jobs that
`make test` runs, but scheduled and tracked by Airflow:

  kyuubi_tests_core        Kyuubi basics: jars, PySpark from RustFS, uploads, Hive, py-files
  kyuubi_tests_formats     CSV -> Parquet -> ORC -> JSON -> Avro -> CSV round trip
  kyuubi_tests_hive        Hive SQL ELT: TEXTFILE -> ORC -> partitioned Parquet -> RCFILE
  kyuubi_tests_postgres    file -> Postgres, Postgres -> files, Postgres -> Postgres
  kyuubi_tests_lakehouse   Iceberg, Delta, Hudi
  kyuubi_tests_orders      real-time order tracking project (waves + streams + report)
  kyuubi_tests_streaming   Kafka producer -> Spark consumers -> Spark publishes, file events
  kyuubi_tests_all         every group above in one run, one group after another

Dependencies follow each job's `after` list. A job whose input failed is not
run (upstream_failed); Kafka jobs are skipped when Kafka isn't installed. The
last task, `report`, prints a pass/fail table and fails the run if anything failed.
Every task is one Kyuubi batch: Airflow -> Kyuubi REST -> spark-submit -> Spark.
"""
from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator

import job_catalog
from lab_common import DEFAULT_ARGS, START, TAGS, job, report

GROUP_DOC = {
    "core": "Kyuubi basics: jars, PySpark from RustFS, uploads, Hive Metastore, py-files, conf/args, expected failure",
    "formats": "file -> file format conversions: CSV, Parquet, ORC, JSON, Avro",
    "hive": "Hive ELT: external TEXTFILE -> ORC -> partitioned PARQUET -> RCFILE archive -> summary",
    "postgres": "file -> Postgres, Postgres -> Parquet/ORC/CSV, Postgres -> Postgres upsert",
    "lakehouse": "Iceberg (MERGE, snapshots), Delta (MERGE, time travel, CDF), Hudi (upsert, incremental)",
    "orders": "real-time order tracking: generate waves, stream into Iceberg + Delta + Postgres, SLA report",
    "streaming": "Kafka -> Spark -> Kafka/Delta/Postgres, Spark batch -> Kafka, file-event streaming",
}


def build_group(cases):
    """Create one task per case, wire `after` inside the set; return (roots, leaves)."""
    names = {c["name"] for c in cases}
    tasks = {c["name"]: job(c["name"]) for c in cases}
    has_child = set()
    for c in cases:
        for up in c.get("after") or []:
            if up in names:
                tasks[up] >> tasks[c["name"]]
                has_child.add(up)
    roots = [tasks[c["name"]] for c in cases
             if not any(u in names for u in (c.get("after") or []))]
    leaves = [t for n, t in tasks.items() if n not in has_child]
    return roots, leaves


def make_group_dag(group):
    cases = [c for c in job_catalog.CASES if c["group"] == group]
    with DAG(
        dag_id=f"kyuubi_tests_{group}",
        description=GROUP_DOC[group],
        doc_md=f"**{group}**: {GROUP_DOC[group]}\n\n" + "\n".join(
            f"* `{c['name']}` - {c['note']}" for c in cases),
        schedule=None, start_date=START, catchup=False,
        default_args=DEFAULT_ARGS,
        max_active_runs=1, max_active_tasks=2,
        tags=TAGS + ["tests", group],
    ) as d:
        _, leaves = build_group(cases)
        leaves >> report()
    return d


for _g in job_catalog.GROUPS:
    globals()[f"kyuubi_tests_{_g}"] = make_group_dag(_g)


with DAG(
    dag_id="kyuubi_tests_all",
    description="every catalog job, group by group, then one pass/fail report",
    doc_md=__doc__,
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1, max_active_tasks=2,
    tags=TAGS + ["tests", "end-to-end"],
) as kyuubi_tests_all:
    previous = EmptyOperator(task_id="start")
    for g in job_catalog.GROUPS:
        with TaskGroup(group_id=g, tooltip=GROUP_DOC[g]) as tg:
            roots, leaves = build_group([c for c in job_catalog.CASES if c["group"] == g])
            # the next group starts when this one is done, pass or fail
            done = EmptyOperator(task_id="done", trigger_rule="all_done")
            leaves >> done
        previous >> roots
        previous = done
    previous >> report()
