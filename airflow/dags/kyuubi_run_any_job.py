"""
kyuubi_run_any_job - trigger ANY job from the lab catalog from the Airflow UI
("Trigger DAG w/ config"), the Airflow version of `kyuubi_submit.py`.

Pick a job from the list, optionally add app arguments and Spark conf. The
task submits it to Kyuubi, streams the driver log into the task log and
fails if the batch fails. Example config (REST/CLI):

  {"job": "iceberg"}
  {"job": "kafka-windowed-metrics", "args": ["--run-seconds", "120"],
   "conf": {"spark.cores.max": "1"}}               <- args replace --available-now
  {"job": "py-wordcount", "args": ["s3a://data/input/sample.txt", "s3a://data/output/wc_af"]}
  {"job": "hive-elt", "conf": {"spark.executor.memory": "1g"}}

"args" replaces the job's own arguments, "extra_args" appends to them.
"""
from airflow.sdk import DAG, Param

import job_catalog
from kyuubi_ops import KyuubiBatchOperator
from lab_common import DEFAULT_ARGS, START, TAGS

NAMES = [c["name"] for c in job_catalog.CASES + job_catalog.MANUAL
         if c.get("kind") != "local" or c["name"] in ("kafka-produce", "file-drop-produce")]

with DAG(
    dag_id="kyuubi_run_any_job",
    description="pick one catalog job, add args/conf, run it through Kyuubi",
    doc_md=__doc__ + "\n\nJobs:\n\n" + "\n".join(
        f"* `{c['name']}` ({c['group']}) - {c['note']}" for c in job_catalog.CASES),
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS, max_active_runs=4,
    render_template_as_native_obj=True,      # params.args -> list, params.conf -> dict
    params={
        "job": Param("py-smoke", type="string", enum=NAMES, description="catalog job name"),
        "args": Param(None, type=["null", "array"], items={"type": "string"},
                      description="app arguments that REPLACE the job's own (null = keep the job's)"),
        "extra_args": Param([], type="array", items={"type": "string"},
                            description="app arguments appended to the job's own"),
        "conf": Param({}, type="object", description='extra Spark conf, e.g. {"spark.cores.max": "1"}'),
    },
    tags=TAGS + ["adhoc"],
) as dag:
    KyuubiBatchOperator(
        task_id="run_job",
        job="{{ params.job }}",
        args="{{ params.args }}",
        extra_args="{{ params.extra_args }}",
        conf="{{ params.conf }}",
    )
