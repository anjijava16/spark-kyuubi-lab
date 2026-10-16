"""
kyuubi_ops.py - Airflow operators for the Kyuubi lab (Airflow 3, Task SDK).

  KyuubiBatchOperator   run a job from client/job_catalog.py:
                        Kyuubi batch over REST (POST /api/v1/batches), or a local
                        helper such as the Kafka producer. Streams the Spark
                        driver log into the Airflow task log, fails if the batch
                        ends in ERROR or the job's success line is missing,
                        skips if Kafka/the jar isn't available, and kills the
                        Kyuubi batch if you stop the Airflow task.
  KyuubiSqlOperator     run SQL on Kyuubi's Spark SQL engine through the
                        Thrift/JDBC port (PyHive), with optional data checks.

The lab's client code is mounted at $LAB_ROOT/client (default /opt/lab), so
DAGs and the CLI/test suite share one implementation (client/lab_runner.py).
Not a DAG file: Airflow skips files without "dag" + "airflow" in them only
heuristically, so this file has no DAG objects to be safe either way.
"""
import os
import sys

LAB_CLIENT = os.path.join(os.getenv("LAB_ROOT", "/opt/lab"), "client")
if LAB_CLIENT not in sys.path:
    sys.path.insert(0, LAB_CLIENT)

from airflow.sdk import BaseOperator  # noqa: E402
from airflow.sdk.exceptions import AirflowSkipException  # noqa: E402
from airflow.exceptions import AirflowException  # noqa: E402

import lab_runner  # noqa: E402
from kyuubi_submit import Kyuubi, lakehouse_conf  # noqa: E402

KYUUBI_REST_URL = os.getenv("KYUUBI_REST_URL", "http://kyuubi:10099")
KYUUBI_USER = os.getenv("KYUUBI_USER", "airflow")
# One id per DAG run, used in file names and Kafka run_ids. run_after is set
# for every run (manual runs in Airflow 3 may have no logical_date).
LAB_RUN_ID = "af{{ dag_run.run_after.strftime('%Y%m%d%H%M%S') }}"


class KyuubiBatchOperator(BaseOperator):
    """Run one job from client/job_catalog.py through Kyuubi.

    :param job: catalog name, e.g. "delta", "hive-elt", "kafka-produce"
    :param case: instead of `job`, a full catalog-style dict for an ad-hoc job
                 (resource, args, conf, py_files, with_formats, marker, ...)
    :param run_id: replaces {run_id} in the job's args (templated)
    :param args: replaces the job's own args (templated; None = keep them)
    :param extra_args: appended to the job's args (templated)
    :param conf: extra Spark conf for the batch request (templated)
    :param max_wait: seconds to wait for the batch to finish
    """

    template_fields = ("job", "run_id", "args", "extra_args", "conf")
    ui_color = "#e8f1fb"

    def __init__(self, *, job=None, case=None, run_id=LAB_RUN_ID, args=None, extra_args=None,
                 conf=None, max_wait=1800, **kwargs):
        if bool(job) == bool(case):
            raise ValueError("pass exactly one of job= or case=")
        super().__init__(**kwargs)
        self.job, self.case = job, case
        self.run_id, self.args, self.extra_args, self.conf = run_id, args, extra_args, conf
        self.max_wait = max_wait
        self._batch_id = None

    def _remember(self, batch_id):
        self._batch_id = batch_id

    def execute(self, context):
        c = dict(self.case) if self.case else lab_runner.case(self.job)
        c.setdefault("name", self.task_id)
        if self.args is not None:
            c["args" if c.get("kind") != "local" else "cmd"] = (
                list(self.args) if c.get("kind") != "local" else c["cmd"][:2] + list(self.args))
        reason = lab_runner.skip_reason(c)
        if reason:
            raise AirflowSkipException(reason)
        kind = "local helper" if c.get("kind") == "local" else f"Kyuubi batch at {KYUUBI_REST_URL}"
        self.log.info("job %s (%s) run_id=%s: %s", c["name"], kind, self.run_id, c.get("note", ""))
        r = lab_runner.run_case(
            c, self.run_id, kyuubi=Kyuubi(KYUUBI_REST_URL, KYUUBI_USER),
            out=self.log.info, max_wait=self.max_wait, extra_args=self.extra_args,
            conf_overrides=self.conf, name_prefix="airflow-", on_submit=self._remember)
        summary = {k: r[k] for k in ("name", "ok", "state", "batch_id", "secs", "detail")}
        self.log.info("result: %s", summary)
        if not r["ok"]:
            raise AirflowException(f"{c['name']}: {r['detail']}")
        return summary          # -> XCom

    def on_kill(self):
        # Airflow task stopped (marked failed, timeout, cleared): stop the Spark app too
        if self._batch_id and self._batch_id != "(local)":
            self.log.info("killing Kyuubi batch %s", self._batch_id)
            try:
                Kyuubi(KYUUBI_REST_URL, KYUUBI_USER).kill(self._batch_id)
            except SystemExit as e:
                self.log.warning("kill failed: %s", e)


def _check(value, rule):
    """rule: '>0', '>=120', '==120', '<5' -> bool"""
    for op in (">=", "<=", "==", ">", "<"):
        if rule.startswith(op):
            target = float(rule[len(op):])
            v = float(value)
            return {">=": v >= target, "<=": v <= target, "==": v == target,
                    ">": v > target, "<": v < target}[op]
    raise ValueError(f"bad rule {rule!r}")


def kyuubi_connect(lake=False, user=None):
    """PyHive connection to Kyuubi's JDBC port; lake=True turns on Delta + Iceberg."""
    from pyhive import hive
    configuration = lakehouse_conf(["delta", "iceberg"]) if lake else None
    user = user or ("airflow_lake" if lake else KYUUBI_USER)
    host = os.getenv("KYUUBI_JDBC_HOST", "kyuubi")
    port = int(os.getenv("KYUUBI_JDBC_PORT", "10009"))
    return hive.connect(host=host, port=port, auth="NONE", username=user,
                        configuration=configuration), f"{host}:{port} as {user}"


def kyuubi_sql(query, lake=False, user=None):
    """Run one query through Kyuubi, return (columns, rows). For @task code."""
    conn, _ = kyuubi_connect(lake, user)
    try:
        cur = conn.cursor()
        cur.execute(query)
        if not cur.description:
            return [], []
        return [d[0].split(".")[-1] for d in cur.description], [list(map(_plain, r)) for r in cur.fetchall()]
    finally:
        conn.close()


class KyuubiSqlOperator(BaseOperator):
    """Run SQL through Kyuubi's Thrift/JDBC port (Spark SQL engine).

    :param sql: one statement or a list, run in order in one session
    :param checks: list of (query returning one number, rule) - e.g.
                   ("SELECT count(*) FROM elt.orders_fact", ">0"). Any failed
                   check fails the task.
    :param lake: open the session with Delta + Iceberg enabled (user "lake")
    :param user: Kyuubi user (each user gets its own engine)
    :param show: rows of the last SELECT to print and return
    """

    template_fields = ("sql", "checks")
    ui_color = "#fdf3e7"

    def __init__(self, *, sql=None, checks=None, lake=False, user=None, show=20, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql or []
        self.checks = checks or []
        self.lake, self.user, self.show = lake, user, show

    def execute(self, context):
        conn, where = kyuubi_connect(self.lake, self.user)
        self.log.info("Kyuubi JDBC %s (lake=%s)", where, self.lake)
        cur = conn.cursor()
        last = None
        try:
            for stmt in ([self.sql] if isinstance(self.sql, str) else self.sql):
                self.log.info("SQL> %s", " ".join(stmt.split()))
                cur.execute(stmt)
                if cur.description:
                    cols = [d[0].split(".")[-1] for d in cur.description]
                    rows = cur.fetchmany(self.show)
                    last = {"columns": cols, "rows": [list(map(_plain, r)) for r in rows]}
                    for r in rows:
                        self.log.info("    %s", dict(zip(cols, r)))
            failed = []
            for query, rule in self.checks:
                cur.execute(query)
                value = cur.fetchone()[0]
                ok = _check(value, rule)
                self.log.info("CHECK %-6s %s -> %s %s", "ok" if ok else "FAILED",
                              " ".join(query.split()), value, rule)
                if not ok:
                    failed.append(f"{query} -> {value} (want {rule})")
            if failed:
                raise AirflowException("data checks failed: " + "; ".join(failed))
        finally:
            cur.close()
            conn.close()
        return last


def _plain(v):
    """XCom-safe value."""
    return v if isinstance(v, (int, float, str, bool, type(None))) else str(v)
