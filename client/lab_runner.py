"""
lab_runner.py - run one job from job_catalog.py and judge the result.

Shared by everything that runs lab jobs, so they all behave the same:
  client/run_tests.py           the test suite
  airflow/dags/kyuubi_ops.py    KyuubiBatchOperator (Airflow tasks)
  notebooks/*.ipynb             via client/kyuubi_lab.py

A job counts as passed when its final state matches `expect` AND, if it
FINISHED, its log contains the job's success marker (each sample job prints
one only after its own checks pass).
"""
import os
import socket
import subprocess
import sys
import time

from job_catalog import CASES, LOCAL_JAR, MANUAL
from kyuubi_submit import Kyuubi, lakehouse_conf

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
BY_NAME = {c["name"]: c for c in CASES + MANUAL}


def case(name):
    try:
        return BY_NAME[name]
    except KeyError:
        raise KeyError(f"no job named {name!r}; see client/job_catalog.py") from None


def kafka_reachable(bootstrap=None):
    host, _, port = (bootstrap or os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")).partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=3):
            return True
    except OSError:
        return False


def fill(values, run_id):
    return [v.replace("{run_id}", run_id) for v in values]


def skip_reason(c):
    """Why this job can't run in the current environment, or None."""
    if c.get("needs_jar") and not os.path.isfile(LOCAL_JAR):
        return "jar not built: make jars && make upload"
    if c.get("needs_kafka") and not kafka_reachable():
        return "Kafka not installed: make kafka-install"
    return None


def run_case(c, run_id, kyuubi=None, out=print, max_wait=900, extra_args=None,
             conf_overrides=None, name_prefix="lab-", on_submit=None):
    """Run one catalog entry. Returns a dict:
    name, ok, state, expect, batch_id, secs, detail, log (list of lines)."""
    t0 = time.time()
    expect = c.get("expect", "FINISHED")
    log = []

    def sink(line):
        log.append(line)
        out(line)

    if c.get("kind") == "local":
        cmd = fill(c["cmd"], run_id) + list(extra_args or [])
        if cmd[0] in ("python", "python3"):
            cmd[0] = sys.executable   # same interpreter (and packages) as the caller
        sink(f"$ {' '.join(cmd)}")
        try:
            p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=max_wait)
            for line in (p.stdout + p.stderr).splitlines():
                sink(line)
            state = "FINISHED" if p.returncode == 0 else "ERROR"
        except Exception as e:  # timeout, missing interpreter, ...
            sink(str(e))
            state = "ERROR"
        batch_id = "(local)"
    else:
        k = kyuubi or Kyuubi(os.getenv("KYUUBI_REST_URL", "http://localhost:10099"),
                             os.getenv("KYUUBI_USER", "anji"))
        conf = lakehouse_conf((c.get("with_formats") or "").split(","))
        conf.update(c.get("conf") or {})
        conf.update(conf_overrides or {})
        args = fill(c.get("args") or [], run_id) + list(extra_args or [])
        try:
            batch = k.submit(c["resource"], f"{name_prefix}{c['name']}", c.get("class_name"), conf,
                             args, c.get("py_files"), c.get("files"), c.get("jars"),
                             remote=c.get("remote", False))
        except SystemExit as e:  # HTTP error from Kyuubi
            return dict(name=c["name"], ok=False, state="SUBMIT_FAILED", expect=expect,
                        batch_id="-", secs=int(time.time() - t0), detail=str(e), log=[str(e)])
        batch_id = batch["id"]
        sink(f"batch id {batch_id}")
        if on_submit:
            on_submit(batch_id)
        info = k.follow(batch_id, out=sink, max_wait=max_wait)
        state = info.get("state", "?")

    ok, detail = state == expect, ""
    marker = c.get("marker")
    if ok and expect == "FINISHED" and marker and not any(marker in line for line in log):
        ok, state, detail = False, "NO_MARKER", f"finished but never printed '{marker}'"
    if not ok and not detail:
        detail = f"expected {expect}, got {state}"
    return dict(name=c["name"], ok=ok, state=state, expect=expect, batch_id=batch_id,
                secs=int(time.time() - t0), detail=detail, log=log)
