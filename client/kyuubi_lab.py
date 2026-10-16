"""
kyuubi_lab.py - a small, notebook-friendly API over the lab.

Two ways into Kyuubi:
    run("delta")                       batch job from job_catalog.py via REST
    sql("SELECT * FROM demo.orders")   interactive SQL via JDBC (Thrift :10009)

And the other lab services, so a notebook can show results end to end:
    pg("SELECT * FROM order_tracking")        Postgres "shop" DB -> DataFrame
    s3_ls("data", "stream/landing/")           list RustFS objects
    produce(orders=20)                         send order events to Kafka
    topic("orders.enriched")                   read a topic -> DataFrame
    services()                                 which services are reachable
    airflow_trigger("kafka_streaming_pipeline") start a DAG run (if Airflow is up)

Everything reads its endpoints from environment variables with in-Docker
defaults (kyuubi:10099, kyuubi:10009, postgres:5432, rustfs:9000, kafka:9092).
"""
import datetime as dt
import json
import os
import socket
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from job_catalog import CASES, GROUPS, MANUAL  # noqa: E402
from kyuubi_submit import Kyuubi, build_request, lakehouse_conf  # noqa: E402
import lab_runner  # noqa: E402

REST_URL = os.getenv("KYUUBI_REST_URL", "http://kyuubi:10099")
JDBC_HOST = os.getenv("KYUUBI_JDBC_HOST", "kyuubi")
JDBC_PORT = int(os.getenv("KYUUBI_JDBC_PORT", "10009"))
USER = os.getenv("KYUUBI_USER", "anji")
PG = dict(host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")),
          dbname="shop", user="app", password="apppass")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow:8080")

# Spark conf for an SQL session that can read Delta + Iceberg tables. Sent
# when the session's engine starts, so it uses its own Kyuubi user ("lake")
# and therefore its own engine (share level USER).
LAKE_CONF = lakehouse_conf(["delta", "iceberg"])


def new_run_id(prefix="nb"):
    return prefix + dt.datetime.now(dt.timezone.utc).strftime("%m%d%H%M%S")


def kyuubi():
    return Kyuubi(REST_URL, USER)


# ---------------------------------------------------------------- batch jobs
def jobs(group=None):
    """The job catalog as a DataFrame (or a list if pandas isn't installed)."""
    rows = [dict(name=c["name"], group=c["group"], kind=c.get("kind", "batch"),
                 connectors=c.get("with_formats", ""), note=c["note"])
            for c in CASES + MANUAL if group in (None, c["group"])]
    try:
        import pandas as pd
        return pd.DataFrame(rows)
    except ImportError:
        return rows


def payload(name, run_id="demo1"):
    """The exact REST body run(name) would send, without sending it."""
    c = lab_runner.case(name)
    conf = lakehouse_conf((c.get("with_formats") or "").split(","))
    conf.update(c.get("conf") or {})
    req, uploads = build_request(c["resource"], f"lab-{name}", c.get("class_name"), conf,
                                 lab_runner.fill(c.get("args") or [], run_id),
                                 c.get("py_files"), remote=c.get("remote", False))
    return {"body": req, "uploads": uploads}


def run(name, run_id=None, quiet=True, extra_args=None, conf=None, max_wait=900):
    """Run one catalog job through Kyuubi (or a local helper) and wait.
    Returns the result dict; raises if the job didn't pass."""
    c = lab_runner.case(name)
    reason = lab_runner.skip_reason(c)
    if reason:
        print(f"SKIP {name}: {reason}")
        return dict(name=name, ok=None, state="SKIPPED", detail=reason, log=[])
    run_id = run_id or new_run_id()
    lines = []

    def out(line):
        lines.append(line)
        if not quiet:
            print(line)

    print(f"running {name} ...", end=" ", flush=True)
    r = lab_runner.run_case(c, run_id, out=out, extra_args=extra_args,
                            conf_overrides=conf, max_wait=max_wait, name_prefix="nb-")
    print(f"{'PASS' if r['ok'] else 'FAIL'} {r['state']} in {r['secs']}s  batch={r['batch_id']}")
    if not r["ok"]:
        print("\n".join(r.get("error") or r["log"][-25:]))
        raise RuntimeError(f"{name} failed: {r['detail']}")
    return r


def run_many(names, run_id=None, stop_on_failure=False):
    """Run several jobs in order; returns a summary DataFrame."""
    run_id = run_id or new_run_id()
    rows = []
    for n in names:
        try:
            r = run(n, run_id=run_id)
        except RuntimeError as e:
            r = dict(name=n, ok=False, state="FAILED", detail=str(e), secs=None, batch_id=None)
            if stop_on_failure:
                rows.append(r)
                break
        rows.append({k: r.get(k) for k in ("name", "ok", "state", "secs", "batch_id", "detail")})
    try:
        import pandas as pd
        return pd.DataFrame(rows)
    except ImportError:
        return rows


def log_tail(result, contains=None, n=20):
    """Lines from a run() result, optionally only those containing a string."""
    lines = [l for l in result.get("log", []) if contains is None or contains in l]
    print("\n".join(lines[-n:]))


def batches(n=20):
    return kyuubi().list(size=n).get("batches", [])


def kill(batch_id):
    return kyuubi().kill(batch_id)


def submit_no_wait(name, run_id=None, extra_args=None, conf=None, batch_name=None):
    """Start a catalog job and return its batch id immediately (always-on streams)."""
    c = lab_runner.case(name)
    conf_all = lakehouse_conf((c.get("with_formats") or "").split(","))
    conf_all.update(c.get("conf") or {})
    conf_all.update(conf or {})
    args = list(extra_args) if extra_args is not None else \
        lab_runner.fill(c.get("args") or [], run_id or new_run_id())
    b = kyuubi().submit(c["resource"], batch_name or f"nb-live-{name}", c.get("class_name"),
                        conf_all, args, c.get("py_files"), remote=c.get("remote", False))
    print(f"started {name} as batch {b['id']}")
    return b["id"]


def state(batch_id):
    return kyuubi().get(batch_id).get("state")


# ------------------------------------------------------------ SQL via JDBC
def connect(lake=False, user=None, conf=None):
    """PyHive connection to Kyuubi's Thrift/JDBC port. lake=True -> Delta+Iceberg."""
    from pyhive import hive
    configuration = dict(LAKE_CONF if lake else {})
    configuration.update(conf or {})
    return hive.connect(host=JDBC_HOST, port=JDBC_PORT, auth="NONE",
                        username=user or ("lake" if lake else USER),
                        configuration=configuration or None)


_conns = {}


def sql(query, lake=False, user=None):
    """Run SQL on Kyuubi's Spark SQL engine; returns a DataFrame for queries.
    The first call per user starts an engine (20-40 s); later calls are fast."""
    import pandas as pd
    key = (lake, user)
    if key not in _conns:
        _conns[key] = connect(lake=lake, user=user)
    cur = _conns[key].cursor()
    try:
        cur.execute(query)
        if cur.description:
            cols = [d[0].split(".")[-1] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
        return None
    finally:
        cur.close()


def close_sql():
    for c in _conns.values():
        try:
            c.close()
        except Exception:
            pass
    _conns.clear()


# ---------------------------------------------------------------- Postgres
def pg(query, params=None):
    import pandas as pd
    import psycopg2
    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    return None


# ------------------------------------------------------------ RustFS (S3)
def s3():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name="us-east-1",
                        aws_access_key_id=os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"),
                        aws_secret_access_key=os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin123"),
                        config=Config(s3={"addressing_style": "path"},
                                      request_checksum_calculation="when_required",
                                      response_checksum_validation="when_required"))


def s3_ls(bucket, prefix=""):
    import pandas as pd
    resp = s3().list_objects_v2(Bucket=bucket, Prefix=prefix)
    return pd.DataFrame([{"key": o["Key"], "bytes": o["Size"], "modified": o["LastModified"]}
                         for o in resp.get("Contents", [])])


# ----------------------------------------------------------------- Kafka
def produce(orders=20, run_id=None, sink="kafka", files=3):
    """Send order lifecycles with the lab's producer app; returns the run_id."""
    run_id = run_id or new_run_id("p")
    args = ["--orders", str(orders), "--run-id", run_id]
    if sink == "s3":
        args += ["--sink", "s3", "--files", str(files)]
    r = lab_runner.run_case(dict(name="producer", kind="local",
                                 cmd=["python", "client/producers/order_producer.py"] + args,
                                 marker="PRODUCER OK"), run_id, out=lambda _l: None)
    print(r["log"][-2] if len(r["log"]) > 1 else r["log"])
    if not r["ok"]:
        raise RuntimeError("\n".join(r["log"][-10:]))
    return run_id


def topic(name, last=50, run_id=None):
    """Newest messages of a topic as a DataFrame (JSON values expanded)."""
    import pandas as pd
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=KAFKA, enable_auto_commit=False, consumer_timeout_ms=3000)
    parts = [TopicPartition(name, p) for p in sorted(c.partitions_for_topic(name) or [])]
    c.assign(parts)
    ends, begins = c.end_offsets(parts), c.beginning_offsets(parts)
    for p in parts:
        c.seek(p, max(begins[p], ends[p] - last))
    rows = []
    for m in c:
        try:
            v = json.loads(m.value)
        except Exception:
            v = {"value": m.value.decode(errors="replace")}
        v.update(_partition=m.partition, _offset=m.offset,
                 _key=m.key.decode() if m.key else None)
        rows.append(v)
    c.close()
    df = pd.DataFrame(rows)
    if run_id is not None and "run_id" in df:
        df = df[df["run_id"] == run_id]
    return df


# ---------------------------------------------------------------- health
def services():
    """Which lab services answer from here."""
    import pandas as pd
    checks = {
        "kyuubi REST": REST_URL.split("//")[1],
        "kyuubi JDBC": f"{JDBC_HOST}:{JDBC_PORT}",
        "postgres": f"{PG['host']}:{PG['port']}",
        "rustfs": S3_ENDPOINT.split("//")[1],
        "hive metastore": "hive-metastore:9083",
        "spark master": "spark-master:7077",
        "kafka (optional)": KAFKA,
        "airflow (optional)": AIRFLOW_URL.split("//")[1],
    }
    rows = []
    for name, hp in checks.items():
        host, _, port = hp.partition(":")
        try:
            with socket.create_connection((host, int(port)), timeout=3):
                ok = True
        except OSError:
            ok = False
        rows.append({"service": name, "address": hp, "reachable": ok})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- Airflow
def _airflow():
    """requests session with a token. The lab runs Airflow's simple auth manager
    with all_admins=true, so GET /auth/token hands out an admin token."""
    import requests
    s = requests.Session()
    r = s.get(f"{AIRFLOW_URL}/auth/token", timeout=10)
    r.raise_for_status()
    s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    return s


def airflow_dags():
    """Lab DAGs known to Airflow as a DataFrame."""
    import pandas as pd
    r = _airflow().get(f"{AIRFLOW_URL}/api/v2/dags", params={"limit": 100}, timeout=10)
    r.raise_for_status()
    return pd.DataFrame([{"dag_id": d["dag_id"], "paused": d["is_paused"],
                          "tags": ",".join(t["name"] for t in d.get("tags", []))}
                         for d in r.json()["dags"]])


def airflow_trigger(dag_id, conf=None, wait=False, poll=10, timeout=3600, unpause=True):
    """Start a DAG run (Airflow -> Kyuubi -> Spark). Returns the run id, or the
    final state when wait=True. A paused DAG only queues runs, so it is
    unpaused first (unpause=False to leave it)."""
    s = _airflow()
    if unpause:
        s.patch(f"{AIRFLOW_URL}/api/v2/dags/{dag_id}", json={"is_paused": False},
                timeout=10).raise_for_status()
    body = {"logical_date": dt.datetime.now(dt.timezone.utc).isoformat(), "conf": conf or {}}
    r = s.post(f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns", json=body, timeout=10)
    if r.status_code >= 400:
        raise RuntimeError(f"Airflow said {r.status_code}: {r.text}")
    run_id = r.json()["dag_run_id"]
    print(f"triggered {dag_id}: {run_id}  ->  {AIRFLOW_URL.replace('airflow:8080', 'localhost:8085')}/dags/{dag_id}")
    if not wait:
        return run_id
    import time
    t0 = time.time()
    while time.time() - t0 < timeout:
        state = airflow_run_state(dag_id, run_id, s)
        if state in ("success", "failed"):
            print(f"{dag_id} {run_id}: {state}")
            return state
        time.sleep(poll)
    return "timeout"


def airflow_run_state(dag_id, run_id, session=None):
    s = session or _airflow()
    r = s.get(f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{run_id}", timeout=10)
    r.raise_for_status()
    return r.json()["state"]


def airflow_tasks(dag_id, run_id):
    """Task states of one DAG run as a DataFrame."""
    import pandas as pd
    r = _airflow().get(f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                       params={"limit": 200}, timeout=10)
    r.raise_for_status()
    return pd.DataFrame([{"task": t["task_id"], "state": t["state"], "secs": t.get("duration")}
                         for t in r.json()["task_instances"]])


__all__ = ["GROUPS", "jobs", "payload", "run", "run_many", "log_tail", "batches", "kill",
           "submit_no_wait", "state", "connect", "sql", "close_sql", "pg", "s3", "s3_ls",
           "produce", "topic", "services", "new_run_id", "LAKE_CONF",
           "airflow_dags", "airflow_trigger", "airflow_run_state", "airflow_tasks"]
