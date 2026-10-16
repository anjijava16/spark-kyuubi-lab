#!/usr/bin/env python3
"""
kyuubi_submit.py - submit Spark jobs through Kyuubi's batch REST API,
with flags that look like spark-submit.

Kyuubi runs the real spark-submit on its side; this script only sends HTTP.

Examples
--------
# PySpark script on your machine (uploaded to Kyuubi)
python kyuubi_submit.py submit jobs/etl.py --name etl \
    --conf spark.executor.memory=2g --py-files jobs/utils.zip -- 2026-10-15

# JAR with main class
python kyuubi_submit.py submit app.jar --class com.acme.Main --name nightly

# File already in RustFS / S3 / HDFS, or on the Kyuubi host (no upload)
python kyuubi_submit.py submit s3a://jobs/python/02_wordcount_s3.py --remote \
    -- s3a://data/input/sample.txt s3a://data/output/wc
python kyuubi_submit.py submit s3a://jobs/jars/spark-kyuubi-jobs.jar --remote \
    --class com.example.JavaWordCount -- s3a://data/input/sample.txt s3a://data/output/wc_java

python kyuubi_submit.py status <batch-id>
python kyuubi_submit.py log <batch-id>
python kyuubi_submit.py kill <batch-id>
python kyuubi_submit.py list

Needs: pip install requests
"""
import argparse
import json
import os
import sys
import time

try:
    import requests
except ImportError:          # lets gen_payloads.py import this file without it
    requests = None

TERMINAL = {"FINISHED", "ERROR", "CANCELED", "CLOSED"}
PYTHON_RUNNER = "org.apache.spark.deploy.PythonRunner"

# Table-format profiles for --with. The jars live on every Spark node at
# /opt/lakehouse/<format>/ (filled by the lakehouse-init service), so a job
# only needs them on its classpath plus the format's SQL extension/catalog.
# Delta and Iceberg combine fine (Iceberg uses its own catalog "ice");
# Hudi wants spark_catalog for itself, so keep Hudi jobs separate.
# "kafka" is only a connector (no catalog), so it combines with anything.
LAKEHOUSE_PROFILES = {
    "kafka": {
        "classpath": "/opt/lakehouse/kafka/*",
        "extensions": "",
        "conf": {},
    },
    "delta": {
        "classpath": "/opt/lakehouse/delta/*",
        "extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "conf": {
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    },
    "iceberg": {
        "classpath": "/opt/lakehouse/iceberg/*",
        "extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "conf": {
            # catalog "ice": Iceberg tables registered in the Hive Metastore
            "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.ice.type": "hive",
            "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg",
        },
    },
    "hudi": {
        "classpath": "/opt/lakehouse/hudi/*",
        "extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "conf": {
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        },
    },
}


def lakehouse_conf(formats):
    """Spark conf for one or more table formats, e.g. ["delta", "iceberg"]."""
    formats = [f.strip().lower() for f in formats if f.strip()]
    unknown = [f for f in formats if f not in LAKEHOUSE_PROFILES]
    if unknown:
        sys.exit(f"--with: unknown format(s) {unknown}; choose from {sorted(LAKEHOUSE_PROFILES)}")
    if "hudi" in formats and ({"delta", "iceberg"} & set(formats)):
        sys.exit("--with: hudi can't be combined with delta/iceberg (both want spark_catalog)")
    if not formats:
        return {}
    cp = ":".join(LAKEHOUSE_PROFILES[f]["classpath"] for f in formats)
    conf = {
        "spark.driver.extraClassPath": cp,
        "spark.executor.extraClassPath": cp,
    }
    extensions = [LAKEHOUSE_PROFILES[f]["extensions"] for f in formats if LAKEHOUSE_PROFILES[f]["extensions"]]
    if extensions:
        conf["spark.sql.extensions"] = ",".join(extensions)
    for f in formats:
        conf.update(LAKEHOUSE_PROFILES[f]["conf"])
    return conf


def build_request(resource, name, class_name=None, conf=None, args=None,
                  py_files=None, files=None, jars=None, remote=False):
    """The exact batch request this client sends to POST /api/v1/batches.

    Returns (request_json, uploads). uploads is [] for remote resources, or
    [(form_field, local_path), ...] for multipart upload mode.
    gen_payloads.py uses this too, so the documented payloads are the real ones.
    """
    # Kyuubi 1.12 rejects SPARK batches without className, Python ones
    # included. For a .py file, spark-submit runs PythonRunner no matter
    # what --class says, so send that and be honest about it.
    if not class_name:
        if resource.lower().endswith(".py"):
            class_name = PYTHON_RUNNER
        else:
            sys.exit("--class is required for JAR jobs (Kyuubi rejects SPARK "
                     "batches without className)")
    req = {
        "batchType": "SPARK",
        "name": name,
        "className": class_name,
        # Kyuubi also requires resource on uploads; it replaces this
        # with the path of the uploaded copy on its side.
        "resource": resource if remote else os.path.basename(resource),
        "conf": dict(conf or {}),
        "args": list(args or []),
    }
    extras = (("spark.submit.pyFiles", py_files), ("spark.files", files), ("spark.jars", jars))
    if remote:
        # paths are resolved by spark-submit on the Kyuubi server
        for key, vals in extras:
            if vals:
                req["conf"][key] = ",".join(vals)
        return req, []
    uploads = [("resourceFile", resource)]
    extra_map = {}
    for key, vals in extras:
        if vals:
            extra_map[key] = ",".join(os.path.basename(v) for v in vals)
            uploads += [(os.path.basename(v), v) for v in vals]
    if extra_map:
        req["extraResourcesMap"] = extra_map
    return req, uploads


class Kyuubi:
    def __init__(self, url, user=None, password=None, timeout=60):
        if requests is None:
            sys.exit("this needs the requests package: pip install requests")
        self.base = url.rstrip("/") + "/api/v1/batches"
        self.s = requests.Session()
        self.timeout = timeout
        if user:
            self.s.auth = (user, password or "")

    def _check(self, r):
        if r.status_code >= 400:
            sys.exit(f"Kyuubi returned {r.status_code}: {r.text}")
        return r.json() if r.text else {}

    def submit(self, resource, name, class_name=None, conf=None, args=None,
               py_files=None, files=None, jars=None, remote=False):
        req, uploads = build_request(resource, name, class_name, conf, args,
                                     py_files, files, jars, remote)
        if remote:
            r = self.s.post(self.base, json=req, timeout=self.timeout)
            return self._check(r)

        # Upload mode: multipart/form-data. Part "batchRequest" is the JSON,
        # part "resourceFile" the main file, and each extra file gets a part
        # named after the file (that's how extraResourcesMap finds it).
        handles = []
        parts = [("batchRequest", (None, json.dumps(req), "application/json"))]
        try:
            for field, path in uploads:
                fh = open(path, "rb")
                handles.append(fh)
                parts.append((field, (os.path.basename(path), fh, "application/octet-stream")))
            r = self.s.post(self.base, files=parts, timeout=self.timeout)
        finally:
            for fh in handles:
                fh.close()
        return self._check(r)

    def get(self, batch_id):
        return self._check(self.s.get(f"{self.base}/{batch_id}",
                                      timeout=self.timeout))

    def log(self, batch_id, start=0, size=500):
        return self._check(self.s.get(f"{self.base}/{batch_id}/localLog",
                                      params={"from": start, "size": size},
                                      timeout=self.timeout))

    def kill(self, batch_id):
        return self._check(self.s.delete(f"{self.base}/{batch_id}",
                                         timeout=self.timeout))

    def list(self, size=100):
        return self._check(self.s.get(self.base,
                                      params={"from": 0, "size": size},
                                      timeout=self.timeout))

    def follow(self, batch_id, interval=3, out=print, max_wait=None):
        """Stream new driver log lines to `out` until the batch reaches a
        final state. Returns the last batch info. If max_wait (seconds)
        passes first, returns the info with state as-is."""
        offset = 0
        started = time.time()

        def drain():
            """Read every log line written since the last call."""
            nonlocal offset
            while True:
                try:
                    chunk = self.log(batch_id, start=offset)
                except SystemExit:
                    return  # log may not exist yet right after submit
                rows = chunk.get("logRowSet", [])
                for line in rows:
                    out(line)
                offset += chunk.get("rowCount", len(rows))
                if not rows:
                    return

        while True:
            drain()
            info = self.get(batch_id)
            if info.get("state") in TERMINAL:
                drain()  # pick up the last lines written before exit
                return info
            if max_wait and time.time() - started > max_wait:
                return info
            time.sleep(interval)


def parse_conf(pairs):
    conf = {}
    for p in pairs or []:
        if "=" not in p:
            sys.exit(f"--conf expects key=value, got: {p}")
        k, v = p.split("=", 1)
        conf[k] = v
    return conf


def split_list(values):
    out = []
    for v in values or []:
        out.extend(x for x in v.split(",") if x)
    return out


def main():
    ap = argparse.ArgumentParser(description="spark-submit through Kyuubi")
    ap.add_argument("--url", default=os.getenv("KYUUBI_REST_URL",
                                               "http://localhost:10099"))
    ap.add_argument("--user", default=os.getenv("KYUUBI_USER"))
    ap.add_argument("--password", default=os.getenv("KYUUBI_PASSWORD"))
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("submit")
    s.add_argument("resource", help=".py or .jar (local path, or remote with --remote)")
    # Job arguments are everything after "--". They're split off before
    # argparse runs (see below), because argparse's handling of "--" with
    # options in between differs across Python versions.
    s.add_argument("--name")
    s.add_argument("--class", dest="class_name")
    s.add_argument("--conf", action="append", help="key=value, repeatable")
    s.add_argument("--py-files", action="append")
    s.add_argument("--files", action="append")
    s.add_argument("--jars", action="append")
    s.add_argument("--remote", action="store_true",
                   help="don't upload; resource path is visible to Kyuubi server")
    s.add_argument("--no-wait", action="store_true",
                   help="return the batch id right away")
    s.add_argument("--with", dest="with_formats", default="",
                   help="connectors the job uses: delta, iceberg, hudi, kafka "
                        "(comma-separated; anything but hudi+delta/iceberg combines)")

    for c in ("status", "log", "kill"):
        p = sub.add_parser(c)
        p.add_argument("batch_id")
    sub.add_parser("list")

    argv = sys.argv[1:]
    app_args = []
    if "--" in argv:
        cut = argv.index("--")
        argv, app_args = argv[:cut], argv[cut + 1:]
    a = ap.parse_args(argv)
    a.app_args = app_args
    k = Kyuubi(a.url, a.user, a.password)

    if a.cmd == "submit":
        if not a.remote and not os.path.isfile(a.resource):
            sys.exit(f"Not a local file: {a.resource} (use --remote for server-side paths)")
        name = a.name or os.path.splitext(os.path.basename(a.resource))[0]
        # format profile first, so an explicit --conf can still override it
        conf = lakehouse_conf(a.with_formats.split(","))
        conf.update(parse_conf(a.conf))
        batch = k.submit(a.resource, name, a.class_name, conf,
                         a.app_args, split_list(a.py_files), split_list(a.files),
                         split_list(a.jars), a.remote)
        bid = batch["id"]
        print(f"Submitted batch {bid} (state={batch.get('state')})", file=sys.stderr)
        if a.no_wait:
            print(bid)
            return
        final = k.follow(bid)
        print(f"\nBatch {bid}: state={final.get('state')} "
              f"appId={final.get('appId')} appState={final.get('appState')}",
              file=sys.stderr)
        sys.exit(0 if final.get("state") == "FINISHED" else 1)

    if a.cmd == "status":
        print(json.dumps(k.get(a.batch_id), indent=2))
    elif a.cmd == "log":
        for line in k.log(a.batch_id).get("logRowSet", []):
            print(line)
    elif a.cmd == "kill":
        print(json.dumps(k.kill(a.batch_id), indent=2))
    elif a.cmd == "list":
        for b in k.list().get("batches", []):
            print(f"{b['id']}  {b.get('state'):10}  {b.get('name')}")


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.ConnectionError:
        sys.exit("Can't reach Kyuubi REST. Is the server up, and is REST enabled "
                 "on that port? (kyuubi.frontend.protocols=THRIFT_BINARY,REST)")
