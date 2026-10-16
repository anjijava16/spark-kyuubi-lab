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

import requests

TERMINAL = {"FINISHED", "ERROR", "CANCELED", "CLOSED"}
PYTHON_RUNNER = "org.apache.spark.deploy.PythonRunner"


class Kyuubi:
    def __init__(self, url, user=None, password=None, timeout=60):
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
            "conf": conf or {},
            "args": args or [],
        }

        if remote:
            # resource path is resolved on the Kyuubi server side
            for key, vals in (("spark.submit.pyFiles", py_files),
                              ("spark.files", files),
                              ("spark.jars", jars)):
                if vals:
                    req["conf"][key] = ",".join(vals)
            r = self.s.post(self.base, json=req, timeout=self.timeout)
            return self._check(r)

        # Upload mode: main resource goes in "resourceFile"; extra local files
        # go in their own parts and are mapped to Spark conf keys through
        # extraResourcesMap (Kyuubi 1.9+).
        parts = []
        handles = []

        def add(field, path):
            fh = open(path, "rb")
            handles.append(fh)
            parts.append((field, (os.path.basename(path), fh,
                                  "application/octet-stream")))
            return os.path.basename(path)

        add("resourceFile", resource)
        extra = {}
        for key, vals in (("spark.submit.pyFiles", py_files),
                          ("spark.files", files),
                          ("spark.jars", jars)):
            if vals:
                extra[key] = ",".join(add(os.path.basename(v), v) for v in vals)
        if extra:
            req["extraResourcesMap"] = extra

        parts.insert(0, ("batchRequest",
                         (None, json.dumps(req), "application/json")))
        try:
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

    def list(self, size=20):
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
            nonlocal offset
            try:
                chunk = self.log(batch_id, start=offset)
                for line in chunk.get("logRowSet", []):
                    out(line)
                offset += chunk.get("rowCount", 0)
            except SystemExit:
                pass  # log may not exist yet right after submit

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
        batch = k.submit(a.resource, name, a.class_name, parse_conf(a.conf),
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
