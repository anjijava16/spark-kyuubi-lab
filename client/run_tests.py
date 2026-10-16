#!/usr/bin/env python3
"""
run_tests.py - submit every sample job through Kyuubi's batch REST API and
print a pass/fail table at the end.

Runs inside the kyuubi-client container by default:
    docker compose --profile tools run --rm kyuubi-client

Or from your Mac (pip install requests first):
    KYUUBI_REST_URL=http://localhost:10099 python client/run_tests.py

Options:
    --only py-smoke,java-wordcount    run a subset by name
    --show-logs                       stream driver logs while each job runs
    --list                            print the test names and exit
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kyuubi_submit import Kyuubi, lakehouse_conf  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
JOBS = os.path.normpath(os.path.join(HERE, "..", "jobs"))
SPARK_VERSION = os.getenv("SPARK_VERSION", "4.1.3")
JAR = "s3a://jobs/jars/spark-kyuubi-jobs.jar"
LOCAL_JAR = os.path.join(JOBS, "jars", "spark-kyuubi-jobs.jar")

# Each case: name, group, resource, and the final batch state we expect.
# "remote" means Kyuubi's spark-submit reads the file from RustFS (or the
# Kyuubi host). remote=False means this client uploads the file itself.
# "marker" is a line the job prints only when its own checks passed; the
# test fails if the batch FINISHED but the marker is missing.
# "with" adds a table-format profile (see LAKEHOUSE_PROFILES in kyuubi_submit.py).
# "after" lists tests whose output this one reads; it is skipped if they failed.
PY = "s3a://jobs/python"
OT = f"{PY}/order_tracking"
CASES = [
    # ---- core: Kyuubi -> Spark -> storage / metastore ----------------------
    dict(name="jar-sparkpi", group="core",
         note="example JAR that ships with Spark, path on the Kyuubi host",
         resource=f"/opt/spark/examples/jars/spark-examples_2.13-{SPARK_VERSION}.jar",
         class_name="org.apache.spark.examples.SparkPi",
         args=["20"], remote=True, marker="Pi is roughly"),
    dict(name="py-smoke", group="core",
         note="PySpark from RustFS, checks executors respond",
         resource=f"{PY}/01_smoke_test.py", remote=True, marker="SMOKE TEST OK"),
    dict(name="py-upload", group="core",
         note="same script, uploaded by the client instead of read from RustFS",
         resource=os.path.join(JOBS, "python", "01_smoke_test.py"), remote=False,
         marker="SMOKE TEST OK"),
    dict(name="py-wordcount", group="core",
         note="reads and writes RustFS through s3a",
         resource=f"{PY}/02_wordcount_s3.py",
         args=["s3a://data/input/sample.txt", "s3a://data/output/wordcount_py"],
         remote=True, marker="WORDCOUNT OK"),
    dict(name="py-hive-etl", group="core",
         note="CSV on RustFS -> Hive table demo.orders (metastore + warehouse)",
         resource=f"{PY}/03_hive_etl.py",
         args=["s3a://data/input/orders.csv"], remote=True, marker="HIVE ETL OK"),
    dict(name="py-pyfiles", group="core",
         note="--py-files zip from RustFS, imported on executors",
         resource=f"{PY}/04_pyfiles_demo.py",
         py_files=["s3a://jobs/python/libs/text_utils.zip"],
         args=["s3a://data/input/sample.txt"], remote=True, marker="PYFILES OK"),
    dict(name="py-conf-args", group="core",
         note="per-job Spark conf and app args reach the driver",
         resource=f"{PY}/06_conf_and_args.py",
         conf={"spark.executor.memory": "768m", "spark.app.custom.team": "data-eng"},
         args=["--run-date", "2026-10-15", "--env", "lab"], remote=True,
         marker="CONF AND ARGS OK"),
    dict(name="java-wordcount", group="core",
         note="Java class from the built jar",
         resource=JAR, class_name="com.example.JavaWordCount",
         args=["s3a://data/input/sample.txt", "s3a://data/output/wordcount_java"],
         remote=True, needs_jar=True, marker="JAVA WORDCOUNT OK"),
    dict(name="scala-sales-report", group="core",
         note="Scala class reading demo.orders, writing demo.sales_summary",
         resource=JAR, class_name="com.example.ScalaSalesReport",
         remote=True, needs_jar=True, after=["py-hive-etl"], marker="SCALA SALES REPORT OK"),
    dict(name="py-expected-failure", group="core",
         note="job raises on purpose; Kyuubi must report ERROR, not FINISHED",
         resource=f"{PY}/05_expected_failure.py",
         remote=True, expect="ERROR"),

    # ---- file formats ------------------------------------------------------
    dict(name="formats", group="formats",
         note="CSV->Parquet->ORC->JSON->Avro->CSV, round trip must be identical",
         resource=f"{PY}/10_format_conversions.py", remote=True,
         marker="FORMAT CONVERSIONS OK"),

    # ---- Postgres ----------------------------------------------------------
    dict(name="pg-file-to-pg", group="postgres",
         note="Parquet on RustFS -> Postgres orders (JDBC write)",
         resource=f"{PY}/11_file_to_postgres.py", remote=True,
         marker="FILE TO POSTGRES OK"),
    dict(name="pg-pg-to-files", group="postgres",
         note="Postgres (parallel + pushdown reads) -> Parquet/ORC/CSV",
         resource=f"{PY}/12_postgres_to_files.py", remote=True,
         after=["pg-file-to-pg"], marker="POSTGRES TO FILES OK"),
    dict(name="pg-pg-to-pg", group="postgres",
         note="Postgres -> Postgres: full refresh + idempotent upsert",
         resource=f"{PY}/13_postgres_to_postgres.py", remote=True,
         after=["pg-file-to-pg"], marker="POSTGRES TO POSTGRES OK"),

    # ---- lakehouse table formats -------------------------------------------
    dict(name="iceberg", group="lakehouse",
         note="Iceberg in Hive Metastore: MERGE, DELETE, schema+partition evolution, time travel",
         resource=f"{PY}/14_iceberg_demo.py", remote=True, with_formats="iceberg",
         marker="ICEBERG DEMO OK"),
    dict(name="delta", group="lakehouse",
         note="Delta: MERGE, DML, mergeSchema, time travel, change data feed, OPTIMIZE",
         resource=f"{PY}/15_delta_demo.py", remote=True, with_formats="delta",
         marker="DELTA DEMO OK"),
    dict(name="hudi", group="lakehouse",
         note="Hudi COW: insert, upsert, delete, incremental query, time travel",
         resource=f"{PY}/16_hudi_demo.py", remote=True, with_formats="hudi",
         marker="HUDI DEMO OK"),

    # ---- order tracking project (runs in this order) ----------------------
    dict(name="orders-wave1", group="orders",
         note="generate wave 1 of order events (resets earlier runs)",
         resource=f"{OT}/20_generate_order_events.py", remote=True,
         args=["--wave", "1", "--reset"], marker="ORDER EVENTS WAVE OK"),
    dict(name="orders-stream1", group="orders",
         note="stream wave 1 -> Iceberg log, Delta status, Postgres (fresh start)",
         resource=f"{OT}/21_track_orders_stream.py", remote=True,
         with_formats="delta,iceberg", args=["--reset"], after=["orders-wave1"],
         marker="ORDER TRACKING STREAM OK"),
    dict(name="orders-wave2", group="orders", note="generate wave 2",
         resource=f"{OT}/20_generate_order_events.py", remote=True,
         args=["--wave", "2"], after=["orders-stream1"], marker="ORDER EVENTS WAVE OK"),
    dict(name="orders-stream2", group="orders",
         note="stream again: only wave 2 files are new (checkpoint)",
         resource=f"{OT}/21_track_orders_stream.py", remote=True,
         with_formats="delta,iceberg", after=["orders-wave2"],
         marker="ORDER TRACKING STREAM OK"),
    dict(name="orders-wave3", group="orders",
         note="generate wave 3 (with a duplicate and a late event)",
         resource=f"{OT}/20_generate_order_events.py", remote=True,
         args=["--wave", "3"], after=["orders-stream2"], marker="ORDER EVENTS WAVE OK"),
    dict(name="orders-stream3", group="orders", note="stream wave 3",
         resource=f"{OT}/21_track_orders_stream.py", remote=True,
         with_formats="delta,iceberg", after=["orders-wave3"],
         marker="ORDER TRACKING STREAM OK"),
    dict(name="orders-report", group="orders",
         note="SLA report + check Iceberg, Delta and Postgres all agree",
         resource=f"{OT}/22_order_sla_report.py", remote=True,
         with_formats="delta,iceberg", after=["orders-stream3"],
         marker="ORDER SLA REPORT OK"),
]
GROUPS = ["core", "formats", "postgres", "lakehouse", "orders"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("KYUUBI_REST_URL", "http://localhost:10099"))
    ap.add_argument("--only", help="comma-separated test names")
    ap.add_argument("--group", help=f"comma-separated groups: {','.join(GROUPS)} (default: all)")
    ap.add_argument("--show-logs", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--max-wait", type=int, default=900, help="seconds per job")
    a = ap.parse_args()

    if a.list:
        for c in CASES:
            print(f"{c['group']:10} {c['name']:22} {c['note']}")
        return

    cases = CASES
    if a.group:
        groups = {x.strip() for x in a.group.split(",")}
        unknown = groups - set(GROUPS)
        if unknown:
            sys.exit(f"unknown group(s) {sorted(unknown)}; choose from {GROUPS}")
        cases = [c for c in cases if c["group"] in groups]
    if a.only:
        wanted = {x.strip() for x in a.only.split(",")}
        cases = [c for c in cases if c["name"] in wanted]

    k = Kyuubi(a.url, os.getenv("KYUUBI_USER", "anji"), os.getenv("KYUUBI_PASSWORD"))
    print(f"Kyuubi REST: {a.url}   Spark {SPARK_VERSION}   {len(cases)} test(s)\n")

    results = []
    outcome = {}
    for c in cases:
        name = c["name"]
        expect = c.get("expect", "FINISHED")

        if c.get("needs_jar") and not os.path.isfile(LOCAL_JAR):
            results.append((name, "-", "SKIPPED", expect, 0, "jar not built: make jars && make upload"))
            continue
        failed_deps = [d for d in c.get("after", []) if outcome.get(d) is False]
        if failed_deps:
            results.append((name, "-", "SKIPPED", expect, 0, f"depends on {failed_deps[0]}, which failed"))
            outcome[name] = False
            continue

        print(f"==> {name}: {c['note']}")
        t0 = time.time()
        log_lines = []
        conf = lakehouse_conf(c.get("with_formats", "").split(","))
        conf.update(c.get("conf", {}))
        try:
            batch = k.submit(
                c["resource"], f"test-{name}", c.get("class_name"),
                conf, c.get("args", []),
                c.get("py_files"), c.get("files"), c.get("jars"),
                remote=c["remote"])
            bid = batch["id"]
            print(f"    batch id {bid}")
            def out(line):
                log_lines.append(line)
                if a.show_logs:
                    print(line)
            info = k.follow(bid, out=out, max_wait=a.max_wait)
            state = info.get("state", "?")
        except SystemExit as e:  # HTTP error from Kyuubi
            bid, state = "-", "SUBMIT_FAILED"
            log_lines.append(str(e))

        secs = int(time.time() - t0)
        ok = state == expect
        detail = ""
        marker = c.get("marker")
        if ok and expect == "FINISHED" and marker and not any(marker in l for l in log_lines):
            ok, state, detail = False, "NO_MARKER", f"finished but never printed '{marker}'"
        outcome[name] = ok
        if not ok:
            tail = [l for l in log_lines if l.strip()][-15:]
            detail = detail or "see log tail above"
            print("    ---- last driver log lines ----")
            for l in tail:
                print("    " + l)
        print(f"    -> {state} in {secs}s ({'PASS' if ok else 'FAIL'})\n")
        results.append((name, bid, state, expect, secs, detail))

    print("\n" + "=" * 96)
    print(f"{'test':22} {'state':14} {'expected':10} {'secs':>5}  {'result':7} batch id / note")
    print("-" * 96)
    failed = 0
    for name, bid, state, expect, secs, detail in results:
        if state == "SKIPPED":
            res = "SKIP"
        elif state == expect:
            res = "PASS"
        else:
            res = "FAIL"
            failed += 1
        print(f"{name:22} {state:14} {expect:10} {secs:5}  {res:7} {bid} {detail}")
    print("=" * 96)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
