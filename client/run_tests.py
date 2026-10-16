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
import datetime as dt
import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kyuubi_submit import Kyuubi, lakehouse_conf  # noqa: E402
from job_catalog import CASES, GROUPS, LOCAL_JAR, SPARK_VERSION  # noqa: E402

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def kafka_reachable():
    host, _, port = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092").partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=3):
            return True
    except OSError:
        return False


def fill(values, run_id):
    return [v.replace("{run_id}", run_id) for v in values]


def run_local(cmd, show):
    """Run a helper (e.g. a producer) in this container; return (state, log lines)."""
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=600)
    lines = (p.stdout + p.stderr).splitlines()
    if show:
        for l in lines:
            print(l)
    return ("FINISHED" if p.returncode == 0 else "ERROR"), lines


def run_local_case(c, a):
    cmd = fill(c["cmd"], a.run_id)
    print(f"    local: {' '.join(cmd)}")
    try:
        state, lines = run_local(cmd, a.show_logs)
    except Exception as e:   # e.g. timeout
        state, lines = "ERROR", [str(e)]
    return "(local)", state, lines


def run_batch_case(k, c, a):
    lines = []

    def out(line):
        lines.append(line)
        if a.show_logs:
            print(line)

    conf = lakehouse_conf(c.get("with_formats", "").split(","))
    conf.update(c.get("conf", {}))
    try:
        batch = k.submit(c["resource"], f"test-{c['name']}", c.get("class_name"), conf,
                         fill(c.get("args", []), a.run_id), c.get("py_files"),
                         c.get("files"), c.get("jars"), remote=c["remote"])
    except SystemExit as e:  # HTTP error from Kyuubi
        return "-", "SUBMIT_FAILED", [str(e)]
    print(f"    batch id {batch['id']}")
    info = k.follow(batch["id"], out=out, max_wait=a.max_wait)
    return batch["id"], info.get("state", "?"), lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("KYUUBI_REST_URL", "http://localhost:10099"))
    ap.add_argument("--only", help="comma-separated test names")
    ap.add_argument("--group", help=f"comma-separated groups: {','.join(GROUPS)} (default: all)")
    ap.add_argument("--show-logs", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--max-wait", type=int, default=900, help="seconds per job")
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("t%m%d%H%M%S"),
                    help="tag for this run's streaming test data")
    a = ap.parse_args()

    if a.list:
        for c in CASES:
            print(f"{c['group']:10} {c['name']:22} {'(local) ' if c.get('kind') == 'local' else ''}{c['note']}")
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
    has_kafka = kafka_reachable() if any(c.get("needs_kafka") for c in cases) else False
    print(f"Kyuubi REST: {a.url}   Spark {SPARK_VERSION}   {len(cases)} test(s)   run_id {a.run_id}")
    if any(c.get("needs_kafka") for c in cases):
        print(f"Kafka: {'reachable' if has_kafka else 'NOT reachable - Kafka tests will be skipped (make kafka-install)'}")
    print()

    results = []
    outcome = {}
    for c in cases:
        name = c["name"]
        expect = c.get("expect", "FINISHED")

        if c.get("needs_jar") and not os.path.isfile(LOCAL_JAR):
            results.append((name, "-", "SKIPPED", expect, 0, "jar not built: make jars && make upload"))
            continue
        if c.get("needs_kafka") and not has_kafka:
            results.append((name, "-", "SKIPPED", expect, 0, "Kafka not installed: make kafka-install"))
            outcome[name] = False
            continue
        failed_deps = [d for d in c.get("after", []) if outcome.get(d) is False]
        if failed_deps:
            results.append((name, "-", "SKIPPED", expect, 0, f"depends on {failed_deps[0]}, which failed"))
            outcome[name] = False
            continue

        print(f"==> {name}: {c['note']}")
        t0 = time.time()
        if c.get("kind") == "local":
            bid, state, log_lines = run_local_case(c, a)
        else:
            bid, state, log_lines = run_batch_case(k, c, a)

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
            print("    ---- last log lines ----")
            for l in tail:
                print("    " + l)
        print(f"    -> {state} in {secs}s ({'PASS' if ok else 'FAIL'})\n")
        results.append((name, bid, state, expect, secs, detail))

    print("\n" + "=" * 96)
    print(f"{'test':24} {'state':14} {'expected':10} {'secs':>5}  {'result':7} batch id / note")
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
        print(f"{name:24} {state:14} {expect:10} {secs:5}  {res:7} {bid} {detail}")
    print("=" * 96)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
