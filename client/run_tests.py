#!/usr/bin/env python3
"""
run_tests.py - run every job in job_catalog.py through Kyuubi and print a
pass/fail table at the end.

Runs inside the kyuubi-client container by default:
    docker compose --profile tools run --rm kyuubi-client

Or from your Mac (pip install requests kafka-python boto3 first):
    KYUUBI_REST_URL=http://localhost:10099 python client/run_tests.py

Options:
    --group core,lakehouse          run some groups (see --list)
    --only py-smoke,java-wordcount  run named tests
    --show-logs                     stream driver logs while each job runs
    --list                          print the tests and exit
"""
import argparse
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from job_catalog import CASES, GROUPS, SPARK_VERSION  # noqa: E402
from kyuubi_submit import Kyuubi  # noqa: E402
from lab_runner import kafka_reachable, run_case, skip_reason  # noqa: E402


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
            local = "(local) " if c.get("kind") == "local" else ""
            print(f"{c['group']:10} {c['name']:24} {local}{c['note']}")
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
    print(f"Kyuubi REST: {a.url}   Spark {SPARK_VERSION}   {len(cases)} test(s)   run_id {a.run_id}")
    if any(c.get("needs_kafka") for c in cases):
        print("Kafka: " + ("reachable" if kafka_reachable()
                           else "NOT reachable - Kafka tests will be skipped (make kafka-install)"))
    print()

    results, outcome = [], {}
    for c in cases:
        name, expect = c["name"], c.get("expect", "FINISHED")
        reason = skip_reason(c)
        failed_deps = [d for d in c.get("after", []) if outcome.get(d) is False]
        if not reason and failed_deps:
            reason = f"depends on {failed_deps[0]}, which failed"
        if reason:
            results.append((name, "-", "SKIPPED", expect, 0, reason))
            # a skipped Kafka step or a broken dependency blocks what comes after it
            outcome[name] = False if (c.get("needs_kafka") or failed_deps) else None
            continue

        print(f"==> {name}: {c['note']}")
        r = run_case(c, a.run_id, kyuubi=k, max_wait=a.max_wait, name_prefix="test-",
                     out=print if a.show_logs else (lambda _line: None))
        outcome[name] = r["ok"]
        if not r["ok"]:
            if r.get("error"):
                print("    ---- why it failed (from the driver log) ----")
                for line in r["error"]:
                    print("    " + line)
            print("    ---- last log lines ----")
            for line in [x for x in r["log"] if x.strip()][-8:]:
                print("    " + line)
            if r["batch_id"] not in ("-", "(local)"):
                print(f"    full log: python client/kyuubi_submit.py log {r['batch_id']}")
        print(f"    -> {r['state']} in {r['secs']}s ({'PASS' if r['ok'] else 'FAIL'})\n")
        results.append((name, r["batch_id"], r["state"], expect, r["secs"],
                        "" if r["ok"] else r["detail"]))

    print("\n" + "=" * 100)
    print(f"{'test':24} {'state':14} {'expected':10} {'secs':>5}  {'result':7} batch id / note")
    print("-" * 100)
    failed = 0
    for name, bid, state, expect, secs, detail in results:
        res = "SKIP" if state == "SKIPPED" else ("PASS" if state == expect else "FAIL")
        failed += res == "FAIL"
        print(f"{name:24} {state:14} {expect:10} {secs:5}  {res:7} {bid} {detail}")
    print("=" * 100)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
