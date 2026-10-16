#!/usr/bin/env python3
"""
gen_payloads.py - write All_execution_payload.md and All_execution_payload.json
for every job in client/job_catalog.py.

For each job you get the kyuubi_submit.py command, the exact JSON body sent
to POST /api/v1/batches (built by the same build_request() the client
uses), a curl command, and a kyuubi-ctl YAML. Streaming jobs also get an
always-on variant, and local helpers (producers, consumers) get their
commands.

    python3 client/gen_payloads.py          (or: make payloads)

No network, no dependencies beyond the standard library.
"""
import json
import os
import shlex
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
from job_catalog import CASES, GROUPS, MANUAL  # noqa: E402
from kyuubi_submit import build_request, lakehouse_conf  # noqa: E402

URL = "http://localhost:10099"
RUN_ID = "demo1"            # stands in for {run_id}
GROUP_TITLES = {
    "core": "Core: Python, Java and Scala jobs, uploads, Hive tables",
    "formats": "File formats: CSV, Parquet, ORC, JSON, Avro",
    "hive": "Hive ELT: TEXTFILE, ORC, Parquet, RCFile tables",
    "postgres": "Postgres: file to table, table to file, table to table",
    "lakehouse": "Lakehouse: Iceberg, Delta Lake, Hudi",
    "orders": "Project: order tracking (file stream into Iceberg, Delta, Postgres)",
    "streaming": "Streaming: Kafka producer/consumer and file events",
}


def fill(values):
    return [v.replace("{run_id}", RUN_ID) for v in values]


def rel(path):
    """Show files inside the lab folder relative to it; leave everything else
    (s3a://..., server-side paths like /opt/spark/...) exactly as is."""
    if os.path.isabs(path) and path.startswith(ROOT + os.sep):
        return os.path.relpath(path, ROOT)
    return path


def cli(c, args, name):
    parts = ["python", "client/kyuubi_submit.py", "--url", URL, "submit", rel(c["resource"])]
    if c.get("remote"):
        parts.append("--remote")
    if c.get("class_name"):
        parts += ["--class", c["class_name"]]
    if c.get("with_formats"):
        parts += ["--with", c["with_formats"]]
    for p in c.get("py_files") or []:
        parts += ["--py-files", rel(p)]
    for k, v in (c.get("conf") or {}).items():
        parts += ["--conf", f"{k}={v}"]
    parts += ["--name", name]
    if args:
        parts += ["--"] + args
    return " ".join(shlex.quote(p) for p in parts)


def request_for(c, args, name):
    conf = lakehouse_conf((c.get("with_formats") or "").split(","))
    conf.update(c.get("conf") or {})
    req, uploads = build_request(c["resource"], name, c.get("class_name"), conf, args,
                                 c.get("py_files"), c.get("files"), c.get("jars"),
                                 remote=c.get("remote", False))
    return req, [(f, rel(p)) for f, p in uploads]


def curl_json(req):
    body = json.dumps(req, indent=2)
    return (f"curl -s -X POST {URL}/api/v1/batches \\\n"
            f"  -u anji: -H 'Content-Type: application/json' \\\n"
            f"  -d @- <<'JSON'\n{body}\nJSON")


def curl_multipart(req, uploads):
    lines = [f"cat > /tmp/batch-request.json <<'JSON'\n{json.dumps(req, indent=2)}\nJSON",
             f"curl -s -X POST {URL}/api/v1/batches -u anji: \\",
             "  -F 'batchRequest=</tmp/batch-request.json;type=application/json' \\"]
    for i, (field, path) in enumerate(uploads):
        end = " \\" if i < len(uploads) - 1 else ""
        lines.append(f"  -F '{field}=@{path}'{end}")
    return "\n".join(lines)


def kyuubi_ctl(req):
    def yaml_list(xs, indent):
        return "\n".join(f"{' ' * indent}- {json.dumps(x)}" for x in xs) or f"{' ' * indent}[]"
    conf = "\n".join(f"    {k}: {json.dumps(v)}" for k, v in req["conf"].items()) or "    {}"
    return (f"apiVersion: v1\nusername: anji\nrequest:\n"
            f"  batchType: SPARK\n  name: {req['name']}\n  resource: {req['resource']}\n"
            f"  className: {req['className']}\n  args:\n{yaml_list(req['args'], 4)}\n"
            f"  configs:\n{conf}\noptions:\n  verbose: true")


def local_cmds(c):
    cmd = fill(c["cmd"])
    inside = "docker compose --profile tools run --rm kyuubi-client " + " ".join(shlex.quote(x) for x in cmd)
    mac = list(cmd)
    env = []
    if "order_producer.py" in " ".join(cmd) or "topic_tail.py" in " ".join(cmd):
        if "--sink" in cmd and "s3" in cmd:
            env = ["S3_ENDPOINT=http://localhost:9100"]
        else:
            mac += ["--bootstrap", "localhost:9094"]
    from_mac = " ".join(env + [shlex.quote(x) for x in mac])
    return inside, from_mac


def entry(c, manual=False):
    e = {"name": c["name"], "group": c["group"], "kind": c.get("kind", "batch"),
         "description": c["note"], "in_test_suite": not manual}
    if c.get("after"):
        e["run_after"] = c["after"]
    if c.get("needs_kafka"):
        e["needs"] = "Kafka (make kafka-install)"
    if c.get("needs_jar"):
        e["needs"] = "built jar (make jars && make upload)"
    if c.get("marker"):
        e["success_marker"] = c["marker"]
    e["expected_state"] = c.get("expect", "FINISHED")

    if e["kind"] == "local":
        inside, mac = local_cmds(c)
        e["command_in_lab"] = inside
        e["command_from_mac"] = mac
        return e

    args = fill(c.get("args") or [])
    req, uploads = request_for(c, args, c["name"])
    e["connectors"] = c.get("with_formats") or ""
    e["cli"] = cli(c, args, c["name"])
    e["rest"] = {"method": "POST", "url": f"{URL}/api/v1/batches",
                 "content_type": "application/json" if not uploads else "multipart/form-data",
                 "body": req}
    if uploads:
        e["rest"]["multipart_parts"] = [{"field": "batchRequest", "content": "the JSON body above"}] + \
            [{"field": f, "file": p} for f, p in uploads]
        e["curl"] = curl_multipart(req, uploads)
    else:
        e["curl"] = curl_json(req)
        e["kyuubi_ctl_yaml"] = kyuubi_ctl(req)

    if "always_on" in c:
        on_args = fill(c["always_on"])
        on_name = f"live-{c['name']}"
        on_req, _ = request_for(c, on_args, on_name)
        e["always_on"] = {
            "note": "runs until killed; micro-batch every 10 s (--trigger-seconds N to change)",
            "cli": cli(c, on_args, on_name) + " --no-wait",
            "rest_body": on_req,
            "curl": curl_json(on_req),
            "stop": "python client/kyuubi_submit.py kill <batch-id>   "
                    f"(or: curl -X DELETE {URL}/api/v1/batches/<batch-id>)",
        }
    return e


def md_entry(e, n):
    out = [f"### {n}. `{e['name']}`", "", e["description"], ""]
    facts = [f"group `{e['group']}`", "local command" if e["kind"] == "local" else "Kyuubi batch"]
    if e.get("connectors"):
        facts.append(f"`--with {e['connectors']}`")
    if e.get("needs"):
        facts.append(f"needs {e['needs']}")
    if e.get("run_after"):
        facts.append("run after " + ", ".join(f"`{x}`" for x in e["run_after"]))
    if e.get("success_marker"):
        facts.append(f"prints `{e['success_marker']}` on success")
    if e["expected_state"] != "FINISHED":
        facts.append(f"**expected state {e['expected_state']}**")
    if not e["in_test_suite"]:
        facts.append("not in the test suite (runs until stopped)")
    out += ["- " + "; ".join(facts), ""]

    if e["kind"] == "local":
        out += ["In the lab (client container on the Docker network):", "",
                "```bash", e["command_in_lab"], "```", "",
                "From your Mac (`pip install kafka-python boto3` first):", "",
                "```bash", e["command_from_mac"], "```", ""]
        return out

    out += ["**CLI**", "", "```bash", e["cli"], "```", "",
            f"**REST** `POST /api/v1/batches` ({e['rest']['content_type']})", "",
            "```json", json.dumps(e["rest"]["body"], indent=2), "```", ""]
    if e["rest"].get("multipart_parts"):
        out += ["Multipart parts: " + ", ".join(
            f"`{p['field']}`" + (f" = `{p['file']}`" if p.get("file") else " (the JSON above)")
            for p in e["rest"]["multipart_parts"]), ""]
    out += ["**curl**", "", "```bash", e["curl"], "```", ""]
    if e.get("kyuubi_ctl_yaml"):
        out += ["<details><summary>kyuubi-ctl YAML</summary>", "", "```yaml", e["kyuubi_ctl_yaml"],
                "```", "", "</details>", ""]
    if e.get("always_on"):
        a = e["always_on"]
        out += [f"**Always-on variant**: {a['note']}", "", "```bash", a["cli"], "```", "",
                "```json", json.dumps(a["rest_body"], indent=2), "```", "",
                f"Stop it: `{a['stop']}`", ""]
    return out


INTRO = f"""# All execution payloads

Every job in this lab, with the exact request that starts it. Generated from
`client/job_catalog.py` by `client/gen_payloads.py` (`make payloads`). The JSON
bodies come from the same `build_request()` function `kyuubi_submit.py` uses,
so they are byte-for-byte what the client sends. Don't edit this file by
hand; change the catalog and regenerate.

## Conventions

| thing | value |
|---|---|
| Kyuubi REST | `{URL}/api/v1/batches` (inside Docker: `http://kyuubi:10099`) |
| auth | lab runs `kyuubi.authentication=NONE`; `-u anji:` just names the user |
| `className` for Python | `org.apache.spark.deploy.PythonRunner` (Kyuubi 1.12 requires a className; spark-submit uses PythonRunner for `.py` anyway) |
| `--with <x>` | adds connector classpath/extension conf; the JSON shows the result |
| `{{run_id}}` | tags one test run's streaming data; shown here as `{RUN_ID}` |
| `--py-files` | becomes `conf["spark.submit.pyFiles"]` for remote files, or `extraResourcesMap` + a multipart part for uploads |
| `spark.master` | never sent; see Understand.md section 16 |

Run the CLI from the lab folder on your Mac (`pip install requests`), or
inside the lab with no local Python:

```bash
alias ks='docker compose --profile tools run --rm kyuubi-client python client/kyuubi_submit.py --url http://kyuubi:10099'
```

## Managing a batch after you submit it

```bash
curl -s {URL}/api/v1/batches                              # list
curl -s {URL}/api/v1/batches/<id>                         # state, appId, appState
curl -s "{URL}/api/v1/batches/<id>/localLog?from=0&size=200"   # driver log lines
curl -s -X DELETE {URL}/api/v1/batches/<id>               # kill (SIGTERM to the driver)
```

A submit returns something like `{{"id": "6f0c...", "state": "PENDING", "batchType": "SPARK", ...}}`.
States go `PENDING -> RUNNING -> FINISHED | ERROR | CANCELED`.
"""


def main():
    catalog = [entry(c) for c in CASES] + [entry(c, manual=True) for c in MANUAL]
    doc = {"generated_from": "client/job_catalog.py", "kyuubi_rest": f"{URL}/api/v1/batches",
           "run_id_placeholder": RUN_ID, "groups": GROUPS, "jobs": catalog}
    with open(os.path.join(ROOT, "All_execution_payload.json"), "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")

    md = [INTRO, "## Contents", ""]
    for g in GROUPS:
        jobs = [e for e in catalog if e["group"] == g]
        md.append(f"- [{GROUP_TITLES[g]}](#{g}) ({len(jobs)})")
    md.append("")
    n = 0
    for g in GROUPS:
        md += [f'<a id="{g}"></a>', f"## {GROUP_TITLES[g]}", ""]
        for e in [e for e in catalog if e["group"] == g]:
            n += 1
            md += md_entry(e, n)
        md.append("")
    with open(os.path.join(ROOT, "All_execution_payload.md"), "w") as f:
        f.write("\n".join(md))
    print(f"wrote All_execution_payload.md and .json: {len(catalog)} entries "
          f"({sum(1 for e in catalog if e['kind'] == 'batch')} Kyuubi batches, "
          f"{sum(1 for e in catalog if 'always_on' in e)} with always-on variants, "
          f"{sum(1 for e in catalog if e['kind'] == 'local')} local commands)")


if __name__ == "__main__":
    main()
