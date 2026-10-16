#!/usr/bin/env python3
"""
gen_payloads.py - write All_execution_payload.md and All_execution_payload.json
for every job in client/job_catalog.py.

For each job you get the kyuubi_submit.py command, the exact JSON body sent
to POST /api/v1/batches (built by the same build_request() the client
uses), a curl command, and a kyuubi-ctl YAML. Streaming jobs also get an
always-on variant, and local helpers (producers, consumers) get their
commands. After the jobs come the Airflow DAGs (how to trigger each one:
make, Airflow CLI, Airflow REST, Python) and the Jupyter notebooks, both
from client/orchestration.py, checked against airflow/dags/ and notebooks/.

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
from orchestration import DAGS, NOTEBOOKS  # noqa: E402

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


AIRFLOW = "http://localhost:8085"
JUPYTER = "http://localhost:8888"
TOKEN = (f"TOKEN=$(curl -s {AIRFLOW}/auth/token | "
         "python3 -c 'import sys, json; print(json.load(sys.stdin)[\"access_token\"])')")


def check_orchestration():
    """Fail loudly if the registry and the real DAG/notebook files drift apart."""
    names = {c["name"] for c in CASES + MANUAL}
    problems = []
    for d in DAGS:
        path = os.path.join(ROOT, "airflow", "dags", d["file"])
        if not os.path.isfile(path):
            problems.append(f"DAG file missing: {d['file']}")
            continue
        text = open(path).read()
        generated = d["dag_id"].startswith("kyuubi_tests_") and "make_group_dag" in text
        if f'"{d["dag_id"]}"' not in text and not generated:
            problems.append(f"dag_id {d['dag_id']} not found in {d['file']}")
        problems += [f"{d['dag_id']}: unknown job {j}" for j in d["jobs"] if j != "<any>" and j not in names]
    for n in NOTEBOOKS:
        if not os.path.isfile(os.path.join(ROOT, "notebooks", n["file"])):
            problems.append(f"notebook missing: {n['file']}")
        problems += [f"{n['file']}: unknown job {j}" for j in n["jobs"] if j not in names]
    if problems:
        sys.exit("orchestration.py is out of date:\n  " + "\n  ".join(problems))


def dag_entry(d):
    conf = json.dumps(d["conf"]) if d["conf"] else "{}"
    body = {"logical_date": None, "conf": d["conf"] or {}}
    rest = "\n".join([
        TOKEN,
        f"curl -s -X PATCH {AIRFLOW}/api/v2/dags/{d['dag_id']} -H \"Authorization: Bearer $TOKEN\" \\",
        "     -H 'Content-Type: application/json' -d '{\"is_paused\": false}'",
        f"curl -s -X POST {AIRFLOW}/api/v2/dags/{d['dag_id']}/dagRuns -H \"Authorization: Bearer $TOKEN\" \\",
        f"     -H 'Content-Type: application/json' -d '{json.dumps(body)}'",
    ])
    return dict(dag_id=d["dag_id"], file=f"airflow/dags/{d['file']}", what=d["what"], jobs=d["jobs"],
                uses=d["uses"], conf=d["conf"],
                make=f"make airflow-run DAG={d['dag_id']}" + (f" CONF='{conf}'" if d["conf"] else ""),
                airflow_cli=f"docker compose --profile airflow exec airflow airflow dags trigger {d['dag_id']} --conf '{conf}'",
                foreground=f"make airflow-test DAG={d['dag_id']}" + (f" CONF='{conf}'" if d["conf"] else ""),
                rest_body=body, rest_curl=rest,
                python=f"kyuubi_lab.airflow_trigger({d['dag_id']!r}, conf={d['conf'] or {}!r}, wait=True)",
                ui=f"{AIRFLOW}/dags/{d['dag_id']}")


def md_dag(e, n):
    jobs = ", ".join(f"`{j}`" for j in e["jobs"])
    return [f"### D{n}. `{e['dag_id']}`", "", e["what"], "",
            f"* file: `{e['file']}`, UI: {e['ui']}",
            f"* Kyuubi jobs: {jobs}", f"* talks to: {', '.join(e['uses'])}", "",
            "```bash", f"{e['make']}", f"{e['airflow_cli']}", f"{e['foreground']}    # foreground, logs in your terminal",
            "```", "", "Airflow REST (lab runs with no login; GET /auth/token hands out a token):", "",
            "```bash", e["rest_curl"], "```", "", "From a notebook:", "", "```python", e["python"], "```", ""]


def md_notebook(e, n):
    return [f"### N{n}. `{e['file']}`", "", e["what"], "",
            f"* open: {e['open']}",
            f"* Kyuubi jobs: {', '.join(f'`{j}`' for j in e['jobs'])}" if len(e["jobs"]) < 12
            else f"* Kyuubi jobs: all {len(e['jobs'])} catalog jobs",
            f"* talks to: {', '.join(e['uses'])}", ""]


ORCH_INTRO = f"""## Airflow DAGs

Start Airflow with `make airflow` (UI {AIRFLOW}). Every task that runs a job
sends exactly the batch payload shown in the job sections above, with name
`airflow-<job>` and a run id `af<yyyymmddHHMMSS>` shared by the whole DAG run;
SQL tasks go through Kyuubi JDBC (`kyuubi:10009`) as user `airflow` (or
`airflow_lake` with Delta + Iceberg on). Chain: Airflow task -> Kyuubi REST ->
spark-submit -> Spark.
"""
NB_INTRO = f"""## Jupyter notebooks

Start JupyterLab with `make jupyter` ({JUPYTER}). The notebooks use
`client/kyuubi_lab.py`: batches go through Kyuubi REST with name `nb-<job>`
(user `notebook`), SQL through Kyuubi JDBC (user `notebook`, or `lake` with
Delta + Iceberg). Notebook 11 triggers the Airflow DAGs.
"""


def main():
    check_orchestration()
    catalog = [entry(c) for c in CASES] + [entry(c, manual=True) for c in MANUAL]
    dags = [dag_entry(d) for d in DAGS]
    notebooks = [dict(n, open=f"{JUPYTER}/lab/tree/notebooks/{n['file']}") for n in NOTEBOOKS]
    doc = {"generated_from": "client/job_catalog.py + client/orchestration.py",
           "kyuubi_rest": f"{URL}/api/v1/batches", "airflow": AIRFLOW, "jupyter": JUPYTER,
           "run_id_placeholder": RUN_ID, "groups": GROUPS, "jobs": catalog,
           "airflow_dags": dags, "notebooks": notebooks}
    with open(os.path.join(ROOT, "All_execution_payload.json"), "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")

    md = [INTRO, "## Contents", ""]
    for g in GROUPS:
        jobs = [e for e in catalog if e["group"] == g]
        md.append(f"- [{GROUP_TITLES[g]}](#{g}) ({len(jobs)})")
    md += [f"- [Airflow DAGs](#airflow-dags) ({len(dags)})",
           f"- [Jupyter notebooks](#jupyter-notebooks) ({len(notebooks)})", ""]
    n = 0
    for g in GROUPS:
        md += [f'<a id="{g}"></a>', f"## {GROUP_TITLES[g]}", ""]
        for e in [e for e in catalog if e["group"] == g]:
            n += 1
            md += md_entry(e, n)
        md.append("")
    md += [ORCH_INTRO]
    for i, e in enumerate(dags, 1):
        md += md_dag(e, i)
    md += [NB_INTRO]
    for i, e in enumerate(notebooks, 1):
        md += md_notebook(e, i)
    with open(os.path.join(ROOT, "All_execution_payload.md"), "w") as f:
        f.write("\n".join(md))
    print(f"wrote All_execution_payload.md and .json: {len(catalog)} entries "
          f"({sum(1 for e in catalog if e['kind'] == 'batch')} Kyuubi batches, "
          f"{sum(1 for e in catalog if 'always_on' in e)} with always-on variants, "
          f"{sum(1 for e in catalog if e['kind'] == 'local')} local commands), "
          f"{len(dags)} Airflow DAGs, {len(notebooks)} notebooks")


if __name__ == "__main__":
    main()
