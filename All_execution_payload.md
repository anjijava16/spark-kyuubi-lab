# All execution payloads

Every job in this lab, with the exact request that starts it. Generated from
`client/job_catalog.py` by `client/gen_payloads.py` (`make payloads`). The JSON
bodies come from the same `build_request()` function `kyuubi_submit.py` uses,
so they are byte-for-byte what the client sends. Don't edit this file by
hand; change the catalog and regenerate.

## Conventions

| thing | value |
|---|---|
| Kyuubi REST | `http://localhost:10099/api/v1/batches` (inside Docker: `http://kyuubi:10099`) |
| auth | lab runs `kyuubi.authentication=NONE`; `-u anji:` just names the user |
| `className` for Python | `org.apache.spark.deploy.PythonRunner` (Kyuubi 1.12 requires a className; spark-submit uses PythonRunner for `.py` anyway) |
| `--with <x>` | adds connector classpath/extension conf; the JSON shows the result |
| `{run_id}` | tags one test run's streaming data; shown here as `demo1` |
| `--py-files` | becomes `conf["spark.submit.pyFiles"]` for remote files, or `extraResourcesMap` + a multipart part for uploads |
| `spark.master` | never sent; see Understand.md section 16 |

Run the CLI from the lab folder on your Mac (`pip install requests`), or
inside the lab with no local Python:

```bash
alias ks='docker compose --profile tools run --rm kyuubi-client python client/kyuubi_submit.py --url http://kyuubi:10099'
```

## Managing a batch after you submit it

```bash
curl -s http://localhost:10099/api/v1/batches                              # list
curl -s http://localhost:10099/api/v1/batches/<id>                         # state, appId, appState
curl -s "http://localhost:10099/api/v1/batches/<id>/localLog?from=0&size=200"   # driver log lines
curl -s -X DELETE http://localhost:10099/api/v1/batches/<id>               # kill (SIGTERM to the driver)
```

A submit returns something like `{"id": "6f0c...", "state": "PENDING", "batchType": "SPARK", ...}`.
States go `PENDING -> RUNNING -> FINISHED | ERROR | CANCELED`.

## Contents

- [Core: Python, Java and Scala jobs, uploads, Hive tables](#core) (10)
- [File formats: CSV, Parquet, ORC, JSON, Avro](#formats) (1)
- [Hive ELT: TEXTFILE, ORC, Parquet, RCFile tables](#hive) (1)
- [Postgres: file to table, table to file, table to table](#postgres) (3)
- [Lakehouse: Iceberg, Delta Lake, Hudi](#lakehouse) (3)
- [Project: order tracking (file stream into Iceberg, Delta, Postgres)](#orders) (7)
- [Streaming: Kafka producer/consumer and file events](#streaming) (13)
- [Airflow DAGs](#airflow-dags) (15)
- [Jupyter notebooks](#jupyter-notebooks) (13)

<a id="core"></a>
## Core: Python, Java and Scala jobs, uploads, Hive tables

### 1. `jar-sparkpi`

example JAR that ships with Spark, path on the Kyuubi host

- group `core`; Kyuubi batch; prints `Pi is roughly` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit /opt/spark/examples/jars/spark-examples_2.13-4.1.3.jar --remote --class org.apache.spark.examples.SparkPi --name jar-sparkpi -- 20
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "jar-sparkpi",
  "className": "org.apache.spark.examples.SparkPi",
  "resource": "/opt/spark/examples/jars/spark-examples_2.13-4.1.3.jar",
  "conf": {},
  "args": [
    "20"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "jar-sparkpi",
  "className": "org.apache.spark.examples.SparkPi",
  "resource": "/opt/spark/examples/jars/spark-examples_2.13-4.1.3.jar",
  "conf": {},
  "args": [
    "20"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: jar-sparkpi
  resource: /opt/spark/examples/jars/spark-examples_2.13-4.1.3.jar
  className: org.apache.spark.examples.SparkPi
  args:
    - "20"
  configs:
    {}
options:
  verbose: true
```

</details>

### 2. `py-smoke`

PySpark from RustFS, checks executors respond

- group `core`; Kyuubi batch; prints `SMOKE TEST OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/01_smoke_test.py --remote --name py-smoke
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "py-smoke",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/01_smoke_test.py",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "py-smoke",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/01_smoke_test.py",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: py-smoke
  resource: s3a://jobs/python/01_smoke_test.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>

### 3. `py-upload`

same script, uploaded by the client instead of read from RustFS

- group `core`; Kyuubi batch; prints `SMOKE TEST OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit jobs/python/01_smoke_test.py --name py-upload
```

**REST** `POST /api/v1/batches` (multipart/form-data)

```json
{
  "batchType": "SPARK",
  "name": "py-upload",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "01_smoke_test.py",
  "conf": {},
  "args": []
}
```

Multipart parts: `batchRequest` (the JSON above), `resourceFile` = `jobs/python/01_smoke_test.py`

**curl**

```bash
cat > /tmp/batch-request.json <<'JSON'
{
  "batchType": "SPARK",
  "name": "py-upload",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "01_smoke_test.py",
  "conf": {},
  "args": []
}
JSON
curl -s -X POST http://localhost:10099/api/v1/batches -u anji: \
  -F 'batchRequest=</tmp/batch-request.json;type=application/json' \
  -F 'resourceFile=@jobs/python/01_smoke_test.py'
```

### 4. `py-wordcount`

reads and writes RustFS through s3a

- group `core`; Kyuubi batch; prints `WORDCOUNT OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/02_wordcount_s3.py --remote --name py-wordcount -- s3a://data/input/sample.txt s3a://data/output/wordcount_py
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "py-wordcount",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/02_wordcount_s3.py",
  "conf": {},
  "args": [
    "s3a://data/input/sample.txt",
    "s3a://data/output/wordcount_py"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "py-wordcount",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/02_wordcount_s3.py",
  "conf": {},
  "args": [
    "s3a://data/input/sample.txt",
    "s3a://data/output/wordcount_py"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: py-wordcount
  resource: s3a://jobs/python/02_wordcount_s3.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "s3a://data/input/sample.txt"
    - "s3a://data/output/wordcount_py"
  configs:
    {}
options:
  verbose: true
```

</details>

### 5. `py-hive-etl`

CSV on RustFS -> Hive table demo.orders (metastore + warehouse)

- group `core`; Kyuubi batch; prints `HIVE ETL OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/03_hive_etl.py --remote --name py-hive-etl -- s3a://data/input/orders.csv
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "py-hive-etl",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/03_hive_etl.py",
  "conf": {},
  "args": [
    "s3a://data/input/orders.csv"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "py-hive-etl",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/03_hive_etl.py",
  "conf": {},
  "args": [
    "s3a://data/input/orders.csv"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: py-hive-etl
  resource: s3a://jobs/python/03_hive_etl.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "s3a://data/input/orders.csv"
  configs:
    {}
options:
  verbose: true
```

</details>

### 6. `py-pyfiles`

--py-files zip from RustFS, imported on executors

- group `core`; Kyuubi batch; prints `PYFILES OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/04_pyfiles_demo.py --remote --py-files s3a://jobs/python/libs/text_utils.zip --name py-pyfiles -- s3a://data/input/sample.txt
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "py-pyfiles",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/04_pyfiles_demo.py",
  "conf": {
    "spark.submit.pyFiles": "s3a://jobs/python/libs/text_utils.zip"
  },
  "args": [
    "s3a://data/input/sample.txt"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "py-pyfiles",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/04_pyfiles_demo.py",
  "conf": {
    "spark.submit.pyFiles": "s3a://jobs/python/libs/text_utils.zip"
  },
  "args": [
    "s3a://data/input/sample.txt"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: py-pyfiles
  resource: s3a://jobs/python/04_pyfiles_demo.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "s3a://data/input/sample.txt"
  configs:
    spark.submit.pyFiles: "s3a://jobs/python/libs/text_utils.zip"
options:
  verbose: true
```

</details>

### 7. `py-conf-args`

per-job Spark conf and app args reach the driver

- group `core`; Kyuubi batch; prints `CONF AND ARGS OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/06_conf_and_args.py --remote --conf spark.executor.memory=768m --conf spark.app.custom.team=data-eng --name py-conf-args -- --run-date 2026-10-15 --env lab
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "py-conf-args",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/06_conf_and_args.py",
  "conf": {
    "spark.executor.memory": "768m",
    "spark.app.custom.team": "data-eng"
  },
  "args": [
    "--run-date",
    "2026-10-15",
    "--env",
    "lab"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "py-conf-args",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/06_conf_and_args.py",
  "conf": {
    "spark.executor.memory": "768m",
    "spark.app.custom.team": "data-eng"
  },
  "args": [
    "--run-date",
    "2026-10-15",
    "--env",
    "lab"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: py-conf-args
  resource: s3a://jobs/python/06_conf_and_args.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--run-date"
    - "2026-10-15"
    - "--env"
    - "lab"
  configs:
    spark.executor.memory: "768m"
    spark.app.custom.team: "data-eng"
options:
  verbose: true
```

</details>

### 8. `java-wordcount`

Java class from the built jar

- group `core`; Kyuubi batch; needs built jar (make jars && make upload); prints `JAVA WORDCOUNT OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/jars/spark-kyuubi-jobs.jar --remote --class com.example.JavaWordCount --name java-wordcount -- s3a://data/input/sample.txt s3a://data/output/wordcount_java
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "java-wordcount",
  "className": "com.example.JavaWordCount",
  "resource": "s3a://jobs/jars/spark-kyuubi-jobs.jar",
  "conf": {},
  "args": [
    "s3a://data/input/sample.txt",
    "s3a://data/output/wordcount_java"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "java-wordcount",
  "className": "com.example.JavaWordCount",
  "resource": "s3a://jobs/jars/spark-kyuubi-jobs.jar",
  "conf": {},
  "args": [
    "s3a://data/input/sample.txt",
    "s3a://data/output/wordcount_java"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: java-wordcount
  resource: s3a://jobs/jars/spark-kyuubi-jobs.jar
  className: com.example.JavaWordCount
  args:
    - "s3a://data/input/sample.txt"
    - "s3a://data/output/wordcount_java"
  configs:
    {}
options:
  verbose: true
```

</details>

### 9. `scala-sales-report`

Scala class reading demo.orders, writing demo.sales_summary

- group `core`; Kyuubi batch; needs built jar (make jars && make upload); run after `py-hive-etl`; prints `SCALA SALES REPORT OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/jars/spark-kyuubi-jobs.jar --remote --class com.example.ScalaSalesReport --name scala-sales-report
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "scala-sales-report",
  "className": "com.example.ScalaSalesReport",
  "resource": "s3a://jobs/jars/spark-kyuubi-jobs.jar",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "scala-sales-report",
  "className": "com.example.ScalaSalesReport",
  "resource": "s3a://jobs/jars/spark-kyuubi-jobs.jar",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: scala-sales-report
  resource: s3a://jobs/jars/spark-kyuubi-jobs.jar
  className: com.example.ScalaSalesReport
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>

### 10. `py-expected-failure`

job raises on purpose; Kyuubi must report ERROR, not FINISHED

- group `core`; Kyuubi batch; **expected state ERROR**

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/05_expected_failure.py --remote --name py-expected-failure
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "py-expected-failure",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/05_expected_failure.py",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "py-expected-failure",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/05_expected_failure.py",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: py-expected-failure
  resource: s3a://jobs/python/05_expected_failure.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>


<a id="formats"></a>
## File formats: CSV, Parquet, ORC, JSON, Avro

### 11. `formats`

CSV->Parquet->ORC->JSON->Avro->CSV, round trip must be identical

- group `formats`; Kyuubi batch; prints `FORMAT CONVERSIONS OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/10_format_conversions.py --remote --name formats
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "formats",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/10_format_conversions.py",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "formats",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/10_format_conversions.py",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: formats
  resource: s3a://jobs/python/10_format_conversions.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>


<a id="hive"></a>
## Hive ELT: TEXTFILE, ORC, Parquet, RCFile tables

### 12. `hive-elt`

Hive SQL ELT: external TEXTFILE -> ORC -> partitioned PARQUET -> RCFILE, CTAS, view, ANALYZE

- group `hive`; Kyuubi batch; prints `HIVE ELT OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/17_hive_elt.py --remote --name hive-elt
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "hive-elt",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/17_hive_elt.py",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "hive-elt",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/17_hive_elt.py",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: hive-elt
  resource: s3a://jobs/python/17_hive_elt.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>


<a id="postgres"></a>
## Postgres: file to table, table to file, table to table

### 13. `pg-file-to-pg`

Parquet on RustFS -> Postgres orders (JDBC write)

- group `postgres`; Kyuubi batch; prints `FILE TO POSTGRES OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/11_file_to_postgres.py --remote --name pg-file-to-pg
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "pg-file-to-pg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/11_file_to_postgres.py",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "pg-file-to-pg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/11_file_to_postgres.py",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: pg-file-to-pg
  resource: s3a://jobs/python/11_file_to_postgres.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>

### 14. `pg-pg-to-files`

Postgres (parallel + pushdown reads) -> Parquet/ORC/CSV

- group `postgres`; Kyuubi batch; run after `pg-file-to-pg`; prints `POSTGRES TO FILES OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/12_postgres_to_files.py --remote --name pg-pg-to-files
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "pg-pg-to-files",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/12_postgres_to_files.py",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "pg-pg-to-files",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/12_postgres_to_files.py",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: pg-pg-to-files
  resource: s3a://jobs/python/12_postgres_to_files.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>

### 15. `pg-pg-to-pg`

Postgres -> Postgres: full refresh + idempotent upsert

- group `postgres`; Kyuubi batch; run after `pg-file-to-pg`; prints `POSTGRES TO POSTGRES OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/13_postgres_to_postgres.py --remote --name pg-pg-to-pg
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "pg-pg-to-pg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/13_postgres_to_postgres.py",
  "conf": {},
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "pg-pg-to-pg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/13_postgres_to_postgres.py",
  "conf": {},
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: pg-pg-to-pg
  resource: s3a://jobs/python/13_postgres_to_postgres.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    {}
options:
  verbose: true
```

</details>


<a id="lakehouse"></a>
## Lakehouse: Iceberg, Delta Lake, Hudi

### 16. `iceberg`

Iceberg in Hive Metastore: MERGE, DELETE, schema+partition evolution, time travel

- group `lakehouse`; Kyuubi batch; `--with iceberg`; prints `ICEBERG DEMO OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/14_iceberg_demo.py --remote --with iceberg --name iceberg
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "iceberg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/14_iceberg_demo.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "iceberg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/14_iceberg_demo.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: iceberg
  resource: s3a://jobs/python/14_iceberg_demo.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/iceberg/*"
    spark.executor.extraClassPath: "/opt/lakehouse/iceberg/*"
    spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.ice: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.ice.type: "hive"
    spark.sql.catalog.ice.uri: "thrift://hive-metastore:9083"
    spark.sql.catalog.ice.warehouse: "s3a://warehouse/iceberg"
options:
  verbose: true
```

</details>

### 17. `delta`

Delta: MERGE, DML, mergeSchema, time travel, change data feed, OPTIMIZE

- group `lakehouse`; Kyuubi batch; `--with delta`; prints `DELTA DEMO OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/15_delta_demo.py --remote --with delta --name delta
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "delta",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/15_delta_demo.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  },
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "delta",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/15_delta_demo.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  },
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: delta
  resource: s3a://jobs/python/15_delta_demo.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/delta/*"
    spark.executor.extraClassPath: "/opt/lakehouse/delta/*"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
options:
  verbose: true
```

</details>

### 18. `hudi`

Hudi COW: insert, upsert, delete, incremental query, time travel

- group `lakehouse`; Kyuubi batch; `--with hudi`; prints `HUDI DEMO OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/16_hudi_demo.py --remote --with hudi --name hudi
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "hudi",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/16_hudi_demo.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/hudi/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/hudi/*",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
  },
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "hudi",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/16_hudi_demo.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/hudi/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/hudi/*",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
  },
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: hudi
  resource: s3a://jobs/python/16_hudi_demo.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/hudi/*"
    spark.executor.extraClassPath: "/opt/lakehouse/hudi/*"
    spark.sql.extensions: "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    spark.serializer: "org.apache.spark.serializer.KryoSerializer"
    spark.kryo.registrator: "org.apache.spark.HoodieSparkKryoRegistrar"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
options:
  verbose: true
```

</details>


<a id="orders"></a>
## Project: order tracking (file stream into Iceberg, Delta, Postgres)

### 19. `orders-wave1`

generate wave 1 of order events (resets earlier runs)

- group `orders`; Kyuubi batch; prints `ORDER EVENTS WAVE OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/order_tracking/20_generate_order_events.py --remote --name orders-wave1 -- --wave 1 --reset
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "orders-wave1",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/20_generate_order_events.py",
  "conf": {},
  "args": [
    "--wave",
    "1",
    "--reset"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "orders-wave1",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/20_generate_order_events.py",
  "conf": {},
  "args": [
    "--wave",
    "1",
    "--reset"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: orders-wave1
  resource: s3a://jobs/python/order_tracking/20_generate_order_events.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--wave"
    - "1"
    - "--reset"
  configs:
    {}
options:
  verbose: true
```

</details>

### 20. `orders-stream1`

stream wave 1 -> Iceberg log, Delta status, Postgres (fresh start)

- group `orders`; Kyuubi batch; `--with delta,iceberg`; run after `orders-wave1`; prints `ORDER TRACKING STREAM OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/order_tracking/21_track_orders_stream.py --remote --with delta,iceberg --name orders-stream1 -- --reset
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "orders-stream1",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/21_track_orders_stream.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": [
    "--reset"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "orders-stream1",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/21_track_orders_stream.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": [
    "--reset"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: orders-stream1
  resource: s3a://jobs/python/order_tracking/21_track_orders_stream.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--reset"
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.executor.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.sql.catalog.ice: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.ice.type: "hive"
    spark.sql.catalog.ice.uri: "thrift://hive-metastore:9083"
    spark.sql.catalog.ice.warehouse: "s3a://warehouse/iceberg"
options:
  verbose: true
```

</details>

### 21. `orders-wave2`

generate wave 2

- group `orders`; Kyuubi batch; run after `orders-stream1`; prints `ORDER EVENTS WAVE OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/order_tracking/20_generate_order_events.py --remote --name orders-wave2 -- --wave 2
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "orders-wave2",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/20_generate_order_events.py",
  "conf": {},
  "args": [
    "--wave",
    "2"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "orders-wave2",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/20_generate_order_events.py",
  "conf": {},
  "args": [
    "--wave",
    "2"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: orders-wave2
  resource: s3a://jobs/python/order_tracking/20_generate_order_events.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--wave"
    - "2"
  configs:
    {}
options:
  verbose: true
```

</details>

### 22. `orders-stream2`

stream again: only wave 2 files are new (checkpoint)

- group `orders`; Kyuubi batch; `--with delta,iceberg`; run after `orders-wave2`; prints `ORDER TRACKING STREAM OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/order_tracking/21_track_orders_stream.py --remote --with delta,iceberg --name orders-stream2
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "orders-stream2",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/21_track_orders_stream.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "orders-stream2",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/21_track_orders_stream.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: orders-stream2
  resource: s3a://jobs/python/order_tracking/21_track_orders_stream.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.executor.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.sql.catalog.ice: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.ice.type: "hive"
    spark.sql.catalog.ice.uri: "thrift://hive-metastore:9083"
    spark.sql.catalog.ice.warehouse: "s3a://warehouse/iceberg"
options:
  verbose: true
```

</details>

### 23. `orders-wave3`

generate wave 3 (with a duplicate and a late event)

- group `orders`; Kyuubi batch; run after `orders-stream2`; prints `ORDER EVENTS WAVE OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/order_tracking/20_generate_order_events.py --remote --name orders-wave3 -- --wave 3
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "orders-wave3",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/20_generate_order_events.py",
  "conf": {},
  "args": [
    "--wave",
    "3"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "orders-wave3",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/20_generate_order_events.py",
  "conf": {},
  "args": [
    "--wave",
    "3"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: orders-wave3
  resource: s3a://jobs/python/order_tracking/20_generate_order_events.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--wave"
    - "3"
  configs:
    {}
options:
  verbose: true
```

</details>

### 24. `orders-stream3`

stream wave 3

- group `orders`; Kyuubi batch; `--with delta,iceberg`; run after `orders-wave3`; prints `ORDER TRACKING STREAM OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/order_tracking/21_track_orders_stream.py --remote --with delta,iceberg --name orders-stream3
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "orders-stream3",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/21_track_orders_stream.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "orders-stream3",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/21_track_orders_stream.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: orders-stream3
  resource: s3a://jobs/python/order_tracking/21_track_orders_stream.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.executor.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.sql.catalog.ice: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.ice.type: "hive"
    spark.sql.catalog.ice.uri: "thrift://hive-metastore:9083"
    spark.sql.catalog.ice.warehouse: "s3a://warehouse/iceberg"
options:
  verbose: true
```

</details>

### 25. `orders-report`

SLA report + check Iceberg, Delta and Postgres all agree

- group `orders`; Kyuubi batch; `--with delta,iceberg`; run after `orders-stream3`; prints `ORDER SLA REPORT OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/order_tracking/22_order_sla_report.py --remote --with delta,iceberg --name orders-report
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "orders-report",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/22_order_sla_report.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "orders-report",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/order_tracking/22_order_sla_report.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hive",
    "spark.sql.catalog.ice.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.ice.warehouse": "s3a://warehouse/iceberg"
  },
  "args": []
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: orders-report
  resource: s3a://jobs/python/order_tracking/22_order_sla_report.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    []
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.executor.extraClassPath: "/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.sql.catalog.ice: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.ice.type: "hive"
    spark.sql.catalog.ice.uri: "thrift://hive-metastore:9083"
    spark.sql.catalog.ice.warehouse: "s3a://warehouse/iceberg"
options:
  verbose: true
```

</details>


<a id="streaming"></a>
## Streaming: Kafka producer/consumer and file events

### 26. `kafka-produce`

Python producer app sends 40 order lifecycles to Kafka orders.events

- group `streaming`; local command; needs Kafka (make kafka-install); prints `PRODUCER OK` on success

In the lab (client container on the Docker network):

```bash
docker compose --profile tools run --rm kyuubi-client python client/producers/order_producer.py --orders 40 --run-id demo1
```

From your Mac (`pip install kafka-python boto3` first):

```bash
python client/producers/order_producer.py --orders 40 --run-id demo1 --bootstrap localhost:9094
```

### 27. `kafka-consumer-basic`

Kafka -> Spark consumer -> Parquet bronze (offsets + per-batch summary)

- group `streaming`; Kyuubi batch; `--with kafka`; needs Kafka (make kafka-install); run after `kafka-produce`; prints `KAFKA CONSUMER BASIC OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/30_kafka_consumer_basic.py --remote --with kafka --py-files s3a://jobs/python/streaming/stream_common.py --name kafka-consumer-basic -- --available-now
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "kafka-consumer-basic",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/30_kafka_consumer_basic.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "kafka-consumer-basic",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/30_kafka_consumer_basic.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: kafka-consumer-basic
  resource: s3a://jobs/python/streaming/30_kafka_consumer_basic.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--available-now"
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/kafka/*"
    spark.executor.extraClassPath: "/opt/lakehouse/kafka/*"
    spark.submit.pyFiles: "s3a://jobs/python/streaming/stream_common.py"
options:
  verbose: true
```

</details>

**Always-on variant**: runs until killed; micro-batch every 10 s (--trigger-seconds N to change)

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/30_kafka_consumer_basic.py --remote --with kafka --py-files s3a://jobs/python/streaming/stream_common.py --name live-kafka-consumer-basic --no-wait
```

```json
{
  "batchType": "SPARK",
  "name": "live-kafka-consumer-basic",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/30_kafka_consumer_basic.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": []
}
```

Stop it: `python client/kyuubi_submit.py kill <batch-id>   (or: curl -X DELETE http://localhost:10099/api/v1/batches/<batch-id>)`

### 28. `kafka-to-delta-pg`

Kafka -> Delta event table (exactly once) + Postgres order status (upsert)

- group `streaming`; Kyuubi batch; `--with kafka,delta`; needs Kafka (make kafka-install); run after `kafka-produce`; prints `KAFKA TO DELTA POSTGRES OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/31_kafka_to_delta_postgres.py --remote --with kafka,delta --py-files s3a://jobs/python/streaming/stream_common.py --name kafka-to-delta-pg -- --available-now
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "kafka-to-delta-pg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/31_kafka_to_delta_postgres.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "kafka-to-delta-pg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/31_kafka_to_delta_postgres.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: kafka-to-delta-pg
  resource: s3a://jobs/python/streaming/31_kafka_to_delta_postgres.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--available-now"
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*"
    spark.executor.extraClassPath: "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.submit.pyFiles: "s3a://jobs/python/streaming/stream_common.py"
options:
  verbose: true
```

</details>

**Always-on variant**: runs until killed; micro-batch every 10 s (--trigger-seconds N to change)

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/31_kafka_to_delta_postgres.py --remote --with kafka,delta --py-files s3a://jobs/python/streaming/stream_common.py --name live-kafka-to-delta-pg --no-wait
```

```json
{
  "batchType": "SPARK",
  "name": "live-kafka-to-delta-pg",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/31_kafka_to_delta_postgres.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": []
}
```

Stop it: `python client/kyuubi_submit.py kill <batch-id>   (or: curl -X DELETE http://localhost:10099/api/v1/batches/<batch-id>)`

### 29. `kafka-enrich-publish`

Kafka -> join customers (Postgres) -> Spark publishes orders.enriched + orders.alerts

- group `streaming`; Kyuubi batch; `--with kafka`; needs Kafka (make kafka-install); run after `kafka-produce`; prints `KAFKA ENRICH PUBLISH OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/32_kafka_enrich_publish.py --remote --with kafka --py-files s3a://jobs/python/streaming/stream_common.py --name kafka-enrich-publish -- --available-now
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "kafka-enrich-publish",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/32_kafka_enrich_publish.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "kafka-enrich-publish",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/32_kafka_enrich_publish.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: kafka-enrich-publish
  resource: s3a://jobs/python/streaming/32_kafka_enrich_publish.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--available-now"
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/kafka/*"
    spark.executor.extraClassPath: "/opt/lakehouse/kafka/*"
    spark.submit.pyFiles: "s3a://jobs/python/streaming/stream_common.py"
options:
  verbose: true
```

</details>

**Always-on variant**: runs until killed; micro-batch every 10 s (--trigger-seconds N to change)

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/32_kafka_enrich_publish.py --remote --with kafka --py-files s3a://jobs/python/streaming/stream_common.py --name live-kafka-enrich-publish --no-wait
```

```json
{
  "batchType": "SPARK",
  "name": "live-kafka-enrich-publish",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/32_kafka_enrich_publish.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": []
}
```

Stop it: `python client/kyuubi_submit.py kill <batch-id>   (or: curl -X DELETE http://localhost:10099/api/v1/batches/<batch-id>)`

### 30. `kafka-windowed-metrics`

event-time windows + watermark -> Spark publishes orders.metrics + Postgres upsert

- group `streaming`; Kyuubi batch; `--with kafka`; needs Kafka (make kafka-install); run after `kafka-produce`; prints `KAFKA WINDOWED METRICS OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/33_kafka_windowed_metrics.py --remote --with kafka --py-files s3a://jobs/python/streaming/stream_common.py --name kafka-windowed-metrics -- --available-now
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "kafka-windowed-metrics",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/33_kafka_windowed_metrics.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "kafka-windowed-metrics",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/33_kafka_windowed_metrics.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: kafka-windowed-metrics
  resource: s3a://jobs/python/streaming/33_kafka_windowed_metrics.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--available-now"
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/kafka/*"
    spark.executor.extraClassPath: "/opt/lakehouse/kafka/*"
    spark.submit.pyFiles: "s3a://jobs/python/streaming/stream_common.py"
options:
  verbose: true
```

</details>

**Always-on variant**: runs until killed; micro-batch every 10 s (--trigger-seconds N to change)

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/33_kafka_windowed_metrics.py --remote --with kafka --py-files s3a://jobs/python/streaming/stream_common.py --name live-kafka-windowed-metrics --no-wait
```

```json
{
  "batchType": "SPARK",
  "name": "live-kafka-windowed-metrics",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/33_kafka_windowed_metrics.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": []
}
```

Stop it: `python client/kyuubi_submit.py kill <batch-id>   (or: curl -X DELETE http://localhost:10099/api/v1/batches/<batch-id>)`

### 31. `kafka-batch-publish`

Spark BATCH producer: Hive demo.orders -> orders.backfill, then batch read-back

- group `streaming`; Kyuubi batch; `--with kafka`; needs Kafka (make kafka-install); prints `BATCH PUBLISH TO KAFKA OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/34_batch_publish_to_kafka.py --remote --with kafka --name kafka-batch-publish -- --run-id demo1b
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "kafka-batch-publish",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/34_batch_publish_to_kafka.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*"
  },
  "args": [
    "--run-id",
    "demo1b"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "kafka-batch-publish",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/34_batch_publish_to_kafka.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*"
  },
  "args": [
    "--run-id",
    "demo1b"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: kafka-batch-publish
  resource: s3a://jobs/python/streaming/34_batch_publish_to_kafka.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--run-id"
    - "demo1b"
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/kafka/*"
    spark.executor.extraClassPath: "/opt/lakehouse/kafka/*"
options:
  verbose: true
```

</details>

### 32. `file-drop-produce`

producer drops 3 JSON files of order events into s3a://data/stream/landing/

- group `streaming`; local command; prints `PRODUCER OK` on success

In the lab (client container on the Docker network):

```bash
docker compose --profile tools run --rm kyuubi-client python client/producers/order_producer.py --sink s3 --orders 20 --files 3 --run-id demo1f
```

From your Mac (`pip install kafka-python boto3` first):

```bash
S3_ENDPOINT=http://localhost:9100 python client/producers/order_producer.py --sink s3 --orders 20 --files 3 --run-id demo1f
```

### 33. `file-drop-stream`

file-event stream: new files in landing/ -> Parquet silver, processed files archived

- group `streaming`; Kyuubi batch; run after `file-drop-produce`; prints `FILE DROP STREAM OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/35_file_drop_stream.py --remote --py-files s3a://jobs/python/streaming/stream_common.py --name file-drop-stream -- --available-now
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "file-drop-stream",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/35_file_drop_stream.py",
  "conf": {
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "file-drop-stream",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/35_file_drop_stream.py",
  "conf": {
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--available-now"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: file-drop-stream
  resource: s3a://jobs/python/streaming/35_file_drop_stream.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--available-now"
  configs:
    spark.submit.pyFiles: "s3a://jobs/python/streaming/stream_common.py"
options:
  verbose: true
```

</details>

**Always-on variant**: runs until killed; micro-batch every 10 s (--trigger-seconds N to change)

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/35_file_drop_stream.py --remote --py-files s3a://jobs/python/streaming/stream_common.py --name live-file-drop-stream --no-wait
```

```json
{
  "batchType": "SPARK",
  "name": "live-file-drop-stream",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/35_file_drop_stream.py",
  "conf": {
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": []
}
```

Stop it: `python client/kyuubi_submit.py kill <batch-id>   (or: curl -X DELETE http://localhost:10099/api/v1/batches/<batch-id>)`

### 34. `verify-streaming`

batch check: every output of jobs 30-35 holds exactly this run's events

- group `streaming`; Kyuubi batch; `--with kafka,delta`; needs Kafka (make kafka-install); run after `kafka-consumer-basic`, `kafka-to-delta-pg`, `kafka-enrich-publish`, `kafka-windowed-metrics`, `file-drop-stream`; prints `VERIFY STREAMING OK` on success

**CLI**

```bash
python client/kyuubi_submit.py --url http://localhost:10099 submit s3a://jobs/python/streaming/39_verify_streaming.py --remote --with kafka,delta --py-files s3a://jobs/python/streaming/stream_common.py --name verify-streaming -- --run-id demo1 --file-run-id demo1f
```

**REST** `POST /api/v1/batches` (application/json)

```json
{
  "batchType": "SPARK",
  "name": "verify-streaming",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/39_verify_streaming.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--run-id",
    "demo1",
    "--file-run-id",
    "demo1f"
  ]
}
```

**curl**

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -u anji: -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{
  "batchType": "SPARK",
  "name": "verify-streaming",
  "className": "org.apache.spark.deploy.PythonRunner",
  "resource": "s3a://jobs/python/streaming/39_verify_streaming.py",
  "conf": {
    "spark.driver.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.executor.extraClassPath": "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.submit.pyFiles": "s3a://jobs/python/streaming/stream_common.py"
  },
  "args": [
    "--run-id",
    "demo1",
    "--file-run-id",
    "demo1f"
  ]
}
JSON
```

<details><summary>kyuubi-ctl YAML</summary>

```yaml
apiVersion: v1
username: anji
request:
  batchType: SPARK
  name: verify-streaming
  resource: s3a://jobs/python/streaming/39_verify_streaming.py
  className: org.apache.spark.deploy.PythonRunner
  args:
    - "--run-id"
    - "demo1"
    - "--file-run-id"
    - "demo1f"
  configs:
    spark.driver.extraClassPath: "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*"
    spark.executor.extraClassPath: "/opt/lakehouse/kafka/*:/opt/lakehouse/delta/*"
    spark.sql.extensions: "io.delta.sql.DeltaSparkSessionExtension"
    spark.sql.catalog.spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark.submit.pyFiles: "s3a://jobs/python/streaming/stream_common.py"
options:
  verbose: true
```

</details>

### 35. `live-producer`

always-on producer: ~5 events/s into orders.events until Ctrl-C

- group `streaming`; local command; needs Kafka (make kafka-install); not in the test suite (runs until stopped)

In the lab (client container on the Docker network):

```bash
docker compose --profile tools run --rm kyuubi-client python client/producers/order_producer.py --forever --rate 5
```

From your Mac (`pip install kafka-python boto3` first):

```bash
python client/producers/order_producer.py --forever --rate 5 --bootstrap localhost:9094
```

### 36. `live-file-dropper`

always-on producer: a JSON file into data/stream/landing/ every ~5 s

- group `streaming`; local command; not in the test suite (runs until stopped)

In the lab (client container on the Docker network):

```bash
docker compose --profile tools run --rm kyuubi-client python client/producers/order_producer.py --sink s3 --forever --rate 5
```

From your Mac (`pip install kafka-python boto3` first):

```bash
S3_ENDPOINT=http://localhost:9100 python client/producers/order_producer.py --sink s3 --forever --rate 5
```

### 37. `tail-enriched`

Python consumer: print the newest messages Spark published to orders.enriched

- group `streaming`; local command; needs Kafka (make kafka-install); not in the test suite (runs until stopped)

In the lab (client container on the Docker network):

```bash
docker compose --profile tools run --rm kyuubi-client python client/producers/topic_tail.py orders.enriched --last 5
```

From your Mac (`pip install kafka-python boto3` first):

```bash
python client/producers/topic_tail.py orders.enriched --last 5 --bootstrap localhost:9094
```

### 38. `tail-metrics-follow`

Python consumer: follow orders.metrics live while job 33 runs always-on

- group `streaming`; local command; needs Kafka (make kafka-install); not in the test suite (runs until stopped)

In the lab (client container on the Docker network):

```bash
docker compose --profile tools run --rm kyuubi-client python client/producers/topic_tail.py orders.metrics --follow
```

From your Mac (`pip install kafka-python boto3` first):

```bash
python client/producers/topic_tail.py orders.metrics --follow --bootstrap localhost:9094
```


## Airflow DAGs

Start Airflow with `make airflow` (UI http://localhost:8085). Every task that runs a job
sends exactly the batch payload shown in the job sections above, with name
`airflow-<job>` and a run id `af<yyyymmddHHMMSS>` shared by the whole DAG run;
SQL tasks go through Kyuubi JDBC (`kyuubi:10009`) as user `airflow` (or
`airflow_lake` with Delta + Iceberg on). Chain: Airflow task -> Kyuubi REST ->
spark-submit -> Spark.

### D1. `kyuubi_tests_all`

Every catalog job, group by group (core, formats, hive, postgres, lakehouse, orders, streaming), then a pass/fail report. The Airflow version of `make test`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_all
* Kyuubi jobs: `jar-sparkpi`, `py-smoke`, `py-upload`, `py-wordcount`, `py-hive-etl`, `py-pyfiles`, `py-conf-args`, `java-wordcount`, `scala-sales-report`, `py-expected-failure`, `formats`, `hive-elt`, `pg-file-to-pg`, `pg-pg-to-files`, `pg-pg-to-pg`, `iceberg`, `delta`, `hudi`, `orders-wave1`, `orders-stream1`, `orders-wave2`, `orders-stream2`, `orders-wave3`, `orders-stream3`, `orders-report`, `kafka-produce`, `kafka-consumer-basic`, `kafka-to-delta-pg`, `kafka-enrich-publish`, `kafka-windowed-metrics`, `kafka-batch-publish`, `file-drop-produce`, `file-drop-stream`, `verify-streaming`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_tests_all
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_all --conf '{}'
make airflow-test DAG=kyuubi_tests_all    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_all -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_all/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_all', conf={}, wait=True)
```

### D2. `kyuubi_tests_core`

The `core` test group only, with the same dependencies as `make test-core`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_core
* Kyuubi jobs: `jar-sparkpi`, `py-smoke`, `py-upload`, `py-wordcount`, `py-hive-etl`, `py-pyfiles`, `py-conf-args`, `java-wordcount`, `scala-sales-report`, `py-expected-failure`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_tests_core
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_core --conf '{}'
make airflow-test DAG=kyuubi_tests_core    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_core -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_core/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_core', conf={}, wait=True)
```

### D3. `kyuubi_tests_formats`

The `formats` test group only, with the same dependencies as `make test-formats`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_formats
* Kyuubi jobs: `formats`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_tests_formats
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_formats --conf '{}'
make airflow-test DAG=kyuubi_tests_formats    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_formats -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_formats/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_formats', conf={}, wait=True)
```

### D4. `kyuubi_tests_hive`

The `hive` test group only, with the same dependencies as `make test-hive`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_hive
* Kyuubi jobs: `hive-elt`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_tests_hive
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_hive --conf '{}'
make airflow-test DAG=kyuubi_tests_hive    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_hive -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_hive/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_hive', conf={}, wait=True)
```

### D5. `kyuubi_tests_postgres`

The `postgres` test group only, with the same dependencies as `make test-postgres`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_postgres
* Kyuubi jobs: `pg-file-to-pg`, `pg-pg-to-files`, `pg-pg-to-pg`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_tests_postgres
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_postgres --conf '{}'
make airflow-test DAG=kyuubi_tests_postgres    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_postgres -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_postgres/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_postgres', conf={}, wait=True)
```

### D6. `kyuubi_tests_lakehouse`

The `lakehouse` test group only, with the same dependencies as `make test-lakehouse`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_lakehouse
* Kyuubi jobs: `iceberg`, `delta`, `hudi`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_tests_lakehouse
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_lakehouse --conf '{}'
make airflow-test DAG=kyuubi_tests_lakehouse    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_lakehouse -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_lakehouse/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_lakehouse', conf={}, wait=True)
```

### D7. `kyuubi_tests_orders`

The `orders` test group only, with the same dependencies as `make test-orders`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_orders
* Kyuubi jobs: `orders-wave1`, `orders-stream1`, `orders-wave2`, `orders-stream2`, `orders-wave3`, `orders-stream3`, `orders-report`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_tests_orders
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_orders --conf '{}'
make airflow-test DAG=kyuubi_tests_orders    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_orders -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_orders/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_orders', conf={}, wait=True)
```

### D8. `kyuubi_tests_streaming`

The `streaming` test group only, with the same dependencies as `make test-streaming`.

* file: `airflow/dags/kyuubi_test_suites.py`, UI: http://localhost:8085/dags/kyuubi_tests_streaming
* Kyuubi jobs: `kafka-produce`, `kafka-consumer-basic`, `kafka-to-delta-pg`, `kafka-enrich-publish`, `kafka-windowed-metrics`, `kafka-batch-publish`, `file-drop-produce`, `file-drop-stream`, `verify-streaming`
* talks to: kyuubi-rest, kafka

```bash
make airflow-run DAG=kyuubi_tests_streaming
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_tests_streaming --conf '{}'
make airflow-test DAG=kyuubi_tests_streaming    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_tests_streaming -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_tests_streaming/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_tests_streaming', conf={}, wait=True)
```

### D9. `kyuubi_run_any_job`

Pick any catalog job in the trigger form; optional args (replace), extra_args (append) and Spark conf.

* file: `airflow/dags/kyuubi_run_any_job.py`, UI: http://localhost:8085/dags/kyuubi_run_any_job
* Kyuubi jobs: `<any>`
* talks to: kyuubi-rest

```bash
make airflow-run DAG=kyuubi_run_any_job CONF='{"job": "iceberg", "args": null, "extra_args": [], "conf": {"spark.executor.memory": "1g"}}'
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_run_any_job --conf '{"job": "iceberg", "args": null, "extra_args": [], "conf": {"spark.executor.memory": "1g"}}'
make airflow-test DAG=kyuubi_run_any_job CONF='{"job": "iceberg", "args": null, "extra_args": [], "conf": {"spark.executor.memory": "1g"}}'    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_run_any_job -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_run_any_job/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {"job": "iceberg", "args": null, "extra_args": [], "conf": {"spark.executor.memory": "1g"}}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_run_any_job', conf={'job': 'iceberg', 'args': None, 'extra_args': [], 'conf': {'spark.executor.memory': '1g'}}, wait=True)
```

### D10. `lakehouse_daily_elt`

Daily pipeline (02:00, created paused): S3 sensor -> Hive ELT (TEXTFILE/ORC/Parquet/RCFILE) -> formats -> Postgres file/table jobs -> Iceberg, Delta, Hudi -> SQL quality checks over Kyuubi JDBC -> top region per month.

* file: `airflow/dags/lakehouse_daily_elt.py`, UI: http://localhost:8085/dags/lakehouse_daily_elt
* Kyuubi jobs: `hive-elt`, `formats`, `pg-file-to-pg`, `pg-pg-to-files`, `pg-pg-to-pg`, `iceberg`, `delta`, `hudi`
* talks to: rustfs, kyuubi-rest, kyuubi-jdbc

```bash
make airflow-run DAG=lakehouse_daily_elt
docker compose --profile airflow exec airflow airflow dags trigger lakehouse_daily_elt --conf '{}'
make airflow-test DAG=lakehouse_daily_elt    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/lakehouse_daily_elt -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/lakehouse_daily_elt/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('lakehouse_daily_elt', conf={}, wait=True)
```

### D11. `kafka_streaming_pipeline`

Producer -> Kafka -> four Structured Streaming jobs (--available-now) -> Kafka/Delta/Postgres, Spark batch backfill to Kafka, file-event stream, verify job, then Python checks of the topics and Postgres. Skips itself when Kafka isn't installed.

* file: `airflow/dags/kafka_streaming_pipeline.py`, UI: http://localhost:8085/dags/kafka_streaming_pipeline
* Kyuubi jobs: `kafka-produce`, `kafka-consumer-basic`, `kafka-to-delta-pg`, `kafka-enrich-publish`, `kafka-windowed-metrics`, `kafka-batch-publish`, `file-drop-produce`, `file-drop-stream`, `verify-streaming`
* talks to: kafka, kyuubi-rest, postgres, rustfs

```bash
make airflow-run DAG=kafka_streaming_pipeline CONF='{"orders": 40}'
docker compose --profile airflow exec airflow airflow dags trigger kafka_streaming_pipeline --conf '{"orders": 40}'
make airflow-test DAG=kafka_streaming_pipeline CONF='{"orders": 40}'    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kafka_streaming_pipeline -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kafka_streaming_pipeline/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {"orders": 40}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kafka_streaming_pipeline', conf={'orders': 40}, wait=True)
```

### D12. `kafka_live_streams`

Always-on streams: starts jobs 31/32/33 without --available-now (1 core each), waits until RUNNING, feeds them with the producer for N minutes, checks Postgres and Kafka, then kills the three batches (always, even after a failure).

* file: `airflow/dags/kafka_live_streams.py`, UI: http://localhost:8085/dags/kafka_live_streams
* Kyuubi jobs: `kafka-to-delta-pg`, `kafka-enrich-publish`, `kafka-windowed-metrics`
* talks to: kafka, kyuubi-rest, postgres

```bash
make airflow-run DAG=kafka_live_streams CONF='{"minutes": 2, "rate": 5}'
docker compose --profile airflow exec airflow airflow dags trigger kafka_live_streams --conf '{"minutes": 2, "rate": 5}'
make airflow-test DAG=kafka_live_streams CONF='{"minutes": 2, "rate": 5}'    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kafka_live_streams -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kafka_live_streams/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {"minutes": 2, "rate": 5}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kafka_live_streams', conf={'minutes': 2, 'rate': 5}, wait=True)
```

### D13. `kyuubi_sql_reporting`

SQL only, over Kyuubi JDBC: branch on whether demo.orders exists (build it with a batch if not), CTAS into Parquet/ORC + a view, quality checks, copy into Iceberg and Delta, upsert the report into Postgres shop.report_region_month.

* file: `airflow/dags/kyuubi_sql_reporting.py`, UI: http://localhost:8085/dags/kyuubi_sql_reporting
* Kyuubi jobs: `py-hive-etl`
* talks to: kyuubi-jdbc, kyuubi-rest, postgres

```bash
make airflow-run DAG=kyuubi_sql_reporting
docker compose --profile airflow exec airflow airflow dags trigger kyuubi_sql_reporting --conf '{}'
make airflow-test DAG=kyuubi_sql_reporting    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/kyuubi_sql_reporting -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_sql_reporting/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('kyuubi_sql_reporting', conf={}, wait=True)
```

### D14. `file_event_pipeline`

JSON files dropped into s3a://data/stream/landing -> sensor -> file-source stream -> Parquet silver -> SQL on the folder -> second run processes nothing (checkpoint).

* file: `airflow/dags/file_event_pipeline.py`, UI: http://localhost:8085/dags/file_event_pipeline
* Kyuubi jobs: `file-drop-produce`, `file-drop-stream`
* talks to: rustfs, kyuubi-rest, kyuubi-jdbc

```bash
make airflow-run DAG=file_event_pipeline CONF='{"orders": 20, "files": 3}'
docker compose --profile airflow exec airflow airflow dags trigger file_event_pipeline --conf '{"orders": 20, "files": 3}'
make airflow-test DAG=file_event_pipeline CONF='{"orders": 20, "files": 3}'    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/file_event_pipeline -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/file_event_pipeline/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {"orders": 20, "files": 3}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('file_event_pipeline', conf={'orders': 20, 'files': 3}, wait=True)
```

### D15. `order_tracking_project`

Order tracking project: three waves, each streamed into Iceberg + Delta + Postgres, SLA report, then SQL on the Delta status table and the Iceberg event log.

* file: `airflow/dags/order_tracking_project.py`, UI: http://localhost:8085/dags/order_tracking_project
* Kyuubi jobs: `orders-wave1`, `orders-stream1`, `orders-wave2`, `orders-stream2`, `orders-wave3`, `orders-stream3`, `orders-report`
* talks to: kyuubi-rest, kyuubi-jdbc, postgres

```bash
make airflow-run DAG=order_tracking_project
docker compose --profile airflow exec airflow airflow dags trigger order_tracking_project --conf '{}'
make airflow-test DAG=order_tracking_project    # foreground, logs in your terminal
```

Airflow REST (lab runs with no login; GET /auth/token hands out a token):

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')
curl -s -X PATCH http://localhost:8085/api/v2/dags/order_tracking_project -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"is_paused": false}'
curl -s -X POST http://localhost:8085/api/v2/dags/order_tracking_project/dagRuns -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"logical_date": null, "conf": {}}'
```

From a notebook:

```python
kyuubi_lab.airflow_trigger('order_tracking_project', conf={}, wait=True)
```

## Jupyter notebooks

Start JupyterLab with `make jupyter` (http://localhost:8888). The notebooks use
`client/kyuubi_lab.py`: batches go through Kyuubi REST with name `nb-<job>`
(user `notebook`), SQL through Kyuubi JDBC (user `notebook`, or `lake` with
Delta + Iceberg). Notebook 11 triggers the Airflow DAGs.

### N1. `00_start_here.ipynb`

service check, the job catalog, a REST payload, the first job

* open: http://localhost:8888/lab/tree/notebooks/00_start_here.ipynb
* Kyuubi jobs: `py-smoke`
* talks to: kyuubi-rest

### N2. `01_kyuubi_rest_basics.ipynb`

raw requests: JSON batch, multipart upload, polling, localLog, list, kill, expected failure

* open: http://localhost:8888/lab/tree/notebooks/01_kyuubi_rest_basics.ipynb
* Kyuubi jobs: `py-upload`, `py-expected-failure`
* talks to: kyuubi-rest

### N3. `02_kyuubi_sql_jdbc.ipynb`

PyHive over Kyuubi JDBC: CTAS Parquet/ORC, DESCRIBE FORMATTED, EXPLAIN, SET, views

* open: http://localhost:8888/lab/tree/notebooks/02_kyuubi_sql_jdbc.ipynb
* Kyuubi jobs: `py-hive-etl`
* talks to: kyuubi-jdbc

### N4. `03_file_formats_and_hive_elt.ipynb`

format round trip, SQL on parquet/orc/json/avro folders, Hive ELT tables incl. RCFILE

* open: http://localhost:8888/lab/tree/notebooks/03_file_formats_and_hive_elt.ipynb
* Kyuubi jobs: `formats`, `hive-elt`
* talks to: kyuubi-rest, kyuubi-jdbc, rustfs

### N5. `04_postgres_pipelines.ipynb`

file -> Postgres, Postgres -> Parquet/ORC/CSV, Postgres -> Postgres upsert (idempotency check)

* open: http://localhost:8888/lab/tree/notebooks/04_postgres_pipelines.ipynb
* Kyuubi jobs: `pg-file-to-pg`, `pg-pg-to-files`, `pg-pg-to-pg`
* talks to: kyuubi-rest, kyuubi-jdbc, postgres, rustfs

### N6. `05_lakehouse_iceberg_delta_hudi.ipynb`

Iceberg snapshots + time travel, Delta history + time travel + change data feed, Hudi timeline

* open: http://localhost:8888/lab/tree/notebooks/05_lakehouse_iceberg_delta_hudi.ipynb
* Kyuubi jobs: `iceberg`, `delta`, `hudi`
* talks to: kyuubi-rest, kyuubi-jdbc, rustfs

### N7. `06_kafka_produce_consume.ipynb`

Python producer -> orders.events, Spark consumer -> Parquet bronze, Kafka -> Delta + Postgres

* open: http://localhost:8888/lab/tree/notebooks/06_kafka_produce_consume.ipynb
* Kyuubi jobs: `kafka-produce`, `kafka-consumer-basic`, `kafka-to-delta-pg`
* talks to: kafka, kyuubi-rest, kyuubi-jdbc, postgres

### N8. `07_spark_publishes_to_kafka.ipynb`

enrich + alerts, windowed metrics, batch backfill to Kafka, then the verify job

* open: http://localhost:8888/lab/tree/notebooks/07_spark_publishes_to_kafka.ipynb
* Kyuubi jobs: `kafka-enrich-publish`, `kafka-windowed-metrics`, `kafka-batch-publish`, `file-drop-stream`, `kafka-consumer-basic`, `kafka-to-delta-pg`, `verify-streaming`
* talks to: kafka, kyuubi-rest, postgres, rustfs

### N9. `08_file_event_streaming.ipynb`

file drops -> file-source stream -> Parquet; rerun is a no-op; live mode with kill

* open: http://localhost:8888/lab/tree/notebooks/08_file_event_streaming.ipynb
* Kyuubi jobs: `file-drop-stream`
* talks to: rustfs, kyuubi-rest, kyuubi-jdbc

### N10. `09_order_tracking_project.ipynb`

the three-wave order tracking project and its Iceberg/Delta/Postgres results

* open: http://localhost:8888/lab/tree/notebooks/09_order_tracking_project.ipynb
* Kyuubi jobs: `orders-wave1`, `orders-stream1`, `orders-wave2`, `orders-stream2`, `orders-wave3`, `orders-stream3`, `orders-report`
* talks to: kyuubi-rest, kyuubi-jdbc, postgres

### N11. `10_always_on_streams.ipynb`

start jobs 31/32/33 always-on, feed them, watch Postgres/Kafka, kill them

* open: http://localhost:8888/lab/tree/notebooks/10_always_on_streams.ipynb
* Kyuubi jobs: `kafka-to-delta-pg`, `kafka-enrich-publish`, `kafka-windowed-metrics`
* talks to: kafka, kyuubi-rest, postgres

### N12. `11_airflow_from_notebook.ipynb`

list DAGs, trigger kyuubi_run_any_job / kyuubi_sql_reporting / kafka_streaming_pipeline over Airflow REST

* open: http://localhost:8888/lab/tree/notebooks/11_airflow_from_notebook.ipynb
* Kyuubi jobs: `py-wordcount`
* talks to: airflow, postgres

### N13. `12_end_to_end_all.ipynb`

every catalog job, group by group, with a pass/fail table and chart

* open: http://localhost:8888/lab/tree/notebooks/12_end_to_end_all.ipynb
* Kyuubi jobs: all 34 catalog jobs
* talks to: kyuubi-rest
