# Testing every job with the Python client (`kyuubi_submit.py`)

These are copy‑paste examples for submitting **every** sample job in this lab
through Kyuubi's batch REST API using [`kyuubi_submit.py`](./kyuubi_submit.py).

Prerequisites (all already done per your note — jars built, jobs & data uploaded):

```bash
make up        # stack is running
make jars      # jobs/jars/spark-kyuubi-jobs.jar built
make upload    # jobs/ and data/ synced to RustFS (s3a://jobs, s3a://data)
pip install requests
```

Conventions used below:

- `--url http://localhost:10099` is the Kyuubi REST endpoint.
- `--remote` means Kyuubi's `spark-submit` reads the file from RustFS / S3 / the
  Kyuubi host — the client does **not** upload it. Drop `--remote` to upload a
  local file instead.
- Everything after `--` is passed to the job as application arguments.
- Add `--user anji` (and `--password ...` if auth is on) if your Kyuubi requires it.
- The client submits, then streams the driver log and waits for a terminal state.
  Add `--no-wait` to just get the batch id back.

> Tip: set the URL once so you can omit `--url` on every command:
> ```bash
> export KYUUBI_REST_URL=http://localhost:10099
> ```

---

## 0. SparkPi — JAR that ships with Spark (host path)

Quick sanity check using the example jar already present on the Kyuubi host.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit /opt/spark/examples/jars/spark-examples_2.13-4.1.3.jar --remote \
  --class org.apache.spark.examples.SparkPi \
  --name test-jar-sparkpi \
  -- 20
```

---

## 1. Smoke test — PySpark from RustFS

Checks the driver starts and executors run Python tasks. No storage, no Hive.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/01_smoke_test.py --remote \
  --name test-py-smoke
```

### 1b. Same script, uploaded by the client (no `--remote`)

Proves the upload path. Point at the **local** file; the client ships it to Kyuubi.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit jobs/python/01_smoke_test.py \
  --name test-py-upload
```

---

## 2. Word count over S3A (read + write RustFS)

Reads a text file, counts words, writes CSV back to RustFS.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/02_wordcount_s3.py --remote \
  --name test-py-wordcount \
  -- s3a://data/input/sample.txt s3a://data/output/wordcount_py
```

---

## 3. Hive ETL — CSV → managed Hive table (`demo.orders`)

Writes a Parquet-backed Hive table via the metastore. **Run this before the
Scala sales report (step 9), which reads `demo.orders`.**

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/03_hive_etl.py --remote \
  --name test-py-hive-etl \
  -- s3a://data/input/orders.csv
```

---

## 4. `--py-files` demo — ship a zip, import it on executors

Imports `text_utils` from a zip uploaded to RustFS; the import runs on executors.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/04_pyfiles_demo.py --remote \
  --py-files s3a://jobs/python/libs/text_utils.zip \
  --name test-py-pyfiles \
  -- s3a://data/input/sample.txt
```

---

## 5. Expected failure — must report `ERROR`, not `FINISHED`

This job raises on purpose. The command is expected to **fail** (non‑zero exit /
batch state `ERROR`) — that is the passing outcome for this test.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/05_expected_failure.py --remote \
  --name test-py-expected-failure
```

---

## 6. Conf + args — per‑job Spark conf and app args reach the driver

Passes custom Spark conf (keys must start with `spark.`) and app arguments.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/06_conf_and_args.py --remote \
  --name test-py-conf-args \
  --conf spark.executor.memory=768m \
  --conf spark.app.custom.team=data-eng \
  -- --run-date 2026-10-15 --env lab
```

---

## 7. Java word count — class from the built jar

Same job as step 2, but a Java class inside `spark-kyuubi-jobs.jar`. Requires
`--class` because it's a JAR.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/jars/spark-kyuubi-jobs.jar --remote \
  --class com.example.JavaWordCount \
  --name test-java-wordcount \
  -- s3a://data/input/sample.txt s3a://data/output/wordcount_java
```

---

## 8. Scala sales report — reads `demo.orders`, writes `demo.sales_summary`

Cross‑language check: Python wrote the table (step 3), Scala reads it via the
metastore. **Run step 3 first.** Uses defaults `demo.orders -> demo.sales_summary`,
so no app args are needed.

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/jars/spark-kyuubi-jobs.jar --remote \
  --class com.example.ScalaSalesReport \
  --name test-scala-sales-report
```

Optional: pass explicit source/target tables:

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/jars/spark-kyuubi-jobs.jar --remote \
  --class com.example.ScalaSalesReport \
  --name test-scala-sales-report \
  -- demo.orders demo.sales_summary
```

---

## Managing batches

```bash
# list recent batches (id, state, name)
python kyuubi_submit.py --url http://localhost:10099 list

# full status JSON for one batch
python kyuubi_submit.py --url http://localhost:10099 status <batch-id>

# driver log for one batch
python kyuubi_submit.py --url http://localhost:10099 log <batch-id>

# kill a running batch
python kyuubi_submit.py --url http://localhost:10099 kill <batch-id>
```

Submit and return immediately (don't wait / stream logs):

```bash
python kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/01_smoke_test.py --remote --no-wait
```

---

## Run them all at once

To submit every job in sequence and get a single pass/fail table, use the
bundled runner instead of calling each command by hand:

```bash
# from your Mac
KYUUBI_REST_URL=http://localhost:10099 python client/run_tests.py

# stream driver logs while each job runs
KYUUBI_REST_URL=http://localhost:10099 python client/run_tests.py --show-logs

# run a subset by name
KYUUBI_REST_URL=http://localhost:10099 python client/run_tests.py \
  --only py-smoke,java-wordcount

# list the test names and exit
python client/run_tests.py --list

# or inside the tools container
make test
```

### Test name → example mapping

| run_tests.py name     | Example above | Resource |
|-----------------------|---------------|----------|
| `jar-sparkpi`         | 0             | `spark-examples_2.13-4.1.3.jar` (host) |
| `py-smoke`            | 1             | `s3a://jobs/python/01_smoke_test.py` |
| `py-upload`           | 1b            | local `jobs/python/01_smoke_test.py` |
| `py-wordcount`        | 2             | `s3a://jobs/python/02_wordcount_s3.py` |
| `py-hive-etl`         | 3             | `s3a://jobs/python/03_hive_etl.py` |
| `py-pyfiles`          | 4             | `s3a://jobs/python/04_pyfiles_demo.py` |
| `py-expected-failure` | 5             | `s3a://jobs/python/05_expected_failure.py` |
| `py-conf-args`        | 6             | `s3a://jobs/python/06_conf_and_args.py` |
| `java-wordcount`      | 7             | `s3a://jobs/jars/spark-kyuubi-jobs.jar` |
| `scala-sales-report`  | 8             | `s3a://jobs/jars/spark-kyuubi-jobs.jar` |
