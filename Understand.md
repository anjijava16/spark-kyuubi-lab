# Spark 4 + Kyuubi + RustFS + Hive Metastore, from zero

This is a lab you can run on one Mac. It is built to answer one question
before you touch the real cluster: if every Spark job goes through Kyuubi
instead of `spark-submit`, does everything still work? That means Python jobs,
Java jars, Scala jars, extra Python files, jobs that read and write object
storage, jobs that create Hive tables, and jobs that fail.

You upload code to RustFS, ask Kyuubi over HTTP to run it, and Kyuubi does
the `spark-submit` for you. Everything below explains why each piece is
there and how to drive it.

---

## Contents

1. [The picture](#1-the-picture)
2. [Each piece, in plain terms](#2-each-piece-in-plain-terms)
3. [What happens when you submit a job](#3-what-happens-when-you-submit-a-job)
4. [Versions, and why these](#4-versions-and-why-these)
5. [What's in the zip](#5-whats-in-the-zip)
6. [Zero to running](#6-zero-to-running)
7. [Putting files in RustFS](#7-putting-files-in-rustfs)
8. [Running jobs through Kyuubi](#8-running-jobs-through-kyuubi)
9. [The test suite](#9-the-test-suite)
10. [The SQL side: JDBC through Kyuubi](#10-the-sql-side-jdbc-through-kyuubi)
11. [Writing your own jobs](#11-writing-your-own-jobs)
12. [Where every setting lives](#12-where-every-setting-lives)
13. [Troubleshooting](#13-troubleshooting)
14. [From this laptop to Cloudera / Nexus](#14-from-this-laptop-to-cloudera--nexus)
15. [Cheat sheet](#15-cheat-sheet)
16. [Why batches showed PENDING, and the fix](#16-why-batches-showed-pending-and-the-fix)
17. [File formats: read one, write another](#17-file-formats-read-one-write-another)
18. [Postgres: files to tables and back](#18-postgres-files-to-tables-and-back)
19. [Lakehouse tables: Iceberg, Delta Lake, Hudi](#19-lakehouse-tables-iceberg-delta-lake-hudi)
20. [Project: real-time order tracking](#20-project-real-time-order-tracking)
21. [Hive ELT: TEXTFILE, ORC, Parquet, RCFile](#21-hive-elt-textfile-orc-parquet-rcfile)
22. [Kafka: optional install](#22-kafka-optional-install)
23. [Streaming: Kafka producers, consumers and file events](#23-streaming-kafka-producers-consumers-and-file-events)
24. [All execution payloads](#24-all-execution-payloads)
25. [Airflow: DAGs that run jobs through Kyuubi](#25-airflow-dags-that-run-jobs-through-kyuubi)
26. [Jupyter: notebooks that call Kyuubi](#26-jupyter-notebooks-that-call-kyuubi)
27. [Airflow and Jupyter: troubleshooting](#27-airflow-and-jupyter-troubleshooting)

---

## 1. The picture

```
   YOUR MAC
   ┌──────────────────────────────────────────────────────────────────────┐
   │  kyuubi_submit.py / run_tests.py / curl / kyuubi-ctl   beeline/DBeaver│
   └───────────────┬───────────────────────────────────────────┬──────────┘
                   │ HTTP  :10099  (batch jobs)                 │ JDBC :10009 (SQL)
   DOCKER NETWORK  ▼                                            ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │ kyuubi                                                               │
   │   Kyuubi server ── runs spark-submit ──► Spark DRIVER (one per job)  │
   │                                          Spark SQL engine (per user) │
   └───────────────┬───────────────────────────────▲──────────────────────┘
                   │ "I need executors"            │ executors report back
                   ▼                               │ (to hostname "kyuubi")
   ┌────────────────────────┐    ┌─────────────────┴──────────────────────┐
   │ spark-master   :7077   │──► │ spark-worker  (4 cores, 4g)            │
   │   UI           :8080   │    │   EXECUTORS run your tasks             │
   └────────────────────────┘    └──────────┬───────────────────┬─────────┘
                                            │ s3a               │ thrift
                                            ▼                   ▼
   ┌────────────────────────────────────┐  ┌───────────────────────────────┐
   │ rustfs :9000 API  :9001 console    │◄─│ hive-metastore :9083           │
   │  jobs/       your .py, .zip, .jar  │  │   table name -> schema+location│
   │  data/       input + output files  │  │        │                       │
   │  warehouse/  Hive table files      │  │        ▼                       │
   │  spark-logs/ event logs            │  │ postgres (metastore DB)        │
   └──────────────▲─────────────────────┘  └───────────────────────────────┘
                  │
   ┌──────────────┴─────────┐
   │ spark-history :18080   │  reads event logs, shows finished jobs
   └────────────────────────┘
```

Three things are worth noticing in that picture.

**The driver runs inside the Kyuubi container.** In client mode, the Spark
driver is a child process of whoever ran `spark-submit`, and here that's
Kyuubi. That's why executors have to be able to reach a host called `kyuubi`.

**Code and data live in the same place.** Your `.py` and `.jar` files sit
in the `jobs` bucket, and Spark pulls them with the same s3a connector it
uses for data. Nothing gets copied into containers by hand.

**The metastore holds no data.** It only maps a name like `demo.orders`
to a schema and an `s3a://warehouse/...` location. The Parquet files are
in RustFS.

---

## 2. Each piece, in plain terms

### Spark standalone: master, worker, driver, executor

Spark ships with its own small cluster manager called standalone mode.
It has two long-running processes:

- The **master** keeps track of which workers exist and how many cores and
  how much memory each has free. It does no data work.
- A **worker** offers its cores and memory to the master. When an
  application asks for resources, the master tells a worker to start
  executors for it.

Every application then gets two kinds of processes of its own:

- The **driver** runs your `main()` or Python script. It builds the plan,
  splits it into tasks, and hands the tasks out.
- **Executors** run on workers and do the actual work: reading files,
  shuffling, writing results.

YARN on Cloudera does the same job as the master and workers. Driver and
executors are the same idea everywhere.

**Deploy mode** decides where the driver runs:

| mode | driver runs on | used here for |
|---|---|---|
| `client` | the machine that ran `spark-submit` (here: the kyuubi container) | everything: Python on standalone only supports client mode |
| `cluster` | a worker / YARN container | what you'll likely use on Cloudera for jars |

**`spark.cores.max`** matters more on standalone than people expect. By
default, the first application takes every free core in the cluster, and a
second application then waits forever. The lab caps each app at 2 cores, so
a batch job and the Kyuubi SQL engine can run side by side on the 4-core worker.

### Kyuubi

Kyuubi is a gateway. Clients never talk to the Spark cluster directly. They
talk to Kyuubi, and Kyuubi launches Spark applications for them. It has two
front doors:

- **Thrift/JDBC, port 10009.** Speaks the HiveServer2 protocol, so Beeline,
  DBeaver, PyHive and any Hive JDBC driver work against it. When a user
  connects, Kyuubi finds or starts a long-running Spark application called an
  **engine**, and sends that user's SQL to it.
- **REST, port 10099.** `POST /api/v1/batches` runs one Spark application from
  start to finish, much like `spark-submit` but over HTTP. This is the
  replacement for what you'd run by hand, or for Livy if you've used it.

For batch jobs, the server-side flow is short. Kyuubi builds a
`spark-submit` command from:

1. `SPARK_HOME/conf/spark-defaults.conf`
2. `kyuubi.batchConf.spark.*` entries in `kyuubi-defaults.conf`
3. the `conf` map in your request (wins over the other two)

It then runs that command, tracks the process, and keeps the driver's
stdout/stderr so you can read it at `/api/v1/batches/{id}/localLog`.

**Engine share level** (SQL side only) controls how engines are reused:

| level | one engine per | trade-off |
|---|---|---|
| CONNECTION | JDBC connection | full isolation, slow start every time |
| USER (lab default) | user | fast after the first query, a user's sessions share the engine |
| GROUP | group | fewer engines, less isolation |
| SERVER | the whole server | like the old Spark Thrift Server |

An idle engine shuts itself down after `kyuubi.session.engine.idle.timeout`
(10 minutes here), which gives its cores back to the cluster.

### RustFS and the s3a connector

RustFS is an S3-compatible object store. Spark doesn't know anything about
RustFS. It knows about Hadoop FileSystems, and `s3a://` is the Hadoop
FileSystem for the S3 API. The connector comes in two jars that have to
match each other and match Spark's Hadoop version exactly:

- `hadoop-aws-<hadoop version>.jar`
- the AWS SDK bundle that version of hadoop-aws was built against (SDK v2
  `bundle-x.y.z.jar` for Hadoop 3.4, v1 `aws-java-sdk-bundle` for 3.1–3.3)

The Dockerfiles work these versions out by reading which Hadoop jar is in
the image, then look up the matching SDK in Hadoop's own pom. That's
`docker/common/fetch-s3a-jars.sh`.

Settings that make s3a work against RustFS and not AWS:

- `fs.s3a.endpoint=http://rustfs:9000`: point at RustFS.
- `fs.s3a.path.style.access=true`: use `http://rustfs:9000/bucket/key`, not
  `http://bucket.rustfs:9000/key`, which needs DNS that doesn't exist here.
- `fs.s3a.connection.ssl.enabled=false`: plain HTTP in the lab.
- `fs.s3a.endpoint.region=us-east-1`: stops the SDK from trying to discover
  a region.

One quirk of object stores: a "directory" exists only if something is
inside it. Spark's event-log writer checks that its directory exists, so
`storage-init` drops a `.keep` file into `spark-logs/events/`.

Why RustFS and not MinIO: MinIO stopped publishing free Docker images in
October 2025, and older tags (including `minio/mc`) have started failing to
pull. RustFS is an Apache-2.0, S3-compatible store written in Rust, with its
own web console, and it's published on Docker Hub as `rustfs/rustfs`. From
Spark's point of view nothing changes. s3a only sees an S3 endpoint, so the
same settings would work against MinIO, Ceph, or real AWS S3.

The lab never needs a vendor client. Buckets are created, and files
uploaded, with the official AWS CLI (`amazon/aws-cli`) pointed at
RustFS. That also works against any other S3 store.

Ports: inside Docker, RustFS is `http://rustfs:9000` (API) and `:9001`
(console). On your Mac they're mapped to **9100** and **9101**, so this lab
can run next to a RustFS or MinIO you already have on 9000/9001. Change
them in `.env` if you want.

### Hive Metastore and Postgres

Without a metastore, a table only lives inside one Spark application and is
gone when the app stops. With a metastore:

- `CREATE DATABASE demo` and `saveAsTable("demo.orders")` write a row in the
  metastore's database: the name, columns, file format, and location
  (`s3a://warehouse/demo.db/orders`).
- Any later application, in any language, connected to the same metastore,
  can call `spark.table("demo.orders")`.

The metastore is a Thrift service on port 9083. It stores its own catalog
in a relational database, Postgres here. On first start, `schematool
-initSchema` creates about 70 tables in Postgres. The startup script in
the lab checks first, so restarts don't fail.

Spark only needs one setting to use it: `hive.metastore.uris=thrift://hive-metastore:9083`.
The metastore itself needs s3a settings too, because when you create a
database or a managed table, it creates the directory in the warehouse bucket.

### Spark History Server

A driver's UI on port 4040 disappears when the job ends. With
`spark.eventLog.enabled=true`, each driver writes an event log to
`s3a://spark-logs/events/`, and the history server at
http://localhost:18080 rebuilds the UI from those logs. This is where you
look at a finished job's stages, task times and SQL plans.

---

## 3. What happens when you submit a job

Take this command, with the script and data already in RustFS:

```bash
python client/kyuubi_submit.py submit s3a://jobs/python/02_wordcount_s3.py --remote \
  -- s3a://data/input/sample.txt s3a://data/output/wc
```

1. **Client → Kyuubi.** The script sends
   `POST http://localhost:10099/api/v1/batches` with JSON:
   `{"batchType":"SPARK","resource":"s3a://jobs/python/02_wordcount_s3.py","args":[...],"conf":{}}`.
   Without `--remote`, it sends a multipart upload instead, with the file
   as `resourceFile`.
2. **Kyuubi records the batch** in its metadata store (SQLite under
   `/opt/kyuubi/work` in the lab) and returns the batch id with state `PENDING`.
3. **Kyuubi builds and runs `spark-submit`** in its own container, with
   `--deploy-mode client --conf spark.driver.host=kyuubi ...`, plus the
   resource and args you gave. There's no `--master` on purpose:
   `spark-submit` picks up `MASTER=spark://spark-master:7077` from the
   container's environment (section 16 explains why).
4. **spark-submit downloads the resource.** In client mode, remote primary
   resources and `--py-files` are fetched through Hadoop FileSystems, so the
   s3a jars on the classpath let it read `s3a://jobs/...`. The Python driver
   starts in the kyuubi container.
5. **The driver registers with the master** and asks for executors (1 core,
   1g each, up to 2 cores total).
6. **The master tells the worker** to launch executors. The executors start,
   then connect back to the driver at `kyuubi:<random port>`.
7. **Tasks run on the executors.** Each executor reads its part of
   `sample.txt` from RustFS, counts words, shuffles, and writes the result
   files back to RustFS.
8. **The driver finishes,** writes the end of its event log to
   `s3a://spark-logs/events`, and the `spark-submit` process exits.
9. **Kyuubi tracks the driver process** (it finds it with `jps` by the batch
   id on its command line). While it's alive the batch is `RUNNING`. When it
   exits, a 0 exit code becomes `FINISHED` and anything else `ERROR`. The
   driver log stays available at `GET /api/v1/batches/{id}/localLog`.
10. **The client** has been polling state and log the whole time, prints the
    final state, and exits 0 or 1.

For a Hive job, steps 7–8 also include the driver calling the metastore
(create database, create table, alter table), and the metastore creating
directories under `s3a://warehouse/`.

---

## 4. Versions, and why these

| component | version | why |
|---|---|---|
| Spark | 4.1.3 (Scala 2.13, Java 17, Python 3) | newest official image tag at the time of writing; Spark 4 is Scala 2.13 only |
| Kyuubi | 1.12.0 | latest release; its notes list Spark 3.3–4.2 as verified, on Scala 2.12 and 2.13 |
| Kyuubi SQL engine jar | `kyuubi-spark-sql-engine_2.13` 1.12.0 | ships inside the Kyuubi tarball next to the 2.12 one; Kyuubi picks 2.13 by reading `spark-core_2.13` in `SPARK_HOME` (it is *not* published to Maven separately) |
| Hadoop (inside Spark) | whatever Spark 4.1.3 ships | detected at build time, never hard-coded |
| hadoop-aws + AWS SDK | matched to the above | read from Hadoop's own pom |
| Hive Metastore | 3.1.3 | see below |
| Postgres | 16 | metastore backing DB |
| RustFS | 1.0.1 | S3-compatible storage + console; MinIO images are no longer published |
| AWS CLI | 2.37.12 | creates buckets and uploads files (`storage-init`) |
| spark-avro, Postgres JDBC | Spark's version / 42.7.4 | in `/opt/spark/jars` of every Spark image |
| Delta Lake | 4.4.0, `delta-spark_4.1_2.13` | Delta publishes per-Spark-minor builds since 4.1; 4.4.0 is the newest `_4.1` build listed |
| Apache Iceberg | 1.12.0, `iceberg-spark-runtime-4.1_2.13` | 1.12 supports Spark 3.5, 4.0 and 4.1 |
| Apache Hudi | 1.2.0, `hudi-spark4.1-bundle_2.13` | first Hudi line with a Spark 4.1 bundle |
| Kafka (optional) | `apache/kafka:4.3.1`, single KRaft node | no ZooKeeper; installed with `make kafka-install` |
| Spark Kafka connector | `spark-sql-kafka-0-10_2.13` = Spark's version, `kafka-clients` 3.9.1 | the client version Spark 4.1.3's own pom pins; talks to a 4.x broker fine |
| Kafka UI (optional) | `ghcr.io/kafbat/kafka-ui:v1.5.0` | maintained fork of the old provectus UI |

**Why Hive Metastore 3.1.3 and not 4.x.** Spark 4 still talks to the
metastore through its built-in Hive 2.3 client. That client has worked with
HMS 3.1 for years. Hive 4's metastore changed some Thrift request structs.
For example, older clients' column-statistics calls are rejected for a
missing `engine` field (HIVE-27984). Basic table work usually goes fine, but
there's no reason to debug that in a lab. It also matters that Cloudera CDP
runs a Hive 3.1-line metastore, so this is the closer match to where your
jobs are going.

All versions live in `.env`. To try Spark 4.0.x, change `SPARK_VERSION`
and `SPARK_IMAGE`, then run `make up` again. The s3a jars follow on their
own, and `jobs/jvm` picks up the version at build time.

---

## 5. What's in the zip

```
spark-kyuubi-lab/
├── .env                          all versions + lab credentials
├── docker-compose.yml            the whole stack
├── Makefile                      make up / jars / upload / test / sql / clean
├── Understand.md                 this file
├── README.md                     the short version
│
├── docker/
│   ├── common/fetch-s3a-jars.sh  picks hadoop-aws + SDK matching the Hadoop in the image
│   ├── spark/Dockerfile          targets: spark-base (master/worker/history), kyuubi
│   ├── storage-init/init-buckets.sh  buckets + upload, via the AWS CLI
│   ├── postgres/                 init-shop.sh + shop-schema.sql (app DB "shop")
│   ├── lakehouse/fetch-lakehouse-jars.sh  Delta/Iceberg/Hudi/Kafka-connector jars -> shared volume
│   ├── kafka/create-topics.sh    the lab's Kafka topics (optional Kafka add-on)
│   ├── airflow/Dockerfile        Airflow 3.3 + PyHive, kafka-python, boto3 (optional)
│   ├── jupyter/Dockerfile        JupyterLab + the same client libraries, no Spark (optional)
│   └── hive-metastore/
│       ├── Dockerfile            apache/hive:3.1.3 + s3a jars + Postgres JDBC
│       └── start-metastore.sh    init schema once, then run the metastore
│
├── conf/
│   ├── spark/spark-defaults.conf master, s3a/RustFS, metastore URI, event logs
│   ├── kyuubi/kyuubi-defaults.conf  ports, auth, engine, batch defaults
│   └── hive/hive-site.xml        metastore DB connection, warehouse, s3a
│
├── jobs/                         uploaded to s3a://jobs/
│   ├── python/
│   │   ├── 01_smoke_test.py      cluster alive? Python on executors?
│   │   ├── 02_wordcount_s3.py read + write RustFS
│   │   ├── 03_hive_etl.py        CSV -> Hive table demo.orders
│   │   ├── 04_pyfiles_demo.py    --py-files zip used on executors
│   │   ├── 05_expected_failure.py must end in ERROR
│   │   ├── 06_conf_and_args.py   per-job conf + args arrive
│   │   ├── 10_format_conversions.py   CSV->Parquet->ORC->JSON->Avro->CSV
│   │   ├── 11_file_to_postgres.py     Parquet -> Postgres
│   │   ├── 12_postgres_to_files.py    Postgres -> Parquet/ORC/CSV
│   │   ├── 13_postgres_to_postgres.py full refresh + upsert
│   │   ├── 14_iceberg_demo.py         --with iceberg
│   │   ├── 15_delta_demo.py           --with delta
│   │   ├── 16_hudi_demo.py            --with hudi
│   │   ├── 17_hive_elt.py             Hive SQL ELT over TEXTFILE/ORC/PARQUET/RCFILE
│   │   ├── order_tracking/            20 generator, 21 stream, 22 report, README
│   │   ├── streaming/                 30-35 Kafka + file-event streams, 39 verify, stream_common.py
│   │   └── libs/text_utils.py + text_utils.zip
│   ├── jvm/                      Maven project, Java + Scala in one jar
│   │   ├── pom.xml
│   │   └── src/main/{java,scala}/com/example/
│   │       ├── JavaWordCount.java
│   │       └── ScalaSalesReport.scala
│   └── jars/                     build output lands here (spark-kyuubi-jobs.jar)
│
├── airflow/dags/                 Airflow DAGs (mounted into the airflow container)
│   ├── kyuubi_ops.py             KyuubiBatchOperator + KyuubiSqlOperator
│   ├── lab_common.py             job() helper, report task, S3 helpers
│   ├── kyuubi_test_suites.py     kyuubi_tests_<group> + kyuubi_tests_all (from the catalog)
│   ├── kyuubi_run_any_job.py     pick any job + args/conf in the trigger form
│   ├── lakehouse_daily_elt.py    sensor -> Hive ELT -> formats -> Postgres -> Iceberg/Delta/Hudi -> SQL checks
│   ├── kafka_streaming_pipeline.py   producer -> streams (available-now) -> verify
│   ├── kafka_live_streams.py     start always-on streams, feed, check, stop
│   ├── kyuubi_sql_reporting.py   SQL-only pipeline over Kyuubi JDBC
│   ├── file_event_pipeline.py    file drops -> sensor -> file stream -> SQL
│   └── order_tracking_project.py the order tracking project as a DAG
│
├── notebooks/                    00-12, mounted into JupyterLab
│
├── data/                         uploaded to s3a://data/input/
│   ├── sample.txt
│   └── orders.csv                120 orders (with customer_id 1-30)
│
└── client/
    ├── kyuubi_submit.py          spark-submit-style CLI over Kyuubi REST
    ├── run_tests.py              runs every sample job, prints pass/fail
    ├── job_catalog.py            every job's definition (tests + payload doc read this)
    ├── gen_payloads.py           writes All_execution_payload.md / .json
    ├── lab_runner.py             runs one catalog job and judges it (tests, DAGs, notebooks share it)
    ├── kyuubi_lab.py             notebook API: run(), sql(), pg(), topic(), airflow_trigger() ...
    ├── orchestration.py          which DAG / notebook runs which jobs (for the payload doc)
    ├── producers/order_producer.py   Python producer app (Kafka or S3 file drops)
    ├── producers/topic_tail.py       Python consumer: print messages from a topic
    ├── batch-yaml/*.yaml         same jobs for Kyuubi's own kyuubi-ctl
    ├── entry.sh, requirements.txt
```

---

## 6. Zero to running

### Before you start

- Docker Desktop with **at least 8 GB RAM and 4 CPUs** (Settings → Resources).
  The worker alone gets 4 GB.
- About 6 GB of free disk for images.
- Free ports: 9100, 9101, 7077, 8080, 8081, 9083, 10009, 10099, 18080, 15432, 4040-4045.
  (9000/9001 are left alone, so another RustFS or MinIO can keep running.)
- Apple Silicon: fine. The Spark, RustFS and Postgres images are multi-arch.
  The `apache/hive` image is amd64 only, so it runs under emulation
  (`platform: linux/amd64` is already set). It's slower to start, but it works.
  Make sure "Use Rosetta for x86/amd64 emulation" is on in Docker Desktop.

### Step 1: unzip and look at `.env`

```bash
unzip spark-kyuubi-lab.zip && cd spark-kyuubi-lab
cat .env
```

You don't need to change anything for the first run.

### Step 2: start the stack

```bash
make up            # = docker compose up -d --build
```

The first build downloads base images, Kyuubi and the s3a jars, which takes
5–10 minutes. Startup order is handled for you:

```
rustfs-perms ─► rustfs ─► storage-init (waits, creates buckets, uploads) ─done─┐
postgres ─healthy────────────────────────────────────────────────────────────┤
          └─healthy─► pg-init (creates the "shop" DB) ─done─┐  hive-metastore ◄┘
lakehouse-init (downloads Delta/Iceberg/Hudi jars) ─done─┐  │
spark-master ─healthy─► spark-worker                     │  │
spark-master + hive-metastore + storage-init + pg-init + lakehouse-init ──► kyuubi
```

Check it:

```bash
docker compose ps
```

You want every service `running (healthy)` or `running`, and `storage-init`,
`rustfs-perms`, `pg-init` and `lakehouse-init` `exited (0)`. If
`lakehouse-init` fails (say, a version in `.env` doesn't exist), Kyuubi still
starts and only the lakehouse jobs fail; fix `.env` and run `make lakehouse`. If `storage-init` failed, its log says why:

```bash
docker compose logs storage-init
```

### Step 3: look at the UIs

| UI | URL | what you should see |
|---|---|---|
| Spark master | http://localhost:8080 | 1 worker ALIVE, 4 cores, 4 GiB |
| Spark worker | http://localhost:8081 | registered to spark://spark-master:7077 |
| RustFS console | http://localhost:9101 | log in with `rustfsadmin` / `rustfsadmin123`; buckets jobs, data, warehouse, spark-logs |
| History server | http://localhost:18080 | empty for now |
| Kyuubi web UI | http://localhost:10099/ui | Kyuubi's own UI: sessions, engines, batches and their state |
| Kyuubi REST | http://localhost:10099/api/v1/batches | JSON with an empty `batches` list |

Port 10009 is the JDBC/Thrift port. It speaks a binary protocol, so a browser
on http://localhost:10009 shows nothing. Use Beeline or DBeaver there (section 10).

From a terminal:

```bash
curl -s http://localhost:10099/api/v1/batches | python3 -m json.tool
docker compose logs hive-metastore | tail -20      # "Starting Hive Metastore on 9083"
docker compose logs kyuubi | grep -i "started"     # REST + Thrift frontends started
```

### Step 4: build the Java/Scala jar

```bash
make jars          # = docker compose --profile tools run --rm jar-builder
ls -l jobs/jars/   # spark-kyuubi-jobs.jar
```

This runs Maven in a throwaway container. The first run downloads Spark's
dependencies (a few minutes) and caches them in a volume. You don't need
Java or Maven on your Mac.

### Step 5: upload the jar

`storage-init` ran before the jar existed, so run it again:

```bash
make upload        # = docker compose run --rm storage-init
```

It mirrors `jobs/python` → `s3a://jobs/python/`, `jobs/jars` →
`s3a://jobs/jars/`, and `data/` → `s3a://data/input/`. Run it again any time
you change a file.

### Step 6: run everything

```bash
make test          # all 24 tests, ~20-30 minutes the first time
make test-core     # just the 10 from this step, ~5-10 minutes
```

The other groups (`make test-formats`, `test-postgres`, `test-lake`,
`test-orders`) are covered in sections 17–20. The core group, the first time: At the end you get a table
like this (ids and timings will differ):

```
test                   state          expected    secs  result  batch id / note
------------------------------------------------------------------------------------------------
jar-sparkpi            FINISHED       FINISHED      35  PASS    4b6e...
py-smoke               FINISHED       FINISHED      30  PASS    ...
py-upload              FINISHED       FINISHED      28  PASS    ...
py-wordcount           FINISHED       FINISHED      40  PASS    ...
py-hive-etl            FINISHED       FINISHED      55  PASS    ...
py-pyfiles             FINISHED       FINISHED      33  PASS    ...
py-conf-args           FINISHED       FINISHED      25  PASS    ...
java-wordcount         FINISHED       FINISHED      38  PASS    ...
scala-sales-report     FINISHED       FINISHED      45  PASS    ...
py-expected-failure    ERROR          ERROR         24  PASS    ...
```

The last row is supposed to say ERROR. That's the check that a failing job
doesn't get reported as a success.

### Step 7: check the results yourself

- RustFS console → `data/output/` has `wordcount_py/` and `wordcount_java/`.
- RustFS console → `warehouse/demo.db/` has `orders/` and `sales_summary/`
  with Parquet files.
- http://localhost:18080 lists every job that ran.
- `make sql`, then:

```sql
SHOW DATABASES;
SELECT region, SUM(revenue) FROM demo.sales_summary GROUP BY region;
```

If all of this works, the full path works: client → Kyuubi → Spark → RustFS
+ metastore, for Python, Java and Scala.

---

## 7. Putting files in RustFS

### Bucket layout

| bucket / prefix | holds | who writes it |
|---|---|---|
| `jobs/python/` | `.py` entry scripts, `libs/*.zip` | you (`make upload` or console) |
| `jobs/jars/` | `.jar` files | you |
| `data/input/` | input data | you |
| `data/output/` | job output | jobs |
| `warehouse/` | Hive managed tables (`<db>.db/<table>/`) | Spark via the metastore |
| `spark-logs/events/` | event logs | every driver |

### Option A: the web console (http://localhost:9101)

Log in with `rustfsadmin` / `rustfsadmin123`, open a bucket in the browser
view, and upload files or folders. Folders in object storage are just key
prefixes: uploading `team1/run.py` makes the `team1/` "folder" appear. The
file's s3a path is `s3a://<bucket>/<key>`. For example, `run.py` under
`team1/` in bucket `jobs` is `s3a://jobs/team1/run.py`.

(This is the lab's RustFS, not the one you may already run on :9001. The
two have separate buckets.)

### Option B: `make upload`

Put files under `jobs/python`, `jobs/jars` or `data/` in the project and
run `make upload`. It only copies files that changed.

### Option C: the AWS CLI, for scripting

From the Mac, if you have the AWS CLI installed (`brew install awscli`):

```bash
export AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin123 AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
alias s3lab='aws --endpoint-url http://localhost:9100 s3'

s3lab cp my_job.py s3://jobs/python/
s3lab cp target/my-app.jar s3://jobs/jars/
s3lab ls --recursive s3://jobs/
```

Without installing anything, as a one-off container on the lab network:

```bash
docker run --rm -it --network spark-kyuubi-lab -v "$PWD":/w \
  -e AWS_ACCESS_KEY_ID=rustfsadmin -e AWS_SECRET_ACCESS_KEY=rustfsadmin123 \
  -e AWS_DEFAULT_REGION=us-east-1 -e AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
  amazon/aws-cli:2.37.12 --endpoint-url http://rustfs:9000 s3 cp /w/my_job.py s3://jobs/python/
```

The two `CHECKSUM` variables matter. Recent AWS CLI versions add CRC
checksums to every upload, and some S3-compatible stores reject those.

### Option D: upload through Kyuubi instead

Leave out `--remote` and `kyuubi_submit.py` sends the local file with the
request. That's handy while iterating, but for anything you'll run more
than once, keeping code in RustFS is better, because then the batch request
fully describes what ran.

---

## 8. Running jobs through Kyuubi

There are three ways in. All of them end up at the same REST endpoint.

### A) `kyuubi_submit.py` (the client you already have)

Run it in a container (no Python setup on the Mac):

```bash
alias ks='docker compose --profile tools run --rm kyuubi-client python client/kyuubi_submit.py'
```

Or straight from the Mac:

```bash
pip install requests
alias ks='python client/kyuubi_submit.py --url http://localhost:10099'
```

Python script in RustFS, with arguments:

```bash
ks submit s3a://jobs/python/02_wordcount_s3.py --remote \
   -- s3a://data/input/sample.txt s3a://data/output/wc_manual
```

Python with a helper zip and conf:

```bash
ks submit s3a://jobs/python/04_pyfiles_demo.py --remote \
   --py-files s3a://jobs/python/libs/text_utils.zip \
   --conf spark.executor.memory=2g \
   -- s3a://data/input/sample.txt
```

Java or Scala class from a jar in RustFS:

```bash
ks submit s3a://jobs/jars/spark-kyuubi-jobs.jar --remote \
   --class com.example.ScalaSalesReport
```

Extra jars (a JDBC driver, say) alongside your app:

```bash
ks submit s3a://jobs/jars/my-app.jar --remote --class com.acme.Main \
   --jars s3a://jobs/jars/postgresql-42.7.4.jar
```

Fire and forget, then check later:

```bash
ks submit s3a://jobs/python/01_smoke_test.py --remote --no-wait   # prints batch id
ks status <id>
ks log <id>
ks kill <id>
ks list
```

How `spark-submit` flags map:

| spark-submit | kyuubi_submit.py | batch request field |
|---|---|---|
| `app.py` / `app.jar` | first positional | `resource` |
| `--class` | `--class` (auto for `.py`) | `className` (**required by Kyuubi 1.12, even for Python**) |
| `--name` | `--name` | `name` |
| `--conf k=v` | `--conf k=v` | `conf` |
| `--py-files` | `--py-files` | `conf["spark.submit.pyFiles"]` |
| `--jars` | `--jars` | `conf["spark.jars"]` |
| `--files` | `--files` | `conf["spark.files"]` |
| app args | after `--` | `args` |
| `--master`, `--deploy-mode` | don't, the server decides | (server-side `kyuubi.batchConf.*`) |

### B) curl

```bash
curl -s -X POST http://localhost:10099/api/v1/batches \
  -H 'Content-Type: application/json' -u anji: \
  -d '{
        "batchType": "SPARK",
        "name": "wc-curl",
        "resource": "s3a://jobs/python/02_wordcount_s3.py",
        "className": "org.apache.spark.deploy.PythonRunner",
        "args": ["s3a://data/input/sample.txt", "s3a://data/output/wc_curl"],
        "conf": {"spark.executor.memory": "1g"}
      }' | python3 -m json.tool
```

Two fields Kyuubi 1.12 always requires, even when you'd think they're
optional:

- `className`: required for every SPARK batch. For a `.py` file,
  `spark-submit` runs `org.apache.spark.deploy.PythonRunner` whatever
  `--class` says, so send that. `kyuubi_submit.py` fills it in for you.
- `resource`: required even when you upload the file as multipart. Put the
  file name there. Kyuubi replaces it with the path of the uploaded copy.

```bash

curl -s http://localhost:10099/api/v1/batches/<id> | python3 -m json.tool
curl -s "http://localhost:10099/api/v1/batches/<id>/localLog?from=0&size=200"
curl -s -X DELETE http://localhost:10099/api/v1/batches/<id>
```

### C) `kyuubi-ctl` (ships with Kyuubi)

```bash
docker compose cp client/batch-yaml kyuubi:/tmp/
docker compose exec kyuubi bin/kyuubi-ctl submit batch \
  -f /tmp/batch-yaml/wordcount_py.yaml --hostUrl http://localhost:10099
```

### Batch states

`PENDING` → `RUNNING` → one of `FINISHED`, `ERROR`, `CANCELED`.

The response also carries `appId` and `appState`. In this lab, `appId` is
the process id of the driver JVM inside the kyuubi container, because that's
how Kyuubi tracks it (section 16). On YARN you'll see real `application_...`
ids instead.

---

## 9. The test suite

`client/run_tests.py` submits each case, waits, and compares the final state
with what it expects. A job that reaches FINISHED also has to print its own
"... OK" line (each sample job prints one only after its checks pass), so a
job that exits 0 without doing its work still fails the test.

Tests are grouped: `core` (below), `formats`, `postgres`, `lakehouse`,
`orders` (sections 17–20). The core group:

| test | resource | proves |
|---|---|---|
| jar-sparkpi | Spark's example jar, on the Kyuubi host | Kyuubi → spark-submit → master → executors works at all |
| py-smoke | `01_smoke_test.py` from RustFS | Python driver + Python on executors; s3a resource download |
| py-upload | same file, uploaded in the request | the multipart upload path |
| py-wordcount | `02_wordcount_s3.py` | s3a reads and writes from executors |
| py-hive-etl | `03_hive_etl.py` | metastore create db/table; table files in `warehouse/` |
| py-pyfiles | `04_pyfiles_demo.py` + zip | `--py-files` from RustFS reaches driver and executors |
| py-conf-args | `06_conf_and_args.py` | request `conf` and `args` reach the job |
| java-wordcount | jar + `com.example.JavaWordCount` | Java jar from RustFS |
| scala-sales-report | jar + `com.example.ScalaSalesReport` | Scala jar; reads a table another language wrote |
| py-expected-failure | `05_expected_failure.py` | failures come back as ERROR |

Useful options:

```bash
make test ARGS="--list"
make test ARGS="--only py-hive-etl,scala-sales-report"
make test-lake ARGS="--show-logs"
make test ARGS="--group postgres,lakehouse"
```

To test **your own** jobs, add entries to the `CASES` list. Each one is just
`name`, `group`, `resource`, `class_name`, `args`, `conf`, `py_files`,
`remote`, and optionally `with_formats`, `after`, `marker`, `expect`. When you migrate jobs from Cloudera, a case per production job is
an easy regression suite.

---

## 10. The SQL side: JDBC through Kyuubi

```bash
make sql
# = docker compose exec kyuubi /opt/kyuubi/bin/beeline -u 'jdbc:hive2://localhost:10009/' -n anji
```

The first query takes 20–40 seconds while Kyuubi starts a Spark engine for
user `anji`. You'll see it appear on the master UI as an app named like
`kyuubi_USER_SPARK_SQL_anji_default_...`. Later queries are fast. After 10
idle minutes, the engine stops.

```sql
SHOW DATABASES;
SHOW TABLES IN demo;
SELECT * FROM demo.orders LIMIT 5;
CREATE TABLE demo.big_orders AS SELECT * FROM demo.orders WHERE amount > 1000;
```

From the Mac with a GUI client such as DBeaver, use the "Apache Kyuubi" or
"Apache Hive" driver with URL `jdbc:hive2://localhost:10009/default`, user
`anji` and an empty password.

From Python (PyHive):

```python
from pyhive import hive
conn = hive.connect(host="localhost", port=10009, username="anji")
cur = conn.cursor(); cur.execute("SELECT count(*) FROM demo.orders"); print(cur.fetchall())
```

If you'd rather use Spark's own Beeline: `docker compose exec kyuubi /opt/spark/bin/beeline -u jdbc:hive2://localhost:10009/`.

Batch jobs and SQL sessions share the same metastore and the same
warehouse. A table a batch job creates shows up in SQL right away.

---

## 11. Writing your own jobs

Rules that save time:

1. **Don't set the master in code.** No `.master("local[*]")` and no
   `setMaster`. Kyuubi decides that. The same file then runs here, on YARN,
   and anywhere else.
2. **Take paths as arguments.** `s3a://...` here might be `hdfs://...` or
   `ofs://...` on the cluster. The args list is where that difference should live.
3. **Python helper code goes in a zip.** Zip the package so `import mypkg`
   works from the zip root, upload it, and pass `--py-files`. Third-party
   wheels with C extensions won't work from `--py-files`. Those have to be
   installed on the image (driver and workers), or shipped as a packed
   virtualenv with `spark.archives`.
4. **Jars: keep Spark `provided`.** Bundle only your own dependencies (use
   the shade plugin). Never bundle Spark, Scala or Hadoop.
5. **Match Scala 2.13.** Jars built for Spark 3.x on Scala 2.12 fail on Spark 4
   with `NoSuchMethodError` or `ClassNotFoundException: scala.collection...`.
   This is the most likely problem when you bring old Cloudera jars over,
   since CDP 7.1 Spark 3 is Scala 2.12. Rebuild them against Spark 4 / 2.13.
6. **Spark 4 behavior changes** worth knowing before migrating:
   ANSI SQL mode is on by default (`spark.sql.ansi.enabled=true`), so bad
   casts and overflows throw errors where Spark 3 returned null. Java 17 is
   the minimum, and Python 3.9+ (check your Spark minor's docs). Run your
   real jobs here first. That's what the lab is for.
7. **Let the job fail loudly.** Exit non-zero (raise, or `System.exit(1)`).
   Kyuubi can only report ERROR if the process says so.

Adding a new Python job:

```bash
cp my_job.py jobs/python/ && make upload
ks submit s3a://jobs/python/my_job.py --remote -- arg1 arg2
```

Adding a new class to the jar: put it under `jobs/jvm/src/main/java` or
`.../scala`, then `make jars && make upload`.

Bringing an existing jar: copy it to `jobs/jars/`, run `make upload`, then submit
with `--class`.

---

## 12. Where every setting lives

| you want to change | file | key |
|---|---|---|
| master URL for all apps | `docker-compose.yml`, kyuubi service | env `MASTER` (deliberately not `spark.master`; see section 16) |
| app Postgres connection | `conf/spark/spark-defaults.conf` | `spark.lab.pg.url`, `.user`, `.password` |
| where Delta/Hudi tables go | `conf/spark/spark-defaults.conf` | `spark.lab.lake.root` |
| Delta/Iceberg/Hudi versions | `.env` | `DELTA_VERSION`, `ICEBERG_VERSION`, `HUDI_VERSION`, then `make lakehouse` |
| what `--with <format>` adds | `client/kyuubi_submit.py` | `LAKEHOUSE_PROFILES` |
| default executor memory/cores | `conf/spark/spark-defaults.conf` | `spark.executor.memory`, `spark.executor.cores` |
| max cores per app | `conf/spark/spark-defaults.conf` | `spark.cores.max` |
| RustFS endpoint / keys | `spark-defaults.conf` **and** `hive-site.xml` (and `.env`) | `fs.s3a.*` |
| metastore address | `conf/spark/spark-defaults.conf` | `spark.hadoop.hive.metastore.uris` |
| metastore DB | `conf/hive/hive-site.xml` | `javax.jdo.option.*` |
| default warehouse | `spark-defaults.conf` + `hive-site.xml` | `spark.sql.warehouse.dir`, `hive.metastore.warehouse.dir` |
| Kyuubi ports / auth | `conf/kyuubi/kyuubi-defaults.conf` | `kyuubi.frontend.*`, `kyuubi.authentication` |
| batch defaults (server side) | `conf/kyuubi/kyuubi-defaults.conf` | `kyuubi.batchConf.spark.<spark key>` |
| SQL engine sharing / idle | `conf/kyuubi/kyuubi-defaults.conf` | `kyuubi.engine.share.level`, `kyuubi.session.engine.idle.timeout` |
| one job's settings | the batch request | `conf` |
| worker size | `.env` | `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` |
| versions | `.env` | `SPARK_*`, `KYUUBI_VERSION`, `HIVE_IMAGE` |

Config files are mounted read-only into the containers. After editing:

```bash
docker compose restart kyuubi            # kyuubi-defaults or spark-defaults
docker compose restart hive-metastore    # hive-site.xml
docker compose restart spark-master spark-worker spark-history   # spark-defaults
```

More workers: `docker compose up -d --scale spark-worker=2` (worker UIs on 8081, 8082).

---

## 13. Troubleshooting

Start with the driver log (`ks log <id>`, or the `localLog` endpoint), then
`docker compose logs kyuubi`, then executor logs (worker UI → app → stderr).

| symptom | likely cause | fix |
|---|---|---|
| `curl :10099` connection refused | Kyuubi still starting, or REST not enabled | `docker compose logs kyuubi`; check `kyuubi.frontend.protocols` includes REST |
| batch stays RUNNING, master UI shows app "WAITING" | no free cores: another app (often the idle SQL engine) holds them | wait for the engine's idle timeout, lower `spark.cores.max`, or add a worker |
| `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` | s3a jars missing in that image | rebuild: `docker compose build --no-cache spark-master kyuubi` |
| `NoSuchMethodError` / `NoClassDefFoundError` naming `software.amazon...` or `com.amazonaws...` | hadoop-aws and SDK versions don't match | set `HADOOP_VERSION`/`AWS_SDK_VERSION` build args, or check the `fetch-s3a` step in the build output |
| batch stays PENDING in the Kyuubi UI after the job finished | `spark.master` visible to Kyuubi (conf file or request) | remove it everywhere; keep only env `MASTER` (section 16); never pass `--conf spark.master` |
| batch stays PENDING and the driver never starts | Kyuubi can't run `jps`, or the worker has no free cores | `docker compose exec kyuubi jps -ml`; check the master UI for WAITING apps |
| `ClassNotFoundException: io.delta...` / `org.apache.iceberg...` / `org.apache.hudi...` | lakehouse jars missing, or job submitted without `--with` | `docker compose logs lakehouse-init`; `make lakehouse`; add `--with delta` etc. |
| `lakehouse-init` fails with "Could not resolve dependencies" | that version doesn't exist for this Spark minor | pick another version in `.env`, then `make lakehouse` |
| `Failed to find data source: avro` | spark-avro missing | rebuild images: `docker compose build --no-cache spark-master kyuubi` |
| `relation "customers" does not exist` / `password authentication failed for user "app"` | shop DB not created | `make pg-init` |
| `Table or view not found: ice.lab...` / `ice.tracking...` | Iceberg catalog `ice` not configured | submit with `--with iceberg` (or `delta,iceberg`) |
| Hudi job fails right after a Delta job in the same engine | both want `spark_catalog` | never combine `--with hudi` with delta/iceberg; separate jobs are fine |
| test shows `NO_MARKER` | job exited 0 but its own checks didn't print the OK line | read the log tail printed above the table; usually an exception caught somewhere |
| Kafka tests show `SKIP ... make kafka-install` | Kafka is optional and not running | `make kafka-install` |
| `Failed to find data source: kafka` | job submitted without the connector | add `--with kafka`; check `ls /opt/lakehouse/kafka` in the kyuubi container |
| streaming job reads nothing from Kafka | checkpoint already past the data, or topic recreated | rerun the producer, or submit with `-- --reset` to start from `--starting-offsets` |
| always-on streams stuck `WAITING` on the master UI | out of cores (each live stream holds its cores) | `make stream-stop`, use `--conf spark.cores.max=1`, or `--scale spark-worker=2` |
| `CAST_INVALID_INPUT` reading raw text | Spark 4 ANSI mode: bad casts throw | `try_cast(...)` for raw input (see 17_hive_elt.py) |
| `UnknownHostException: jobs.rustfs` | path-style access off | `fs.s3a.path.style.access=true` |
| `storage-init` exits non-zero | RustFS not up yet, or wrong keys | `docker compose logs rustfs storage-init`; then `make upload` again |
| `rustfs` restarting, "permission denied" on /data | volume not owned by uid 10001 | `docker compose up rustfs-perms` then `docker compose restart rustfs` |
| `Bind for 0.0.0.0:9100 failed: port is already allocated` | something else on 9100/9101 | change `RUSTFS_API_PORT` / `RUSTFS_CONSOLE_PORT` in `.env` |
| `pull access denied for minio/...` | old MinIO images being removed upstream | you're on an old copy of the lab; this version uses `rustfs/rustfs` + `amazon/aws-cli` |
| `403 Forbidden` / `InvalidAccessKeyId` from s3a | key mismatch | keys in `spark-defaults.conf`, `hive-site.xml` and `.env` must agree |
| `FileNotFoundException: s3a://spark-logs/events` | events prefix missing | `make upload` (it creates `events/.keep`) |
| executors start then die; `Failed to connect to kyuubi/...` | driver host not reachable | `spark.driver.host=kyuubi` in `kyuubi.batchConf` and `spark.driver.host` in kyuubi-defaults |
| `ModuleNotFoundError` in a Python job | helper module not shipped, or wrong zip layout | `--py-files` the zip; `unzip -l` should show `mymod.py` or `mypkg/__init__.py` at the root |
| first JDBC query times out | engine startup slow (hive-metastore under emulation on M-series) | raise `kyuubi.session.engine.initialize.timeout`; retry |
| SQL engine fails, `NoClassDefFoundError: scala/...` | 2.12 engine jar used with Spark 4 | don't set `kyuubi.session.engine.spark.main.resource`; check `ls /opt/kyuubi/externals/engines/spark/` has the `_2.13` jar and `SPARK_HOME` points at the Spark 4 install |
| hive-metastore restarts in a loop | Postgres not ready, or schema half-created | `docker compose logs hive-metastore`; worst case `make clean` and start over |
| `Table or view not found: demo.orders` from Scala | Hive ETL test didn't run or failed | run `py-hive-etl` first |
| jar job: `ClassNotFoundException: com.example.X` | wrong `--class`, or an old jar in RustFS | `make jars && make upload`; `unzip -l jobs/jars/*.jar` |
| jar job: `NoSuchMethodError: scala.*` | jar compiled for Scala 2.12 | rebuild against Spark 4 / Scala 2.13 |
| Kyuubi Docker build fails at `test -f .../kyuubi-spark-sql-engine_2.13-*.jar` | that Kyuubi tarball didn't ship a Scala 2.13 engine | use Kyuubi 1.10 or newer (`KYUUBI_VERSION` in `.env`) |
| `400 ... classname is a required parameter for SPARK` | batch sent without `className` | for `.py` use `org.apache.spark.deploy.PythonRunner`; `kyuubi_submit.py` does this for you |
| `400 ... resource is a required parameter` on upload | multipart batch with no `resource` field | put the file name in `resource`; Kyuubi swaps in the uploaded path |
| RustFS image can't be pulled | tag removed upstream | use another 2025 `RELEASE.*` tag, or build RustFS from source |
| driver UI link on master page (`http://kyuubi:4040`) doesn't open | `kyuubi` doesn't resolve on the Mac | use `http://localhost:4040` (to 4045), Kyuubi's proxy `http://localhost:10099/engine-ui/kyuubi:4040/`, or the history server afterwards |
| browser shows nothing on http://localhost:10009 | 10009 is Thrift/JDBC, not HTTP | web UI is http://localhost:10099/ui |

Handy commands:

```bash
docker compose exec kyuubi ls /opt/spark/jars | grep -E 'hadoop-aws|bundle|hadoop-client'
docker compose exec kyuubi ls /opt/kyuubi/work                     # batch working dirs
docker compose exec postgres psql -U hive -d metastore -c 'select "NAME","DB_LOCATION_URI" from "DBS";'
docker compose exec postgres psql -U hive -d metastore -c 'select "TBL_NAME","TBL_TYPE" from "TBLS";'
```

---

## 14. From this laptop to Cloudera / Nexus

The client side stays the same: the same REST call, the same
`kyuubi_submit.py`, the same batch JSON. What changes is the server
configuration and the storage paths.

| here | Cloudera CDP / production | what to change |
|---|---|---|
| Spark standalone master (via env `MASTER`) | YARN ResourceManager | `kyuubi.batchConf.spark.spark.master=yarn` (on YARN, Kyuubi tracks apps itself, so the env trick goes away); `HADOOP_CONF_DIR`/`YARN_CONF_DIR` on the Kyuubi host |
| client mode, driver in Kyuubi | cluster mode for JVM jobs (driver in a YARN container) | `kyuubi.batchConf.spark.spark.submit.deployMode=cluster`; Kyuubi then picks up the real YARN app id |
| RustFS `s3a://` | HDFS `hdfs://`, Ozone `ofs://`, or S3 | job args, `spark.sql.warehouse.dir` |
| Hive Metastore 3.1.3, no auth | CDP HMS (3.1 line), Kerberos, Ranger | `hive.metastore.uris`, plus `hive.metastore.sasl.enabled` and the principal |
| `kyuubi.authentication=NONE` | Kerberos and/or LDAP | `kyuubi.authentication=KERBEROS,LDAP`, keytab/principal, `kyuubi.engine.doAs.enabled=true` so jobs run as the real user |
| one Kyuubi server, embedded ZooKeeper | 2+ Kyuubi servers behind ZooKeeper | `kyuubi.ha.addresses`, `kyuubi.ha.namespace`; clients use the ZK-discovery JDBC URL |
| SQLite batch metadata | MySQL/Postgres | `kyuubi.metadata.store.jdbc.*`, needed for HA so any server can answer for any batch |
| Livy (if you used it on CDP) | Kyuubi REST batches | request shape is similar: `file`→`resource`, `className`, `args`, `conf` |
| Spark 3.x / Scala 2.12 jars | Spark 4 / Scala 2.13 | rebuild every JVM job; retest Python under ANSI mode |

A sensible order for the migration:

1. Get every production job passing **here** against copies of its input
   data (add a test case per job).
2. Point the same Kyuubi config at a dev YARN cluster: change the master and
   deploy mode, keep everything else.
3. Swap storage paths and metastore to the real ones, then turn on Kerberos/LDAP.
4. Add Kyuubi HA and an external metadata store.
5. Run the same `run_tests.py` cases against the new endpoint and compare.

---

## 15. Cheat sheet

```bash
make up              # build + start
make test-core | test-formats | test-postgres | test-lake | test-orders
make sql-lake        # SQL shell with Delta + Iceberg enabled
make kafka-install   # optional: Kafka broker + topics + UI + Spark connector
make produce         # live Kafka producer (Ctrl-C to stop); make produce-files for S3 drops
make stream-start    # submit the always-on streaming jobs; make stream-stop to kill them
make tail TOPIC=orders.metrics FOLLOW=1   # watch what Spark publishes
make test-hive | test-streaming
make payloads        # regenerate All_execution_payload.md / .json
make psql            # psql into the shop database
make lakehouse       # re-download lakehouse jars after changing .env
make pg-init         # recreate the shop database
make jars            # build Java/Scala jar
make upload          # push jobs/ and data/ to RustFS
make test            # run all tests through Kyuubi
make sql             # beeline into Kyuubi
make logs            # follow Kyuubi logs
make down            # stop (keep data)
make clean           # stop and delete everything

ks submit <s3a://...py|jar> --remote [--class C] [--py-files z] [--conf k=v] [--with delta,iceberg|hudi] -- args
ks status <id> | ks log <id> | ks kill <id> | ks list
```

| port | service |
|---|---|
| 8080 | Spark master UI |
| 8081 | Spark worker UI |
| 7077 | Spark master RPC |
| 18080 | Spark history server |
| 9100 | RustFS S3 API (`rustfs:9000` inside Docker) |
| 9101 | RustFS console |
| 9083 | Hive Metastore (thrift) |
| 15432 | Postgres (metastore DB) |
| 10009 | Kyuubi JDBC / Thrift (not a web page) |
| 10099 | Kyuubi REST + web UI at `/ui` |
| 9094 | Kafka from your Mac (optional add-on; `kafka:9092` inside Docker) |
| 8090 | Kafka UI (optional add-on) |
| 4040-4045 | live driver UIs (inside kyuubi) |

---

## 16. Why batches showed PENDING, and the fix

If you ran an earlier copy of this lab, the Kyuubi UI showed batches stuck in
`PENDING` (operation `BatchJobSubmission`, state `PENDING_STATE`) even though
the driver log ended with `... OK` and a clean exit.

What Kyuubi 1.12 does after it starts `spark-submit`:

1. It looks for an *application manager* that understands the job's master.
   There are three: YARN, Kubernetes, and a local one that finds the driver
   process with `jps -ml | grep <batch id>`. The local one is only used when
   Kyuubi sees **no** master at all, or `local`.
2. A batch moves to `RUNNING` only when one of them reports an application id.
3. After `spark-submit` exits, Kyuubi keeps polling for an id or a terminal
   state before it decides the batch is done.

With `spark.master=spark://...`, none of the three managers applies. No id
ever arrives, so the batch never leaves `PENDING`, even after success. This
is a gap in Kyuubi's standalone-mode support, not something wrong with your
jobs.

The fix, without patching Kyuubi:

- `spark.master` is removed from **every place Kyuubi reads**: the batch
  request, `kyuubi-defaults.conf` (including `kyuubi.batchConf.spark.*`), and
  `$SPARK_HOME/conf/spark-defaults.conf`.
- The kyuubi container gets `MASTER=spark://spark-master:7077` as an
  environment variable. `spark-submit` still uses it, because its master
  lookup ends with `env.get("MASTER")` (checked in Spark 4.1.3's
  `SparkSubmitArguments`).
- Kyuubi now sees no master, so it uses the local process tracker. That's
  accurate here: in client mode the driver really is a local process in the
  kyuubi container. The base image is a full JDK, so `jps` is there.

Result: `PENDING` → `RUNNING` (with the driver's pid as `appId`) →
`FINISHED` or `ERROR`. In the UI and the API, `kill` works too: Kyuubi sends
SIGTERM to the driver.

Rules that keep it working:

- Never pass `--conf spark.master=...` in a batch request.
- If you run `spark-submit` by hand inside the master or worker containers,
  add `--master spark://spark-master:7077` yourself, since the shared
  `spark-defaults.conf` no longer has it.
- On YARN (Cloudera), go back to `spark.master=yarn`. Kyuubi tracks YARN
  apps natively, so none of this is needed there.

To watch it work:

```bash
docker compose exec kyuubi jps -ml          # while a job runs: SparkSubmit ... <batch id>
curl -s http://localhost:10099/api/v1/batches | python3 -m json.tool | grep -E '"(id|state|appId)"'
```

---

## 17. File formats: read one, write another

`jobs/python/10_format_conversions.py` (`make test-formats`) takes the orders
CSV through five conversions. Each step reads the files the previous step
wrote, not a DataFrame in memory:

```
CSV ─► Parquet ─► ORC ─► JSON ─► Avro ─► CSV          (+ Parquet partitioned by region)
```

At the end it checks the final CSV equals the original row for row
(`exceptAll` both ways), and prints how big the same 120 rows are in each
format.

Things to notice in the code and output:

| format | schema stored in file? | notes |
|---|---|---|
| CSV | no | pass a schema, or every column comes back as string |
| JSON | no | same; dates become strings unless you give a schema; biggest on disk |
| Parquet | yes | columnar, snappy-compressed; the default for analytics |
| ORC | yes | columnar; Hive's native format, smallest here with zlib |
| Avro | yes | row-based, good for record streams; needs `spark-avro` (in the image) |

The partitioned Parquet write (`partitionBy("region")`) creates
`region=North/`, `region=South/`, ... folders. Reading with
`where region = 'North'` only opens one of them (partition pruning).

Running a single conversion yourself:

```bash
ks submit s3a://jobs/python/10_format_conversions.py --remote -- \
   s3a://data/input/orders.csv s3a://data/output/formats
```

---

## 18. Postgres: files to tables and back

The lab's Postgres hosts two databases:

| database | user | used by |
|---|---|---|
| `metastore` | hive | Hive Metastore only. Don't touch. |
| `shop` | app / apppass | the example jobs |

`pg-init` creates `shop` and seeds `customers` (30 rows), plus the
`customer_ltv` and `order_tracking` tables with primary keys (upserts need
them). Jobs read the connection from `spark.lab.pg.*` in `spark-defaults.conf`,
and the JDBC driver is in the image. Run them with `make test-postgres`:

| job | direction | what it shows |
|---|---|---|
| `11_file_to_postgres.py` | Parquet → `orders` table | JDBC write with `truncate=true` (keep table, replace rows), `batchsize`, 2 parallel connections; reads the count back from Postgres |
| `12_postgres_to_files.py` | `customers` + `orders` → Parquet (partitioned), ORC, CSV | the three JDBC read styles: whole table; parallel ranges (`partitionColumn`/`lowerBound`/`upperBound`/`numPartitions`); `query` pushdown so the GROUP BY runs inside Postgres |
| `13_postgres_to_postgres.py` | `orders` → `daily_sales`, `customer_ltv` | full refresh vs upsert. Spark's JDBC writer has no MERGE, so: write to a staging table, run `INSERT ... ON CONFLICT DO UPDATE` in Postgres from the driver, drop staging. Runs the upsert twice and checks the second changes 0 rows. |

Look at the results:

```bash
make psql
shop=> \dt
shop=> select * from customer_ltv order by revenue desc limit 5;
shop=> select * from daily_sales order by order_date desc limit 5;
```

A few habits worth keeping from these jobs:

- Keep `numPartitions` small on writes. Each partition is a database
  connection, and an OLTP database won't like 200 of them.
- Push filters and aggregations into Postgres with the `query` option rather
  than reading whole tables into Spark.
- For upserts, make the database do the merge (staging table + `ON CONFLICT`),
  and make it idempotent so a retried job can't double count.

---

## 19. Lakehouse tables: Iceberg, Delta Lake, Hudi

All three add database features to files in object storage: ACID commits,
updates/deletes/MERGE, time travel, and schema changes without rewriting
data. They differ in design and ecosystem:

| | Iceberg | Delta Lake | Hudi |
|---|---|---|---|
| catalog in this lab | `ice` (Iceberg catalog over the Hive Metastore) | Spark session catalog → Hive Metastore | path-based (no catalog) |
| table location | `s3a://warehouse/iceberg/<ns>.db/<table>` | `s3a://warehouse/lake/delta/...` | `s3a://warehouse/lake/hudi/...` |
| strengths | engine-neutral (Spark, Trino, Flink, Snowflake...), hidden partitioning, partition evolution | simplest with Spark; change data feed; OPTIMIZE/Z-ORDER | record-key upserts, incremental pulls, streaming ingestion |
| on Cloudera | CDP's main open table format | less common | supported |

### How a job opts in

The jars aren't in `/opt/spark/jars`. Each format wants to own the session
catalog or add SQL extensions, and loading all three into every job causes
conflicts. Instead, `lakehouse-init` downloads them into a shared volume
(`/opt/lakehouse/<format>/`), and a job asks for the formats it needs:

```bash
ks submit s3a://jobs/python/14_iceberg_demo.py --remote --with iceberg
ks submit s3a://jobs/python/15_delta_demo.py   --remote --with delta
ks submit s3a://jobs/python/16_hudi_demo.py    --remote --with hudi
ks submit .../21_track_orders_stream.py        --remote --with delta,iceberg
```

`--with` adds plain Spark conf to the batch request. You can see exactly
what in `LAKEHOUSE_PROFILES` in `client/kyuubi_submit.py`. For Iceberg:

```
spark.driver.extraClassPath / spark.executor.extraClassPath = /opt/lakehouse/iceberg/*
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.ice = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice.type = hive
spark.sql.catalog.ice.uri = thrift://hive-metastore:9083
spark.sql.catalog.ice.warehouse = s3a://warehouse/iceberg
```

Delta and Iceberg combine fine, because Iceberg uses its own catalog name.
Hudi can't be combined with them, since it and Delta both replace
`spark_catalog`. `kyuubi_submit.py` refuses `--with hudi,delta`.

### What each demo does (`make test-lake`)

**`14_iceberg_demo.py`**: creates `ice.lab.orders` partitioned by
`months(order_date)` (hidden partitioning: queries just filter on
`order_date`), then appends, MERGEs (10 updates + 5 inserts), deletes rows,
adds a column, adds `region` as a second partition field for new data only,
and time-travels to the first snapshot with `VERSION AS OF`. It finishes by
querying the metadata tables `history`, `snapshots` and `files`.

**`15_delta_demo.py`**: writes `s3a://warehouse/lake/delta/orders`
(registered as `default.delta_orders`), turns on the change data feed, then
MERGE, DELETE, UPDATE, an append with a new column (`mergeSchema`), time
travel to version 0, reads the change feed since the MERGE, and runs
`OPTIMIZE ... ZORDER BY (customer_id)` and `DESCRIBE HISTORY`.

**`16_hudi_demo.py`**: a Copy-on-Write table keyed by `order_id`, partitioned
by `region`, with `updated_at` deciding which version of a key wins. It
inserts, upserts, deletes by key, runs an incremental query (only rows
changed after commit 1: exactly 15), and time-travels with `as.of.instant`.
It also shows Hudi's `_hoodie_*` metadata columns.

### Querying them with SQL

`make sql-lake` opens Beeline as user `lake` with Delta and Iceberg enabled.
That's a separate Kyuubi engine, so your plain `make sql` session is
unaffected:

```sql
SELECT * FROM ice.lab.orders.snapshots;
SELECT region, count(*) FROM ice.lab.orders GROUP BY region;
SELECT count(*) FROM ice.lab.orders VERSION AS OF <snapshot_id>;
DESCRIBE HISTORY default.delta_orders;
SELECT * FROM default.order_status WHERE status = 'SHIPPED';
```

For Hudi, read by path in a `--with hudi` job:
`spark.read.format("hudi").load("s3a://warehouse/lake/hudi/orders_cow")`.

### Changing versions

Edit `DELTA_VERSION`, `ICEBERG_VERSION` or `HUDI_VERSION` in `.env`, then run
`make lakehouse`. Artifact names follow the Spark minor version
automatically (`delta-spark_4.1_2.13`, `iceberg-spark-runtime-4.1_2.13`,
`hudi-spark4.1-bundle_2.13`). If you move the lab to Spark 4.0, they become
the `4.0` artifacts without other changes.

---

## 20. Project: real-time order tracking

A small but complete pipeline in `jobs/python/order_tracking/` (its own
README has the details). Run it all with `make test-orders`.

```
20_generate_order_events.py --wave N    events land as JSON files (3 waves over one simulated day)
              │
              ▼
21_track_orders_stream.py   Structured Streaming, availableNow trigger, checkpointed
   ├─► Iceberg ice.tracking.order_events   event log; MERGE on event_id = each event once
   ├─► Delta   default.order_status        current status per order, rebuilt from the log
   └─► Postgres order_tracking             serving table; upsert that never goes backwards
              │
              ▼
22_order_sla_report.py      delivery SLA by carrier/city → Parquet + Postgres order_sla,
                            and checks all three stores against the raw events
```

The test group runs: wave 1 → stream → wave 2 → stream → wave 3 → stream →
report. Watch Postgres between steps to see orders move along
(`make test-orders ARGS="--show-logs"` in one terminal, `make psql` in another):

```sql
select status, count(*) from order_tracking group by 1 order by 1;
```

What the numbers should look like (the generator is seeded, so they're stable):

| after | events stored | orders by status |
|---|---|---|
| wave 1 | 147 | 59 orders, mostly PAID/PACKED |
| wave 2 | 232 | 60 orders, 52 SHIPPED, 7 CANCELLED, 1 OUT_FOR_DELIVERY |
| wave 3 | 338 (339 sent, 1 duplicate) | 53 DELIVERED, 7 CANCELLED |

The design choices are the part to take to real projects:

- **Exactly-once results from at-least-once input.** Files can be re-sent and
  streams replay after failures. The event log inserts only unseen
  `event_id`s, and every other table is *derived* from the log, so
  reprocessing the same event changes nothing.
- **Late data.** One order's PACKED event arrives after the order already
  shipped. Current status is recomputed from all of that order's events,
  ordered by event time (with lifecycle rank breaking ties), so the late
  event updates the history without moving the status backwards. The
  Postgres upsert also has a guard: `WHERE t.status_time <= EXCLUDED.status_time`.
- **Each store does one job.** Iceberg is the append-only log and is
  engine-neutral. Delta holds mutable current state, and its change data
  feed doubles as an audit trail of status changes. Postgres is the serving
  layer an application reads.
- **Streaming on a schedule.** `trigger(availableNow=True)` processes
  whatever has arrived and stops, so a streaming job can run as a Kyuubi
  batch (or from Airflow, cron, ...) and still keep exactly-once state in
  its checkpoint. Switch to `processingTime="30 seconds"` for always-on.

The stream's status logic was also run on plain Spark 4.1.3 during
development, with Parquet standing in for the MERGE targets. Same three
waves, the duplicate dropped, ORD-0016 stayed DELIVERED, a rerun with no new
files processed 0 batches, and all 60 statuses matched the raw events.

Moving to Kafka later only changes the source line (`readStream.format("kafka")`
plus `from_json` with the same schema); everything in `process()` stays.

---

## 21. Hive ELT: TEXTFILE, ORC, Parquet, RCFile

`jobs/python/17_hive_elt.py` (`make test-hive`) is written the way a lot of
Hive and Cloudera jobs are: load raw files untouched, then transform them
with SQL inside the warehouse. Everything goes into database `elt` in the
Hive Metastore.

```
raw CSV folder ──► raw_orders           EXTERNAL, STORED AS TEXTFILE (nothing copied; every column a string)
                     │ try_cast + clean
                     ▼
                 orders_clean            STORED AS ORC         (CTAS)
                     │ INSERT OVERWRITE ... PARTITION (order_month)   dynamic partitions
                     ▼
                 orders_fact             STORED AS PARQUET, PARTITIONED BY (order_month)
                 orders_archive_rc       STORED AS RCFILE      (same rows, older Hive format)
                 monthly_summary         STORED AS ORC         (CTAS aggregate)
                 v_monthly_top           VIEW with a window function
                 + ANALYZE TABLE ... COMPUTE STATISTICS
```

At the end it prints what the metastore recorded for each table (input
format and location), and checks that revenue is identical in the Parquet
fact, the RCFile archive and the ORC summary.

Two lessons came out of testing this job on Spark 4.1.3, and both matter
when you bring old Hive SQL over:

- **ANSI mode is on.** `CAST('quantity' AS INT)` throws in Spark 4 instead of
  returning NULL. Raw text needs `try_cast`.
- **Header lines.** Spark's reader for Hive text tables can ignore
  `skip.header.line.count`, so the CSV header comes through as a row. The
  job filters it with `WHERE try_cast(order_id AS INT) IS NOT NULL`, which
  would also catch any other junk row.

Query the result with `make sql`:

```sql
SHOW TABLES IN elt;
SHOW PARTITIONS elt.orders_fact;
SELECT * FROM elt.v_monthly_top WHERE rank_in_month = 1;
DESCRIBE FORMATTED elt.orders_archive_rc;
```

---

## 22. Kafka: optional install

Kafka is an add-on. `make up` doesn't start it, so the lab stays light when
you don't need streaming. To add it:

```bash
make kafka-install
```

That starts one Kafka broker in KRaft mode (no ZooKeeper), creates the
topics, starts a web UI and makes sure Spark's Kafka connector jars are in
`/opt/lakehouse/kafka/`.

| | |
|---|---|
| broker, inside Docker | `kafka:9092` (what Spark and the client container use) |
| broker, from your Mac | `localhost:9094` |
| web UI | http://localhost:8090 (topics, messages, consumer groups) |
| topics | `orders.events`, `orders.enriched`, `orders.metrics` (compacted), `orders.alerts`, `orders.backfill`, 3 partitions each |

Other targets: `make kafka-status`, `make kafka-topics`, `make kafka-reset`
(deletes and recreates the topics, emptying them) and `make kafka-uninstall`.
Kafka keeps no volume, so recreating the container also empties the topics.
That's fine for a lab, and the streaming jobs handle it with `failOnDataLoss=false`.

In compose terms, the three services (`kafka`, `kafka-init`, `kafka-ui`)
carry `profiles: ["kafka"]`, which is why plain `docker compose up` skips them.

---

## 23. Streaming: Kafka producers, consumers and file events

### The pieces

```
                      ┌───────────── Python apps (client/producers/) ──────────────┐
                      │ order_producer.py  --sink kafka   -> orders.events          │
                      │ order_producer.py  --sink s3      -> data/stream/landing/   │
                      │ topic_tail.py <topic>             <- any topic (consumer)   │
                      └─────────────────────────────────────────────────────────────┘
Spark jobs (jobs/python/streaming/), all submitted through Kyuubi:

 30 Kafka consumer basic      orders.events ─► Parquet bronze, with partition/offset
 31 Kafka -> Delta + Postgres orders.events ─► Delta event table (MERGE on event_id) + Postgres status
 32 Kafka -> Kafka            orders.events ─► join customers ─► orders.enriched, orders.alerts   (Spark PUBLISHES)
 33 windowed metrics          orders.events ─► 5-min windows + watermark ─► orders.metrics + Postgres
 34 batch producer/consumer   Hive demo.orders ─► orders.backfill (batch write) ─► batch read-back
 35 file-event stream         data/stream/landing/*.json ─► Parquet silver, files archived after use
 39 verify                    batch job: checks 30-35's outputs for one producer run
```

The producer is the "order service": each order goes through CREATED →
PAID → PACKED → SHIPPED → OUT_FOR_DELIVERY → DELIVERED (or CANCELLED), one
JSON event per step, keyed by `order_id`. All of an order's events land in
the same partition, in order.

### Three ways to run any streaming job

Every job in `streaming/` takes the same flags (from `stream_common.py`,
shipped with `--py-files`):

| flag | trigger | when |
|---|---|---|
| none | micro-batch every 10 s, runs until the Kyuubi batch is killed | always-on |
| `--run-seconds N` | same, stops itself after N seconds | demos, smoke tests |
| `--available-now` | process everything available now, then stop | tests, scheduled runs |

Plus `--trigger-seconds N`, `--starting-offsets earliest|latest` (used only
without a checkpoint), `--max-offsets N` (records per micro-batch) and
`--reset` (delete the checkpoint and start over).

Checkpoints live in `s3a://data/stream/_checkpoints/<job>`. They're what
makes reruns pick up where they stopped instead of re-reading the topic.

### Always-on walkthrough

Four terminals, or tabs:

```bash
# 1. live producer, ~5 events/s
make produce

# 2. start three always-on jobs (each capped at 1 core so they all fit)
make stream-start
make stream-list            # they show RUNNING

# 3. watch Spark's output topics
make tail TOPIC=orders.enriched
make tail TOPIC=orders.metrics FOLLOW=1

# 4. watch Postgres change
make psql
shop=> select status, count(*) from kafka_order_status group by 1 order by 1;
shop=> select window_start, city, events, revenue_paid from order_metrics_live
       order by window_start desc limit 10;
```

Also open http://localhost:8090 for the topics and consumer lag, and the
Spark UI of a running job at http://localhost:4040 (4041, 4042 for the
others) to see its **Structured Streaming** tab. Stop everything with
Ctrl-C on the producer and `make stream-stop`.

For file events, `make produce-files` drops a JSON file into
`data/stream/landing/` every few seconds. Submit job 35 without
`--available-now` and watch the files move to `landing_archive/` as they're
processed. You can also upload a file through the RustFS console.

### What each job teaches

- **30** shows the raw shape of a Kafka record (`key`, `value` bytes,
  `partition`, `offset`, `timestamp`) and the usual first step: keep a
  "bronze" copy of every event as received.
- **31** handles at-least-once input. Kafka can redeliver, so the Delta
  table is written with `MERGE ... WHEN NOT MATCHED INSERT` on `event_id`,
  and Postgres status is recomputed from the stored events with an upsert
  that refuses to go backwards in time.
- **32** is Spark as a producer inside a stream: a stream-static join with a
  Postgres lookup, and two output queries (two topics, two checkpoints) from
  one input.
- **33** covers event time, a watermark and `outputMode("update")`. The
  watermark is 2 hours in the demo because the finite producer spreads event
  times over the last hour; in a live system you'd use minutes. The metrics
  topic is compacted, so it keeps the latest value per window/city key.
- **34** is Kafka without streaming: `df.write.format("kafka")` to publish a
  table, and `spark.read.format("kafka")` with `startingOffsets`/`endingOffsets`
  to read a fixed slice back. It includes Kafka headers.
- **35** is a file-event stream with `maxFilesPerTrigger`,
  `cleanSource=archive`, and the hidden `_metadata` column (which file each
  row came from).

### Tested before shipping

Kafka isn't reachable from where these were written, so the Kafka jobs get
their first real run on your machine; `make test-streaming` checks every one
of them. What could be run on Spark 4.1.3 was:

- job 35 in `--available-now` mode: 161 events from 3 files, 2 micro-batches,
  processed files archived
- job 35 always-on (`--run-seconds 40`, 5 s trigger) while files were dropped
  at t+9, 18 and 27 s: each file picked up in the next micro-batch, 166/166
  events written once, clean stop
- the producer's event generation and JSON format (the same parser reads it)

### Test it all

```bash
make kafka-install
make test-streaming          # producer -> jobs 30-35 (--available-now) -> 39 verify
```

The producer step tags its events with a fresh `run_id`, and job 39 checks
each output for exactly that run, so repeated runs don't interfere with each
other.

---

## 24. All execution payloads

`All_execution_payload.md` (readable) and `All_execution_payload.json` (for
tools) list every job in the lab: 32 Kyuubi batches plus 6 producer/consumer
commands. For each batch you get:

- the `kyuubi_submit.py` command
- the exact JSON body for `POST /api/v1/batches` (or the multipart parts for uploads)
- a ready-to-paste curl command
- a `kyuubi-ctl` YAML
- for streaming jobs, an always-on variant and how to stop it

They're generated from `client/job_catalog.py`, the same list the test runner
uses, and the JSON comes from the same `build_request()` function the client
uses to submit. Before shipping, every one of the 74 documented commands (CLI
and curl, including always-on variants) was run against a fake Kyuubi
endpoint that applies Kyuubi 1.12's validation rules, and each sent exactly
the documented body.

To add your own job: add an entry to `client/job_catalog.py`, then run
`make payloads`. It's in the test suite and the payload docs at once.

The same two files also list the 15 Airflow DAGs (trigger with make, the
Airflow CLI, Airflow's REST API or Python) and the 13 notebooks, with the
jobs each one runs. That part comes from `client/orchestration.py`, and
`make payloads` refuses to write the files if a DAG id, notebook or job
name in there doesn't exist.


---

## 25. Airflow: DAGs that run jobs through Kyuubi

Airflow is optional, like Kafka. It doesn't run Spark and it doesn't call
`spark-submit`. Each task is a thin client:

```
Airflow task --REST :10099--> Kyuubi --spark-submit--> Spark      (batch jobs)
Airflow task --JDBC :10009--> Kyuubi --SQL engine---> Spark      (SQL tasks)
```

That's the same rule as the rest of the lab: nobody but Kyuubi talks to the
Spark master.

### Start it

```bash
make up              # the core stack, if it isn't running
make airflow         # builds the image the first time (~3 min), then http://localhost:8085
make airflow-dags    # the DAGs it loaded, and any import errors
```

The lab runs `airflow standalone` (API server, scheduler, DAG processor and
triggerer in one container) with the LocalExecutor and its metadata database
in the lab's Postgres (database `airflow`, created by `pg-init`). Login is off:
the simple auth manager runs with `all_admins`, so the UI opens straight
away. Fine for a laptop, not for anything shared.

`airflow/dags`, `client/` and `jobs/` are mounted, not copied. Edit a DAG and
Airflow picks it up within about 30 seconds; no rebuild.

### The operators

`airflow/dags/kyuubi_ops.py` has two operators. They're written for
Airflow 3 (`airflow.sdk`), and they don't use any Kyuubi provider package,
just the lab's own client code.

**`KyuubiBatchOperator(job="delta")`** runs one job from
`client/job_catalog.py`. It builds the same request `kyuubi_submit.py` and
`make test` send (via `client/lab_runner.py`), posts it to Kyuubi, and then
streams the driver log into the Airflow task log line by line. The task:

- succeeds only if the batch ends in the state the catalog expects AND the
  job printed its success line (every sample job prints one after its own
  checks)
- is *skipped* (pink, not red) if Kafka isn't installed or the jar isn't built
- kills the Kyuubi batch if you stop the task in Airflow (mark failed, clear)
- returns `{name, ok, state, batch_id, secs}` as its XCom

Options you'll use: `args=[...]` replaces the job's own arguments,
`extra_args=[...]` appends, `conf={...}` adds Spark conf. All three are
templated. Every task in one DAG run shares a run id (`af` + the run's
timestamp), which the streaming jobs use to find "their" events.

**`KyuubiSqlOperator(sql=[...], checks=[...])`** runs SQL through Kyuubi's
JDBC port with PyHive, as user `airflow`. `lake=True` opens the session as
`airflow_lake` with Delta and Iceberg switched on (a separate engine, since
engine conf is fixed when the engine starts). `checks` are
`(query, rule)` pairs, for example `("SELECT count(*) FROM elt.orders_fact", ">0")`;
one failed check fails the task.

### The DAGs

| DAG | what it does |
|---|---|
| `kyuubi_tests_all` | every catalog job, group after group, then a pass/fail report. `make test` as a DAG |
| `kyuubi_tests_core` ... `kyuubi_tests_streaming` | one test group each (core, formats, hive, postgres, lakehouse, orders, streaming) |
| `kyuubi_run_any_job` | pick any job from a dropdown, add args and conf. The Airflow version of `kyuubi_submit.py` |
| `lakehouse_daily_elt` | S3 sensor, Hive ELT (TEXTFILE, ORC, partitioned Parquet, RCFILE), format conversions, file to Postgres, Postgres to files and to Postgres, Iceberg, Delta, Hudi, then SQL quality checks. Scheduled 02:00, created paused |
| `kafka_streaming_pipeline` | producer, four Kafka streams (`--available-now`), Spark batch backfill to Kafka, file-event stream, the verify job, then Python checks of the topics and Postgres |
| `kafka_live_streams` | starts jobs 31/32/33 always-on, waits for RUNNING, feeds them for N minutes, checks the outputs, and always kills the streams at the end |
| `kyuubi_sql_reporting` | SQL only: branch on whether `demo.orders` exists, CTAS into Parquet and ORC, a view, checks, copies into Iceberg and Delta, upsert into Postgres |
| `file_event_pipeline` | JSON files dropped into RustFS, a sensor that waits for them, the file-source stream, SQL straight on the Parquet folder, and a second run that processes nothing |
| `order_tracking_project` | the three-wave project from section 20, with SQL on the Delta status table and the Iceberg event log |

The test DAGs are generated from the job catalog, so a job you add there
shows up as a task without touching Airflow. Dependencies follow each job's
`after` list: if `pg-file-to-pg` fails, `pg-pg-to-files` turns
`upstream_failed` instead of failing for a confusing reason. In
`kyuubi_tests_all` the next group starts when the previous one is done,
pass or fail, so one broken job doesn't hide the rest. The last task,
`report`, prints a table of every task's state and fails the run if anything
failed.

Concurrency is capped at 2 tasks per DAG (`MAX_ACTIVE_TASKS_PER_DAG`) because
every job asks for 2 cores and the worker has 4. `kafka_live_streams` is the
exception: it runs three streams at 1 core each.

### Triggering

```bash
make airflow-run DAG=kyuubi_tests_formats
make airflow-run DAG=kyuubi_run_any_job CONF='{"job": "iceberg"}'
make airflow-run DAG=kafka_live_streams CONF='{"minutes": 3, "rate": 10}'
make airflow-test DAG=kyuubi_tests_hive       # runs in your terminal, logs inline
```

Or the UI: open a DAG, "Trigger", fill in the form. Or REST:

```bash
TOKEN=$(curl -s http://localhost:8085/auth/token | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
curl -s -X POST http://localhost:8085/api/v2/dags/kyuubi_run_any_job/dagRuns \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"logical_date": null, "conf": {"job": "hive-elt"}}'
```

`All_execution_payload.md` has these commands for every DAG. While a DAG
runs you can watch the same batches in Kyuubi's UI
(http://localhost:10099/ui): their names start with `airflow-`.

### Writing your own DAG

```python
from airflow.sdk import DAG
from kyuubi_ops import KyuubiBatchOperator, KyuubiSqlOperator
from lab_common import DEFAULT_ARGS, START, report

with DAG("my_pipeline", schedule=None, start_date=START, catchup=False,
         default_args=DEFAULT_ARGS):
    load = KyuubiBatchOperator(task_id="load", job="py-hive-etl")
    custom = KyuubiBatchOperator(
        task_id="custom",
        case=dict(name="my-job", resource="s3a://jobs/python/my_job.py",
                  remote=True, marker="MY JOB OK"),
        args=["--run-date", "{{ dag_run.run_after.strftime('%Y-%m-%d') }}"])
    check = KyuubiSqlOperator(task_id="check",
        checks=[("SELECT count(*) FROM demo.orders", ">0")])
    load >> custom >> check >> report()
```

`case=` takes the same fields as a catalog entry, so a job doesn't have to be
in the catalog to run from Airflow. Put templated values in `args`, not
inside `case` (only `args`, `extra_args`, `conf`, `job` and `run_id` are
templated). Use `dag_run.run_after` rather than `ds`: in Airflow 3 a manual
run can have no logical date. Upload the script first (`make upload`).

---

## 26. Jupyter: notebooks that call Kyuubi

```bash
make jupyter          # http://localhost:8888, no token
```

The Jupyter image has no Java and no Spark: just JupyterLab, pandas,
matplotlib and the client libraries (requests, PyHive, psycopg2, boto3,
kafka-python). Notebooks talk to Kyuubi exactly like the CLI does. The
`notebooks/` folder is mounted, so what you save stays on your Mac.

Everything goes through `client/kyuubi_lab.py`:

```python
import kyuubi_lab as lab
lab.services()                            # what's reachable from here
lab.run("iceberg")                        # catalog job via Kyuubi REST, waits, checks the marker
lab.sql("SELECT * FROM demo.orders")      # Kyuubi JDBC -> pandas DataFrame
lab.sql("SELECT * FROM ice.lab.orders.snapshots", lake=True)   # Delta + Iceberg session
lab.pg("SELECT * FROM order_tracking")    # Postgres
lab.s3_ls("data", "stream/landing/")      # RustFS
lab.produce(orders=20)                    # producer -> Kafka
lab.topic("orders.enriched")              # newest messages -> DataFrame
lab.submit_no_wait("kafka-enrich-publish", extra_args=[])     # always-on stream, returns batch id
lab.kill(batch_id)
lab.airflow_trigger("kafka_streaming_pipeline", conf={"orders": 25}, wait=True)
```

The notebooks run as Kyuubi user `notebook`, so their SQL engine is separate
from yours (`anji`) and from Airflow's. The first `sql()` call starts that
engine, which takes 20-40 seconds; after that queries are quick.

| notebook | covers |
|---|---|
| 00_start_here | service check, the catalog, what a payload looks like, first job |
| 01_kyuubi_rest_basics | raw `requests`: JSON batch, multipart upload, polling, logs, list, kill, an expected failure |
| 02_kyuubi_sql_jdbc | CTAS into Parquet and ORC, DESCRIBE FORMATTED, EXPLAIN, SET, window functions, views |
| 03_file_formats_and_hive_elt | the format round trip, SQL straight on Parquet/ORC/JSON/Avro folders, the Hive ELT tables including RCFILE |
| 04_postgres_pipelines | file to Postgres, Postgres to files, Postgres to Postgres, and proof the upsert is idempotent |
| 05_lakehouse_iceberg_delta_hudi | Iceberg snapshots and time travel, Delta history, time travel and change data feed, the Hudi timeline |
| 06_kafka_produce_consume | Python producer into Kafka, Spark consumer into Parquet, Kafka into Delta + Postgres |
| 07_spark_publishes_to_kafka | enrichment and alerts, windowed metrics, batch backfill, then the verify job |
| 08_file_event_streaming | file drops, the file-source stream, a no-op rerun, and live mode with a kill |
| 09_order_tracking_project | the three waves and their Iceberg, Delta and Postgres results |
| 10_always_on_streams | three always-on streams: start, feed, watch Postgres and Kafka, stop |
| 11_airflow_from_notebook | list DAGs, trigger them, read task states (needs `make airflow`) |
| 12_end_to_end_all | every catalog job with a pass/fail table and chart |

Notebooks 06, 07 and 10 need Kafka. 12 skips the Kafka jobs if Kafka isn't
there, and the JVM jobs until you `make jars && make upload`.

Run the notebooks in order the first time: later ones read tables earlier
ones create (02 needs `demo.orders`, which it builds if it's missing; 04
uses the formats output from 03).

---

## 27. Airflow and Jupyter: troubleshooting

**`make airflow` hangs on "waiting for Airflow".** The first start migrates
the metadata database, which takes a minute or two. `make airflow-logs`
shows progress. If it says it can't connect to the `airflow` database, your
Postgres volume predates the Airflow add-on: `make pg-init` creates the
database, then `make airflow` again.

**A DAG is missing from the UI.** `make airflow-dags` prints import errors.
The usual one is a typo in a DAG file; the DAG processor re-reads files
every 30 seconds.

**A task is pink (skipped).** On purpose: Kafka isn't installed
(`make kafka-install`) or the Java/Scala jar isn't built (`make jars && make upload`).
The task log says which.

**A task failed with "finished but never printed ...".** The Spark job
exited 0 but its own checks didn't run to the end. Read the task log: it's
the full driver log from Kyuubi.

**`kafka_live_streams` was stopped half way.** The `stop_streams` task runs
whatever happened before it and kills the three batches. If you deleted the
DAG run while it was going, kill them yourself: `make stream-list`,
`make stream-stop`.

**Runs sit in "queued".** The DAG is paused. `make airflow-run` and
`lab.airflow_trigger()` unpause it for you; in the UI, flip the toggle.
`lakehouse_daily_elt` is created paused on purpose because it has a schedule.

**A notebook's `sql()` hangs for half a minute.** That's the engine
starting for user `notebook`. Only the first call waits.

**`lab.airflow_trigger` raises a connection error.** Airflow isn't running
(`make airflow`). Jupyter reaches it at `http://airflow:8080` inside Docker;
from your Mac it's `http://localhost:8085`.

**Notebooks or DAGs can't import `kyuubi_lab` / `lab_runner`.** Both images
put `client/` on `PYTHONPATH`. If you run a notebook from somewhere else,
start it from the lab folder or `sys.path.insert(0, "client")`.

### How this was checked before shipping

All 15 DAGs were loaded by Airflow 3.3.2 with no import errors, and each
was run end to end with `airflow dags test` against a fake Kyuubi REST server
(same request validation as Kyuubi 1.12, per-batch states, kill support),
a fake S3 and a fake Kafka. That covered the success paths, a failing job
turning its dependents `upstream_failed`, Kafka-less skips, the run-any-job
form with args and conf, and the always-on DAG starting three streams and
killing all three at the end. Notebooks 00-12 were executed top to bottom
with the same fakes; notebook 11 ran against a real local Airflow 3.3.2
scheduler, triggering DAGs whose tasks then submitted to the fake Kyuubi.
What the fakes can't prove is Spark's own behaviour: that part was tested
earlier with PySpark 4.1.3 (see section 23), and `kyuubi_tests_all` is the
quickest way to confirm it on your machine.
