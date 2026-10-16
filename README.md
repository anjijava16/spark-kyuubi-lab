# spark-kyuubi-lab

Spark 4.1 standalone cluster, Apache Kyuubi 1.12, RustFS and a Hive Metastore,
all in one `docker compose`. You upload Python scripts and Java/Scala jars to
RustFS, then run them through Kyuubi's REST API instead of `spark-submit`.

```bash
make up        # build and start everything (first time: 5-10 min)
make jars      # compile the sample Java + Scala jar
make upload    # copy jobs/ and data/ into RustFS
make test      # run every sample job through Kyuubi, print pass/fail
make sql       # SQL shell over Kyuubi JDBC
```

What the 34 tests cover (run a group with `make test-<group>`):

| group | examples |
|---|---|
| core | Python, Java and Scala jobs; uploads; `--py-files`; Hive tables; an expected failure |
| formats | CSV → Parquet → ORC → JSON → Avro → CSV, round trip must be identical |
| postgres | file → Postgres, Postgres → files (parallel + pushdown reads), Postgres → Postgres upsert |
| lakehouse (`make test-lake`) | Iceberg, Delta Lake and Hudi: MERGE, deletes, time travel, schema evolution, CDC/incremental |
| hive | Hive SQL ELT: external TEXTFILE, ORC, partitioned Parquet, RCFile, CTAS, views |
| orders | real-time order tracking: streaming into an Iceberg event log, Delta status table and Postgres, plus an SLA report |
| streaming | Kafka producer → Spark consumer, Spark → Kafka publisher (stream and batch), windowed metrics, file-event streams |

Kafka is optional: `make kafka-install` adds a broker, topics and a UI
(http://localhost:8090). Streaming jobs run always-on, for N seconds, or
`--available-now`; `make produce` + `make stream-start` gives you a live demo.

Every job's CLI command, REST payload and curl example is in
**All_execution_payload.md** (and `.json`).

Jobs that use a table format are submitted with `--with iceberg`, `--with delta`,
`--with delta,iceberg` or `--with hudi`.

| | |
|---|---|
| Spark master | http://localhost:8080 |
| RustFS console | http://localhost:9101 (rustfsadmin / rustfsadmin123) |
| RustFS S3 API | http://localhost:9100 (rustfs:9000 inside Docker) |
| Postgres (shop DB) | `make psql`, or localhost:15432 user `app` / `apppass` |
| History server | http://localhost:18080 |
| Kyuubi web UI | http://localhost:10099/ui (sessions, engines, batches) |
| Kyuubi REST | http://localhost:10099/api/v1/batches |
| Kyuubi JDBC | jdbc:hive2://localhost:10009/default (Thrift, not a web page) |
| Kafka (optional) | localhost:9094 from your Mac; UI http://localhost:8090 |

Submit your own job:

```bash
pip install requests
python client/kyuubi_submit.py --url http://localhost:10099 \
  submit s3a://jobs/python/02_wordcount_s3.py --remote \
  -- s3a://data/input/sample.txt s3a://data/output/wc
```

Read **Understand.md** for how the pieces fit, the full step-by-step,
troubleshooting, and what changes when you move this to Cloudera.

Needs Docker Desktop with 8 GB RAM / 4 CPUs. No Java, Maven or Spark on the host.
