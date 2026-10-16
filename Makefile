.PHONY: up jars upload lakehouse pg-init test test-core test-formats test-hive test-postgres \
        test-lake test-orders test-streaming sql sql-lake psql ps logs down clean all payloads \
        kafka-install kafka-uninstall kafka-status kafka-reset kafka-topics \
        produce produce-files tail stream-start stream-list stream-stop

# client container (Python + requests + kafka-python + boto3), on the lab network
CLIENT_RUN = docker compose --profile tools run --rm kyuubi-client
CLIENT     = $(CLIENT_RUN) python client/run_tests.py
KS         = $(CLIENT_RUN) python client/kyuubi_submit.py
ST         = s3a://jobs/python/streaming
COMMON     = --py-files $(ST)/stream_common.py

## start the core stack (builds images the first time; ~5-10 min). Kafka is optional: make kafka-install
up:
	docker compose up -d --build
	docker compose ps

## build the Java/Scala jar into jobs/jars/
jars:
	docker compose --profile tools run --rm jar-builder

## (re)upload jobs/ and data/ to RustFS
upload:
	docker compose run --rm storage-init

## (re)download Delta/Iceberg/Hudi/Kafka-connector jars after changing versions in .env
lakehouse:
	docker compose run --rm lakehouse-init

## (re)create the shop database and its tables in Postgres
pg-init:
	docker compose run --rm pg-init

# ---------------------------------------------------------------------------
# Kafka (optional add-on)
# ---------------------------------------------------------------------------

## install Kafka: 1 KRaft broker + topics + web UI (http://localhost:8090) + Spark's Kafka connector
kafka-install:
	docker compose --profile kafka up -d kafka kafka-ui
	docker compose --profile kafka run --rm kafka-init
	docker compose run --rm lakehouse-init
	@echo ""
	@echo "Kafka ready: kafka:9092 inside Docker, localhost:9094 from your Mac, UI http://localhost:8090"

## remove Kafka (containers only; Spark's connector jars stay, they're tiny)
kafka-uninstall:
	docker compose --profile kafka rm -sf kafka kafka-ui kafka-init

kafka-status:
	docker compose --profile kafka ps kafka kafka-ui
	docker compose --profile kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

## delete and recreate the lab topics (empties them)
kafka-reset:
	for t in orders.events orders.enriched orders.metrics orders.alerts orders.backfill; do \
	  docker compose --profile kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic $$t --if-exists; done
	sleep 3
	docker compose --profile kafka run --rm kafka-init

kafka-topics:
	docker compose --profile kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

# ---------------------------------------------------------------------------
# Streaming playground (always-on). Open http://localhost:8090 and `make psql` to watch.
# ---------------------------------------------------------------------------

## live producer: ~5 order events/s into Kafka orders.events until Ctrl-C (RATE=10 to change)
produce:
	$(CLIENT_RUN) python client/producers/order_producer.py --forever --rate $(or $(RATE),5)

## live file dropper: a JSON events file into data/stream/landing/ every ~5 s until Ctrl-C
produce-files:
	$(CLIENT_RUN) python client/producers/order_producer.py --sink s3 --forever --rate $(or $(RATE),5)

## print newest messages of a topic: make tail TOPIC=orders.enriched  (FOLLOW=1 to keep watching)
tail:
	$(CLIENT_RUN) python client/producers/topic_tail.py $(or $(TOPIC),orders.enriched) --last 5 $(if $(FOLLOW),--follow,)

## submit the always-on streaming jobs (they keep running; stop with make stream-stop)
stream-start:
	@# 1 core each so three live streams + the SQL engine fit on the 4-core worker
	$(KS) submit $(ST)/31_kafka_to_delta_postgres.py --remote --with kafka,delta $(COMMON) --no-wait --name live-31-kafka-to-delta-pg --conf spark.cores.max=1
	$(KS) submit $(ST)/32_kafka_enrich_publish.py   --remote --with kafka       $(COMMON) --no-wait --name live-32-enrich-publish     --conf spark.cores.max=1
	$(KS) submit $(ST)/33_kafka_windowed_metrics.py --remote --with kafka       $(COMMON) --no-wait --name live-33-windowed-metrics   --conf spark.cores.max=1
	@echo "running; watch: make stream-list, http://localhost:4040-4045, make tail TOPIC=orders.metrics FOLLOW=1"

stream-list:
	$(KS) list

## kill every RUNNING batch whose name starts with live-
stream-stop:
	$(CLIENT_RUN) sh -c 'python client/kyuubi_submit.py list | awk "/RUNNING/ && /live-/ {print \$$1}" | xargs -r -n1 python client/kyuubi_submit.py kill'

# ---------------------------------------------------------------------------
# Tests: make test ARGS="--only delta,hudi --show-logs"
# ---------------------------------------------------------------------------
test:
	$(CLIENT) $(ARGS)
test-core:
	$(CLIENT) --group core $(ARGS)
test-formats:
	$(CLIENT) --group formats $(ARGS)
test-hive:
	$(CLIENT) --group hive $(ARGS)
test-postgres:
	$(CLIENT) --group postgres $(ARGS)
test-lake:
	$(CLIENT) --group lakehouse $(ARGS)
test-orders:
	$(CLIENT) --group orders $(ARGS)
test-streaming:
	$(CLIENT) --group streaming $(ARGS)

## regenerate All_execution_payload.md / .json from client/job_catalog.py
payloads:
	python3 client/gen_payloads.py

## SQL shell through Kyuubi JDBC (Hive tables)
sql:
	docker compose exec kyuubi /opt/kyuubi/bin/beeline -u 'jdbc:hive2://localhost:10009/' -n anji

## SQL shell with Delta + Iceberg enabled (separate engine, user "lake")
sql-lake:
	docker compose exec kyuubi /opt/kyuubi/bin/beeline -n lake -u 'jdbc:hive2://localhost:10009/;#spark.driver.extraClassPath=/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*;spark.executor.extraClassPath=/opt/lakehouse/delta/*:/opt/lakehouse/iceberg/*;spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog;spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog;spark.sql.catalog.ice.type=hive;spark.sql.catalog.ice.uri=thrift://hive-metastore:9083;spark.sql.catalog.ice.warehouse=s3a://warehouse/iceberg'

## psql into the shop database
psql:
	docker compose exec postgres psql -U app -d shop

ps:
	docker compose --profile kafka ps

logs:
	docker compose logs -f kyuubi

## first run, end to end (Kafka tests are skipped unless you ran make kafka-install)
all: up jars upload test

down:
	docker compose --profile kafka --profile tools down

## stop and delete all data (buckets, metastore DB, connector jars, maven cache)
clean:
	docker compose --profile kafka --profile tools down -v
