.PHONY: up jars upload lakehouse pg-init test test-core test-formats test-postgres test-lake \
        test-orders sql sql-lake psql ps logs down clean all

# docker compose run for the test client; ARGS are passed to run_tests.py
CLIENT = docker compose --profile tools run --rm kyuubi-client python client/run_tests.py

## start the core stack (builds images the first time; ~5-10 min)
up:
	docker compose up -d --build
	docker compose ps

## build the Java/Scala jar into jobs/jars/
jars:
	docker compose --profile tools run --rm jar-builder

## (re)upload jobs/ and data/ to RustFS
upload:
	docker compose run --rm storage-init

## (re)download Delta/Iceberg/Hudi jars after changing versions in .env
lakehouse:
	docker compose run --rm lakehouse-init

## (re)create the shop database and seed data in Postgres
pg-init:
	docker compose run --rm pg-init

## run every test; narrow with e.g. make test ARGS="--only delta,hudi --show-logs"
test:
	$(CLIENT) $(ARGS)

test-core:
	$(CLIENT) --group core $(ARGS)
test-formats:
	$(CLIENT) --group formats $(ARGS)
test-postgres:
	$(CLIENT) --group postgres $(ARGS)
test-lake:
	$(CLIENT) --group lakehouse $(ARGS)
test-orders:
	$(CLIENT) --group orders $(ARGS)

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
	docker compose ps

logs:
	docker compose logs -f kyuubi

## first run, end to end
all: up jars upload test

down:
	docker compose down

## stop and delete all data (buckets, metastore DB, lakehouse jars, maven cache)
clean:
	docker compose --profile tools down -v
