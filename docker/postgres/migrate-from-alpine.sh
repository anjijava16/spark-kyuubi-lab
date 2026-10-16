#!/bin/sh
# Copy every database from the lab's OLD Postgres volume (postgres:16-alpine,
# volume <project>_pg-data) into the NEW one (Debian + pgvector + jev, volume
# <project>_pg16-data). Run from the lab folder: make pg-migrate
#
# Why not reuse the old data directory: alpine (musl) and Debian (glibc) sort
# text differently, so btree indexes on text columns built under one would be
# silently wrong under the other. A dump + restore rebuilds them.
set -eu
OLD_VOLUME="${OLD_PG_VOLUME:-spark-kyuubi-lab_pg-data}"
SUPER="${PG_SUPER:-hive}"

if ! docker volume inspect "$OLD_VOLUME" >/dev/null 2>&1; then
  echo "no old volume $OLD_VOLUME: nothing to migrate"
  exit 0
fi
mkdir -p backups
echo ">> stopping services that hold connections"
docker compose stop hive-metastore kyuubi >/dev/null 2>&1 || true
docker compose --profile airflow stop airflow >/dev/null 2>&1 || true

echo ">> starting the old Postgres (alpine) on $OLD_VOLUME, read for the dump"
docker rm -f lab-pg-old >/dev/null 2>&1 || true
docker run -d --name lab-pg-old -v "$OLD_VOLUME":/var/lib/postgresql/data postgres:16-alpine >/dev/null
until docker exec lab-pg-old pg_isready -q -U "$SUPER"; do sleep 1; done
docker exec lab-pg-old pg_dumpall -U "$SUPER" --clean --if-exists > backups/pg-data-alpine.sql
docker rm -f lab-pg-old >/dev/null
echo ">> dump: backups/pg-data-alpine.sql ($(wc -c < backups/pg-data-alpine.sql) bytes)"

echo ">> restoring into the new Postgres"
docker compose up -d --build --wait postgres
docker compose exec -T postgres psql -U "$SUPER" -d postgres < backups/pg-data-alpine.sql > backups/pg-restore.log 2>&1 || true
echo ">> errors during restore (role $SUPER already existing is expected):"
grep -c ERROR backups/pg-restore.log || true

echo ">> pgvector + jev + AI tables"
docker compose run --rm pg-init
docker compose up -d
echo ">> done. The old volume is still there; remove it when happy: docker volume rm $OLD_VOLUME"
