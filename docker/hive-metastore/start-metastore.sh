#!/bin/bash
# The stock image runs `schematool -initSchema` on every start, which fails
# the second time because the tables already exist. Check first, init once.
set -euo pipefail

DB_TYPE="${DB_TYPE:-postgres}"
export HIVE_CONF_DIR=/opt/hive/conf

echo ">> Checking metastore schema in ${DB_TYPE}..."
if /opt/hive/bin/schematool -dbType "${DB_TYPE}" -info > /tmp/schema-info.log 2>&1; then
  echo ">> Schema already exists:"
  grep -i "schema version" /tmp/schema-info.log || true
else
  echo ">> No schema yet, initializing (first start only)"
  /opt/hive/bin/schematool -dbType "${DB_TYPE}" -initSchema
fi

echo ">> Starting Hive Metastore on 9083"
exec /opt/hive/bin/hive --service metastore
