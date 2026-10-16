#!/bin/sh
# Creates the "shop" database + "app" user used by the Spark jobs, then
# seeds a customers table. Safe to run any number of times.
# The metastore keeps its own database ("metastore"); jobs never touch it.
set -eu

echo ">> waiting for postgres"
until pg_isready -q; do sleep 1; done

psql -v ON_ERROR_STOP=1 <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app') THEN
    CREATE ROLE app LOGIN PASSWORD 'apppass';
  END IF;
END $$;
SELECT 'CREATE DATABASE shop OWNER app'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'shop')\gexec
SQL

echo ">> creating tables in shop"
PGUSER=app PGPASSWORD=apppass PGDATABASE=shop psql -v ON_ERROR_STOP=1 -f /sql/shop-schema.sql

echo ">> shop database ready:"
PGUSER=app PGPASSWORD=apppass PGDATABASE=shop psql -c '\dt'
