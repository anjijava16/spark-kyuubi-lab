#!/bin/sh
# Creates the "shop" database + "app" user used by the Spark jobs, seeds the
# customers table, installs pgvector + jev and the AI sample tables, and
# creates Airflow's database. Safe to run any number of times.
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

-- Airflow's metadata database (only used if you run `make airflow`)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE ROLE airflow LOGIN PASSWORD 'airflow';
  END IF;
END $$;
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SQL

echo ">> creating tables in shop"
PGUSER=app PGPASSWORD=apppass PGDATABASE=shop psql -v ON_ERROR_STOP=1 -f /sql/shop-schema.sql

echo ">> pgvector + jev in shop (superuser), jev.api_url = ${JEV_API_URL:-http://jev-local:8787/v1/systemone}"
PGDATABASE=shop psql -v ON_ERROR_STOP=1 \
  -v jev_api_url="${JEV_API_URL:-http://jev-local:8787/v1/systemone}" -f /sql/ai-extensions.sql

echo ">> AI tables in shop (people, tickets, products, kb_articles, product_embeddings, ticket_routing)"
PGUSER=app PGPASSWORD=apppass PGDATABASE=shop psql -v ON_ERROR_STOP=1 -q -f /sql/ai-schema.sql

echo ">> shop database ready:"
PGUSER=app PGPASSWORD=apppass PGDATABASE=shop psql -c '\dt'
