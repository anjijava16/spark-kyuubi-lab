"""
kyuubi_sql_reporting - no spark-submit at all: everything goes through
Kyuubi's Thrift/JDBC port (10009), the way a BI tool or dbt would use it.
Kyuubi keeps one Spark SQL engine per user and reuses it across tasks.

  check_source      SQL: does demo.orders exist?
  route             branch: build it with the py-hive-etl batch, or go straight on
  load_source       (only if missing) Kyuubi batch 03_hive_etl.py
  build_reports     CTAS into a `reporting` database (Parquet + ORC tables, a view)
  quality           checks: row counts, revenue reconciles with the source
  lake_copy         same session type with Delta + Iceberg on: copy the report into
                    an Iceberg table and a Delta table, read Iceberg snapshots
  to_postgres       pull the small report from Kyuubi, upsert it into Postgres
                    (shop.report_region_month) for apps to read
  report
"""
from airflow.sdk import DAG, task

from kyuubi_ops import KyuubiSqlOperator, kyuubi_sql
from lab_common import DEFAULT_ARGS, START, TAGS, job, report

REVENUE_GAP = """
SELECT abs((SELECT sum(revenue) FROM reporting.region_month)
         - (SELECT round(sum(amount), 2) FROM demo.orders))
"""

with DAG(
    dag_id="kyuubi_sql_reporting",
    description="SQL-only pipeline over Kyuubi JDBC: CTAS, checks, Iceberg/Delta copies, Postgres serving table",
    doc_md=__doc__,
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS, max_active_runs=1,
    tags=TAGS + ["sql", "jdbc", "hive", "iceberg", "delta", "postgres"],
) as dag:

    @task
    def check_source():
        cols, rows = kyuubi_sql("SHOW DATABASES")
        if "demo" not in {r[0] for r in rows}:
            return False
        cols, rows = kyuubi_sql("SHOW TABLES IN demo")
        names = {r[cols.index("tableName")] for r in rows} if "tableName" in cols else {r[1] for r in rows}
        return "orders" in names

    @task.branch
    def route(exists: bool):
        print("demo.orders exists" if exists else "demo.orders missing -> building it")
        return "source_ready" if exists else "load_source"

    @task
    def source_ready():
        print("source table present")

    load_source = job("py-hive-etl", task_id="load_source")

    build_reports = KyuubiSqlOperator(
        task_id="build_reports",
        trigger_rule="none_failed_min_one_success",
        sql=[
            "CREATE DATABASE IF NOT EXISTS reporting",
            "DROP TABLE IF EXISTS reporting.region_month",
            """CREATE TABLE reporting.region_month USING parquet AS
               SELECT date_format(order_date, 'yyyy-MM') AS month, region,
                      count(*) AS orders, sum(quantity) AS units,
                      round(sum(amount), 2) AS revenue
               FROM demo.orders GROUP BY 1, 2""",
            "DROP TABLE IF EXISTS reporting.product_rank",
            """CREATE TABLE reporting.product_rank USING orc AS
               SELECT product, round(sum(amount), 2) AS revenue,
                      dense_rank() OVER (ORDER BY sum(amount) DESC) AS rnk
               FROM demo.orders GROUP BY product""",
            """CREATE OR REPLACE VIEW reporting.v_top_products AS
               SELECT * FROM reporting.product_rank WHERE rnk <= 3""",
            "SELECT * FROM reporting.v_top_products ORDER BY rnk",
        ],
    )

    quality = KyuubiSqlOperator(
        task_id="quality",
        checks=[
            ("SELECT count(*) FROM reporting.region_month", ">0"),
            ("SELECT count(*) FROM reporting.v_top_products", ">=1"),
            (REVENUE_GAP, "<0.05"),
        ],
    )

    lake_copy = KyuubiSqlOperator(
        task_id="lake_copy", lake=True,
        sql=[
            "CREATE NAMESPACE IF NOT EXISTS ice.reporting",
            "DROP TABLE IF EXISTS ice.reporting.region_month PURGE",
            "CREATE TABLE ice.reporting.region_month USING iceberg AS SELECT * FROM reporting.region_month",
            "INSERT INTO ice.reporting.region_month SELECT month, 'ALL', sum(orders), sum(units), sum(revenue) "
            "FROM reporting.region_month GROUP BY month",
            # CREATE OR REPLACE keeps the Delta history: each run is a new version
            "CREATE OR REPLACE TABLE default.delta_region_month USING delta "
            "LOCATION 's3a://warehouse/lake/delta/region_month' AS SELECT * FROM reporting.region_month",
            "SELECT snapshot_id, operation, summary['added-records'] AS added "
            "FROM ice.reporting.region_month.snapshots ORDER BY committed_at",
        ],
        checks=[
            ("SELECT count(*) FROM ice.reporting.region_month.snapshots", "==2"),
            ("SELECT count(*) FROM default.delta_region_month", ">0"),
        ],
    )

    @task
    def to_postgres():
        import psycopg2
        import os
        cols, rows = kyuubi_sql("SELECT month, region, orders, units, revenue FROM reporting.region_month")
        conn = psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), dbname="shop",
                                user="app", password="apppass")
        with conn, conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS report_region_month (
                             month text, region text, orders bigint, units bigint,
                             revenue numeric(14,2), loaded_at timestamptz DEFAULT now(),
                             PRIMARY KEY (month, region))""")
            cur.executemany("""INSERT INTO report_region_month (month, region, orders, units, revenue)
                               VALUES (%s, %s, %s, %s, %s)
                               ON CONFLICT (month, region) DO UPDATE SET
                                 orders = EXCLUDED.orders, units = EXCLUDED.units,
                                 revenue = EXCLUDED.revenue, loaded_at = now()""", rows)
        conn.close()
        print(f"upserted {len(rows)} rows into shop.report_region_month")
        return len(rows)

    branch = route(check_source())
    branch >> [source_ready(), load_source] >> build_reports
    build_reports >> quality >> [lake_copy, to_postgres()] >> report()
