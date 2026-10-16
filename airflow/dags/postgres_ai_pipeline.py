"""
postgres_ai_pipeline - pgvector + jev in one pipeline, every Spark step a
Kyuubi batch, every AI step inside Postgres.

  check_extensions   Postgres: vector + jev installed, where jev.api_url points
  embed_products     Kyuubi batch 18: Spark embeds products -> pgvector (+ Spark == SQL check)
  semantic_search    Postgres: nearest products for the `search` param (cosine <=>, HNSW index)
  route_tickets      Kyuubi batch 19: one JDBC query with jev_choice / jev_prob /
                     jev_score_norm + nearest KB article -> Parquet + Hive + Postgres
  lake_report        Kyuubi SQL on Hive table ai.ticket_routing (+ checks)
  escalations        Postgres: angriest tickets with their suggested article
  report

jev answers come from the offline jev-local stand-in unless you set
JEV_API_URL / TYPESAFE_API_KEY in .env (Understand.md section 28).
"""
import os

from airflow.sdk import DAG, Param, task

from kyuubi_ops import KyuubiSqlOperator
from lab_common import DEFAULT_ARGS, START, TAGS, job, report


def pg_rows(sql, params=None):
    import psycopg2
    conn = psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")), dbname="shop",
                            user="app", password="apppass")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


with DAG(
    dag_id="postgres_ai_pipeline",
    description="Spark -> pgvector embeddings, semantic search, jev routing -> lake, Kyuubi SQL report",
    doc_md=__doc__,
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS, max_active_runs=1,
    params={"search": Param("warm clothes for cold winter", type="string",
                            description="semantic search over products")},
    tags=TAGS + ["ai", "pgvector", "jev", "postgres"],
) as dag:

    @task
    def check_extensions():
        rows = pg_rows("SELECT extname, extversion FROM pg_extension "
                       "WHERE extname IN ('vector', 'jev') ORDER BY 1")
        url = pg_rows("SELECT current_setting('jev.api_url', true) AS url")[0]["url"]
        print(rows, "jev.api_url =", url)
        if len(rows) != 2:
            raise RuntimeError("pgvector or jev missing in shop: run `make pg-init`")
        return {"extensions": {r["extname"]: r["extversion"] for r in rows}, "jev_api_url": url}

    embed = job("pgvector-embeddings", task_id="embed_products")

    @task
    def semantic_search(params=None):
        q = params["search"]
        hits = pg_rows("""SELECT p.name, p.price::float8 AS price,
                                 round((e.embedding <=> lab_embed(%s))::numeric, 4)::float8 AS distance
                          FROM product_embeddings e JOIN products p ON p.id = e.product_id
                          ORDER BY e.embedding <=> lab_embed(%s) LIMIT 5""", (q, q))
        print(f"'{q}':")
        for h in hits:
            print(f"  {h['distance']:.4f}  {h['name']}  ({h['price']})")
        if not hits:
            raise RuntimeError("product_embeddings is empty")
        return hits

    route = job("jev-tickets-to-lake", task_id="route_tickets")

    lake_report = KyuubiSqlOperator(
        task_id="lake_report",
        sql="SELECT team, count(*) AS tickets, round(avg(angry_p), 3) AS avg_angry, "
            "round(avg(urgency), 3) AS avg_urgency FROM ai.ticket_routing GROUP BY team ORDER BY tickets DESC",
        checks=[("SELECT count(*) FROM ai.ticket_routing", ">0"),
                ("SELECT count(*) FROM ai.ticket_routing "
                 "WHERE team NOT IN ('billing', 'technical', 'security', 'sales')", "==0")],
    )

    @task
    def escalations():
        rows = pg_rows("""SELECT r.ticket_id, t.subject, r.team, round(r.angry_p::numeric, 2)::float8 AS angry,
                                 k.title AS article
                          FROM ticket_routing r JOIN tickets t ON t.id = r.ticket_id
                          LEFT JOIN kb_articles k ON k.id = r.kb_article
                          ORDER BY r.angry_p DESC LIMIT 5""")
        for r in rows:
            print(f"  #{r['ticket_id']:<3} {r['angry']:.2f}  {r['team']:9}  {r['subject']}  ->  {r['article']}")
        return rows

    check_extensions() >> embed >> semantic_search() >> route >> lake_report >> escalations() >> report()
