"""
19_jev_tickets_to_lake.py [output_root]

jev + pgvector inside a Spark pipeline. Spark sends ONE query to Postgres
(JDBC); Postgres does the AI work per row and Spark lands the result:

  jev_choice(t, 'which team should handle this?', ARRAY[...])   routing
  jev_prob(t, 'the customer is angry')                         escalation signal
  jev_score_norm(t, 'how urgent is this?', ARRAY[...])         0..1 urgency
  nearest kb_articles row by  embedding <=> lab_embed(ticket text)   suggested answer

then
  -> Parquet   s3a://data/output/ai/ticket_routing
  -> Hive      ai.ticket_routing  (query it from Kyuubi SQL / beeline)
  -> Postgres  ticket_routing     (upsert, for an app to read)

Where jev sends its questions is a Postgres setting (jev.api_url, set per
database by pg-init: the offline jev-local service by default, TypeSafe if you
configure a key). --jev-api-url overrides it for this job's connection.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ap = argparse.ArgumentParser()
ap.add_argument("output_root", nargs="?", default="s3a://data/output/ai")
ap.add_argument("--jev-api-url", default=None)
ap.add_argument("--database", default="ai")
args = ap.parse_args()

TEAMS = ["billing", "technical", "security", "sales"]
URGENCY = ["low", "medium", "high", "urgent"]

spark = (SparkSession.builder.appName("19-jev-tickets-to-lake")
         .enableHiveSupport().getOrCreate())
c = spark.conf
PG = {"url": c.get("spark.lab.pg.url"), "user": c.get("spark.lab.pg.user"),
      "password": c.get("spark.lab.pg.password"), "driver": "org.postgresql.Driver"}

arr = lambda xs: "ARRAY[" + ", ".join(f"'{x}'" for x in xs) + "]"   # noqa: E731
QUERY = f"""
    SELECT t.id AS ticket_id, t.customer_id, t.subject,
           jev_choice(t, 'which team should handle this?', {arr(TEAMS)}) AS team,
           jev_prob(t, 'the customer is angry')                          AS angry_p,
           jev_score_norm(t, 'how urgent is this?', {arr(URGENCY)})       AS urgency,
           (SELECT k.id FROM kb_articles k
             ORDER BY k.embedding <=> lab_embed(t.subject || ' ' || t.body) LIMIT 1) AS kb_article
    FROM tickets t
"""
reader = spark.read.format("jdbc").options(**PG).option("query", QUERY)
if args.jev_api_url:
    reader = reader.option("sessionInitStatement", f"SET jev.api_url = '{args.jev_api_url}'")
routed = reader.load().withColumn("routed_at", F.current_timestamp()).cache()

n = routed.count()
n_tickets = spark.read.format("jdbc").options(**PG).option("query", "SELECT count(*) AS n FROM tickets") \
                 .load().first()["n"]
print(f"jev judged {n} tickets (Postgres has {n_tickets})")
assert n == n_tickets and n > 0

bad_team = routed.where(~F.col("team").isin(TEAMS)).count()
bad_p = routed.where((F.col("angry_p") < 0) | (F.col("angry_p") > 1) |
                     (F.col("urgency") < 0) | (F.col("urgency") > 1)).count()
assert bad_team == 0, f"{bad_team} tickets got a team outside {TEAMS}"
assert bad_p == 0, f"{bad_p} probabilities outside 0..1"
assert routed.where(F.col("kb_article").isNull()).count() == 0, "a ticket got no KB article"

print("tickets per team:")
routed.groupBy("team").count().orderBy(F.desc("count")).show()
print("most likely angry (escalate first):")
routed.orderBy(F.desc("angry_p")).select("ticket_id", "subject", "team",
                                          F.round("angry_p", 3).alias("angry_p"),
                                          F.round("urgency", 2).alias("urgency"), "kb_article") \
      .show(5, truncate=False)

# ---- land it --------------------------------------------------------------
out = f"{args.output_root.rstrip('/')}/ticket_routing"
routed.write.mode("overwrite").parquet(out)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database}")
routed.write.mode("overwrite").format("parquet").saveAsTable(f"{args.database}.ticket_routing")
print(f"wrote {out} and Hive table {args.database}.ticket_routing")

(routed.select("ticket_id", "team", "angry_p", "urgency", "kb_article")
       .write.format("jdbc").options(**PG).option("dbtable", "ticket_routing_stage")
       .mode("overwrite").save())
conn = spark._jvm.java.sql.DriverManager.getConnection(PG["url"], PG["user"], PG["password"])
try:
    st = conn.createStatement()
    changed = st.executeUpdate("""
        INSERT INTO ticket_routing (ticket_id, team, angry_p, urgency, kb_article, routed_at)
        SELECT ticket_id, team, angry_p, urgency, kb_article, now() FROM ticket_routing_stage
        ON CONFLICT (ticket_id) DO UPDATE SET team = EXCLUDED.team, angry_p = EXCLUDED.angry_p,
          urgency = EXCLUDED.urgency, kb_article = EXCLUDED.kb_article, routed_at = now()""")
    st.executeUpdate("DROP TABLE IF EXISTS ticket_routing_stage")
finally:
    conn.close()
print(f"upserted {changed} rows into Postgres ticket_routing")

assert spark.table(f"{args.database}.ticket_routing").count() == n
print("JEV TICKETS TO LAKE OK")
spark.stop()
