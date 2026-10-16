"""
18_pgvector_embeddings.py [query ...]

Spark -> pgvector, end to end:

  1. read products from Postgres (JDBC)
  2. embed name + description in Spark (a Python UDF on the executors)
  3. write the vectors into Postgres table product_embeddings (vector(512),
     HNSW index) through a stage table + INSERT ... ON CONFLICT, casting text
     to vector on the way in
  4. check Spark's vectors equal what Postgres' own lab_embed() makes for the
     same text (so query vectors made in SQL are in the same space)
  5. semantic search from Spark: a JDBC query that pushes the
     ORDER BY embedding <=> lab_embed('...') LIMIT k down to Postgres

The embedding is the lab's hashing-trick function, not a language model (see
docker/postgres/ai-extensions.sql): it finds products that share words with
the query. Replace embed() with a real model and nothing else has to change.
"""
import hashlib
import math
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

DIM = 512
MODEL = f"lab-hash-{DIM}"
QUERIES = sys.argv[1:] or ["warm clothes for cold winter", "luxury gold gift",
                           "professional kitchen knife"]

# --- same algorithm as lab_embed() in docker/postgres/ai-extensions.sql -------
STOP = {"a", "an", "the", "and", "or", "of", "to", "in", "on", "at", "for", "with", "by", "from",
        "is", "are", "was", "were", "be", "it", "its", "this", "that", "i", "me", "my", "we", "our",
        "you", "your", "do", "does", "did", "can", "how", "what", "not", "no", "please", "since"}


def stem(t):
    for suf in ("ing", "ed", "s"):
        if len(t) > len(suf) + 3 and t.endswith(suf) and not t.endswith("ss"):
            t = t[: -len(suf)]
            break
    return t[:-1] if len(t) > 4 and t.endswith("e") else t


def embed(txt, dim=DIM):
    if txt is None:
        return None
    toks = [stem(t) for t in re.findall(r"[a-z0-9]+", txt.lower()) if t not in STOP]
    feats = toks + [a + "_" + b for a, b in zip(toks, toks[1:])]
    v = [0.0] * dim
    for f in feats:
        h = hashlib.md5(f.encode()).hexdigest()
        v[int(h[:8], 16) % dim] += 1.0 if int(h[8], 16) % 2 == 0 else -1.0
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return "[" + ",".join(repr(round(x / n, 6)) for x in v) + "]"
# ------------------------------------------------------------------------------


spark = SparkSession.builder.appName("18-pgvector-embeddings").getOrCreate()
c = spark.conf
PG = {"url": c.get("spark.lab.pg.url"), "user": c.get("spark.lab.pg.user"),
      "password": c.get("spark.lab.pg.password"), "driver": "org.postgresql.Driver"}


def pg_exec(sql):
    conn = spark._jvm.java.sql.DriverManager.getConnection(PG["url"], PG["user"], PG["password"])
    try:
        return conn.createStatement().executeUpdate(sql)
    finally:
        conn.close()


def pg_query(sql):
    return spark.read.format("jdbc").options(**PG).option("query", sql).load()


products = spark.read.format("jdbc").options(**PG).option("dbtable", "products").load()
n = products.count()
print(f"products in Postgres: {n}")
assert n > 0, "no products - run make pg-init"

embed_udf = F.udf(embed, StringType())
vectors = (products
           .select(F.col("id").alias("product_id"),
                   embed_udf(F.concat_ws(" ", "name", "description")).alias("embedding"))
           .withColumn("model", F.lit(MODEL)))

# JDBC can't write a vector column directly: land text in a stage table, cast in SQL
vectors.write.format("jdbc").options(**PG).option("dbtable", "product_embeddings_stage") \
       .mode("overwrite").save()
changed = pg_exec("""
    INSERT INTO product_embeddings (product_id, model, embedding, updated_at)
    SELECT product_id, model, embedding::vector, now() FROM product_embeddings_stage
    ON CONFLICT (product_id) DO UPDATE
      SET model = EXCLUDED.model, embedding = EXCLUDED.embedding, updated_at = now()""")
pg_exec("DROP TABLE IF EXISTS product_embeddings_stage")
print(f"upserted {changed} vectors into product_embeddings ({MODEL})")

# Spark's vectors vs Postgres' lab_embed() for the same text: must be identical
diff = pg_query("""
    SELECT max(e.embedding <=> lab_embed(concat_ws(' ', p.name, p.description))) AS max_dist,
           count(*) AS n
    FROM product_embeddings e JOIN products p ON p.id = e.product_id""").first()
print(f"Spark vs SQL embeddings: {diff['n']} rows, max cosine distance {diff['max_dist']:.2e}")
assert diff["n"] == n and diff["max_dist"] < 1e-4, "Spark and lab_embed() disagree"

for q in QUERIES:
    qq = q.replace("'", "''")
    hits = pg_query(f"""
        SELECT p.name, p.category, p.price,
               round((e.embedding <=> lab_embed('{qq}'))::numeric, 4)::float8 AS distance
        FROM product_embeddings e JOIN products p ON p.id = e.product_id
        ORDER BY e.embedding <=> lab_embed('{qq}') LIMIT 3""")
    print(f"\nsemantic search: '{q}'")
    hits.show(truncate=False)
    assert hits.count() == 3

print("PGVECTOR EMBEDDINGS OK")
spark.stop()
