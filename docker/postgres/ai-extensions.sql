-- Run as the superuser in database "shop" (pg-init does this).
-- pgvector + jev, the lab's embedding function, and jev's default endpoint.
-- psql variable :jev_api_url comes from JEV_API_URL in .env.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS jev CASCADE;          -- CASCADE also creates plpython3u

-- lab_embed(text) -> vector(512)
-- A deterministic "hashing trick" embedding: every word and word pair
-- (stop words dropped, endings like -ing/-ed/-s cut) is hashed (md5) into one of 512 buckets with a +/-1 sign, then the vector is
-- L2-normalised. No model, no network: texts that share words end up close
-- under cosine distance, which is enough to exercise pgvector end to end.
-- jobs/python/18_pgvector_embeddings.py computes the SAME function in Spark
-- (and checks that both agree), so vectors written by Spark and query
-- vectors made in SQL live in the same space. Swap in real embeddings later
-- by writing vectors from a model instead; the table and queries don't change.
CREATE OR REPLACE FUNCTION lab_embed(txt text, dim int DEFAULT 512)
RETURNS vector
LANGUAGE plpython3u IMMUTABLE STRICT PARALLEL SAFE AS $$
import hashlib, math, re
STOP = {"a", "an", "the", "and", "or", "of", "to", "in", "on", "at", "for", "with", "by", "from",
        "is", "are", "was", "were", "be", "it", "its", "this", "that", "i", "me", "my", "we", "our",
        "you", "your", "do", "does", "did", "can", "how", "what", "not", "no", "please", "since"}
def stem(t):
    for suf in ("ing", "ed", "s"):
        if len(t) > len(suf) + 3 and t.endswith(suf) and not t.endswith("ss"):
            t = t[: -len(suf)]
            break
    return t[:-1] if len(t) > 4 and t.endswith("e") else t
toks = [stem(t) for t in re.findall(r"[a-z0-9]+", txt.lower()) if t not in STOP]
feats = toks + [a + "_" + b for a, b in zip(toks, toks[1:])]
v = [0.0] * dim
for f in feats:
    h = hashlib.md5(f.encode()).hexdigest()
    v[int(h[:8], 16) % dim] += 1.0 if int(h[8], 16) % 2 == 0 else -1.0
n = math.sqrt(sum(x * x for x in v)) or 1.0
return "[" + ",".join(repr(round(x / n, 6)) for x in v) + "]"
$$;
COMMENT ON FUNCTION lab_embed(text, int) IS
  'Deterministic hashing-trick embedding (lab only, not a language model). Same algorithm as 18_pgvector_embeddings.py.';

-- Where jev sends its questions, for every new session in this database.
-- Default: the offline jev-local service. For TypeSafe set in .env:
--   JEV_API_URL=https://api.typesafe.ai/v1/systemone and TYPESAFE_API_KEY=...
SELECT format('ALTER DATABASE shop SET jev.api_url = %L', :'jev_api_url') \gexec
ALTER DATABASE shop SET jev.notices = off;          -- per-request NOTICEs are noisy in JDBC logs
ALTER DATABASE shop SET jev.max_rows_per_statement = 5000;   -- spend guard if you switch to the paid API

SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector', 'jev', 'plpython3u') ORDER BY 1;
