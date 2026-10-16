-- pgvector + jev examples for the lab's "shop" database.
--   make ai-sql            runs this whole file
--   make psql              then paste any block
-- jev answers come from jev.api_url: the offline jev-local stand-in by default
-- (word matching, not a model), TypeSafe's Jev if you configured a key.
\pset pager off
\timing on

\echo
\echo ===== the extensions =====
SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector', 'jev', 'plpython3u') ORDER BY 1;
SELECT jev_version() AS jev, current_setting('jev.api_url') AS jev_api_url;

-- ===================================================================== jev
\echo
\echo ===== 1. jev(): a WHERE clause in plain language =====
SELECT * FROM people WHERE jev(people, 'the name is European');

\echo ===== jev() composes with ordinary SQL: cheap predicates run first =====
SELECT name, country, age FROM people
WHERE age > 40 AND jev(people, 'works in healthcare')
ORDER BY age DESC;

\echo ===== 2. jev_prob(): rank by probability =====
SELECT subject, round(jev_prob(tickets, 'the customer is angry')::numeric, 3) AS p
FROM tickets ORDER BY p DESC LIMIT 20;

\echo ===== 3. jev_choice(): route each row to one option =====
SELECT jev_choice(tickets, 'which team should handle this?',
                  ARRAY['billing', 'technical', 'security', 'sales']) AS team, count(*)
FROM tickets GROUP BY 1 ORDER BY 2 DESC;

\echo ===== 4. jev_score(): position on ordered levels (0 = budget .. 3 = luxury) =====
SELECT name, price, round(jev_score(products, 'how luxurious is this product?',
                       ARRAY['budget', 'mid-range', 'premium', 'luxury'])::numeric, 2) AS luxury
FROM products ORDER BY luxury DESC;

\echo ===== jev on a view: send only the columns the judgment needs =====
CREATE OR REPLACE VIEW ticket_text AS SELECT id, subject, body FROM tickets;
SELECT id, subject,
       jev_choice(ticket_text, 'which team should handle this?', ARRAY['billing', 'technical', 'security', 'sales']) AS team,
       round(jev_confidence(ticket_text, 'which team should handle this?', 'choice',
                            ARRAY['billing', 'technical', 'security', 'sales'])::numeric, 2) AS confidence
FROM ticket_text ORDER BY id LIMIT 8;

\echo ===== the raw answer behind one row =====
SELECT jev_eval(t, 'how urgent is this?', 'score', ARRAY['low', 'medium', 'high', 'urgent'])
FROM tickets t WHERE id = 20;

\echo ===== what this session cost (requests, tokens, cache hits) =====
SELECT jsonb_pretty(jev_stats());

-- ================================================================ pgvector
\echo
\echo ===== 5. pgvector: nearest help articles for a question (cosine distance <=>) =====
SELECT id, title, round((embedding <=> lab_embed('refund a double charge on my card'))::numeric, 3) AS distance
FROM kb_articles ORDER BY embedding <=> lab_embed('refund a double charge on my card') LIMIT 3;

\echo ===== the same search with the HNSW index forced on (tiny tables use a seq scan otherwise) =====
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT id FROM kb_articles ORDER BY embedding <=> (SELECT lab_embed('suspicious login alert')) LIMIT 3;
RESET enable_seqscan;

\echo ===== 6. products by meaning: vectors written by Spark (job 18) =====
SELECT p.name, p.price, round((e.embedding <=> lab_embed('warm clothes for cold winter'))::numeric, 3) AS distance
FROM product_embeddings e JOIN products p ON p.id = e.product_id
ORDER BY e.embedding <=> lab_embed('warm clothes for cold winter') LIMIT 5;

\echo ===== other distance operators: <-> L2, <#> negative inner product =====
SELECT title,
       round((embedding <->  lab_embed('installer error on windows'))::numeric, 3) AS l2,
       round((embedding <#>  lab_embed('installer error on windows'))::numeric, 3) AS neg_ip,
       round((embedding <=>  lab_embed('installer error on windows'))::numeric, 3) AS cosine
FROM kb_articles ORDER BY embedding <=> lab_embed('installer error on windows') LIMIT 3;

-- ======================================================== both together
\echo
\echo ===== 7. jev + pgvector: route every ticket AND suggest a help article =====
SELECT t.id, t.subject,
       jev_choice(t, 'which team should handle this?', ARRAY['billing', 'technical', 'security', 'sales']) AS team,
       round(jev_prob(t, 'the customer is angry')::numeric, 2) AS angry,
       kb.title AS suggested_article
FROM tickets t
CROSS JOIN LATERAL (
  SELECT k.title FROM kb_articles k
  ORDER BY k.embedding <=> lab_embed(t.subject || ' ' || t.body) LIMIT 1) kb
ORDER BY angry DESC, t.id;

\echo ===== 8. what Spark wrote back (job 19): ticket_routing =====
SELECT r.ticket_id, t.subject, r.team, round(r.angry_p::numeric, 2) AS angry_p,
       round(r.urgency::numeric, 2) AS urgency, k.title AS kb_article, r.routed_at
FROM ticket_routing r JOIN tickets t ON t.id = r.ticket_id LEFT JOIN kb_articles k ON k.id = r.kb_article
ORDER BY r.angry_p DESC NULLS LAST LIMIT 10;
