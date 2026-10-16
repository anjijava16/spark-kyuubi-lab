# Postgres for the lab: metastore DB, the "shop" app DB, Airflow's DB, plus
# two extensions in "shop":
#
#   pgvector  vector type + <=> / <-> / <#> distance operators + HNSW/IVFFlat indexes
#   jev       natural-language WHERE clauses (github.com/realZachi/pg-jev):
#             jev(), jev_prob(), jev_choice(), jev_score() ... PL/Python, calls a
#             Jev-compatible HTTP API (TypeSafe's, or the lab's offline jev-local)
#
# Debian based (not alpine): jev needs plpython3u, which the alpine image
# doesn't ship, and pgvector comes as a PGDG package here.
ARG PG_MAJOR=16

# ---- fetch pg-jev at a pinned commit (it's pure SQL + PL/Python, nothing to compile)
FROM debian:bookworm-slim AS jev-src
ARG JEV_REPO=https://github.com/realZachi/pg-jev.git
ARG JEV_REF=e3450d44f7f6745aa2d728c9289b9ca4a6feda9a
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
 && git clone "$JEV_REPO" /pg-jev && cd /pg-jev && git checkout --quiet "$JEV_REF" \
 && test -f jev.control && ls sql/jev--*.sql

FROM postgres:${PG_MAJOR}-bookworm
ARG PG_MAJOR
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      postgresql-plpython3-${PG_MAJOR} \
      postgresql-${PG_MAJOR}-pgvector \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=jev-src /pg-jev/jev.control /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=jev-src /pg-jev/sql/ /tmp/jev-sql/
RUN cp /tmp/jev-sql/jev--*.sql /usr/share/postgresql/${PG_MAJOR}/extension/ && rm -rf /tmp/jev-sql \
 && test "$(ls /usr/share/postgresql/${PG_MAJOR}/extension/ | grep -cE '^(vector|jev|plpython3u)\.control$')" = 3
