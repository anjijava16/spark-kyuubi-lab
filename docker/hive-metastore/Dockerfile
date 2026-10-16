# syntax=docker/dockerfile:1.6
#
# Hive Metastore (standalone thrift service on 9083), backed by Postgres,
# with the warehouse on RustFS (s3a). Build context is ./docker.
#
# Why 3.1.3 and not 4.x: Spark's built-in Hive client is 2.3.x, and HMS 3.1
# is the version it has been run against for years. It is also what
# Cloudera CDP uses, which matters for the migration this lab is for.

ARG HIVE_IMAGE=apache/hive:3.1.3

FROM ${HIVE_IMAGE} AS hive-dist

FROM alpine:3.20 AS fetch
RUN apk add --no-cache curl
COPY common/fetch-s3a-jars.sh /usr/local/bin/fetch-s3a-jars.sh
COPY --from=hive-dist /opt/hadoop/share/hadoop/common/ /hadoop-common/
ARG HADOOP_VERSION=
ARG AWS_SDK_VERSION=
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN set -eux; \
    HADOOP_VERSION="${HADOOP_VERSION}" AWS_SDK_VERSION="${AWS_SDK_VERSION}" \
      sh /usr/local/bin/fetch-s3a-jars.sh /hadoop-common /out; \
    curl -fsSL -o /out/postgresql-${POSTGRES_JDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

FROM hive-dist
USER root
COPY --from=fetch /out/ /opt/hive/lib/
COPY --chmod=755 hive-metastore/start-metastore.sh /opt/start-metastore.sh
USER hive
EXPOSE 9083
ENTRYPOINT ["/opt/start-metastore.sh"]
