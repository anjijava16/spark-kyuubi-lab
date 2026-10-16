# syntax=docker/dockerfile:1.6
#
# One Dockerfile, two targets:
#   spark-base -> used by spark-master, spark-worker, spark-history
#   kyuubi     -> spark-base + Kyuubi server (Kyuubi needs SPARK_HOME locally,
#                 because it runs spark-submit itself)
#
# Build context is ./docker (so the shared fetch script is reachable).

ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

FROM ${SPARK_IMAGE} AS spark-dist

# ---- download s3a jars that match Spark's bundled Hadoop -----------------
FROM alpine:3.20 AS fetch-s3a
RUN apk add --no-cache curl
COPY common/fetch-s3a-jars.sh /usr/local/bin/fetch-s3a-jars.sh
COPY --from=spark-dist /opt/spark/jars/ /spark-jars/
ARG HADOOP_VERSION=
ARG AWS_SDK_VERSION=
RUN HADOOP_VERSION="${HADOOP_VERSION}" AWS_SDK_VERSION="${AWS_SDK_VERSION}" \
    sh /usr/local/bin/fetch-s3a-jars.sh /spark-jars /out
# spark-avro is Spark's own module but ships separately; it must match the
# Spark version exactly, so read that from spark-core's file name. Plus the
# Postgres JDBC driver for the read/write-Postgres examples.
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN set -eux; \
    SV=$(ls /spark-jars | sed -n 's/^spark-core_2\.13-\([0-9.]*\)\.jar$/\1/p'); \
    test -n "$SV"; echo "Spark $SV"; \
    M=https://repo1.maven.org/maven2; \
    curl -fsSL -o /out/spark-avro_2.13-$SV.jar "$M/org/apache/spark/spark-avro_2.13/$SV/spark-avro_2.13-$SV.jar"; \
    curl -fsSL -o /out/postgresql-${POSTGRES_JDBC_VERSION}.jar "$M/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    ls -l /out

# ---- download Kyuubi + the Scala 2.13 Spark SQL engine jar ---------------
FROM alpine:3.20 AS fetch-kyuubi
RUN apk add --no-cache curl tar
ARG KYUUBI_VERSION=1.12.0
ARG SCALA_BINARY=2.13
RUN set -eux; \
    TGZ="apache-kyuubi-${KYUUBI_VERSION}-bin.tgz"; \
    curl -fsSL -o /tmp/kyuubi.tgz "https://dlcdn.apache.org/kyuubi/kyuubi-${KYUUBI_VERSION}/${TGZ}" \
      || curl -fsSL -o /tmp/kyuubi.tgz "https://archive.apache.org/dist/kyuubi/kyuubi-${KYUUBI_VERSION}/${TGZ}"; \
    mkdir -p /opt && tar -xzf /tmp/kyuubi.tgz -C /opt; \
    mv /opt/apache-kyuubi-*-bin /opt/kyuubi; \
    # The tarball ships the Spark SQL engine for BOTH Scala 2.12 and 2.13 in
    # externals/engines/spark/, and Kyuubi picks the one matching the
    # spark-core jar in SPARK_HOME. Spark 4 is 2.13 only, so make sure the
    # 2.13 engine is really there; fail the build now rather than at the
    # first SQL query.
    ls -l /opt/kyuubi/externals/engines/spark/; \
    test -f "/opt/kyuubi/externals/engines/spark/kyuubi-spark-sql-engine_${SCALA_BINARY}-${KYUUBI_VERSION}.jar"; \
    mkdir -p /opt/kyuubi/work /opt/kyuubi/logs /opt/kyuubi/pid

# ---- spark-base ----------------------------------------------------------
FROM spark-dist AS spark-base
USER root
COPY --from=fetch-s3a /out/ /opt/spark/jars/
RUN mkdir -p /opt/spark/work /opt/spark/logs /opt/lakehouse \
 && chown -R spark:spark /opt/spark/work /opt/spark/logs
ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}
USER spark
WORKDIR /opt/spark

# ---- kyuubi --------------------------------------------------------------
FROM spark-base AS kyuubi
USER root
COPY --from=fetch-kyuubi --chown=spark:spark /opt/kyuubi /opt/kyuubi
ENV KYUUBI_HOME=/opt/kyuubi \
    PATH=/opt/kyuubi/bin:${PATH}
USER spark
WORKDIR /opt/kyuubi
EXPOSE 10009 10099
# "run" keeps Kyuubi in the foreground so Docker can supervise it
CMD ["/opt/kyuubi/bin/kyuubi", "run"]
