# Airflow 3 for the lab: the official image plus the client libraries the
# DAGs use to talk to Kyuubi (REST via requests, SQL via PyHive), Kafka,
# Postgres and RustFS. The lab's own code (client/, jobs/) is mounted, not
# copied, so edits show up without rebuilding.
ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.12
FROM ${AIRFLOW_IMAGE}

USER airflow
# "apache-airflow==$AIRFLOW_VERSION" keeps pip from up/downgrading Airflow
# while resolving the extra packages (AIRFLOW_VERSION is set by the base image).
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "pyhive[hive_pure_sasl]==0.7.0" thrift \
      "kafka-python>=2.2.0" "boto3>=1.35" "requests>=2.31" \
      pandas psycopg2-binary
