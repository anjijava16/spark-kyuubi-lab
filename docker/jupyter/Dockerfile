# JupyterLab for the lab. Notebooks don't run Spark themselves: they send
# work to Kyuubi (batch jobs over REST, SQL over the Thrift/JDBC port) and
# read results from Postgres, RustFS and Kafka. So the image only needs
# JupyterLab and small client libraries, no Java, no Spark.
ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE}

RUN pip install --no-cache-dir \
      "jupyterlab>=4.2" ipywidgets \
      pandas matplotlib \
      "pyhive[hive_pure_sasl]==0.7.0" thrift \
      psycopg2-binary "boto3>=1.35" "kafka-python>=2.2.0" "requests>=2.31" \
 && useradd -m -u 1000 lab \
 && mkdir -p /home/lab/work && chown lab:lab /home/lab/work

USER lab
WORKDIR /home/lab/work
ENV PYTHONPATH=/home/lab/work/client \
    KYUUBI_REST_URL=http://kyuubi:10099 \
    KYUUBI_JDBC_HOST=kyuubi \
    KAFKA_BOOTSTRAP=kafka:9092 \
    S3_ENDPOINT=http://rustfs:9000
EXPOSE 8888
# no token: this is a local lab on your laptop. Don't expose port 8888 elsewhere.
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--IdentityProvider.token=", "--ServerApp.password=", "--ServerApp.root_dir=/home/lab/work"]
