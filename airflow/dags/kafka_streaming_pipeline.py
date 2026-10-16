"""
kafka_streaming_pipeline - Kafka producer -> Spark Structured Streaming
consumers -> Spark publishes back to Kafka, plus a file-event stream, run as
"incremental batches" (--available-now): each stream reads what's new since
its checkpoint and stops, which is how you schedule streams with Airflow.

  kafka_ready            short-circuit: no Kafka (make kafka-install) -> whole run skipped
  produce_orders         Python producer, N order lifecycles into orders.events
  streams/consumer_basic Kafka -> Parquet bronze
  streams/to_delta_pg    Kafka -> Delta (exactly once) + Postgres order status
  streams/enrich_publish Kafka + Postgres customers -> Spark publishes orders.enriched / orders.alerts
  streams/windowed       event-time windows -> Spark publishes orders.metrics
  batch_publish          Spark batch job: Hive demo.orders -> orders.backfill
  file_events/*          JSON files land in s3a://data/stream/landing -> file stream -> Parquet silver
  verify                 job 39: every output holds exactly this run's events
  peek_topics            Python consumer: what Spark published (Kafka)
  postgres_status        Python: order status rows Spark upserted (Postgres)
"""
import json
import os

from airflow.sdk import DAG, Param, TaskGroup, task

from lab_common import DEFAULT_ARGS, START, TAGS, job, report

with DAG(
    dag_id="kafka_streaming_pipeline",
    description="producer -> Kafka -> Spark streams (available-now) -> Kafka/Delta/Postgres, + file events",
    doc_md=__doc__,
    schedule=None, start_date=START, catchup=False,
    default_args=DEFAULT_ARGS, max_active_runs=1, max_active_tasks=2,
    params={"orders": Param(40, type="integer", minimum=1, maximum=5000,
                            description="order lifecycles to produce")},
    tags=TAGS + ["kafka", "streaming", "structured-streaming", "delta", "postgres"],
) as dag:

    @task.short_circuit
    def kafka_ready():
        import lab_runner
        ok = lab_runner.kafka_reachable()
        print("Kafka reachable" if ok else "Kafka not reachable - run `make kafka-install`; skipping")
        return ok

    # the catalog command is order_producer.py --orders 40 --run-id <run>;
    # the "orders" param replaces the count
    produce = job("kafka-produce", task_id="produce_orders",
                  extra_args=["--orders", "{{ params.orders }}"])

    with TaskGroup("streams") as streams:
        s = [job("kafka-consumer-basic", task_id="consumer_basic"),
             job("kafka-to-delta-pg", task_id="to_delta_pg"),
             job("kafka-enrich-publish", task_id="enrich_publish"),
             job("kafka-windowed-metrics", task_id="windowed")]

    with TaskGroup("file_events") as file_events:
        job("file-drop-produce", task_id="drop_files") >> job("file-drop-stream", task_id="file_stream")

    batch_publish = job("kafka-batch-publish", task_id="batch_publish")
    verify = job("verify-streaming", task_id="verify")

    @task
    def peek_topics():
        from kafka import KafkaConsumer, TopicPartition
        out = {}
        for topic in ("orders.enriched", "orders.metrics", "orders.alerts", "orders.backfill"):
            c = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                              consumer_timeout_ms=3000, enable_auto_commit=False)
            parts = [TopicPartition(topic, p) for p in (c.partitions_for_topic(topic) or [])]
            c.assign(parts)
            ends = c.end_offsets(parts)
            out[topic] = sum(ends.values())
            for tp in parts:
                c.seek(tp, max(0, ends[tp] - 2))
            for msg in c:
                print(f"{topic}[{msg.partition}@{msg.offset}] {msg.value.decode()[:160]}")
            c.close()
        print("messages per topic:", json.dumps(out))
        return out

    @task
    def postgres_status():
        import psycopg2
        conn = psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), dbname="shop",
                                user="app", password="apppass")
        with conn, conn.cursor() as cur:
            cur.execute("SELECT status, count(*) FROM kafka_order_status GROUP BY status ORDER BY 2 DESC")
            rows = cur.fetchall()
        conn.close()
        for status, n in rows:
            print(f"{status:12} {n}")
        if not rows:
            raise ValueError("kafka_order_status is empty: the Delta/Postgres stream wrote nothing")
        return dict(rows)

    gate = kafka_ready()
    gate >> produce >> streams
    gate >> batch_publish
    [streams, file_events] >> verify
    done = report()
    verify >> [peek_topics(), postgres_status()] >> done
    batch_publish >> done
