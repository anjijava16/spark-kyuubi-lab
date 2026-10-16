# Order tracking: a small streaming project

Events about orders (created, paid, packed, shipped, out for delivery,
delivered, cancelled) arrive as JSON files. A Structured Streaming job turns
them into three things a real team would want:

| store | table | what it's for |
|---|---|---|
| Iceberg | `ice.tracking.order_events` | the full event log, each event stored once |
| Delta | `default.order_status` (`s3a://warehouse/lake/delta/order_status`) | one row per order with its current status; change data feed shows every transition |
| Postgres | `shop.public.order_tracking` | what an app or API reads: "where is my order?" |
| Postgres + Parquet | `order_sla`, `s3a://data/output/order_tracking/` | delivery SLA per carrier and city |

```
20_generate_order_events.py --wave N   ─►  s3a://data/stream/order_events/wave=N/*.json
21_track_orders_stream.py              ─►  Iceberg log ─► Delta status ─► Postgres
22_order_sla_report.py                 ─►  SLA report + consistency checks
```

## Run it

From the lab folder (or let `make test-orders` do all of this):

```bash
alias ks='python client/kyuubi_submit.py --url http://localhost:10099'

ks submit s3a://jobs/python/order_tracking/20_generate_order_events.py --remote -- --wave 1 --reset
ks submit s3a://jobs/python/order_tracking/21_track_orders_stream.py --remote --with delta,iceberg -- --reset
ks submit s3a://jobs/python/order_tracking/20_generate_order_events.py --remote -- --wave 2
ks submit s3a://jobs/python/order_tracking/21_track_orders_stream.py --remote --with delta,iceberg
ks submit s3a://jobs/python/order_tracking/20_generate_order_events.py --remote -- --wave 3
ks submit s3a://jobs/python/order_tracking/21_track_orders_stream.py --remote --with delta,iceberg
ks submit s3a://jobs/python/order_tracking/22_order_sla_report.py   --remote --with delta,iceberg
```

Between runs, look at Postgres and watch statuses move:

```bash
docker compose exec postgres psql -U app -d shop \
  -c "select status, count(*) from order_tracking group by 1 order by 1"
```

## Things it handles on purpose

- **Duplicates.** Wave 3 re-sends an event from wave 1 with the same
  `event_id`. The Iceberg MERGE inserts only unseen ids, so it's stored once.
- **Late events.** One order's PACKED event (event time in wave 2) arrives in
  wave 3, after that order already shipped. Status is always recomputed from
  the order's full history by event time, so it stays correct. The Postgres
  upsert also refuses to replace a newer `status_time` with an older one.
- **Reruns.** The checkpoint records which files were processed, so rerunning
  the stream job only reads new files. Running it twice in a row is a no-op.
- **Batch or continuous.** `trigger(availableNow=True)` drains whatever has
  arrived and stops, which suits a scheduled Kyuubi batch. Change it to
  `processingTime="30 seconds"` for a long-running stream.

## Swapping in Kafka later

Replace the `readStream...json(...)` source in `21_track_orders_stream.py`
with `spark.readStream.format("kafka")`, add the `spark-sql-kafka-0-10_2.13`
jar for Spark 4.1, and parse the message value with `from_json` using
`EVENT_SCHEMA`. Everything inside `process()` stays the same.
