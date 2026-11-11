# Runbook

**English** | [Русский](RUNBOOK.ru.md)

Troubleshooting service failures and restoring operation.
Run commands from the repository root. Service configuration is in
[compose.yaml](compose.yaml); ClickHouse SQL scripts are in
[src/clickhouse](src/clickhouse). HTTP examples use the default ports.
If you changed them in `.env`, substitute your values.
`just` commands load `.env` automatically.

[Initial checks](#initial-checks) · [API](#api-unavailable) ·
[ClickHouse](#api-returns-503) · [Redis](#redis-unavailable) ·
[PostgreSQL](#postgresql-unavailable) · [Kafka](#kafka-unavailable) ·
[Connect](#connector-or-task-not-running) ·
[Stale data](#api-returns-stale-data) ·
[Monitoring](#missing-metrics) · [Recovery checks](#after-recovery)

## Initial checks

Before restarting anything, record the failure time, dataset, request
parameters, expected result and command output. Replace `demo` and the period
below with your own values.

```sh
just status
just logs api connect clickhouse
just api-ping
curl -i --max-time 12 --get \
  'http://127.0.0.1:18001/v1/datasets/demo/metrics/orders/summary' \
  --data-urlencode 'from=2026-01-01T00:00:00Z' \
  --data-urlencode 'to=2026-01-02T00:00:00Z' \
  --data-urlencode 'refresh=true'
```

`just api-ping` should return `{"status":"ok"}`.
The summary request should return HTTP `200`.
A successful `/ping` only confirms that the API process is reachable.
`refresh=true` bypasses the cache but still queries data already loaded into ClickHouse.

| Observation | What to check |
|---|---|
| `/ping` does not respond | [API](#api-unavailable) |
| `/ping` works, but the summary returns `503` | [ClickHouse and SQL errors](#api-returns-503) |
| `429` | Load concurrency: the API allows up to 4 concurrent ClickHouse queries and waits at most 0.5 s for a free slot |
| `422` | Request parameters; the response body identifies the validation error |
| `200` with Redis errors in logs | [Redis](#redis-unavailable); the API can operate without the cache |
| `200`, but values do not change | [Event delivery](#api-returns-stale-data) |
| Only a chart or status tile shows a problem | [Metrics collection](#missing-metrics) and a direct service check |

Do not delete volumes, the connector, replication slot or consumer group,
or reset offsets as a restart procedure: you can lose data or the read position.
If the service fails again, investigate the logs.

## API unavailable

`ApiUnavailable` fires when Prometheus cannot scrape `/metrics`.
Check the API itself and its connection to Prometheus.

```sh
just logs api
just status
```

Once the cause of the shutdown is resolved, start the API:

```sh
just api-start
just api-ping
```

Check that the container is `healthy` and `/ping` returns `{"status":"ok"}`.
Repeat the summary request from the start of this runbook.
If it returns `503`, follow the next section.
If the API responds directly but the alert remains active, check the `api`
target in Prometheus.

## API returns 503

`ApiErrors` fires when 5xx responses exceed 50% for one business endpoint
over the last 30 seconds and stay above that threshold for another 10 seconds.

```sh
just logs api clickhouse
just ch --query 'SELECT 1'
```

If ClickHouse is reachable, test the API's database credentials:

```sh
docker compose exec -T clickhouse clickhouse-client \
  --user reader --password reader --database capstone --query 'SELECT 1'
```

A result of `1` confirms server access, but does not check that the views exist.
Find the cause in the API and ClickHouse logs: a missing table, insufficient
permissions, invalid SQL or a timeout. API queries are in [src/api/sql](src/api/sql).
`just clickhouse-setup` uses `IF NOT EXISTS` and leaves existing tables unchanged.

If the ClickHouse container is stopped, check its mounted volume first:

```sh
docker inspect --format '{{json .Mounts}}' "$(docker compose ps -a -q clickhouse)"
```

For disk errors or table corruption, check storage first.
Once the cause is resolved and the original volume is attached, start the service:

```sh
just clickhouse-start
just ch --query 'SELECT 1'
```

Repeat the summary request with `refresh=true`; expect HTTP `200`.
Regular requests may have been served from Redis until their TTL expired
while ClickHouse was unavailable. After starting ClickHouse, check
[Kafka event processing](#api-returns-stale-data).

## Redis unavailable

`RedisUnavailable` and `RedisErrors` detect Redis failures.
The latter responds to API cache access errors; without cache requests,
it will not fire.

```sh
just logs redis redis-exporter api
docker compose exec -T redis redis-cli PING
```

Redis should return `PONG`. If the container is stopped, start it:

```sh
just redis-start
docker compose exec -T redis redis-cli PING
```

Request a summary with `refresh=true`, then make two requests with the same
parameters without refresh. Before the TTL expires, `meta.computed_at` should
match because the API is serving the cached response. `Redis cache` errors
should stop appearing in logs. If Redis responds but the exporter cannot
connect, check its logs and the `redis` target separately.

Without Redis, the API queries ClickHouse directly and can still return
`200`, but ClickHouse load increases. You do not need to flush the cache
to restore the connection.

## PostgreSQL unavailable

Relevant alerts are `PostgresUnavailable` and, possibly, `CdcTaskNotRunning`.
The API may still serve ClickHouse data, but no new source changes will arrive.

```sh
just logs postgres postgres-exporter connect
just pg -c 'SELECT 1'
```

The query should return `1`. Once the cause of the shutdown is resolved,
start PostgreSQL:

```sh
just postgres-start
just pg -c 'SELECT 1'
just cdc-status
```

Check the replication slot and retained WAL size:

```sh
just pg -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots WHERE slot_name = 'capstone_slot';"
```

While the connector is reading WAL, `capstone_slot` should have `active=true`.
Compare `confirmed_flush_lsn` before and after a source change: it should
advance once the change is processed. The slot can retain WAL while Connect
is down. Check free disk space:

```sh
docker compose exec -T postgres df -h /var/lib/postgresql/data
```

If the slot is lost or the required WAL segment has already been removed,
a restart will not recover the missing changes. Save logs and read positions.
Before taking another snapshot, plan reconciliation with ClickHouse.
A snapshot loads current source rows; it cannot recover records of earlier deletes.

## Kafka unavailable

```sh
just logs kafka connect clickhouse
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

The list should include `capstone.capstone.orders`,
`capstone.capstone.order_items` and `capstone.capstone.payments`.
Once the cause of the shutdown is resolved, start the broker:

```sh
just kafka-start
just cdc-status
```

Check the topic list, offsets and API data. After Kafka recovers,
the Connect task may remain `RUNNING` without publishing new events.
If a source change exists but offsets do not advance, inspect Connect logs.
After resolving the cause, run `just connect-restart` and reconcile the data again.

After a long outage, check both Kafka and PostgreSQL disks.
Connect may stop acknowledging WAL reads, and ClickHouse will stop receiving
Kafka events. If the Kafka volume is lost, first establish which data and
offsets were lost. Recreating topics does not restore history.

## Connector or task not RUNNING

`CdcTaskNotRunning` monitors task state.

```sh
just cdc-status
just logs connect
```

The connector should report `connector.state=RUNNING` and the task
`tasks[0].state=RUNNING`. If the task is `FAILED`, read its `trace` and compare
it with Connect logs. Depending on the error, check PostgreSQL or Kafka
connectivity, permissions and the replication slot.

If Connect is stopped, start it:

```sh
just connect-start
just cdc-status
```

If the cause is resolved but the task is still `FAILED`, restart the worker.
A restart is also appropriate for a `RUNNING` task if source checks, offsets
and logs confirm that new event processing has stalled:

```sh
just connect-restart
```

This restarts the whole Connect process; the environment has one connector.
Recheck both states and verify that changes reach ClickHouse.
`RUNNING` does not guarantee that new events are being processed.

For a REST API `404`, investigate why the connector is missing and check
that Kafka data, offsets and the replication slot are intact.
`just cdc-register` registers the configuration; it does not restore lost CDC state.

## API returns stale data

### Rule out the cache and check the source

Compare summary responses with and without refresh, using the same parameters.
If `refresh=true` returns updated data, wait for the 30-second TTL or use
refresh. Reads do not extend the TTL. A new `computed_at` confirms a new
calculation, but says nothing about source data freshness.

If both responses are stale, check the PostgreSQL record.
Substitute your dataset and order ID:

```sh
just pg -c "SELECT id, status, amount_cents, version, updated_at
FROM capstone.orders WHERE dataset_id = 'show_01' AND id = 'o1';"
just cdc-status
```

Confirm that the transaction committed and the order version increased.
For payments, check `capstone.payments`; for line items, check
`capstone.order_items`. If the source has no change, investigate the write
before looking at CDC.

### Check Kafka and the ClickHouse consumer

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group capstone-orders-clickhouse
```

Line items use `capstone-order-items-clickhouse`;
payments use `capstone-payments-clickhouse`.
Repeat the check after 10 seconds and compare offsets.

| Observation | Next action |
|---|---|
| Events are expected, but `LOG-END-OFFSET` is not advancing | Check the source, Connect and its errors |
| The log end advances, but `CURRENT-OFFSET` does not | Check the ClickHouse consumer and MVs |
| Both advance, but lag is growing | Compare input and processing rates; check ClickHouse resources |
| Committed offsets reached the required boundary, but the response is wrong | Compare raw events, latest versions and API SQL |
| The group or committed offset is absent | Check the group name and Kafka table state; do not reset offsets |

`ClickHouseConsumerLag` fires when a group's total lag for a topic stays above
zero for 30 seconds. Zero lag alone does not confirm freshness: Connect
may not have published the source change to Kafka yet.

Deletion testing found a specific case in ClickHouse 25.8: the consumer read
the final tombstone but did not commit its offset. Lag stays at 1 and the
alert remains active even though DELETE has been processed. Compare
`assignments.current_offset` from the query below with the committed offset
and partition end. Read the last message at those coordinates, then verify
the deletion in `*_latest` and the API. Exclude it from business-event
reconciliation only after confirming `value=null`. The next regular message
in the same partition advances the committed offset. Do not reset offsets.

```sh
just ch --query "SELECT table, assignments.topic, assignments.partition_id,
  assignments.current_offset, last_poll_time, exceptions.time, exceptions.text
FROM system.kafka_consumers WHERE database = 'capstone' FORMAT Vertical"
just ch --query "SELECT name, engine FROM system.tables
WHERE database = 'capstone' ORDER BY name"
just ch --query "SELECT table FROM system.detached_tables
WHERE database = 'capstone'"
```

Check that the `*_cdc_queue` Kafka tables, `*_cdc_to_raw` and
`*_raw_to_current` MVs, raw/current tables and latest views all exist.
Definitions are in [setup](src/clickhouse/setup). Check error timestamps:
old exceptions remain in consumer history after processing recovers.

If an MV is detached, establish why before running `ATTACH`.
A new MV processes only new inserts: creating it over a populated raw table
does not load existing rows into current. Those rows need a backfill.
Before running it, reconcile the data range and deduplication rules.

After a long outage, check which offsets are still available in the topic:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 --topic capstone.capstone.orders --time -2
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 --entity-type topics \
  --entity-name capstone.capstone.orders --describe --all
docker compose exec -T kafka df -h /var/lib/kafka/data
```

The first command prints the earliest offset for each partition.
If it is already greater than the committed offset, Kafka has removed some
uncommitted messages. Those records may still have reached ClickHouse before
the offset commit. Save the positions and reconcile the records before
planning a reload.

### Compare raw, latest and the API

```sh
just ch --query "SELECT id, status, amount_cents, version, is_deleted,
  _source_lsn, _operation, _kafka_partition, _kafka_offset, _ingested_at
FROM capstone.orders_raw WHERE dataset_id = 'show_01' AND id = 'o1'
ORDER BY version, _ingested_at"
just ch --query "SELECT id, status, amount_cents, version, is_deleted
FROM capstone.orders_latest WHERE dataset_id = 'show_01' AND id = 'o1'"
```

If the version is missing from raw, check CDC and Kafka consumption.
If it exists in raw but not in latest, check the raw-to-current MV and
version selection rule. If latest is correct, check the period, campaign,
`is_deleted` filter and API SQL.

For line items, compare `_source_lsn` and `is_deleted`.
For payments, check each operation's ID, kind and amount:
`payments` is an append-only log. A hard-deleted order should retain
a version with `is_deleted=1` in latest.

## Missing metrics

Relevant alerts are `PipelineMetricsUnavailable` and `PipelineMetricsCollectionFailed`.

```sh
just status
just logs prometheus postgres-exporter redis-exporter kafka-exporter
just prometheus-config-check
```

Find the affected job on the [Targets](http://127.0.0.1:19005/targets) page
and read `Last Error`. There are eight jobs: `api`, `postgres`, `redis`,
`kafka`, `connect`, `kafka-consumers`, `clickhouse` and `pipeline`.
Target status `UP` confirms a successful scrape. Use `pg_up` and `redis_up`
to check PostgreSQL and Redis themselves. The `pipeline` target can fail
because of a `pipeline_metrics` query even when ClickHouse responds.

`PipelineMetricsCollectionFailed` reports a collection error inside the
PostgreSQL exporter or Kafka/Connect JMX agent. Check their logs, permissions
and configuration. Prometheus does not evaluate alerts while it is stopped.

After editing Prometheus configuration, run:

```sh
just prometheus-config-reload
```

The command validates rules and sends Prometheus SIGHUP to reload its
configuration. If the container is stopped, start it first with
`docker compose up -d --wait prometheus`.
If metrics are arriving but Grafana shows no data, check its connection
to Prometheus, the selected time range and `just logs grafana`.
The data source and dashboards use
[provisioning](monitoring/grafana/provisioning).

## After recovery

1. Repeat the direct check of the failed service and an API request with `refresh=true`.
2. For each partition, record the offset of the last required business event
   plus one. Wait for the committed offset to reach that boundary.
   Usually `LOG-END-OFFSET` is the reference; check a final tombstone separately
   as described above. With an ongoing stream, use the recorded boundary.
3. Reconcile records against the source or saved expectations: versions,
   statuses, amounts and deletes. A matching aggregate amount alone does not
   establish completeness.
4. Make a new source change, check it through the API, and replay the events.
5. Confirm that metrics are arriving and the relevant alerts are `Inactive`
   on the [Alerts](http://127.0.0.1:19005/alerts) page.

Check new events and deduplication in a new dataset:

```sh
just demo-create recovery_01
just demo-pay recovery_01
just demo-duplicates recovery_01
```

The API should still show one paid order for 10000 kopecks, two SKUs,
three units and one capture. Duplicates add raw rows without changing
results from the five endpoints. This scenario checks new events;
reconcile the outage backlog separately in steps 2 and 3.

If `demo-pay` times out waiting for the API, its PostgreSQL transaction
may already have committed. Check the source and `just demo-show recovery_01`.
Do not repeat the payment before establishing the order and payment state.

Alerts with a 30-second window do not clear immediately after the error is
resolved. Keep making requests, checking responses and watching for new
metrics. An empty chart or a disappeared series does not confirm recovery.

Record the cause, actions taken, recovery time and reconciliation results.
If you checked only part of the data, specify the dataset and range.

The last test run was on 11 November 2026. It covered stopping and starting
PostgreSQL, Kafka, Connect, ClickHouse, the API and Redis, detaching the Kafka
ingestion MV, and a PostgreSQL exporter permission failure. A full restart
with `just down` and `just start` preserved volumes. After each failure,
the source and all five endpoint responses were reconciled. All nine alerts
fired and cleared after recovery. After Kafka started, no new events arrived
for 45 seconds despite the task remaining `RUNNING`. `just connect-restart`
resumed processing. The full restart preserved data and offsets without
another snapshot.
