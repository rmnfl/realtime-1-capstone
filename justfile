set shell := ["bash", "-euo", "pipefail", "-c"]
set dotenv-load

[private]
default:
    @just --list --unsorted

# -----------------------------------------------------------------------------
# Stack
# -----------------------------------------------------------------------------

# Build, initialize and start the complete stack; preserve data and offsets.
[group('Stack')]
up: compose-check
    docker compose up -d --build --wait postgres kafka connect clickhouse redis
    just clickhouse-setup
    just cdc-register
    docker compose up -d --build --wait api postgres-exporter redis-exporter kafka-exporter prometheus grafana
    just api-ping
    just cdc-status

# Start an already initialized stack without changing schemas or connector config.
[group('Stack')]
start: compose-check
    docker compose up -d --wait

# Remove containers and network; pass --volumes to also delete volumes and offsets.
[group('Stack')]
[positional-arguments]
down *args:
    docker compose down "$@"

# Validate Compose configuration.
[group('Stack')]
compose-check:
    docker compose config --quiet

# -----------------------------------------------------------------------------
# Core services
# -----------------------------------------------------------------------------

# Start only postgres and wait for readiness; leave dependencies unchanged.
[group('Services')]
postgres-start:
    docker compose up -d --no-deps --wait postgres

# Stop only postgres; preserve its container and data.
[group('Services')]
postgres-stop:
    docker compose stop postgres

# -----------------------------------------------------------------------------

# Start only kafka and wait for readiness; leave dependencies unchanged.
[group('Services')]
kafka-start:
    docker compose up -d --no-deps --wait kafka

# Stop only kafka; preserve its container and data.
[group('Services')]
kafka-stop:
    docker compose stop kafka

# -----------------------------------------------------------------------------

# Start only connect and wait for readiness; leave dependencies unchanged.
[group('Services')]
connect-start:
    docker compose up -d --no-deps --wait connect

# Stop only connect; preserve its container and data.
[group('Services')]
connect-stop:
    docker compose stop connect

# Restart the Connect worker after stalled delivery; then verify the API result.
[group('Services')]
connect-restart:
    docker compose restart connect
    docker compose up -d --no-deps --wait connect
    just cdc-status

# -----------------------------------------------------------------------------

# Start only clickhouse and wait for readiness; leave dependencies unchanged.
[group('Services')]
clickhouse-start:
    docker compose up -d --no-deps --wait clickhouse

# Stop only clickhouse; preserve its container and data.
[group('Services')]
clickhouse-stop:
    docker compose stop clickhouse

# -----------------------------------------------------------------------------

# Start only api and wait for readiness; leave dependencies unchanged.
[group('Services')]
api-start:
    docker compose up -d --no-deps --wait api

# Stop only api; preserve its container and data.
[group('Services')]
api-stop:
    docker compose stop api

# -----------------------------------------------------------------------------

# Start only redis and wait for readiness; leave dependencies unchanged.
[group('Services')]
redis-start:
    docker compose up -d --no-deps --wait redis

# Stop only redis; preserve its container and data.
[group('Services')]
redis-stop:
    docker compose stop redis

# -----------------------------------------------------------------------------
# Storage and business views
# -----------------------------------------------------------------------------

# Prepare ClickHouse storage and views before enabling Kafka consumption.
[group('ClickHouse')]
clickhouse-setup:
    docker compose exec -T clickhouse clickhouse-client --user admin --password admin --multiquery < src/clickhouse/setup/00-init.sql
    docker compose exec -T clickhouse clickhouse-client --user admin --password admin --multiquery < src/clickhouse/setup/01-current.sql
    just clickhouse-views-setup
    docker compose exec -T clickhouse clickhouse-client --user admin --password admin --multiquery < src/clickhouse/setup/02-cdc.sql
    docker compose exec -T clickhouse clickhouse-client --user admin --password admin --multiquery < monitoring/clickhouse/pipeline.sql

# Create business views; IF NOT EXISTS does not update existing definitions.
[group('ClickHouse')]
clickhouse-views-setup:
    for path in src/clickhouse/metrics/*.sql; do docker compose exec -T clickhouse clickhouse-client --user admin --password admin --multiquery < "$path"; done

# -----------------------------------------------------------------------------
# CDC
# -----------------------------------------------------------------------------

# Register/update Debezium configuration, keeping its existing offsets.
[group('CDC')]
cdc-register:
    curl -fsS -X PUT "http://127.0.0.1:${CONNECT_PORT:-28084}/connectors/capstone-cdc/config" -H 'Content-Type: application/json' --data-binary @src/debezium/connector.json | python3 -m json.tool

# Show connector and task states; both should be RUNNING.
[group('CDC')]
cdc-status:
    curl -fsS "http://127.0.0.1:${CONNECT_PORT:-28084}/connectors/capstone-cdc/status" | python3 -m json.tool

# -----------------------------------------------------------------------------
# Diagnostics
# -----------------------------------------------------------------------------

# Show this project's containers, health and published ports.
[group('Diagnostics')]
status:
    docker compose ps -a

# Show recent logs; optionally specify services, e.g. just logs api connect.
[group('Diagnostics')]
[positional-arguments]
logs *services:
    docker compose logs --no-color --tail=100 "$@"

# Check the API process; this does not check data freshness.
[group('Diagnostics')]
api-ping:
    curl -fsS "http://127.0.0.1:${API_PORT:-18001}/ping" | python3 -m json.tool

# -----------------------------------------------------------------------------
# SQL consoles
# -----------------------------------------------------------------------------

# Open PostgreSQL or pass psql arguments, e.g. just pg -c 'SELECT 1'.
[group('SQL')]
[positional-arguments]
pg *args:
    docker compose exec postgres psql -U admin -d capstone -v ON_ERROR_STOP=1 "$@"

# Open ClickHouse or pass client arguments, e.g. just ch --query 'SELECT 1'.
[group('SQL')]
[positional-arguments]
ch *args:
    docker compose exec clickhouse clickhouse-client --user admin --password admin --database capstone "$@"

# -----------------------------------------------------------------------------
# Monitoring
# -----------------------------------------------------------------------------

# Validate Prometheus configuration and alert rules.
[group('Monitoring')]
prometheus-config-check:
    docker compose exec -T prometheus promtool check config /etc/prometheus/prometheus.yml

# Reload Prometheus configuration and rules after validation.
[group('Monitoring')]
prometheus-config-reload: prometheus-config-check
    docker compose kill -s SIGHUP prometheus

# -----------------------------------------------------------------------------
# Demo
# -----------------------------------------------------------------------------

# Run all checks before the presentation: snapshot, order lifecycle, Kafka replay and deletion.
[group('Demo')]
demo:
    uv run --locked --group generator src/demo/run.py all

# Check all five endpoints against the initial demo dataset.
[group('Demo')]
demo-snapshot:
    uv run --locked --group generator src/demo/run.py snapshot

# Run create, pay, Kafka replay and refund in a new dataset; skip the initial snapshot.
[group('Demo')]
demo-pipeline:
    uv run --locked --group generator src/demo/run.py pipeline

# Create one order with two items; show the expected API changes.
[group('Demo')]
[positional-arguments]
demo-create dataset:
    uv run --locked --group generator src/demo/run.py create --dataset "$1"

# Pay o1 and record its capture; wait for the expected API results.
[group('Demo')]
[positional-arguments]
demo-pay dataset:
    uv run --locked --group generator src/demo/run.py pay --dataset "$1"

# Replay Kafka events after payment; show raw growth and unchanged API amounts.
[group('Demo')]
[positional-arguments]
demo-duplicates dataset:
    uv run --locked --group generator src/demo/run.py duplicates --dataset "$1"

# Cancel o1 and record its refund; keep the payment history.
[group('Demo')]
[positional-arguments]
demo-refund dataset:
    uv run --locked --group generator src/demo/run.py refund --dataset "$1"

# Demonstrate physical deletion with a separate unpaid order in a new dataset.
[group('Demo')]
demo-delete:
    uv run --locked --group generator src/demo/run.py delete

# Print the current data from all five endpoints and the payment total as JSON.
[group('Demo')]
[positional-arguments]
demo-show dataset:
    uv run --locked --group generator src/demo/run.py show --dataset "$1"

# -----------------------------------------------------------------------------
# Data and API load generation
# -----------------------------------------------------------------------------

# Generate PostgreSQL data for 5 minutes; forward overrides.
[group('Load generation')]
[positional-arguments]
data-load *args:
    uv run --locked --group generator src/generator/data_load.py postgres --dsn "postgresql://admin:admin@127.0.0.1:${PG_PORT:-55434}/capstone" --duration 300 "$@"

# Generate API load for 5 minutes with 5% invalid requests; forward overrides.
[group('Load generation')]
[positional-arguments]
api-load *args:
    uv run --locked --group generator src/generator/api_load.py --url "http://127.0.0.1:${API_PORT:-18001}" --error-rate 0.05 --duration 300 "$@"
