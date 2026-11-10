set shell := ["bash", "-euo", "pipefail", "-c"]
set dotenv-load

[private]
default:
    @just --list

# -----------------------------------------------------------------------------
# Stack
# -----------------------------------------------------------------------------

# Build, initialize and start the complete stack; preserve data and offsets.
up: check
    docker compose up -d --build --wait postgres kafka connect clickhouse redis
    just setup
    just cdc-register
    docker compose up -d --build --wait api postgres-exporter redis-exporter kafka-exporter prometheus grafana
    just ping
    just cdc-status

# Start an already initialized stack without changing schemas or connector config.
start: check
    docker compose up -d --wait

# Remove containers and network; pass --volumes to also delete volumes and offsets.
[positional-arguments]
down *args:
    docker compose down "$@"

# Validate Compose configuration.
check:
    docker compose config --quiet

# -----------------------------------------------------------------------------
# Core services
# -----------------------------------------------------------------------------

# Start only postgres and wait for readiness; leave dependencies unchanged.
postgres-start:
    docker compose up -d --no-deps --wait postgres

# Stop only postgres; preserve its container and data.
postgres-stop:
    docker compose stop postgres

# -----------------------------------------------------------------------------

# Start only kafka and wait for readiness; leave dependencies unchanged.
kafka-start:
    docker compose up -d --no-deps --wait kafka

# Stop only kafka; preserve its container and data.
kafka-stop:
    docker compose stop kafka

# -----------------------------------------------------------------------------

# Start only connect and wait for readiness; leave dependencies unchanged.
connect-start:
    docker compose up -d --no-deps --wait connect

# Stop only connect; preserve its container and data.
connect-stop:
    docker compose stop connect

# -----------------------------------------------------------------------------

# Start only clickhouse and wait for readiness; leave dependencies unchanged.
clickhouse-start:
    docker compose up -d --no-deps --wait clickhouse

# Stop only clickhouse; preserve its container and data.
clickhouse-stop:
    docker compose stop clickhouse

# -----------------------------------------------------------------------------

# Start only api and wait for readiness; leave dependencies unchanged.
api-start:
    docker compose up -d --no-deps --wait api

# Stop only api; preserve its container and data.
api-stop:
    docker compose stop api

# -----------------------------------------------------------------------------

# Start only redis and wait for readiness; leave dependencies unchanged.
redis-start:
    docker compose up -d --no-deps --wait redis

# Stop only redis; preserve its container and data.
redis-stop:
    docker compose stop redis

# -----------------------------------------------------------------------------
# Storage and business views
# -----------------------------------------------------------------------------

# Prepare ClickHouse storage and views before enabling Kafka consumption.
setup:
    docker compose exec -T clickhouse clickhouse-client --user admin --password password --multiquery < src/clickhouse/setup/00-init.sql
    docker compose exec -T clickhouse clickhouse-client --user admin --password password --multiquery < src/clickhouse/setup/01-current.sql
    just metrics-setup
    docker compose exec -T clickhouse clickhouse-client --user admin --password password --multiquery < src/clickhouse/setup/02-cdc.sql
    docker compose exec -T clickhouse clickhouse-client --user admin --password password --multiquery < monitoring/clickhouse/pipeline.sql

# Create business views; IF NOT EXISTS does not update existing definitions.
metrics-setup:
    for path in src/clickhouse/metrics/*.sql; do docker compose exec -T clickhouse clickhouse-client --user admin --password password --multiquery < "$path"; done

# -----------------------------------------------------------------------------
# CDC
# -----------------------------------------------------------------------------

# Register/update Debezium configuration, keeping its existing offsets.
cdc-register:
    curl -fsS -X PUT "http://127.0.0.1:${CONNECT_PORT:-28084}/connectors/capstone-cdc/config" -H 'Content-Type: application/json' --data-binary @src/debezium/connector.json | python3 -m json.tool

# Show connector and task states; both should be RUNNING.
cdc-status:
    curl -fsS "http://127.0.0.1:${CONNECT_PORT:-28084}/connectors/capstone-cdc/status" | python3 -m json.tool

# Restart the Connect worker after stalled delivery; then verify the API result.
cdc-restart:
    docker compose restart connect
    docker compose up -d --no-deps --wait connect
    just cdc-status

# -----------------------------------------------------------------------------
# Diagnostics
# -----------------------------------------------------------------------------

# Show this project's containers, health and published ports.
status:
    docker compose ps -a

# Show recent logs; optionally specify services, e.g. just logs api connect.
[positional-arguments]
logs *services:
    docker compose logs --no-color --tail=100 "$@"

# Check the API process; this does not check data freshness.
ping:
    curl -fsS "http://127.0.0.1:${API_PORT:-18001}/ping" | python3 -m json.tool

# -----------------------------------------------------------------------------
# SQL consoles
# -----------------------------------------------------------------------------

# Open PostgreSQL or pass psql arguments, e.g. just pg -c 'SELECT 1'.
[positional-arguments]
pg *args:
    docker compose exec postgres psql -U user -d capstone -v ON_ERROR_STOP=1 "$@"

# Open ClickHouse or pass client arguments, e.g. just ch --query 'SELECT 1'.
[positional-arguments]
ch *args:
    docker compose exec clickhouse clickhouse-client --user admin --password password --database capstone "$@"

# -----------------------------------------------------------------------------
# Monitoring
# -----------------------------------------------------------------------------

# Validate Prometheus configuration and alert rules.
rules-check:
    docker compose exec -T prometheus promtool check config /etc/prometheus/prometheus.yml

# Reload Prometheus configuration and rules after validation.
monitoring-reload: rules-check
    docker compose kill -s SIGHUP prometheus

# -----------------------------------------------------------------------------
# Demo
# -----------------------------------------------------------------------------

# Run all checks before the presentation: snapshot, order lifecycle, Kafka replay and deletion.
demo:
    uv run --locked --group generator src/demo/run.py all

# Check all five endpoints against the initial demo dataset.
demo-snapshot:
    uv run --locked --group generator src/demo/run.py snapshot

# Run create, pay, Kafka replay and refund in a new dataset; skip the initial snapshot.
demo-pipeline:
    uv run --locked --group generator src/demo/run.py pipeline

# Create one order with two items; show the expected API changes.
[positional-arguments]
demo-create dataset:
    uv run --locked --group generator src/demo/run.py create --dataset "$1"

# Pay o1 and record its capture; wait for the expected API results.
[positional-arguments]
demo-pay dataset:
    uv run --locked --group generator src/demo/run.py pay --dataset "$1"

# Cancel o1 and record its refund; keep the payment history.
[positional-arguments]
demo-refund dataset:
    uv run --locked --group generator src/demo/run.py refund --dataset "$1"

# Replay Kafka events after payment; show raw growth and unchanged API amounts.
[positional-arguments]
demo-duplicates dataset:
    uv run --locked --group generator src/demo/run.py duplicates --dataset "$1"

# Demonstrate physical deletion with a separate unpaid order in a new dataset.
demo-delete:
    uv run --locked --group generator src/demo/run.py delete

# Print the current data from all five endpoints and the payment total as JSON.
[positional-arguments]
demo-show dataset:
    uv run --locked --group generator src/demo/run.py show --dataset "$1"

# -----------------------------------------------------------------------------
# Data and API load generation
# -----------------------------------------------------------------------------

# Generate PostgreSQL data for 5 minutes; forward overrides.
[positional-arguments]
data-load *args:
    uv run --locked --group generator src/generator/data_load.py postgres --dsn "postgresql://user:password@127.0.0.1:${PG_PORT:-55434}/capstone" --duration 300 "$@"

# Generate API load for 5 minutes with 5% invalid requests; forward overrides.
[positional-arguments]
api-load *args:
    uv run --locked --group generator src/generator/api_load.py --url "http://127.0.0.1:${API_PORT:-18001}" --error-rate 0.05 --duration 300 "$@"
