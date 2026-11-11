CREATE DATABASE IF NOT EXISTS capstone;

CREATE USER IF NOT EXISTS reader IDENTIFIED WITH sha256_password BY 'reader';
GRANT SELECT ON capstone.* TO reader;

-- -----------------------------------------------------------------------------
-- orders
-- -----------------------------------------------------------------------------

-- Delivery history: retain all versions and duplicates.
CREATE TABLE IF NOT EXISTS capstone.orders_raw (
    dataset_id       String,
    id               String,
    campaign_id      String,
    status           String,
    amount_cents     Int64,
    version          UInt32,
    created_at       DateTime64(6, 'UTC'),
    updated_at       DateTime64(6, 'UTC'),
    is_deleted       UInt8,
    _source_lsn      UInt64,
    _source_ts       Nullable(DateTime64(3, 'UTC')),
    _debezium_ts     Nullable(DateTime64(3, 'UTC')),
    _ingested_at     DateTime64(3, 'UTC'),
    _operation       LowCardinality(String),
    _kafka_topic     LowCardinality(String),
    _kafka_partition Int32,
    _kafka_offset    UInt64
)
ENGINE = MergeTree
ORDER BY (dataset_id, id, version);

-- -----------------------------------------------------------------------------
-- order_items
-- -----------------------------------------------------------------------------

-- Items can be deleted; CDC metadata determines their version order.
CREATE TABLE IF NOT EXISTS capstone.order_items_raw (
    dataset_id       String,
    id               String,
    order_id         String,
    sku              String,
    quantity         Int32,
    unit_price_cents Int64,
    is_deleted       UInt8,
    _source_lsn      UInt64,
    _source_ts       Nullable(DateTime64(3, 'UTC')),
    _debezium_ts     Nullable(DateTime64(3, 'UTC')),
    _ingested_at     DateTime64(3, 'UTC'),
    _operation       LowCardinality(String),
    _kafka_topic     LowCardinality(String),
    _kafka_partition Int32,
    _kafka_offset    UInt64
)
ENGINE = MergeTree
ORDER BY (dataset_id, id, _source_lsn);

-- -----------------------------------------------------------------------------
-- payments
-- -----------------------------------------------------------------------------

-- Payments are immutable facts. Raw tables also retain duplicate deliveries.
CREATE TABLE IF NOT EXISTS capstone.payments_raw (
    dataset_id       String,
    id               String,
    order_id         String,
    kind             String,
    amount_cents     Int64,
    occurred_at      DateTime64(6, 'UTC'),
    _source_lsn      UInt64,
    _source_ts       Nullable(DateTime64(3, 'UTC')),
    _debezium_ts     Nullable(DateTime64(3, 'UTC')),
    _ingested_at     DateTime64(3, 'UTC'),
    _operation       LowCardinality(String),
    _kafka_topic     LowCardinality(String),
    _kafka_partition Int32,
    _kafka_offset    UInt64
)
ENGINE = MergeTree
ORDER BY (dataset_id, id);
