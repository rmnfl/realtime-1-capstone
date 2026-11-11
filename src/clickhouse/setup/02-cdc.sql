-- -----------------------------------------------------------------------------
-- orders
-- -----------------------------------------------------------------------------

-- A dedicated consumer group reads the original Debezium envelope.
CREATE TABLE IF NOT EXISTS capstone.orders_cdc_queue (
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list         = 'kafka:9092',
    kafka_topic_list          = 'capstone.capstone.orders',
    kafka_group_name          = 'capstone-orders-clickhouse',
    kafka_format              = 'JSONAsString',
    kafka_num_consumers       = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size      = 10000,
    kafka_flush_interval_ms   = 1000;

-- Snapshot, INSERT and UPDATE events contain the new state in after.
-- DELETE: use before, increment version by 1 and set is_deleted = 1.
-- Kafka tombstones are not order states and are excluded from raw.
CREATE MATERIALIZED VIEW IF NOT EXISTS capstone.orders_cdc_to_raw
TO capstone.orders_raw AS
WITH
    JSONExtractString(payload, 'op') AS _operation,
    if(
        _operation = 'd',
        JSONExtractRaw(payload, 'before'),
        JSONExtractRaw(payload, 'after')
    ) AS row_data
SELECT
    JSONExtractString(row_data, 'dataset_id')                               AS dataset_id,
    JSONExtractString(row_data, 'id')                                       AS id,
    JSONExtractString(row_data, 'campaign_id')                              AS campaign_id,
    JSONExtractString(row_data, 'status')                                   AS status,
    JSONExtract(row_data, 'amount_cents', 'Int64')                          AS amount_cents,
    JSONExtract(row_data, 'version', 'UInt32') + toUInt32(_operation = 'd') AS version,
    parseDateTime64BestEffort(
        JSONExtractString(row_data, 'created_at'),
        6,
        'UTC'
    )                                                                       AS created_at,
    parseDateTime64BestEffort(
        JSONExtractString(row_data, 'updated_at'),
        6,
        'UTC'
    )                                                                       AS updated_at,
    toUInt8(_operation = 'd')                                               AS is_deleted,
    JSONExtract(payload, 'source', 'lsn', 'UInt64')                         AS _source_lsn,
    fromUnixTimestamp64Milli(
        JSONExtract(payload, 'source', 'ts_ms', 'Nullable(Int64)'),
        'UTC'
    )                                                                       AS _source_ts,
    fromUnixTimestamp64Milli(
        JSONExtract(payload, 'ts_ms', 'Nullable(Int64)'),
        'UTC'
    )                                                                       AS _debezium_ts,
    now64(3, 'UTC')                                                         AS _ingested_at,
    _operation,
    _topic                                                                  AS _kafka_topic,
    _partition                                                              AS _kafka_partition,
    _offset                                                                 AS _kafka_offset
FROM capstone.orders_cdc_queue
WHERE _operation IN ('r', 'c', 'u', 'd');

-- -----------------------------------------------------------------------------
-- order_items
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS capstone.order_items_cdc_queue (
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list         = 'kafka:9092',
    kafka_topic_list          = 'capstone.capstone.order_items',
    kafka_group_name          = 'capstone-order-items-clickhouse',
    kafka_format              = 'JSONAsString',
    kafka_num_consumers       = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size      = 10000,
    kafka_flush_interval_ms   = 1000;

-- Items have no business version. Store the LSN and deletion flag.
CREATE MATERIALIZED VIEW IF NOT EXISTS capstone.order_items_cdc_to_raw
TO capstone.order_items_raw AS
WITH
    JSONExtractString(payload, 'op') AS _operation,
    if(
        _operation = 'd',
        JSONExtractRaw(payload, 'before'),
        JSONExtractRaw(payload, 'after')
    ) AS row_data
SELECT
    JSONExtractString(row_data, 'dataset_id')          AS dataset_id,
    JSONExtractString(row_data, 'id')                  AS id,
    JSONExtractString(row_data, 'order_id')            AS order_id,
    JSONExtractString(row_data, 'sku')                 AS sku,
    JSONExtract(row_data, 'quantity', 'Int32')         AS quantity,
    JSONExtract(row_data, 'unit_price_cents', 'Int64') AS unit_price_cents,
    toUInt8(_operation = 'd')                          AS is_deleted,
    JSONExtract(payload, 'source', 'lsn', 'UInt64')    AS _source_lsn,
    fromUnixTimestamp64Milli(
        JSONExtract(payload, 'source', 'ts_ms', 'Nullable(Int64)'),
        'UTC'
    )                                                  AS _source_ts,
    fromUnixTimestamp64Milli(
        JSONExtract(payload, 'ts_ms', 'Nullable(Int64)'),
        'UTC'
    )                                                  AS _debezium_ts,
    now64(3, 'UTC')                                    AS _ingested_at,
    _operation,
    _topic                                             AS _kafka_topic,
    _partition                                         AS _kafka_partition,
    _offset                                            AS _kafka_offset
FROM capstone.order_items_cdc_queue
WHERE _operation IN ('r', 'c', 'u', 'd');

-- -----------------------------------------------------------------------------
-- payments
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS capstone.payments_cdc_queue (
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list         = 'kafka:9092',
    kafka_topic_list          = 'capstone.capstone.payments',
    kafka_group_name          = 'capstone-payments-clickhouse',
    kafka_format              = 'JSONAsString',
    kafka_num_consumers       = 1,
    kafka_thread_per_consumer = 1,
    kafka_max_block_size      = 10000,
    kafka_flush_interval_ms   = 1000;

-- Payments are append-only: snapshots and new capture/refund operations.
-- The MV handles snapshots and INSERTs only; payment UPDATEs and DELETEs are ignored.
CREATE MATERIALIZED VIEW IF NOT EXISTS capstone.payments_cdc_to_raw
TO capstone.payments_raw AS
SELECT
    JSONExtract(payload, 'after', 'dataset_id', 'String')  AS dataset_id,
    JSONExtract(payload, 'after', 'id', 'String')          AS id,
    JSONExtract(payload, 'after', 'order_id', 'String')    AS order_id,
    JSONExtract(payload, 'after', 'kind', 'String')        AS kind,
    JSONExtract(payload, 'after', 'amount_cents', 'Int64') AS amount_cents,
    parseDateTime64BestEffort(
        JSONExtractString(payload, 'after', 'occurred_at'),
        6,
        'UTC'
    )                                                      AS occurred_at,
    JSONExtract(payload, 'source', 'lsn', 'UInt64')        AS _source_lsn,
    fromUnixTimestamp64Milli(
        JSONExtract(payload, 'source', 'ts_ms', 'Nullable(Int64)'),
        'UTC'
    )                                                      AS _source_ts,
    fromUnixTimestamp64Milli(
        JSONExtract(payload, 'ts_ms', 'Nullable(Int64)'),
        'UTC'
    )                                                      AS _debezium_ts,
    now64(3, 'UTC')                                        AS _ingested_at,
    JSONExtractString(payload, 'op')                       AS _operation,
    _topic                                                 AS _kafka_topic,
    _partition                                             AS _kafka_partition,
    _offset                                                AS _kafka_offset
FROM capstone.payments_cdc_queue
WHERE JSONExtractString(payload, 'op') IN ('r', 'c');
