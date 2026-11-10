CREATE OR REPLACE VIEW capstone.pipeline_metrics AS
WITH
    deliveries AS (
        SELECT 'orders' AS source_table, _source_ts, _debezium_ts, _ingested_at
        FROM capstone.orders_raw
        WHERE _operation IN ('c', 'u', 'd')
            AND _ingested_at >= now64(3, 'UTC') - INTERVAL 5 MINUTE

        UNION ALL

        SELECT 'order_items' AS source_table, _source_ts, _debezium_ts, _ingested_at
        FROM capstone.order_items_raw
        WHERE _operation IN ('c', 'u', 'd')
            AND _ingested_at >= now64(3, 'UTC') - INTERVAL 5 MINUTE

        UNION ALL

        SELECT 'payments' AS source_table, _source_ts, _debezium_ts, _ingested_at
        FROM capstone.payments_raw
        WHERE _operation IN ('c', 'u', 'd')
            AND _ingested_at >= now64(3, 'UTC') - INTERVAL 5 MINUTE
    ),
    measured AS (
        SELECT
            *,
            dateDiff('millisecond', _source_ts, _debezium_ts) / 1000.0 AS cdc_seconds,
            dateDiff('millisecond', _debezium_ts, _ingested_at) / 1000.0 AS ingestion_seconds
        FROM deliveries
    ),
    valid AS (
        SELECT *
        FROM measured
        WHERE cdc_seconds >= 0 AND ingestion_seconds >= 0
    ),
    stages AS (
        SELECT
            source_table,
            stage.1 AS stage_name,
            assumeNotNull(stage.2) AS delay_seconds
        FROM valid
        ARRAY JOIN arrayZip(
            ['source_to_debezium', 'debezium_to_clickhouse', 'source_to_clickhouse'],
            [cdc_seconds, ingestion_seconds, cdc_seconds + ingestion_seconds]
        ) AS stage
    ),
    latency AS (
        SELECT
            source_table,
            stage_name,
            avg(delay_seconds) AS mean_seconds,
            quantileExact(0.95)(delay_seconds) AS p95_seconds
        FROM stages
        GROUP BY source_table, stage_name
    ),
    counts AS (
        SELECT
            source_table,
            count() AS deliveries_count,
            countIf(
                cdc_seconds IS NULL OR ingestion_seconds IS NULL
                OR cdc_seconds < 0 OR ingestion_seconds < 0
            ) AS invalid_count
        FROM measured
        GROUP BY source_table
    )
SELECT
    'capstone_pipeline_delivery_delay_seconds' AS name,
    'gauge' AS type,
    'Delivery delay over the last five minutes; snapshots and invalid timestamps excluded' AS help,
    map('table', source_table, 'stage', stage_name, 'statistic', statistic.1) AS labels,
    statistic.2 AS value
FROM latency
ARRAY JOIN [('mean', mean_seconds), ('p95', p95_seconds)] AS statistic

UNION ALL

SELECT
    metric.1 AS name,
    'gauge' AS type,
    metric.2 AS help,
    map('table', source_table) AS labels,
    metric.3 AS value
FROM (
    SELECT arrayJoin(['orders', 'order_items', 'payments']) AS source_table
) AS tables
LEFT JOIN counts USING (source_table)
ARRAY JOIN [
    (
        'capstone_pipeline_deliveries_5m',
        'Streaming deliveries observed in raw over the last five minutes, including retries',
        toFloat64(ifNull(deliveries_count, 0))
    ),
    (
        'capstone_pipeline_invalid_deliveries_5m',
        'Deliveries excluded from latency because timestamps are missing or out of order',
        toFloat64(ifNull(invalid_count, 0))
    )
] AS metric
ORDER BY name, labels;
