CREATE VIEW IF NOT EXISTS capstone.metrics_campaign_summary AS
WITH stats AS (
    SELECT
        campaign_id,
        count() AS orders_count,
        countIf(status = 'created') AS created_orders_count,
        countIf(status = 'paid') AS paid_orders_count,
        countIf(status = 'cancelled') AS cancelled_orders_count,
        sumIf(amount_cents, status = 'paid') AS paid_amount_cents
    FROM capstone.orders_latest
    WHERE dataset_id = {p_dataset_id:String}
        AND is_deleted = 0
        AND created_at >= {p_from:DateTime64(6, 'UTC')}
        AND created_at < {p_to:DateTime64(6, 'UTC')}
    GROUP BY campaign_id
)
SELECT
    {p_dataset_id:String} AS dataset_id,
    campaign_id,
    orders_count,
    created_orders_count,
    paid_orders_count,
    cancelled_orders_count,
    paid_amount_cents,
    paid_amount_cents / nullIf(paid_orders_count, 0) AS avg_paid_amount_cents,
    paid_orders_count / nullIf(orders_count, 0) AS paid_share,
    cancelled_orders_count / nullIf(orders_count, 0) AS cancelled_share
FROM stats
ORDER BY paid_amount_cents DESC, campaign_id ASC;
