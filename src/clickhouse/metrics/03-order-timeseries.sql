CREATE VIEW IF NOT EXISTS capstone.metrics_order_timeseries AS
SELECT
    {p_dataset_id:String} AS dataset_id,
    toDateTime64(toStartOfHour(o.created_at, 'UTC'), 6, 'UTC') AS bucket_start,
    count() AS orders_count,
    countIf(o.status = 'created') AS created_orders_count,
    countIf(o.status = 'paid') AS paid_orders_count,
    countIf(o.status = 'cancelled') AS cancelled_orders_count,
    sumIf(o.amount_cents, o.status = 'paid') AS paid_amount_cents,
    paid_amount_cents / nullIf(paid_orders_count, 0) AS avg_paid_amount_cents
FROM capstone.orders_latest AS o
WHERE o.dataset_id = {p_dataset_id:String}
    AND o.is_deleted = 0
    AND o.created_at >= {p_from:DateTime64(6, 'UTC')}
    AND o.created_at < {p_to:DateTime64(6, 'UTC')}
    AND ({p_campaign_id:String} = '' OR o.campaign_id = {p_campaign_id:String})
GROUP BY bucket_start
ORDER BY bucket_start;
