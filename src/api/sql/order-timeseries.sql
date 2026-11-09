SELECT
    bucket_start,
    orders_count,
    created_orders_count,
    paid_orders_count,
    cancelled_orders_count,
    paid_amount_cents,
    avg_paid_amount_cents
FROM capstone.metrics_order_timeseries(
    p_dataset_id = {dataset_id:String},
    p_from = {from:DateTime64(6, 'UTC')},
    p_to = {to:DateTime64(6, 'UTC')},
    p_campaign_id = {campaign_id:String}
)
ORDER BY bucket_start
