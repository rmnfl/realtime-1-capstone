SELECT
    campaign_id,
    orders_count,
    created_orders_count,
    paid_orders_count,
    cancelled_orders_count,
    paid_amount_cents,
    avg_paid_amount_cents,
    paid_share,
    cancelled_share
FROM capstone.metrics_campaign_summary(
    p_dataset_id = {dataset_id:String},
    p_from = {from:DateTime64(6, 'UTC')},
    p_to = {to:DateTime64(6, 'UTC')}
)
ORDER BY paid_amount_cents DESC, campaign_id ASC
