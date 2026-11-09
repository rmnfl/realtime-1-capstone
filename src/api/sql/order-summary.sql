WITH totals AS (
    SELECT
        sum(c.orders_count) AS orders_count,
        sum(c.created_orders_count) AS created_orders_count,
        sum(c.paid_orders_count) AS paid_orders_count,
        sum(c.cancelled_orders_count) AS cancelled_orders_count,
        sum(c.paid_amount_cents) AS paid_amount_cents
    FROM capstone.metrics_campaign_summary(
        p_dataset_id = {dataset_id:String},
        p_from = {from:DateTime64(6, 'UTC')},
        p_to = {to:DateTime64(6, 'UTC')}
    ) AS c
    WHERE {campaign_id:String} = '' OR c.campaign_id = {campaign_id:String}
)
SELECT
    orders_count,
    created_orders_count,
    paid_orders_count,
    cancelled_orders_count,
    paid_amount_cents,
    paid_amount_cents / nullIf(paid_orders_count, 0) AS avg_paid_amount_cents,
    paid_orders_count / nullIf(orders_count, 0) AS paid_share,
    cancelled_orders_count / nullIf(orders_count, 0) AS cancelled_share
FROM totals
