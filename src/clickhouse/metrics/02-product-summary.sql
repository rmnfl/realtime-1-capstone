CREATE VIEW IF NOT EXISTS capstone.metrics_product_summary AS
SELECT
    {p_dataset_id:String} AS dataset_id,
    i.sku,
    uniqExact(o.id) AS paid_orders_count,
    sum(toInt64(i.quantity)) AS units_count,
    sum(toInt64(i.quantity) * i.unit_price_cents) AS items_amount_cents
FROM capstone.order_items_latest AS i
INNER JOIN capstone.orders_latest AS o
    ON i.dataset_id = o.dataset_id AND i.order_id = o.id
WHERE o.dataset_id = {p_dataset_id:String}
    AND i.dataset_id = {p_dataset_id:String}
    AND o.is_deleted = 0
    AND i.is_deleted = 0
    AND o.status = 'paid'
    AND o.created_at >= {p_from:DateTime64(6, 'UTC')}
    AND o.created_at < {p_to:DateTime64(6, 'UTC')}
    AND ({p_campaign_id:String} = '' OR o.campaign_id = {p_campaign_id:String})
GROUP BY i.sku
ORDER BY items_amount_cents DESC, i.sku ASC;
