SELECT
    sku,
    paid_orders_count,
    units_count,
    items_amount_cents
FROM capstone.metrics_product_summary(
    p_dataset_id = {dataset_id:String},
    p_from = {from:DateTime64(6, 'UTC')},
    p_to = {to:DateTime64(6, 'UTC')},
    p_campaign_id = {campaign_id:String}
)
ORDER BY items_amount_cents DESC, sku ASC
