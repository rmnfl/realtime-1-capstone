SELECT
    bucket_start,
    captures_count,
    captured_amount_cents,
    refunds_count,
    refunded_amount_cents,
    net_amount_cents
FROM capstone.metrics_payment_timeseries(
    p_dataset_id = {dataset_id:String},
    p_from = {from:DateTime64(6, 'UTC')},
    p_to = {to:DateTime64(6, 'UTC')}
)
ORDER BY bucket_start
