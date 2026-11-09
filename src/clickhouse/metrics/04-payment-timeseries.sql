CREATE VIEW IF NOT EXISTS capstone.metrics_payment_timeseries AS
SELECT
    {p_dataset_id:String} AS dataset_id,
    toDateTime64(toStartOfHour(p.occurred_at, 'UTC'), 6, 'UTC') AS bucket_start,
    countIf(p.kind = 'capture') AS captures_count,
    sumIf(p.amount_cents, p.kind = 'capture') AS captured_amount_cents,
    countIf(p.kind = 'refund') AS refunds_count,
    sumIf(p.amount_cents, p.kind = 'refund') AS refunded_amount_cents,
    captured_amount_cents - refunded_amount_cents AS net_amount_cents
FROM capstone.payments_latest AS p
WHERE p.dataset_id = {p_dataset_id:String}
    AND p.occurred_at >= {p_from:DateTime64(6, 'UTC')}
    AND p.occurred_at < {p_to:DateTime64(6, 'UTC')}
GROUP BY bucket_start
ORDER BY bucket_start;
