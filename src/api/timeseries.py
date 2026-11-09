from datetime import datetime, timedelta, timezone


def build_hour_grid(start: datetime, end: datetime) -> list[datetime]:
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    if start >= end:
        return []

    current = start.replace(minute=0, second=0, microsecond=0)
    grid = []
    while current < end:
        grid.append(current)
        current += timedelta(hours=1)
    return grid


def fill_hour_grid(grid: list[datetime], rows: list[dict], empty_values: dict) -> list[dict]:
    rows_by_hour = {
        datetime.fromisoformat(row["bucket_start"]).replace(tzinfo=timezone.utc): row
        for row in rows
    }
    result = []
    for hour in grid:
        row = rows_by_hour.get(hour, {})
        result.append({**empty_values, **row, "bucket_start": hour})
    return result


def fill_order_timeseries(rows: list[dict], start: datetime, end: datetime) -> list[dict]:
    grid = build_hour_grid(start, end)
    points = fill_hour_grid(grid, rows, {
        "orders_count": 0,
        "created_orders_count": 0,
        "paid_orders_count": 0,
        "cancelled_orders_count": 0,
        "paid_amount_cents": 0,
        "avg_paid_amount_cents": None,
    })
    cumulative = 0
    for point in points:
        cumulative += point["paid_amount_cents"]
        point["cumulative_paid_amount_cents"] = cumulative
    return points


def fill_payment_timeseries(rows: list[dict], start: datetime, end: datetime) -> list[dict]:
    grid = build_hour_grid(start, end)
    return fill_hour_grid(grid, rows, {
        "captures_count": 0,
        "captured_amount_cents": 0,
        "refunds_count": 0,
        "refunded_amount_cents": 0,
        "net_amount_cents": 0,
    })


def calculate_payment_total(rows: list[dict], start: datetime) -> dict:
    captured = sum(row["captured_amount_cents"] for row in rows)
    refunded = sum(row["refunded_amount_cents"] for row in rows)
    return {
        "bucket_start": start,
        "captures_count": sum(row["captures_count"] for row in rows),
        "captured_amount_cents": captured,
        "refunds_count": sum(row["refunds_count"] for row in rows),
        "refunded_amount_cents": refunded,
        "net_amount_cents": captured - refunded,
    }
