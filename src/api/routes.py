import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Path as PathParam, Query, Request

from .filters import (
    ID_PATTERN,
    CampaignSummaryFilters,
    OrderSummaryFilters,
    OrderTimeseriesFilters,
    PaymentTimeseriesFilters,
    PeriodFilters,
    ProductSummaryFilters,
)
from .timeseries import (
    calculate_payment_total,
    fill_order_timeseries,
    fill_payment_timeseries,
)

SQL_DIR = Path(__file__).parent / "sql"
ORDER_SUMMARY_SQL = (SQL_DIR / "order-summary.sql").read_text(encoding="utf-8")
CAMPAIGN_SUMMARY_SQL = (SQL_DIR / "campaign-summary.sql").read_text(encoding="utf-8")
PRODUCT_SUMMARY_SQL = (SQL_DIR / "product-summary.sql").read_text(encoding="utf-8")
ORDER_TIMESERIES_SQL = (SQL_DIR / "order-timeseries.sql").read_text(encoding="utf-8")
PAYMENT_TIMESERIES_SQL = (SQL_DIR / "payment-timeseries.sql").read_text(encoding="utf-8")

router = APIRouter(prefix="/v1/datasets/{dataset_id}/metrics")
DatasetId = Annotated[str, PathParam(pattern=ID_PATTERN)]


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def build_period_parameters(dataset_id: str, query: PeriodFilters) -> dict[str, str]:
    return {
        "dataset_id": dataset_id,
        "from": query.from_.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "to": query.to.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


def build_metric_response(data: dict | list) -> dict:
    return {
        "data": data,
        "meta": {"computed_at": format_timestamp(datetime.now(timezone.utc))},
    }


def build_timeseries_response(points: list[dict]) -> dict:
    return build_metric_response([
        {**point, "bucket_start": format_timestamp(point["bucket_start"])}
        for point in points
    ])


def build_summary_cache_key(
    entity: str, dataset_id: str, query: PeriodFilters, extra: list[str | int | None]
) -> str:
    values = [dataset_id, format_timestamp(query.from_), format_timestamp(query.to), *extra]
    return f"capstone:{entity}:summary:v1:" + json.dumps(values, separators=(",", ":"))


@router.get("/orders/summary")
async def get_order_summary(
    request: Request,
    dataset_id: DatasetId,
    query: Annotated[OrderSummaryFilters, Query()],
):
    campaign = query.campaign_id or ""
    key = build_summary_cache_key("orders", dataset_id, query, [campaign])
    cache = request.app.state.cache
    cached = await cache.get(key, refresh=query.refresh)
    if cached is not None:
        return cached

    rows = await request.app.state.clickhouse.query(
        ORDER_SUMMARY_SQL,
        {**build_period_parameters(dataset_id, query), "campaign_id": campaign},
    )
    response = build_metric_response(rows[0])
    await cache.put(key, response)
    return response


@router.get("/campaigns/summary")
async def get_campaign_summary(
    request: Request,
    dataset_id: DatasetId,
    query: Annotated[CampaignSummaryFilters, Query()],
):
    key = build_summary_cache_key("campaigns", dataset_id, query, [query.limit])
    cache = request.app.state.cache
    cached = await cache.get(key, refresh=query.refresh)
    if cached is not None:
        return cached

    rows = await request.app.state.clickhouse.query(
        CAMPAIGN_SUMMARY_SQL, build_period_parameters(dataset_id, query), limit=query.limit
    )
    response = build_metric_response(rows)
    await cache.put(key, response)
    return response


@router.get("/products/summary")
async def get_product_summary(
    request: Request,
    dataset_id: DatasetId,
    query: Annotated[ProductSummaryFilters, Query()],
):
    campaign = query.campaign_id or ""
    key = build_summary_cache_key("products", dataset_id, query, [campaign, query.limit])
    cache = request.app.state.cache
    cached = await cache.get(key, refresh=query.refresh)
    if cached is not None:
        return cached

    rows = await request.app.state.clickhouse.query(
        PRODUCT_SUMMARY_SQL,
        {**build_period_parameters(dataset_id, query), "campaign_id": campaign},
        limit=query.limit,
    )
    response = build_metric_response(rows)
    await cache.put(key, response)
    return response


@router.get("/orders/timeseries")
async def get_order_timeseries(
    request: Request,
    dataset_id: DatasetId,
    query: Annotated[OrderTimeseriesFilters, Query()],
):
    rows = await request.app.state.clickhouse.query(
        ORDER_TIMESERIES_SQL,
        {**build_period_parameters(dataset_id, query), "campaign_id": query.campaign_id or ""},
    )
    points = fill_order_timeseries(rows, query.from_, query.to)
    return build_timeseries_response(points)


@router.get("/payments/timeseries")
async def get_payment_timeseries(
    request: Request,
    dataset_id: DatasetId,
    query: Annotated[PaymentTimeseriesFilters, Query()],
):
    rows = await request.app.state.clickhouse.query(
        PAYMENT_TIMESERIES_SQL, build_period_parameters(dataset_id, query)
    )
    if query.interval == "total":
        return build_timeseries_response([calculate_payment_total(rows, query.from_)])
    points = fill_payment_timeseries(rows, query.from_, query.to)
    return build_timeseries_response(points)
