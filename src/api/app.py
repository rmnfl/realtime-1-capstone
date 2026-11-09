import os
from contextlib import asynccontextmanager
from time import monotonic

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from .cache import RedisCache
from .clickhouse import ClickHouse
from .metrics import HTTP_DURATION, HTTP_REQUESTS
from .routes import router

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://127.0.0.1:18133")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "capstone")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "reader")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "reader")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with (
        httpx.AsyncClient(
            base_url=CLICKHOUSE_URL,
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
            timeout=5,
            trust_env=False,
        ) as clickhouse_client,
        Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
            retry=Retry(NoBackoff(), 0),
        ) as redis_client,
    ):
        app.state.clickhouse = ClickHouse(clickhouse_client, CLICKHOUSE_DATABASE)
        app.state.cache = RedisCache(redis_client, ttl=30)
        yield


app = FastAPI(title="Capstone: order analytics", lifespan=lifespan)
app.include_router(router)


@app.middleware("http")
async def track_http(request: Request, call_next):
    started = monotonic()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = getattr(request.scope.get("route"), "path", "unmatched")
        if route != "/metrics":
            HTTP_REQUESTS.labels(route=route, status=str(status)).inc()
            HTTP_DURATION.labels(route=route).observe(monotonic() - started)


@app.get("/ping")
async def ping():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
