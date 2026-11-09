import json
import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .metrics import CACHE_REQUESTS, REDIS_ERRORS

LOG = logging.getLogger("uvicorn.error")


class RedisCache:
    def __init__(self, client: Redis, ttl: int = 30):
        self.client = client
        self.ttl = ttl

    async def get(self, key: str, *, refresh: bool = False) -> dict | None:
        if refresh:
            CACHE_REQUESTS.labels(result="miss").inc()
            return None

        try:
            value = await self.client.get(key)
        except RedisError:
            REDIS_ERRORS.labels(operation="get").inc()
            LOG.warning("Redis cache GET failed; continuing without cache")
            value = None
        if value is None:
            CACHE_REQUESTS.labels(result="miss").inc()
            return None
        cached = json.loads(value)
        CACHE_REQUESTS.labels(result="hit" if cached is not None else "miss").inc()
        return cached

    async def put(self, key: str, response: dict) -> None:
        value = json.dumps(response)
        try:
            await self.client.set(key, value, ex=self.ttl)
        except RedisError:
            REDIS_ERRORS.labels(operation="set").inc()
            LOG.warning("Redis cache SET failed; continuing without cache")
