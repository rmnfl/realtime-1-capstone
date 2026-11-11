import asyncio
import logging

import httpx
from fastapi import HTTPException


LOG = logging.getLogger("uvicorn.error")


class ClickHouse:
    def __init__(self, client: httpx.AsyncClient, database: str):
        self.client = client
        self.database = database
        self.gate = asyncio.Semaphore(4)

    async def query(
        self,
        sql: str,
        parameters: dict[str, str | int],
        *,
        limit: int | None = None,
    ) -> list[dict]:
        if limit is not None:
            sql += "\nLIMIT {limit:UInt32}"
            parameters = {**parameters, "limit": limit}

        try:
            await asyncio.wait_for(self.gate.acquire(), timeout=0.5)
        except TimeoutError:
            raise HTTPException(429, "API busy") from None

        try:
            async with asyncio.timeout(5):
                response = await self.client.post(
                    "/",
                    params={
                        "database": self.database,
                        "output_format_json_quote_64bit_integers": "0",
                        "max_execution_time": "5",
                        **{f"param_{name}": value for name, value in parameters.items()},
                    },
                    content=sql + "\nFORMAT JSON",
                )
                response.raise_for_status()
                return response.json()["data"]
        except (httpx.HTTPError, TimeoutError, ValueError, KeyError) as error:
            if isinstance(error, httpx.HTTPStatusError):
                detail = f"HTTP {error.response.status_code}: {error.response.text[:1000]}"
            else:
                detail = f"{type(error).__name__}: {error}"
            LOG.error("ClickHouse query failed: %s", detail)
            raise HTTPException(503, "ClickHouse unavailable") from None
        finally:
            self.gate.release()
