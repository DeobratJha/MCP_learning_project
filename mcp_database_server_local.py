"""
Run:
    python mcp_database_server_local.py
"""

from __future__ import annotations

import sqlite3
import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

import aiosqlite
from dotenv import load_dotenv
from fastmcp import FastMCP, Context
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import BaseModel, Field, field_validator

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
logger = logging.getLogger("mcp_database_server_local")

SQLITE_PATH = os.getenv("SQLITE_PATH","learning_demo.db")
DATABASE_URL = os.getenv("DATABASE_URL")
API_KEY = os.getenv("MCP_API_KEY") #if unset -> auth is diabled

TOOL_TIMEOUT_SECONDS = float(os.getenv("TOOL_TIMEOUT_SECONDS","10"))
MAX_RESULT_ROWS = int(os.getenv("MAX_RESULT_ROWS","100"))
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE","300"))

USE_POSTGRES = bool(DATABASE_URL)
if USE_POSTGRES:
    import asyncpg

TOOL_CALLS = Counter("mcp_tool_calls_total","Total tool invocations",["tool_name","outcome"])
TOOL_LATENCY = Histogram(
    "mcp_tool_latency_seconds","Tool call latency (s)", ["tool_name"],
    buckets = (0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5,10),
)
DB_QUERY_LATENCY = Histogram("mcp_db_query_latency_seconds","Raw DB query latency (s)", ["tool_name"])
RATE_LIMIT_REJECTIONS = Counter("mcp_rate_limit_rejections_total","Rejected due to rate limit",["client_id"])
AUTH_FAILURES = Counter("mcp_auth_failures_total","Auth failures",["reason"])

class RateLimiter:
    def __init__(self, limit_per_minute: int):
        self.limit = limit_per_minute
        self._hits: dict[str, list[float]] = {}

    def allow(self,client_id:str) -> bool:
        now = time.monotonic()
        window_start = now-60
        hits = [t for t in self._hits.get(client_id,[]) if t > window_start]
        if len(hits) >= self.limit:
            self._hits[client_id] = hits
            return False

        hits.append(now)
        self._hits[client_id] = hits
        return True

rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)

class ToolError(Exception):
    def __init__(self,code: str, message: str, http_status: int = 400):
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(message)

_pg_pool = None

async def init_sqlite() -> None:
    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute("""

        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            total_amount REAL NOT NULL)
        """
        )

        count = await db.execute_fetchall("SELECT COUNT(*) FROM ORDERS")
        if count[0][0] == 0:
            demo_rows = [
                ("ACC-12345","2026-09-01","SHIPPED",129.99),
                ("ACC-12345","2026-09-05","PROCESSING",45.50),
                ("ACC-12345","2026-09-10","DELIVERED",89.00),
                ("ACC-67890","2026-08-20","DELIVERED",15.75),
                ]

            await db.executemany(
                "INSERT INTO orders (account_id, order_date, status, total_amount) VALUES(?,?,?,?)",
                demo_rows,
            )
            await db.commit()
    logger.info(f"SQLite DB ready at {SQLITE_PATH} (seeded with the demo data if empty)")

async def get_pg_pool():
    global _pg_pool
    if _pg_pool is None:
        _pg_pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
        async with _pg_pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS orders(
                order_id SERIAL PRIMARY KEY,
                account_id TEXT NOT NULL,
                order_date DATE NOT NULL,
                status TEXT NOT NULL,
                total_amount NUMERIC NOT NULL
                )
                """
            )
        logger.info("Postgres pool ready")
    return _pg_pool

@asynccontextmanager
async def instrumented_tool_call(tool_name: str, client_id: str):
    correlation_id = str(uuid.uuid4())
    if not rate_limiter.allow(client_id):
        RATE_LIMIT_REJECTIONS.labels(client_id=client_id).inc()
        raise ToolError("RATE_LIMIT_EXCEEDED","Too many requests, slow down.",429)

    start = time.perf_counter()
    outcome = "success"
    try:
        yield correlation_id
    except ToolError:
        outcome = "client_error"
        raise
    except asyncio.TimeoutError:
        outcome = "timeout"
        raise ToolError("REQUEST_TIMEOUT",f"Timed out after {TOOL_TIMEOUT_SECONDS}s",504)
    except Exception as exc:
        outcome = "server_error"
        logger.error(f"[{correlation_id}] tool={tool_name} failed: {exc!r}")
        raise ToolError("INTERNAL_ERROR","Internal error - see server logs.", 500)
    finally:
        elapsed = time.perf_counter()-start
        TOOL_LATENCY.labels(tool_name=tool_name).observe(elapsed)
        TOOL_CALLS.labels(tool_name=tool_name, outcome=outcome).inc()
        logger.info(f"[{correlation_id}] tool={tool_name} outcome={outcome} latency={elapsed:.3f}s")

class GetOrdersInput(BaseModel):
    account_id: str = Field(..., description="Customer account ID, e.g. 'ACC-12345'")
    limit: int = Field(10, ge=1, le=MAX_RESULT_ROWS, description= "Max rows to return (1-100)")

    @field_validator("account_id")
    @classmethod
    def validate_account_id(cls, v:str) -> str:
        if not v.replace("-","").isalnum():
            raise ValueError("account_id must be alpahnumeric (dashes allowed)")
        return v


mcp = FastMCP(name="learning-order-db-mcp")

@mcp.tool
async def get_orders(input: GetOrdersInput, ctx: Context)-> dict:
    api_key = ctx.get_http_header("x-api-key",None) if hasattr(ctx,"get_http_header") else None
    if API_KEY and api_key != API_KEY:
        AUTH_FAILURES.labels(reason="invalide_api_key").inc()
        raise ToolError("UNAUTHORIZED","Missing or invalid API Key.",401)

    client_id = api_key or "anonymous"

    async with instrumented_tool_call("get_orders",client_id) as correlation_id:
        db_start = time.perf_counter()
        try:
            async with asyncio.timeout(TOOL_TIMEOUT_SECONDS):
                if USE_POSTGRES:
                    pool = await get_pg_pool()
                    async with pool.acquire() as conn:
                        rows = await conn.fetch(
                            """
                            SELECT order_id, order_date, status, total_amount
                            FROM orders where account_id = $1
                            ORDER BY order_date DESC LIMIT $2
                            """,
                            input.account_id,
                            input.limit,
                        )
                        rows = [dict(r) for r in rows]
                else:
                    async with aiosqlite.connect(SQLITE_PATH) as db:
                        db.row_factory = sqlite3.Row
                        cursor = await db.execute(
                            """
                            SELECT order_id, order_date, status, total_amount
                            FROM orders WHERE account_id = ?
                            order by order_date DESC limit ?
                            """,
                            (input.account_id, input.limit),
                        )
                        rows = [dict(r) for r in await cursor.fetchall()]

        finally:
            DB_QUERY_LATENCY.labels(tool_name="get_orders").observe(time.perf_counter()-db_start)

        return {
            "correlation_id": correlation_id,
            "account_id": input.account_id,
            "count":len(rows),
            "orders":rows,
        }

@mcp.custom_route("/health",methods=["GET"])
async def health(request):
    from starlette.responses import JSONResponse
    return JSONResponse({"status":"alive"})

metrics_app = make_asgi_app()

if __name__ == "__main__":
    if not USE_POSTGRES:
        asyncio.run(init_sqlite())
    logger.info(f"Starting local MCP server on http://0.0.0.0:8000/mcp (postgres={USE_POSTGRES}, Auth={'ON' if API_KEY else 'OFF (learning mode)'})")
    mcp.run(transport="streamable-http",host="0.0.0.0",port=8000)