"""
Load testing client for a FastMCP Streamable HTTP MCP server.

Example:
    python mcp_load_test.py --tool get_orders

With custom concurrency:
    python mcp_load_test.py --tool get_orders --concurrency 5,10,25,50

With custom requests:
    python mcp_load_test.py --tool get_orders --request-per-level 100
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
import uuid
from dataclasses import dataclass, field

import aiohttp
from dotenv import load_dotenv


load_dotenv()


# ============================================================
# Configuration
# ============================================================

DEFAULT_URL = os.getenv(
    "MCP_SERVER_URL",
    "http://localhost:8000/mcp"
)

DEFAULT_API_KEY = os.getenv("MCP_API_KEY")


def parse_mcp_response(response_text: str) -> dict:
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        for line in response_text.splitlines():
            if line.startswith("data:"):
                try:
                    return json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue

    raise ValueError(
        "Invalid MCP response: "
        + response_text[:500]
    )


# ============================================================
# Result models
# ============================================================

@dataclass
class CallResult:
    latency_s: float
    success: bool
    status_code: int
    error: str | None = None


@dataclass
class LevelReport:
    concurrency: int
    results: list[CallResult] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def latencies(self) -> list[float]:
        return [
            r.latency_s
            for r in self.results
            if r.success
        ]

    @property
    def error_rate(self) -> float:
        if not self.results:
            return 0.0

        failures = sum(
            1 for r in self.results
            if not r.success
        )

        return failures / len(self.results)

    def percentile(self, pct: float) -> float:
        lats = sorted(self.latencies)

        if not lats:
            return float("nan")

        index = int(
            round(
                (pct / 100)
                * (len(lats) - 1)
            )
        )

        return lats[index]

    @property
    def throughput_rps(self) -> float:
        if self.duration_s <= 0:
            return 0.0

        return len(self.results) / self.duration_s


# ============================================================
# MCP Session
# ============================================================

async def initialize_mcp_session(
    session: aiohttp.ClientSession,
    url: str,
    token: str | None,
) -> str:
    """
    Create an MCP Streamable HTTP session.

    Returns:
        MCP session ID
    """

    request_id = str(uuid.uuid4())

    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {
                "name": "mcp-load-tester",
                "version": "1.0.0",
            },
        },
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    if token:
        headers["x-api-key"] = token

    async with session.post(
        url,
        json=body,
        headers=headers,
    ) as response:

        response_text = await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"MCP initialize failed "
                f"(HTTP {response.status}): "
                f"{response_text[:500]}"
            )

        session_id = response.headers.get(
            "Mcp-Session-Id"
        )

        if not session_id:
            raise RuntimeError(
                "MCP server did not return "
                "Mcp-Session-Id"
            )

        return session_id


async def send_initialized_notification(
    session: aiohttp.ClientSession,
    url: str,
    session_id: str,
    token: str | None,
) -> None:
    """
    Tell MCP server that initialization is complete.
    """

    body = {
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Mcp-Session-Id": session_id,
    }

    if token:
        headers["x-api-key"] = token

    async with session.post(
        url,
        json=body,
        headers=headers,
    ) as response:

        await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"Initialized notification failed: "
                f"HTTP {response.status}"
            )


# ============================================================
# MCP Tool Call
# ============================================================

async def call_tool_once(
    session: aiohttp.ClientSession,
    url: str,
    tool: str,
    token: str | None,
    session_id: str,
    payload: dict,
) -> CallResult:

    request_id = str(uuid.uuid4())

    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": tool,
            "arguments": {
                "input":payload},
        },
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Mcp-Session-Id": session_id,
    }

    if token:
        headers["x-api-key"] = token

    start = time.perf_counter()

    try:

        async with session.post(
            url,
            json=body,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:

            response_text = await response.text()

            elapsed = time.perf_counter() - start

            if response.status >= 400:

                return CallResult(
                    latency_s=elapsed,
                    success=False,
                    status_code=response.status,
                    error=response_text[:500],
                )

            try:
                data = parse_mcp_response(response_text)
            except ValueError as exc:
                return CallResult(
                    latency_s=elapsed,
                    success=False,
                    status_code=response.status,
                    error=str(exc),
                )

            if "error" in data:

                return CallResult(
                    latency_s=elapsed,
                    success=False,
                    status_code=response.status,
                    error=str(data["error"])[:500],
                )

            return CallResult(
                latency_s=elapsed,
                success=True,
                status_code=response.status,
            )

    except asyncio.TimeoutError:

        return CallResult(
            latency_s=time.perf_counter() - start,
            success=False,
            status_code=0,
            error="client_timeout",
        )

    except Exception as exc:

        return CallResult(
            latency_s=time.perf_counter() - start,
            success=False,
            status_code=0,
            error=str(exc)[:500],
        )


# ============================================================
# Run one concurrency level
# ============================================================

async def run_level(
    url: str,
    tool: str,
    token: str | None,
    payload: dict,
    concurrency: int,
    total_requests: int,
) -> LevelReport:

    report = LevelReport(
        concurrency=concurrency
    )

    connector = aiohttp.TCPConnector(
        limit=concurrency
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        # ----------------------------------------------------
        # Create MCP session
        # ----------------------------------------------------

        try:

            session_id = await initialize_mcp_session(
                session=session,
                url=url,
                token=token,
            )

            await send_initialized_notification(
                session=session,
                url=url,
                session_id=session_id,
                token=token,
            )

        except Exception as exc:

            print(
                f"\nMCP session initialization failed "
                f"at concurrency {concurrency}:"
            )

            print(f"  {exc}")

            report.results = [
                CallResult(
                    latency_s=0,
                    success=False,
                    status_code=0,
                    error=str(exc),
                )
                for _ in range(total_requests)
            ]

            return report

        # ----------------------------------------------------
        # Run concurrent requests
        # ----------------------------------------------------

        semaphore = asyncio.Semaphore(
            concurrency
        )

        async def bounded_call():

            async with semaphore:

                return await call_tool_once(
                    session=session,
                    url=url,
                    tool=tool,
                    token=token,
                    session_id=session_id,
                    payload=payload,
                )

        start_time = time.perf_counter()

        tasks = [
            asyncio.create_task(
                bounded_call()
            )
            for _ in range(total_requests)
        ]

        report.results = await asyncio.gather(
            *tasks
        )

        report.duration_s = (
            time.perf_counter() - start_time
        )

    return report


# ============================================================
# Print report
# ============================================================

def print_report(
    report: LevelReport,
) -> None:

    print(
        f"\n---- Concurrency = "
        f"{report.concurrency} ----"
    )

    print(
        f" Request sent       : "
        f"{len(report.results)}"
    )

    print(
        f" Error rate         : "
        f"{report.error_rate * 100:.2f}%"
    )

    print(
        f" Duration           : "
        f"{report.duration_s:.3f} s"
    )

    print(
        f" Throughput         : "
        f"{report.throughput_rps:.2f} req/s"
    )

    if report.latencies:

        print(
            f" Latency avg        : "
            f"{statistics.mean(report.latencies) * 1000:.1f} ms"
        )

        print(
            f" Latency P50        : "
            f"{report.percentile(50) * 1000:.1f} ms"
        )

        print(
            f" Latency P95        : "
            f"{report.percentile(95) * 1000:.1f} ms"
        )

        print(
            f" Latency P99        : "
            f"{report.percentile(99) * 1000:.1f} ms"
        )

    errors = [
        r.error
        for r in report.results
        if not r.success
    ][:3]

    if errors:

        print(" Sample errors      :")

        for error in errors:
            print(f"   {error}")


# ============================================================
# Main async
# ============================================================

async def main_async(
    args: argparse.Namespace,
) -> None:

    concurrencies = [
        int(c.strip())
        for c in args.concurrency.split(",")
    ]

    payload = (
        json.loads(args.payload)
        if args.payload
        else {
            "account_id": "ACC-12345",
            "limit": 10,
        }
    )

    print(
        f"Target: {args.url}"
    )

    print(
        f"Tool: {args.tool}"
    )

    print(
        f"Payload: {payload}"
    )

    print(
        f"Requests per level: "
        f"{args.request_per_level}"
    )

    all_reports = []

    for concurrency in concurrencies:

        report = await run_level(
            url=args.url,
            tool=args.tool,
            token=args.token,
            payload=payload,
            concurrency=concurrency,
            total_requests=args.request_per_level,
        )

        print_report(report)

        all_reports.append(report)

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print(
        "\n======== SUMMARY ========"
    )

    print(
        f"{'Concurrency':>12} "
        f"{'P50(ms)':>10} "
        f"{'P95(ms)':>10} "
        f"{'P99(ms)':>10} "
        f"{'Err%':>8} "
        f"{'RPS':>10}"
    )

    for report in all_reports:

        p50 = (
            report.percentile(50) * 1000
            if report.latencies
            else float("nan")
        )

        p95 = (
            report.percentile(95) * 1000
            if report.latencies
            else float("nan")
        )

        p99 = (
            report.percentile(99) * 1000
            if report.latencies
            else float("nan")
        )

        print(
            f"{report.concurrency:>12} "
            f"{p50:>10.1f} "
            f"{p95:>10.1f} "
            f"{p99:>10.1f} "
            f"{report.error_rate * 100:>7.2f}% "
            f"{report.throughput_rps:>10.2f}"
        )


# ============================================================
# CLI
# ============================================================

def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "Load tester for FastMCP "
            "Streamable HTTP server"
        )
    )

    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=(
            "MCP server endpoint. "
            "Default: http://localhost:8000/mcp"
        ),
    )

    parser.add_argument(
        "--tool",
        required=True,
        help="MCP tool name, e.g. get_orders",
    )

    parser.add_argument(
        "--payload",
        default=None,
        help=(
            "JSON string containing "
            "tool arguments"
        ),
    )

    parser.add_argument(
        "--concurrency",
        default="5,10,25,50,100",
        help=(
            "Comma-separated concurrency "
            "levels"
        ),
    )

    parser.add_argument(
        "--request-per-level",
        type=int,
        default=200,
        help=(
            "Number of requests per "
            "concurrency level"
        ),
    )

    parser.add_argument(
        "--token",
        default=DEFAULT_API_KEY,
        help="MCP API key",
    )

    args = parser.parse_args()

    asyncio.run(
        main_async(args)
    )


if __name__ == "__main__":
    main()