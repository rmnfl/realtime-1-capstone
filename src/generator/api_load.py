"""uv run --group generator src/generator/api_load.py --help"""

import argparse
import asyncio
import json
import math
import random
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import cycle, islice

import httpx
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn


ENDPOINTS = (
    "orders/summary",
    "campaigns/summary",
    "products/summary",
    "orders/timeseries",
    "payments/timeseries",
)
ERROR_TYPES = ("missing-from", "invalid-date", "reversed-period", "invalid-dataset")


def generate_requests(args, totals):
    rng = random.Random(args.seed)
    endpoints = cycle(args.endpoint or ENDPOINTS)
    number = 0
    while args.requests is None or number < args.requests:
        endpoint = next(endpoints)
        dataset = args.dataset
        params = {"from": args.start, "to": args.end}
        if endpoint.endswith("/summary") and rng.random() < args.refresh_rate:
            params["refresh"] = "true"

        error_type = None
        if rng.random() < args.error_rate:
            error_type = rng.choice(args.error_type or ERROR_TYPES)
            match error_type:
                case "missing-from":
                    del params["from"]
                case "invalid-date":
                    params["from"] = "not-a-date"
                case "reversed-period":
                    params["from"], params["to"] = params["to"], params["from"]
                case "invalid-dataset":
                    dataset = "INVALID"
            totals["invalid_requests"] += 1

        number += 1
        totals["generated"] += 1
        yield {
            "endpoint": endpoint,
            "path": f"v1/datasets/{dataset}/metrics/{endpoint}",
            "params": params,
            "error_type": error_type,
            "expected_status": 422 if error_type else 200,
        }


@dataclass
class RequestStats:
    statuses: Counter[int] = field(default_factory=Counter)
    durations: list[float] = field(default_factory=list)
    transport_errors: int = 0
    unexpected_responses: int = 0
    last_transport_error: str = ""


class Http:
    def __init__(self, args, totals, stats):
        self.args = args
        self.totals = totals
        self.stats = stats
        self.client = None

    async def open(self):
        self.client = httpx.AsyncClient(
            base_url=self.args.url.rstrip("/") + "/",
            timeout=self.args.timeout,
            limits=httpx.Limits(
                max_connections=self.args.concurrency,
                max_keepalive_connections=self.args.concurrency,
            ),
            trust_env=False,
        )

    async def send_request(self, request):
        result = self.stats[request["endpoint"]]
        started = time.monotonic()
        try:
            async with asyncio.timeout(self.args.timeout):
                response = await self.client.get(request["path"], params=request["params"])
            result.statuses[response.status_code] += 1
            self.totals[f"http_{response.status_code}"] += 1
            if response.status_code >= 400:
                self.totals["http_errors"] += 1
            if response.status_code != request["expected_status"]:
                result.unexpected_responses += 1
                self.totals["unexpected_responses"] += 1
            elif request["error_type"]:
                self.totals["expected_errors"] += 1
        except (httpx.HTTPError, TimeoutError) as error:
            result.transport_errors += 1
            result.last_transport_error = f"{type(error).__name__}: {error}"
            self.totals["transport_errors"] += 1
        result.durations.append(time.monotonic() - started)
        self.totals["completed"] += 1

    async def send(self, batch):
        await asyncio.gather(*(self.send_request(request) for request in batch))
        return len(batch)

    async def close(self):
        if self.client is not None:
            await self.client.aclose()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate valid and invalid requests to the analytics API.",
        epilog="Example: just api-load --duration 600 --rate 10 --error-rate 0.1. Preview requests: just api-load --dry-run.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:18001",
        help="API base URL",
    )
    parser.add_argument(
        "--dataset",
        default="demo",
        help="Dataset for valid requests",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible output",
    )
    parser.add_argument(
        "--from",
        dest="start",
        default="2026-01-01T00:00:00Z",
        help="Period start, ISO 8601 with a timezone",
    )
    parser.add_argument(
        "--to",
        dest="end",
        default="2026-01-02T00:00:00Z",
        help="Period end, ISO 8601 with a timezone",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=180,
        help="Load duration in seconds",
    )
    parser.add_argument(
        "--requests",
        type=int,
        help="Maximum number of requests",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10,
        help="Total requests per second; 0 means unlimited",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Batch size and maximum concurrent requests",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10,
        help="Total request timeout in seconds",
    )
    parser.add_argument(
        "--refresh-rate",
        type=float,
        default=0.2,
        help="Fraction of summary requests with refresh=true",
    )
    parser.add_argument(
        "--error-rate",
        type=float,
        default=0.1,
        help="Fraction of invalid requests; expected HTTP status: 422",
    )
    parser.add_argument(
        "--error-type",
        action="append",
        choices=ERROR_TYPES,
        help="Error type; repeat to select several, defaults to all",
    )
    parser.add_argument(
        "--endpoint",
        action="append",
        choices=ENDPOINTS,
        help="Endpoint; repeat to select several, defaults to all five",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print JSONL without connections or delays; defaults to 10 requests unless --requests is set",
    )
    args = parser.parse_args(argv)
    for name in ("duration", "timeout"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            parser.error(f"--{name} must be a finite number > 0")
    if not math.isfinite(args.rate) or args.rate < 0:
        parser.error("--rate must be a finite number >= 0")
    if args.concurrency <= 0 or (args.requests is not None and args.requests <= 0):
        parser.error("--concurrency and --requests must be > 0")
    for name in ("refresh_rate", "error_rate"):
        if not 0 <= getattr(args, name) <= 1:
            parser.error(f"--{name.replace('_', '-')} must be between 0 and 1")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", args.dataset):
        parser.error("--dataset: 1–64 characters from a-z, 0-9, _ and -; start with a letter or digit")
    try:
        url = httpx.URL(args.url)
        if url.scheme not in ("http", "https") or not url.host or url.query or url.fragment:
            raise ValueError("--url: HTTP(S) URL without a query string or fragment")
        start, end = datetime.fromisoformat(args.start), datetime.fromisoformat(args.end)
        if start.utcoffset() is None or end.utcoffset() is None:
            raise ValueError("--from and --to must include a timezone")
        if not timedelta(0) < end - start <= timedelta(days=31):
            raise ValueError("The period must be positive and no longer than 31 days")
    except (ValueError, httpx.InvalidURL) as error:
        parser.error(str(error))
    if args.dry_run and args.requests is None:
        args.requests = 10
    return args


def format_percentile_ms(sorted_durations: list[float], fraction: float) -> str:
    if not sorted_durations:
        return "-"
    return f"{sorted_durations[math.ceil(len(sorted_durations) * fraction) - 1] * 1000:.1f}"


def print_report(totals, stats, elapsed: float):
    print(
        f"Done: generated {totals['generated']}, completed {totals['completed']} requests "
        f"in {elapsed:.2f} s; {totals['completed'] / elapsed:.2f} requests/s.", file=sys.stderr,
    )
    print(json.dumps(dict(totals), ensure_ascii=False, indent=2), file=sys.stderr)
    if not any(result.durations for result in stats.values()):
        return
    print(f"{'Endpoint':<23} {'Requests':>8} {'p50 ms':>9} {'p95 ms':>9} {'p99 ms':>9}  Results", file=sys.stderr)
    for endpoint, result in stats.items():
        ordered = sorted(result.durations)
        statuses = ", ".join(f"{code}={count}" for code, count in sorted(result.statuses.items()))
        percentiles = " ".join(f"{format_percentile_ms(ordered, p):>9}" for p in (0.5, 0.95, 0.99))
        print(
            f"{endpoint:<23} {len(ordered):>8} {percentiles}  {statuses or '-'}; "
            f"transport_errors={result.transport_errors}; unexpected_responses={result.unexpected_responses}",
            file=sys.stderr,
        )
        if result.last_transport_error:
            print(f"  Last transport error: {result.last_transport_error}", file=sys.stderr)


async def run(args):
    totals = Counter({
        "generated": 0, "invalid_requests": 0, "completed": 0,
        "http_errors": 0, "expected_errors": 0,
        "unexpected_responses": 0, "transport_errors": 0,
    })
    stats = {endpoint: RequestStats() for endpoint in args.endpoint or ENDPOINTS}
    source = generate_requests(args, totals)
    sink = None
    started = reported = time.monotonic()
    deadline = started + args.duration
    console = Console(stderr=True, highlight=False, markup=False)
    progress = Progress(
        SpinnerColumn(), TextColumn("{task.description}"),
        TextColumn("requests: {task.completed:,.0f}"),
        TextColumn("errors: {task.fields[errors]:,}"),
        TextColumn("{task.fields[speed]:.0f}/s"), TimeElapsedColumn(),
        console=console, disable=args.dry_run or not console.is_terminal,
        redirect_stdout=False, redirect_stderr=False,
    )
    print(f"Dataset {args.dataset}. seed={args.seed}. API={args.url}. error-rate={args.error_rate:g}", file=sys.stderr)
    try:
        if not args.dry_run:
            sink = Http(args, totals, stats)
            await sink.open()
        with progress:
            task = progress.add_task("Sending", total=None, errors=0, speed=0)
            while args.dry_run or time.monotonic() < deadline:
                batch_size = args.concurrency
                if args.rate and not args.dry_run:
                    remaining = deadline - time.monotonic()
                    batch_size = min(batch_size, max(1, math.ceil(remaining * args.rate)))
                batch = list(islice(source, batch_size))
                if not batch:
                    break
                batch_started = time.monotonic()
                if sink is None:
                    for request in batch:
                        print(json.dumps(request, ensure_ascii=False, separators=(",", ":")))
                else:
                    await sink.send(batch)
                if args.rate and not args.dry_run:
                    pause = len(batch) / args.rate - (time.monotonic() - batch_started)
                    await asyncio.sleep(max(0, min(pause, deadline - time.monotonic())))
                now = time.monotonic()
                errors = totals["http_errors"]
                progress.update(
                    task, completed=totals["completed"],
                    errors=errors + totals["transport_errors"],
                    speed=totals["completed"] / (now - started),
                )
                if not console.is_terminal and not args.dry_run and now - reported >= 2:
                    print(
                        f"Completed requests: {totals['completed']}; HTTP errors: {errors}; "
                        f"unexpected responses: {totals['unexpected_responses']}; "
                        f"transport errors: {totals['transport_errors']}; rate: {totals['completed'] / (now - started):.0f}/s",
                        file=sys.stderr, flush=True,
                    )
                    reported = now
    finally:
        if sink is not None:
            await sink.close()
        print_report(totals, stats, time.monotonic() - started)


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
        sys.exit(130)
