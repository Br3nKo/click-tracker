#!/usr/bin/env python3
"""Load generator for exercising the click-tracker overload path.

--unique-ratio drives cache behaviour: 1.0 = all misses (rate-limited to
~200/s, backlog grows); 0.0 = mostly hits (workers drain, little backlog).
XACK does not shrink XLEN, so watch consumer-group lag, not stream length.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

import httpx


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Click-tracker load generator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--url", default="http://localhost:8000", help="API base URL")
    p.add_argument("--rate", type=float, default=500.0, help="target requests/sec")
    p.add_argument("--duration", type=float, default=15.0, help="seconds to run")
    p.add_argument(
        "--concurrency", type=int, default=200, help="max in-flight requests"
    )
    p.add_argument(
        "--unique-ratio",
        type=float,
        default=1.0,
        help="fraction of requests using a brand-new user (0..1)",
    )
    p.add_argument(
        "--pool-size",
        type=int,
        default=100,
        help="number of distinct reused users (drives cache hits)",
    )
    p.add_argument(
        "--redis-url",
        default=None,
        help="if set, print consumer-group lag once per second",
    )
    p.add_argument("--stream-key", default="clicks:incoming")
    p.add_argument("--group", default="enrichers")
    return p.parse_args()


def _make_payload(user_id: str) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "shop_url": "https://shop.example.com/item/42",
    }


async def _send(client, url, sem, payload, results) -> None:
    async with sem:
        start = time.monotonic()
        try:
            resp = await client.post(f"{url}/clicks", json=payload)
            code = resp.status_code
        except Exception:
            code = 0  # connection error / timeout
        results.append((code, time.monotonic() - start))


async def _poll_backlog(redis_url, stream_key, group, stop) -> None:
    """Print consumer-group lag/pending once per second until stopped."""
    import redis.asyncio as redis

    client = redis.from_url(redis_url, decode_responses=True)
    try:
        while not stop.is_set():
            try:
                groups = await client.xinfo_groups(stream_key)
                info = next((g for g in groups if g.get("name") == group), {})
                print(
                    f"  [backlog] lag={info.get('lag')} "
                    f"pending={info.get('pending')}"
                )
            except Exception as exc:
                print(f"  [backlog] unavailable: {exc}")
            try:
                await asyncio.wait_for(stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    finally:
        await client.aclose()


def _percentile(values, pct) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = min(len(ordered) - 1, int(round((pct / 100) * (len(ordered) - 1))))
    return ordered[k]


async def main() -> None:
    args = _parse_args()
    pool = [str(uuid4()) for _ in range(max(1, args.pool_size))]
    sem = asyncio.Semaphore(args.concurrency)
    results: list[tuple[int, float]] = []
    stop = asyncio.Event()

    backlog_task = None
    if args.redis_url:
        backlog_task = asyncio.create_task(
            _poll_backlog(args.redis_url, args.stream_key, args.group, stop)
        )

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    tasks: list[asyncio.Task] = []
    start = time.monotonic()
    next_at = start
    sent = 0
    progress_every = max(1, int(args.rate))

    async with httpx.AsyncClient(timeout=10.0) as client:
        while time.monotonic() - start < args.duration:
            now = time.monotonic()
            if now < next_at:
                await asyncio.sleep(next_at - now)
            next_at += interval

            if random.random() < args.unique_ratio:
                user_id = str(uuid4())
            else:
                user_id = random.choice(pool)

            tasks.append(
                asyncio.create_task(
                    _send(client, args.url, sem, _make_payload(user_id), results)
                )
            )
            sent += 1
            if sent % progress_every == 0:
                elapsed = time.monotonic() - start
                print(
                    f"t={elapsed:5.1f}s sent={sent} "
                    f"achieved={sent / elapsed:7.1f} req/s"
                )

        await asyncio.gather(*tasks)

    stop.set()
    if backlog_task:
        await backlog_task

    elapsed = time.monotonic() - start
    latencies = [lat for _, lat in results]
    codes: dict = {}
    for code, _ in results:
        codes[code] = codes.get(code, 0) + 1

    print("\n=== load test summary ===")
    print(f"target rate   : {args.rate:.0f} req/s")
    print(f"duration      : {elapsed:.1f}s")
    print(f"requests sent : {len(results)}")
    print(f"achieved rate : {len(results) / elapsed:.1f} req/s")
    print(f"unique-ratio  : {args.unique_ratio}")
    accepted = codes.get(202, 0)
    print(f"accepted (202): {accepted}")
    other = {k: v for k, v in codes.items() if k != 202}
    if other:
        label = ", ".join(f"{k or 'ERR'}:{v}" for k, v in sorted(other.items()))
        print(f"non-202       : {label}")
    if latencies:
        print(f"latency p50   : {_percentile(latencies, 50) * 1000:.0f} ms")
        print(f"latency p90   : {_percentile(latencies, 90) * 1000:.0f} ms")
        print(f"latency p99   : {_percentile(latencies, 99) * 1000:.0f} ms")
    print(
        "\nBacklog drains via the workers, not the API. XACK does not shrink "
        "XLEN — watch lag:\n"
        f"  redis-cli XINFO GROUPS {args.stream_key}"
    )


if __name__ == "__main__":
    asyncio.run(main())
