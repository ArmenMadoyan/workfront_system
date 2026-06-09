"""Throughput driver: each virtual user owns a project and hammers
PATCH /schedule. Reports req/s, latency percentiles, and status distribution.

  python tests/perf/load.py --duration 20 --concurrency 200

One VU per project (no root-version contention), so 200 conc => 200 projects.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = "http://127.0.0.1:8000"
T0 = datetime(2027, 1, 1, 9, 0, tzinfo=timezone.utc)


async def _sync_version(client, pid, tid):
    try:
        g = await client.get(f"/api/v1/projects/{pid}/gantt", headers={"X-Customer-Id": CUSTOMER})
        return next(x["version"] for x in g.json()["tasks"] if x["id"] == tid)
    except Exception:
        return None


async def vu(client, root, stop_at, lat, codes):
    pid, tid = root["project_id"], root["root_task_id"]
    ver = await _sync_version(client, pid, tid) or root["version"]
    n = 0
    while time.monotonic() < stop_at:
        start = T0 + timedelta(days=n)
        t = time.monotonic()
        try:
            r = await client.patch(
                f"/api/v1/tasks/{tid}/schedule",
                headers={"X-Customer-Id": CUSTOMER, "If-Match": str(ver)},
                json={"planned_start": start.isoformat(),
                      "planned_completion": (start + timedelta(hours=8)).isoformat()},
            )
        except Exception:
            codes["exc"] += 1
            continue
        lat.append((time.monotonic() - t) * 1000)
        codes[r.status_code] += 1
        if r.status_code == 200:
            ver += 1
            n += 1
        elif r.status_code == 409:
            synced = await _sync_version(client, pid, tid)
            if synced is not None:
                ver = synced


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))]


async def main():
    global CUSTOMER
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=20)
    ap.add_argument("--concurrency", type=int, default=200)
    ap.add_argument("--offset", type=int, default=0)  # project slice (for multi-process runs)
    ap.add_argument("--manifest", default="tests/perf/manifest.json")
    args = ap.parse_args()

    m = json.load(open(args.manifest))
    CUSTOMER = m["customer_id"]
    roots = m["throughput"][args.offset : args.offset + args.concurrency]
    lat: list[float] = []
    codes: Counter = Counter()

    limits = httpx.Limits(max_connections=args.concurrency + 50, max_keepalive_connections=args.concurrency + 50)
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30, limits=limits) as client:
        stop_at = time.monotonic() + args.duration
        print(f"driving {len(roots)} VUs for {args.duration}s ...")
        t = time.monotonic()
        await asyncio.gather(*[vu(client, r, stop_at, lat, codes) for r in roots])
        elapsed = time.monotonic() - t

    ok = codes[200]
    print(f"\n=== throughput ({elapsed:.1f}s, {len(roots)} VUs) ===")
    print(f"  schedule moves OK : {ok}")
    print(f"  throughput        : {ok / elapsed:,.0f} moves/s")
    print(f"  latency p50/p95/p99: {pct(lat,50):.1f} / {pct(lat,95):.1f} / {pct(lat,99):.1f} ms")
    print(f"  status codes      : {dict(codes)}")


if __name__ == "__main__":
    asyncio.run(main())