"""Mega-cascade benchmark: move the root of the deep FS chain and time how long
the full async pipeline takes to propagate to the LAST task. Reports tasks/sec.

  python tests/perf/cascade_bench.py
"""

from __future__ import annotations

import json
import os
import time

import httpx
import psycopg

BASE_URL = "http://127.0.0.1:8000"
CONNINFO = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://")


def main() -> None:
    m = json.load(open("tests/perf/manifest.json"))
    cust, deep = m["customer_id"], m["deep"]
    tid, last, size = deep["root_task_id"], deep["last_task_id"], deep["size"]

    conn = psycopg.connect(CONNINFO, autocommit=True)
    cur = conn.cursor()
    cur.execute("select planned_start, version from task where id=%s", (tid,))
    root_start, root_ver = cur.fetchone()
    cur.execute("select planned_start from task where id=%s", (last,))
    last_before = cur.fetchone()[0]

    new_start = "2099-01-01T09:00:00Z"
    print(f"moving root of {size}-task chain; waiting for last task to shift ...")
    t0 = time.monotonic()
    r = httpx.patch(
        f"{BASE_URL}/api/v1/tasks/{tid}/schedule",
        headers={"X-Customer-Id": cust, "If-Match": str(root_ver)},
        json={"planned_start": new_start, "planned_completion": "2099-01-01T17:00:00Z"},
        timeout=30,
    )
    print("PATCH ->", r.status_code, f"(api returned in {(time.monotonic()-t0)*1000:.0f}ms)")

    deadline = t0 + 300
    while time.monotonic() < deadline:
        cur.execute("select planned_start from task where id=%s", (last,))
        if cur.fetchone()[0] != last_before:
            break
        time.sleep(0.05)
    dt = time.monotonic() - t0

    cur.execute("select count(*) from task where project_id=%s and planned_start >= %s",
                (deep["project_id"], "2099-01-01"))
    moved = cur.fetchone()[0]
    conn.close()

    print(f"\n=== mega-cascade ({size} tasks) ===")
    print(f"  end-to-end (PATCH -> last task moved): {dt:.2f}s")
    print(f"  tasks moved: {moved}")
    print(f"  cascade throughput: {moved/dt:,.0f} tasks/s" if dt else "")


if __name__ == "__main__":
    main()