"""Bulk-seed perf data via Postgres COPY (fast). Writes a manifest the load
scripts read.

  python tests/perf/seed.py --projects 5000 --tasks 10 --deep 50000

Creates:
  * `projects` chained projects of `tasks` tasks each (throughput workload)
  * one `deep`-task FS chain (mega-cascade workload)
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import psycopg

from wf_core.ids import uuid7

CONNINFO = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://")
BASE = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)


def _task_row(cid, pid, i):
    start = BASE + timedelta(days=i)
    return (str(uuid7()), str(cid), str(pid), f"T{i}", "new", start,
            start + timedelta(hours=8), 480, 0, 1, "{}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--projects", type=int, default=5000)
    ap.add_argument("--tasks", type=int, default=10)
    ap.add_argument("--deep", type=int, default=50000)
    ap.add_argument("--manifest", default="tests/perf/manifest.json")
    args = ap.parse_args()

    conn = psycopg.connect(CONNINFO, autocommit=False)
    cur = conn.cursor()
    cid = uuid7()
    cur.execute("INSERT INTO customer (id, name) VALUES (%s, %s)", (cid, "perf"))

    manifest = {"customer_id": str(cid), "throughput": [], "deep": {}}
    tcols = ("id", "customer_id", "project_id", "name", "status", "planned_start",
             "planned_completion", "work_required_minutes", "percent_complete", "version", "custom_attributes")
    tcopy = f"COPY task ({','.join(tcols)}) FROM STDIN"
    pcopy = "COPY predecessor (id, customer_id, predecessor_id, successor_id, type, lag_minutes) FROM STDIN"

    def seed_chain(pid, n):
        ids = []
        with cur.copy(tcopy) as cp:
            for i in range(n):
                row = _task_row(cid, pid, i)
                ids.append(row[0])
                cp.write_row(row)
        with cur.copy(pcopy) as cp:
            for i in range(1, n):
                cp.write_row((str(uuid7()), str(cid), ids[i - 1], ids[i], "FS", 0))
        return ids

    # throughput projects
    print(f"seeding {args.projects} projects x {args.tasks} tasks ...")
    proj_rows = [(str(uuid7()), str(cid), f"P{i}") for i in range(args.projects)]
    with cur.copy("COPY project (id, customer_id, name) FROM STDIN") as cp:
        for r in proj_rows:
            cp.write_row(r)
    for r in proj_rows:
        ids = seed_chain(r[0], args.tasks)
        manifest["throughput"].append({"project_id": r[0], "root_task_id": ids[0], "version": 1})

    # deep cascade project
    print(f"seeding deep chain of {args.deep} tasks ...")
    dpid = str(uuid7())
    cur.execute("INSERT INTO project (id, customer_id, name) VALUES (%s, %s, %s)", (dpid, cid, "DEEP"))
    dids = seed_chain(dpid, args.deep)
    manifest["deep"] = {"project_id": dpid, "root_task_id": dids[0], "last_task_id": dids[-1], "size": args.deep}

    conn.commit()
    cur.execute("select count(*) from task")
    total = cur.fetchone()[0]
    conn.close()

    with open(args.manifest, "w") as f:
        json.dump(manifest, f)
    print(f"done. total tasks in DB: {total}. manifest -> {args.manifest}")


if __name__ == "__main__":
    main()