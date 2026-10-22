# Chapter 8 — Performance & Scale

> The part most candidates hand-wave. We actually load-tested it and found the
> bottlenecks in order. Talking from measured numbers is a massive credibility win.

## In our system

Harness in `tests/perf/`: `seed.py` (bulk via Postgres `COPY`), `load.py`
(multi-process throughput driver), `cascade_bench.py` (mega-cascade timing),
`monitor.sh` (consumer lag + replication-slot lag). Run on an **M4 Pro (12
cores)** with *everything on one box* (8 API workers + 8 cascade workers +
Postgres + Kafka + Debezium + Redis + the load drivers all competing).

### Methodology
1. **Seed** 80,000 tasks via `COPY` in ~3s (3,000 projects × 10 + a 50k chain).
2. **Drive** `PATCH /schedule` at target concurrency; measure p50/p95/p99,
   throughput, status mix.
3. **Watch** the async side: **Kafka consumer lag** (the worker health metric)
   and **replication-slot lag** (Debezium health).
4. **Stress** the mega-cascade separately.

### Results
| Workload | Result |
|---|---|
| Sustained throughput | **~582 schedule moves/s**, worker-fleet lag ~0 (≈5,800 cascaded task-writes/s) |
| API latency (write) | **45ms** to confirm a move (writes outbox, returns) |
| Mega-cascade | **50,000 tasks in 5.81s = 8,602 tasks/s**, API still 45ms |
| Single API process | ~190 req/s (one core) |

~582/s ≈ **50M moves/day-equivalent — ~5× the 10M/day target**, on one laptop.

### Bottlenecks, in the order they appeared (this is the story to tell)
1. **The load generator itself** — a single async Python process caps ~190 req/s
   on one core. *Always confirm the load tool isn't what you're measuring.*
   Parallelizing to 6 driver processes unlocked ~582/s.
2. **API = one core per uvicorn worker.** ORM + async has real per-request CPU
   cost; scale horizontally with multiple workers/replicas.
3. **DB connection-pool starvation** — the async engine defaulted to ~5
   connections; 300 concurrent requests queued behind them (p99 spiked to 6s).
   Raised pool to 40+20 and `max_connections` to 300.
4. **The worker fleet was *never* the bottleneck** — 8 workers across 12
   partitions held lag at ~0 even at full tilt. The cascade engine is fast.
5. **Mega-cascade is intentionally serial per project** (one partition, one
   worker). 8,602 tasks/s is one giant transaction — and the API stayed at 45ms,
   so the user is never blocked.

### Where it breaks (know your ceilings)
- **Single Postgres master** — first to saturate on writes; scale with read
  replicas, partitioning, then sharding by `customer_id`.
- **Debezium** — a single connector reading the WAL; can cap throughput before
  Postgres does. Mitigate with multiple connectors / partitioned capture.
- **Partition count** — caps concurrent-cascade parallelism (12 partitions → 12
  busy projects in parallel).
- **The mega-cascade message size** — the 8MB event that killed Debezium (Ch. 4);
  chunk it.
- **Subgraph load** — the worker loads the whole project per event (O(project));
  this is exactly why `graph_service`/Neo4j exists in the design.

## Capacity math to 10M (say this)
> 10M/day = ~116/s avg. We sustained ~582/s on *one* oversubscribed laptop with
> the worker tier idle (lag ~0). So 10M/day needs modest horizontal API replicas
> and a handful of workers — the architecture is comfortably over-provisioned for
> the target. The scaling *risk* isn't steady throughput; it's a pathological
> mega-cascade, which we bound by chunking events and serializing per project.

## Alternatives & trade-offs
- **Vertical vs horizontal**: the API scales horizontally (stateless); Postgres
  scales vertically first (bigger box / Aurora), then read replicas, then shards.
- **Load tooling**: we used a custom multi-process Python driver; **k6** (Go,
  multi-core) or **Locust** are better for sustained high RPS — but our driver
  proved the point and tracks per-VU version state for the `If-Match` flow.

## Interview soundbites

- "I load-tested it: **~582 moves/s sustained on one laptop, ~5× the target**,
  with the worker tier at **zero lag** — the cascade engine isn't the bottleneck."
- "Bottlenecks in order: **load generator → single-core API process → connection
  pool → DB**. The workers never struggled."
- "A 50k-task cascade takes 5.8s but the **API still answers in 45ms** — that's
  the async design paying off."

## Likely follow-ups

- *"How would you 10× it?"* → More API replicas + DB read replicas/partitioning;
  more Kafka partitions + workers; chunk mega-cascade events; move subgraph reads
  to Neo4j.
- *"What's your worst-case latency?"* → A single mega-cascade for one project
  (seconds); bounded and isolated — other projects are unaffected.
- *"How do you know workers keep up?"* → Consumer lag stayed ~0 under load; it's
  the alert I'd page on.
