# Designing a 10M-Event Work-Management System
### An Adobe System-Design Interview Companion, grounded in a system we actually built and load-tested

This book is not a generic "system design primer." Every chapter is backed by a
**real, running codebase** (`workfront_system`) — a Workfront-style work-management
backend with an event-driven scheduling engine — and by **numbers we actually
measured** on an M4 Pro. When a chapter says "we chose X," there is a file you can
open and a benchmark you can re-run.

Use it two ways:
1. **Read Part I** to understand the system end-to-end (the narrative).
2. **Drill Part III** the night before — mock interview + rapid-fire Q&A.

---

## The system in one diagram

```
Browser
  │  REST (commands/queries)        WebSocket (real-time Gantt)
  ▼                                   ▲
┌─────────────────────────── API (async FastAPI) ──────────────────────────┐
│  validate + authz → write Postgres (truth) + outbox row  (ONE tx)         │
│  optimistic locking (version_id_col) · If-Match / 409                     │
└───────────────┬───────────────────────────────────────────▲──────────────┘
                │                                             │ Redis pub/sub
        Postgres (source of truth) + outbox                   │ (WS fan-out)
                │  WAL                                         │
         Debezium CDC (idempotent producer)             ws-fanout
                │                                             ▲
   topic: task.schedule.changed  (key = project_id)          │
                │  ordered per project                        │
      worker fleet (consumer group, N partitions)            │
        gRPC→graph_service · cascade.py · version-guarded     │
        write-back + dedup inbox (ProcessedEvent)             │
                │                                             │
   topic: project.changed ───────────────────────────────────┘
                └──► projector ──► OpenSearch (CQRS read model)   [deferred stub]
```

## What's real vs. deferred

| Built & tested | Deferred (stub) | Discussed comparatively |
|---|---|---|
| Postgres schema, async API, outbox→Debezium→Kafka→worker, Redis fan-out, optimistic locking, OpenTelemetry/Jaeger, perf harness | `projector`→OpenSearch, `graph_service`→Neo4j gRPC | Mongo, RabbitMQ, GraphQL, k8s, AWS, Datadog, saga |

## The numbers we measured (cite these in the room)

| Metric | Value |
|---|---|
| Target load | 10M changes/day ≈ **116/s avg**, ~1–2k/s peak |
| Sustained throughput (M4 Pro, all on one box) | **~582 schedule moves/s**, worker lag ~0 |
| Mega-cascade | **50,000 tasks rescheduled in 5.81s = 8,602 tasks/s**; API returned in **45ms** |
| Single API process ceiling | ~190 req/s (one core) |

## Table of contents

**Part I — The system we built**
1. [Problem & requirements](part1/01-problem-and-requirements.md)
2. [Data model & schema](part1/02-data-model-and-schema.md)
3. [The API layer (REST + WebSocket, async)](part1/03-api-layer.md)
4. [The eventing core (outbox → CDC → Kafka → workers)](part1/04-eventing-core.md)
5. [Concurrency & data integrity](part1/05-concurrency-and-integrity.md)
6. [Real-time fan-out](part1/06-realtime-fanout.md)
7. [Observability (the God View)](part1/07-observability.md)
8. [Performance & scale](part1/08-performance-and-scale.md)

**Part II — Interview topics, comparatively**
9. [Microservices & communication](part2/09-microservices-and-communication.md)
10. [Messaging patterns](part2/10-messaging-patterns.md)
11. [Design patterns](part2/11-design-patterns.md)
12. [Caching](part2/12-caching.md)
13. [Datastores](part2/13-datastores.md)
14. [Webhooks at scale](part2/14-webhooks-at-scale.md)
15. [Infrastructure & cloud](part2/15-infra-and-cloud.md)
16. [CI/CD & operations](part2/16-cicd-and-ops.md)

**Part III — Mock interview**
17. [The main question: design a 10M-event system that fires webhooks](part3/17-mock-the-main-question.md)
18. [Rapid-fire Q&A](part3/18-rapid-fire-qa.md)

**Appendix**
19. [Glossary & decision log](appendix/19-glossary-and-decision-log.md)

## How each chapter is structured

1. **In our system** — what we built (file references + numbers).
2. **The general pattern** — the concept generalized.
3. **Alternatives & trade-offs** — what we rejected and when it wins.
4. **Interview soundbites** — lines to say out loud.
5. **Likely follow-ups** — the next questions, answered.


---

# Chapter 1 — Problem & Requirements

> The first five minutes of any system-design interview are about *scoping*, not
> architecture. Whoever defines the requirements controls the design.

## In our system

We built a Workfront-style **work-management** backend. The domain hierarchy:

```
Portfolio → Program → Project → Task (tree) / Issue
                          └── Predecessor (dependency edges: FS/SS/FF/SF + lag)
```

The hard, defining feature is the **dependency cascade**: when a task's dates
move, every downstream task (its transitive successors) must be rescheduled,
respecting dependency type and lag. One drag on a Gantt chart can ripple to
thousands of tasks. That single requirement is what forces almost every
architectural decision in this book.

## Dissecting "10M"

Adobe's prompt is usually phrased "handle 10M *something* per day." **Always
convert the headline number to a rate, then find the real stressor.**

- **10M requests/day** = 10,000,000 / 86,400 ≈ **116 requests/sec average**.
- Real traffic isn't flat; assume a **5–10× peak** → **~600–1,200/sec peak**.
- That is *not* a scary number. A single well-indexed Postgres + a few app
  instances handle it. Saying this out loud signals seniority: *"10M/day is
  ~116/s average — the interesting problem isn't raw QPS."*

The actual stressors hide behind the headline:

| Dimension | Why it hurts |
|---|---|
| **Fan-out** | One edit → 10k–50k dependent task updates. A *single* request becomes a huge unit of work. |
| **Data volume** | 10M tasks means cascade queries and indexes must stay fast at scale. |
| **Real-time** | The Gantt must update live for every connected user, so writes must propagate to browsers. |
| **Ordering & correctness** | Concurrent edits + cascades must not corrupt schedules. |

We **measured** these on one laptop: steady throughput peaked at ~582 schedule
moves/s (well past 116/s), but a **50,000-task cascade took 5.81s** — confirming
that fan-out, not QPS, is the design driver.

## Functional vs. non-functional requirements

**Functional** (what it does)
- CRUD on portfolios/programs/projects/tasks/issues.
- Add/remove dependency edges between tasks.
- Reschedule a task → cascade to dependents.
- Real-time Gantt updates for connected clients.
- (Later) fire webhooks to external partners on changes.

**Non-functional** (how well)
- **Scale**: 10M changes/day; projects up to tens of thousands of tasks.
- **Latency**: an interactive edit must feel instant (the *origin* task confirms
  in tens of ms; the ripple can be eventually consistent over ~1s).
- **Correctness**: no lost updates under concurrency; no double-applied cascades.
- **Availability**: a slow/large cascade for one project must not block others.
- **Multi-tenant**: it's SaaS — strict tenant isolation.
- **Observability**: trace one request across all hops.

## The capacity-estimation muscle (do this out loud)

> 10M writes/day ≈ 116/s avg, ~1k/s peak. Each write is a small row + an outbox
> row ≈ a few KB of WAL. ~1k/s × a few KB = single-digit MB/s of WAL — trivial
> for one Postgres. Storage: 10M tasks × ~1KB ≈ 10GB + indexes — also small.
> So the bottleneck is **not** storage or steady QPS; it's **cascade fan-out
> latency** and **keeping the read/real-time side consistent**. That reframing
> is the whole design.

## Alternatives & trade-offs (scoping choices)

- **Synchronous cascade** (compute the whole ripple inside the request) vs.
  **asynchronous** (confirm the origin, ripple later). We chose async — a 50k
  cascade can't block an HTTP request for 6s. Sync is simpler and fine *only* if
  fan-out is provably tiny.
- **Strong vs. eventual consistency** for the ripple. We chose: **origin task =
  strongly consistent** (read-your-write), **dependents = eventually consistent**
  (arrive via WebSocket in ~1s). This is the single most important
  consistency decision and it falls straight out of the latency requirement.

## Interview soundbites

- "First, 10M/day is ~116/s average — raw QPS isn't the hard part. The hard part
  is **fan-out**: one edit can reschedule 50,000 tasks."
- "I'll make the **edited task strongly consistent and its dependents eventually
  consistent** — that's what lets the API answer in 45ms while a huge cascade
  runs in the background."
- "This is multi-tenant SaaS, so tenancy is a *data-model* concern from row one,
  not an afterthought."

## Likely follow-ups

- *"What if a cascade touches 1M tasks?"* → It's still one ordered unit per
  project (Ch. 4–5); you bound it, chunk the emitted event (Ch. 4's real bug),
  and accept longer eventual-consistency for that project while others are
  unaffected.
- *"Where would you push back on the requirement?"* → On synchronous cascade and
  on exactly-once (Ch. 4) — both are usually misframed.


---

# Chapter 2 — Data Model & Schema

## In our system

The schema lives in `libs/wf_core/wf_core/models.py` (SQLAlchemy 2.0) with Alembic
migrations `0001_initial` and `0002_dedup_inbox`. 15 tables, validated by actually
running `alembic upgrade head` against Postgres 16.

Core spine: `customer` (tenant) → `portfolio` → `program` → `project` → `task`
(self-referencing `parent_id` tree) and `issue`; plus `predecessor` (the
dependency DAG), `assignment`, `hour`/`timesheet`, and two infra tables
(`outbox`, `processed_event`).

Five deliberate decisions, each an interview talking point:

### 1. Multi-tenant from row one
Every table carries `customer_id`, and it is the **leading column of every
composite index** (`(customer_id, project_id)`, `(customer_id, status,
planned_completion)`, …). At scale `customer_id` is also the natural **partition
key**. Tenancy baked into the indexes means tenant-scoped queries stay selective
and one noisy tenant can be isolated later by partition.

### 2. UUIDv7 primary keys (`wf_core/ids.py`)
We hand-rolled `uuid7()` (RFC 9562): 48-bit millisecond timestamp + randomness.
Why not UUIDv4? Random UUIDs scatter inserts across a btree → page splits and
index bloat on the hottest tables. **UUIDv7 is time-ordered**, so inserts are
append-mostly *and* IDs are still globally unique (great for an event-driven,
multi-service world). We verified the version nibble is `7` and that freshly
generated IDs share a time-ordered prefix.

### 3. Custom fields = JSONB, not EAV
Workfront historically used **Entity-Attribute-Value** (a giant `PARAMETERVALUE`
table) for user-defined fields — which means agonizing self-joins at read time.
We use a `custom_attributes JSONB` column per object instead. The interview point:
*promote hot/filterable custom fields to Postgres **generated columns** with
btree indexes*, keeping the JSONB blob for the long tail. You get NoSQL
flexibility with relational query power, no join explosion.

### 4. Denormalized rollups
`project.percent_complete` is an aggregate of its tasks, **stored** on the
project row (recomputed by background work) so list/dashboard reads don't
aggregate on every request. Classic read-optimization: trade write-time work +
a little staleness for fast reads.

### 5. Concurrency & infra columns
- `version BIGINT` on `task`/`project` → optimistic locking (Ch. 5).
- `outbox` → transactional outbox for events (Ch. 4).
- `processed_event (consumer_group, event_id)` → dedup inbox (Ch. 4–5).

## The general pattern

A relational schema for a multi-tenant, hierarchical, event-driven product:
- **Adjacency list** for trees (`task.parent_id`) and the dependency DAG
  (`predecessor`), with a unique constraint on edges to block duplicates.
- **Tenant-leading composite indexes** everywhere.
- **Hybrid storage**: typed columns for the known/hot fields, JSONB for the
  flexible tail, generated columns to index inside JSONB when needed.
- **Audit/version columns** as first-class (`created_at`, `updated_at`, `version`).
- **Partitioning** the highest-volume table (`hour` — logged time) by date range,
  sub-keyed by tenant.

## Alternatives & trade-offs

| Choice | We picked | Alternative | When the alternative wins |
|---|---|---|---|
| Custom fields | JSONB + generated columns | EAV table | Almost never today; EAV only if you need per-attribute row-level ACLs |
| Custom fields | JSONB in Postgres | Separate document store (Mongo) | If the *whole* object is schemaless and rarely joined |
| PK | UUIDv7 | BIGINT identity | Single-DB, no cross-service IDs, want smallest index → bigint |
| PK | UUIDv7 | UUIDv4 | Never for hot tables (index bloat); fine for low-write tables |
| Tree | Adjacency list | Closure table / `ltree` | If you need cheap "all descendants" queries far more than writes |
| Tenancy | Shared tables + `customer_id` | DB-per-tenant | Few huge enterprise tenants needing hard isolation/compliance |

## Interview soundbites

- "Custom fields are **JSONB with hot fields promoted to generated columns** —
  the 2010s EAV approach forces self-joins that don't scale."
- "**UUIDv7**, not v4 — time-ordered keys keep btree inserts append-mostly and
  avoid index bloat on hot tables, while staying globally unique for events."
- "`customer_id` **leads every index** and is the partition key — tenancy is a
  schema decision, not middleware."

## Likely follow-ups

- *"How do you query a custom field at scale?"* → Generated stored column +
  btree (equality/range), or a GIN index on the JSONB for containment.
- *"How do you keep `percent_complete` correct?"* → Recompute on the cascade /
  via the same event pipeline; accept brief staleness; it's a rollup, not truth.
- *"10M tasks — does this still work?"* → Yes; partition `hour` by month, keep
  indexes tenant-leading, and push heavy reporting to the read model (Ch. 12–13).


---

# Chapter 3 — The API Layer (REST + WebSocket, async)

## In our system

Two halves that share one vocabulary (`services/api/src/{main,schemas,ws}.py`,
spec in `docs/api-contract.md`):

- **REST** — the browser *asks* (create project, load Gantt, move a task).
- **WebSocket** — the backend *tells* (the downstream tasks a cascade moved).

The browser **never polls** for cascade results: it does the move over REST, gets
the authoritative answer for *that* task, and the ripple arrives over WS.

### The cascade response contract (the crux)
```
PATCH /api/v1/tasks/{id}/schedule    If-Match: <version>
  → 200 with the moved task's NEW dates + new version     (instant: "A is done")
  → 409 if the client's version is stale
... downstream tasks arrive later as ONE batched schedule.cascade WS event
```
So the **edited task is synchronous truth; its dependents are async deltas.** This
is the consistency split from Chapter 1, expressed as an API contract.

### `seq`: the cursor that ties REST and WS together
Every WS envelope carries a per-project monotonic `seq`. `GET /gantt` returns the
current `seq`; the client ignores lower-seq events and **refetches if it sees a
gap**. Our test proved it: a client that connected late got `subscribed seq 3`
(it had missed 3 events) — exactly the signal to resync. This is how a
real-time UI stays correct over an imperfect connection.

### Why the API is async
FastAPI gives two execution models:
- **`def` (sync)** handlers run in a **threadpool** — one thread parked per
  in-flight request while it waits on the DB.
- **`async def`** handlers run on the **event loop** — thousands of in-flight
  awaits per process, no thread each.

We chose **async** because the API is I/O-bound (it mostly waits on Postgres) and
— the clincher — **it also holds the WebSocket connections**, which is precisely
an event-loop job. psycopg 3 does async, so no new driver; `wf_core/db.py` exposes
both a sync engine (worker/migrations) and an async engine (API). The hard rule:
inside `async def` you must never make a blocking call, or you stall the whole loop.

## The general pattern

- **Command/query over REST, server push over WebSocket** for collaborative UIs.
- **Optimistic-concurrency on writes** via `If-Match`/`ETag`-style version headers.
- **Cursor + gap-detection** to reconcile a snapshot (REST) with a live stream (WS).
- **Async runtime end-to-end** when the service holds many long-lived connections.

## Alternatives & trade-offs

| Choice | We picked | Alternative | When the alternative wins |
|---|---|---|---|
| Protocol | REST + WebSocket | GraphQL + subscriptions | Many heterogeneous clients needing flexible field selection; willing to own subscription scaling |
| Protocol | REST + WebSocket | REST + SSE | One-way push only, want auto-reconnect & simplicity (no client→server channel) |
| Runtime | async (event loop) | sync (threadpool) | CPU-bound endpoints, simple code, no long-lived connections |
| Cascade response | 200 now + WS ripple | 202, everything via WS | Purer CQRS, but the UI has nothing to confirm synchronously |
| Cascade response | 200 now + WS ripple | 200 with full cascade inline | Tiny, bounded fan-out only |

**GraphQL note for the room:** GraphQL shines for read flexibility and many
client shapes, but subscriptions are operationally heavier and you still need the
same eventing backend underneath. We used REST because our write surface is small
and the real complexity is the async pipeline, not query flexibility. FastAPI's
`/docs` (Swagger) is our interactive playground — the GraphiQL equivalent.

## Interview soundbites

- "The edited task returns **200 in ~45ms**; its dependents stream back over a
  **single batched WebSocket event** — that's the consistency split as a contract."
- "I made the API **async** primarily because it owns the WebSocket fleet; with
  psycopg 3 the same process does async REST and async DB with no extra driver."
- "A monotonic **`seq` per project** lets a reconnecting client detect a gap and
  refetch — real-time correctness without a perfect socket."

## Likely follow-ups

- *"sync `def` in FastAPI isn't single-threaded though?"* → Correct — it's a
  threadpool; the real axis is thread-per-request vs. event-loop concurrency, and
  WebSockets force the event loop.
- *"How does the client avoid showing stale data after reconnect?"* → `seq` gap
  detection → refetch `/gantt`.
- *"Why not block and return the whole cascade?"* → A 50k cascade is 5.8s; you
  can't hold an HTTP request that long, and it would serialize unrelated load.


---

# Chapter 4 — The Eventing Core (the centerpiece)

> This is the chapter the interview is really about: *"design an eventing system
> to handle 10M changes."* Everything else supports this.

## In our system

The flow (spec in `docs/event-flow.md`):

```
API write ─ONE tx─► Postgres (truth) + outbox row
        │
   Debezium CDC (reads WAL, idempotent producer)
        ▼
  task.schedule.changed   (Kafka, key = project_id)
        │ ordered per project
   worker fleet  (services/worker/) — consumer group, N partitions
     gRPC→graph_service · cascade.py · version-guarded write-back
     + dedup inbox (ProcessedEvent) ─ ALL in one Postgres tx
        ▼
  project.changed   (Kafka) ──► ws-fanout ──► Redis ──► WebSocket
                            └──► projector ──► OpenSearch  [deferred]
```

### 1. The transactional outbox (kills the dual-write bug)
The naive design — "update Postgres, then publish to Kafka" — is **two writes
with no shared transaction**. A crash between them loses or duplicates the event.
Instead, the API writes the domain change **and** an `outbox` row in the *same
Postgres transaction* (`OutboxEvent` in `models.py`, written in the schedule
endpoint). Atomic. Then a relay ships outbox rows to Kafka.

### 2. CDC via Debezium (the relay)
Postgres doesn't push to Kafka itself. **Debezium** tails the **WAL** (logical
replication) and turns committed outbox rows into Kafka messages
(`debezium/register-outbox.json`). Its **Outbox Event Router** SMT maps our
columns: `event_type` → topic name, `partition_key` (= `project_id`) → Kafka
message key, `id` → a header (our dedup key), `payload` → the message body. We ran
this for real: replication slot `wf_outbox_slot` active, connector `RUNNING`.

### 3. Partition by `project_id` = the ordering invariant
Kafka guarantees order **within a partition**. By keying every event on
`project_id`, all of a project's events land on one partition and are consumed
**in order by one worker** → a project's cascades are **serialized**. Parallelism
happens *across* projects (across partitions). Partition count is your ceiling on
concurrent-cascade throughput — we bumped topics to 12 partitions and ran 8
workers to parallelize, and the worker tier held **~0 lag** under load.

### 4. Pure core, thin transport
The cascade math is a **pure function** (`services/worker/src/cascade.py`): given
the root and the subgraph, compute new dates by constraint propagation
(FS/SS/FF/SF + lag), topological order, with cycle detection. It has **no I/O**,
so it's unit-tested without infra. The Kafka consumer (`consumer.py`) and the DB
glue (`handler.py`) are a thin shell around it. This separation is why we could
test the whole worker against Postgres with no broker running.

### 5. "Exactly-once" is a myth here → effectively-once
A favorite trap. **Kafka's transactional EOS only covers Kafka→Kafka.** Our sinks
are *external* (Postgres, OpenSearch) and Debezium CDC is *at-least-once*, so true
end-to-end exactly-once is impossible with Kafka transactions alone. We deliver
**effectively-once** by composing four mechanisms:
1. **Transactional outbox** — atomic "DB change + event" on the producer side.
2. **Idempotent producers** — `enable.idempotence`, `acks=all` on Debezium's producer.
3. **Dedup inbox** — `processed_event (consumer_group, event_id)` checked +
   inserted in the *same tx* as the cascade write; a redelivery is skipped.
4. **Idempotent sinks** — the cascade is deterministic over current state, so
   reprocessing recomputes to *zero changes*; the read model upserts by version.

## The worker algorithm (per message)
```
1. read task.schedule.changed     (manual offset commit, AFTER success)
2. BEGIN tx
3.   if processed_event has (worker, event_id): skip (dedup)
4.   load subgraph  (today: from Postgres; design: gRPC→graph_service/Neo4j)
5.   changes = cascade(...)        (pure function)
6.   UPDATE downstream tasks (version-guarded; auto-bumped)
7.   INSERT processed_event + INSERT outbox(project.changed)
8. COMMIT  →  commit Kafka offset
   on version conflict: rollback, DON'T commit offset → redelivery (Ch. 5)
   on poison: route to <topic>.dlq
```

## A real failure this design produced (and the lesson)
The 50,000-task mega-cascade emitted a `project.changed` event containing **all
50k changes in one ~8MB message**, which blew past Kafka's ~1MB default and
**killed the Debezium task** ("unrecoverable producer send"). Lesson, straight
from production-shaped pain: **bound your event payloads** — chunk or summarize
large cascades (emit "project X rescheduled, fetch the delta") rather than
embedding N changes. We band-aided with bigger `max.message.bytes`; the right fix
is chunking. *Mentioning a self-inflicted failure like this is a strong signal in
an interview.*

## Alternatives & trade-offs

| Choice | We picked | Alternative | When the alternative wins |
|---|---|---|---|
| DB→broker | Outbox + Debezium CDC | Direct publish from app | Never for correctness (dual-write); only for throwaway demos |
| DB→broker | Debezium CDC | Polling-publisher of outbox | Simpler local/dev; lower throughput, more DB load |
| Broker | Kafka (log) | RabbitMQ (queue) | Need complex per-message routing/priorities, not replay/ordering-by-key (Ch. 10) |
| Delivery | Effectively-once (idempotent) | Kafka EOS transactions | Pure Kafka→Kafka stream processing (Kafka Streams) |
| Cascade compute | Async worker | Synchronous in request | Provably tiny fan-out only |

## Interview soundbites

- "I use the **transactional outbox** so the DB write and the event are atomic,
  then **Debezium CDC** ships it — no dual-write."
- "**Partition by `project_id`** — that single choice gives me per-project
  ordering, which *is* my concurrency control for cascades."
- "**Exactly-once across Kafka and Postgres is a myth**; I get *effectively-once*
  with outbox + idempotent producer + a dedup inbox + idempotent sinks."
- "The cascade math is a **pure function**, so I unit-test correctness with zero
  infrastructure; Kafka is just transport."

## Likely follow-ups

- *"How do you not lose events if the worker crashes mid-cascade?"* → Offset is
  committed only after the tx commits; a crash → redelivery → idempotent recompute.
- *"What orders two edits to the same project?"* → Same partition (keyed by
  `project_id`) → one consumer, in order.
- *"Debezium falls behind — what do you watch?"* → Replication-slot lag
  (`pg_replication_slots`) and consumer-group lag; both we monitor in Ch. 7–8.
- *"Why not just use Kafka transactions for exactly-once?"* → They don't span the
  Postgres write; see the four-part effectively-once argument.


---

# Chapter 5 — Concurrency & Data Integrity

## In our system

Three independent concurrency hazards, each handled at a different layer.

### 1. Two humans edit the same task (optimistic locking)
Every `task`/`project` has a `version`. We declared it as SQLAlchemy's
**`version_id_col`** (`__mapper_args__ = {"version_id_col": version}` in
`models.py`), so *every* UPDATE/DELETE automatically appends `AND version =
:loaded` and bumps it; a concurrent write makes rowcount 0 → `StaleDataError`.

**The bug we actually had first:** the check lived in *application code* —
`if task.version != if_match: 409` then `task.version += 1`. Under two
simultaneous requests, both `SELECT` see `version=1`, both pass the check, both
write `version=2` → **lost update**. The check must be in the `WHERE` clause, not
Python. `version_id_col` enforces it at the DB.

We keep **two guards** because they catch different races:
- **`If-Match` header** (client's version vs current) → the stale-client case
  ("Manager B's browser was showing old data").
- **`version_id_col`** (DB `WHERE`) → the simultaneous in-flight case.

**Proven** (`tests/test_concurrency.py`): 5 concurrent moves of one task →
`[200, 409, 409, 409, 409]`, final version exactly **2** (no corruption); stale
`If-Match` → 409; missing → 428.

### 2. The worker writes while a human edits (optimistic + redelivery)
While the worker cascades (reads B, computes ~200ms, writes), a human edits B.
The worker's version-guarded write hits `StaleDataError` → the cascade tx rolls
back → **the Kafka offset isn't committed** → Kafka redelivers → the worker
recomputes against fresh state (now including the human's edit). Because the
cascade is deterministic, this **self-heals with no locks**
(`consumer.py` seeks back on `StaleDataError`). Optimistic locking + at-least-once
delivery working *together*.

### 3. Two cascades for one project (ordering, not locking)
Solved one layer up: Kafka **partitioning by `project_id`** serializes a
project's events onto one partition → one worker → no concurrent cascades for the
same project. The row lock handles human-vs-worker; the partition handles
cascade-vs-cascade.

### The hard edge case: concurrent dependency edits → a cycle
Two managers each add an edge; neither sees the other; together they form
A→B→A. Row versioning can't catch it (different rows). The fix is a **per-project
guard** when mutating the graph (a Postgres advisory lock on `project_id`, or a
serialized cycle-check), backed by the `DependencyCycleError` our `cascade.py`
already raises as a last line of defense.

## The general pattern

- **Optimistic locking** (version column in the `WHERE` clause) for low-contention,
  read-heavy data — no locks held, retry on the rare conflict.
- **Pessimistic locking** (`SELECT … FOR UPDATE`) for high-contention hot rows —
  hold the row, no retries, but you serialize and can deadlock.
- **Partition/serialize** at the queue layer for multi-row units of work.
- **Advisory/structural locks** for cross-row invariants (cycles, uniqueness).

## Alternatives & trade-offs

| Hazard | We picked | Alternative | When alternative wins |
|---|---|---|---|
| Human vs human | Optimistic (`version_id_col`) | Pessimistic `SELECT FOR UPDATE` | Very high contention on the same row |
| Worker vs human | Optimistic + Kafka redelivery | Lock subgraph `FOR UPDATE` during cascade | You can't tolerate any recompute and edits are rare |
| Cascade vs cascade | Partition-by-project | Distributed lock per project | Cross-broker coordination needed |
| Conflict scope | Whole-row version | Field-level merge | Frequent edits to disjoint fields of one row |

## Interview soundbites

- "Optimistic locking only works if the version is **in the UPDATE's WHERE
  clause** — an app-level check is a TOCTOU race that loses updates. I use
  SQLAlchemy's `version_id_col` so it's enforced on every write."
- "Human-vs-worker conflicts **self-heal**: the worker's guarded write fails, the
  Kafka offset isn't committed, it's redelivered, and the deterministic cascade
  recomputes against fresh state — no locks."
- "Cascade-vs-cascade isn't a row problem; **partitioning by project serializes
  it** at the queue."

## Likely follow-ups

- *"Optimistic vs pessimistic?"* → Optimistic for low contention + no held locks;
  pessimistic when many writers hammer the same row.
- *"What if the same edit keeps conflicting (livelock)?"* → Rare because per-project
  serialization means the only contender is the human; add bounded retries → DLQ.
- *"How do you stop a dependency cycle under concurrency?"* → Advisory lock per
  project on graph mutation + cycle check; `cascade.py` raises if one slips through.


---

# Chapter 6 — Real-time Fan-out

## In our system

The cascade's results must reach every browser watching that project — live. The
non-obvious problem: **the API runs many instances, and a Kafka consumer group
gives each partition to *one* instance, but a user's WebSocket might be connected
to a *different* instance.** "Who consumes the event" ≠ "who holds the socket."

Our solution (`services/api/src/ws.py`, `services/ws_fanout/`, API Redis lifespan):

```
project.changed (Kafka)
   → ws-fanout consumer group  (bridges Kafka → Redis)
       → PUBLISH project:{id}  (Redis pub/sub)
           → EVERY api instance is SUBSCRIBEd  → push to its local sockets
```

Redis pub/sub has **broadcast** semantics — every API instance receives every
`project:{id}` message and pushes to whichever local sockets are subscribed to
that project. That sidesteps the partition-vs-socket mismatch entirely. The
`ConnectionManager` holds per-project "rooms"; `push_local` sends a pre-formed
event (the `seq` came from the producer, not re-assigned).

`seq` here is the **Kafka offset** (monotonic per partition = per project, since
keyed), which feeds the gap-detection from Chapter 3.

### A real bug we hit
`redis.asyncio`'s `pubsub.listen()` **hangs/times out on redis 8.0**. We switched
to `get_message(timeout=...)` polling inside a **resilient reconnect loop** — a
single transient must not permanently disable fan-out. (First version caught the
exception once and returned, killing fan-out forever; the reconnect loop fixed it.)

## The general pattern

Decouple "consume the durable stream" from "deliver to connection holders":
- A small **bridge** consumes Kafka (durable, replayable) and republishes to a
  **broadcast bus** (Redis pub/sub) that all stateless front-ends subscribe to.
- Front-ends keep an **in-memory room registry** of local connections.
- A **cursor (`seq`)** lets clients detect missed messages and resync from the
  source of truth.

## Alternatives & trade-offs

| Approach | How | Pro | Con |
|---|---|---|---|
| **Redis pub/sub** (ours) | bridge Kafka→Redis, all instances subscribe | simple broadcast, no routing | every instance sees every event (fine at moderate scale); pub/sub is fire-and-forget |
| **Per-instance Kafka group** | each API instance is its own consumer group | no Redis | every instance reads *all* of Kafka; wasteful at scale |
| **Sticky routing** | hash sockets so a project's clients land on the partition's consumer | most efficient (no broadcast) | complex: consistent hashing, rebalancing, connection draining |
| **Dedicated push service** (e.g. Centrifugo, Ably, API GW WebSockets) | offload fan-out entirely | scales connections, less code | another system / vendor, cost |

At very high connection counts you'd move from Redis pub/sub to **Redis Streams**
(durable, consumer-tracked) or a managed push tier, and shard channels.

## Interview soundbites

- "The subtle bug is that **the instance consuming the partition isn't the one
  holding the socket** — so I bridge Kafka to **Redis pub/sub** and every API
  instance broadcasts to its local connections."
- "`seq` is the Kafka offset; clients use it for **gap detection** and refetch on
  a miss — fan-out is best-effort, the snapshot is the source of truth."
- "Fan-out must be **self-healing**: a reconnect loop, not a one-shot subscriber."

## Likely follow-ups

- *"Redis pub/sub drops messages if a subscriber is down — isn't that a problem?"*
  → Yes, it's fire-and-forget; correctness comes from `seq` gap-detection +
  refetch, and from Postgres being the source of truth. For stronger delivery use
  Redis Streams.
- *"How does this scale to 1M concurrent sockets?"* → Shard channels, move to a
  dedicated push tier, and stop broadcasting every event to every instance
  (sticky routing or topic sharding).
- *"Why not push straight from the worker?"* → The worker doesn't know which API
  instance holds the socket; the broadcast bus is exactly the decoupling.


---

# Chapter 7 — Observability (the God View)

> "If you debug a system this complex by reading raw terminal logs, you lose your
> mind in five minutes." A request bounces API → Postgres → Debezium → Kafka →
> worker → Redis → WebSocket. You need to *see* where it died.

## In our system

### Distributed tracing across the async boundary (the interesting part)
`libs/wf_core/wf_core/telemetry.py` sets up **OpenTelemetry** exporting to
**Jaeger** (OTLP). The hard problem: one logical request crosses a **message
queue**, so the trace must survive Kafka.

The mechanism is **W3C trace-context propagation**:
1. The API has an active span (auto-created by `FastAPIInstrumentor`).
2. Before writing the outbox row, it injects the `traceparent` into the payload:
   `payload["_trace"] = inject_trace({})`.
3. Debezium carries the payload through Kafka unchanged.
4. The worker calls `extract_context(payload["_trace"])` and starts
   `worker.cascade` **with that as parent**.

**Proven:** one trace ID spanned both services:
```
trace b6ba89…   (services: workfront-api, workfront-worker)
  workfront-api    → PATCH /api/v1/tasks/{id}/schedule
  workfront-worker → worker.cascade          ← resumed AFTER Kafka
```
So when a user says *"my date didn't update,"* you paste the trace ID into Jaeger
and see exactly which hop was slow or threw.

### Component UIs (jump in once the trace localizes the failure)
We run, in `docker-compose.yml`:
- **Jaeger** (`:16686`) — the waterfall / God View.
- **Kafka UI** (`:8080`) — browse `task.schedule.changed` JSON, consumer-group
  lag, and **Debezium connector health**.
- **RedisInsight** (`:5540`) — inspect keys, watch `project:*` pub/sub live.
- **Swagger** (`:8000/docs`) — the REST playground.

This literally found a real bug: the Debezium **connector status** showed `task:
FAILED` (the 8MB mega-cascade message) — invisible in app logs, obvious in the UI.

## The three pillars (name them)
- **Traces** — one request across services (OpenTelemetry → Jaeger/Tempo).
- **Metrics** — rates/latency/saturation; the killer metric here is **consumer
  lag** (are workers keeping up?) and **replication-slot lag** (is Debezium
  behind?). Export via OTel/Prometheus → Grafana/Datadog.
- **Logs** — structured, **correlated by trace ID** so logs join the trace.

## Alternatives & trade-offs

| Concern | We used | Alternatives | Note |
|---|---|---|---|
| Tracing backend | Jaeger (local) | Grafana Tempo, Datadog APM, Honeycomb | OTel is vendor-neutral — swap the exporter, not the code |
| Instrumentation | OpenTelemetry | vendor agents | OTel avoids lock-in; auto-instrumentation for FastAPI/SQLAlchemy |
| Metrics+dashboards | (would add) Prometheus+Grafana | Datadog, CloudWatch | Datadog = managed, all-in-one, pricey; Prom+Grafana = self-host |
| Kafka UI | provectus Kafka-UI | Redpanda Console, Confluent CP | feature parity for browsing/lag |

## Interview soundbites

- "I propagate the **W3C `traceparent` through the Kafka message** so the worker's
  span is a *child* of the API span — one trace ID for the whole pipeline,
  including the async hop."
- "The metric that matters most in an eventing system is **consumer lag** — it
  tells you the workers are falling behind before users notice."
- "OpenTelemetry keeps it **vendor-neutral**: Jaeger locally, swap the exporter
  for Datadog/Tempo in prod without touching app code."

## Likely follow-ups

- *"How does the trace cross Kafka if the worker is a different process?"* →
  Inject `traceparent` into the message payload; extract it and set it as the
  parent context — standard context propagation over a custom carrier.
- *"User says it didn't update — walk me through debugging."* → Trace ID in
  Jaeger → see which hop failed; if it's Kafka, Kafka-UI for lag/connector
  status; if fan-out, RedisInsight for the `project:*` publish. (This is the
  whole Chapter, and the question your prompt asked.)
- *"What do you alert on?"* → Consumer lag, slot lag, DLQ rate, p99 latency,
  error rate — SLO-based (Ch. 16).


---

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


---

# Chapter 9 — Microservices & Communication

## In our system

Services and *why each is its own deployable*:
- **api** (async FastAPI) — request fan-in + WebSocket connections.
- **worker fleet** — CPU/IO-heavy cascade compute; scales independently of the API.
- **ws-fanout** — bridges Kafka → Redis for real-time delivery.
- **projector** — builds the read model (deferred stub).
- **graph_service** — gRPC subgraph provider over Neo4j (deferred stub).
- Shared code (models, db, events, telemetry) lives in a library `libs/wf_core`,
  *imported* by services — not a network call.

The split is driven by **independent scaling and failure isolation**: a cascade
spike scales `worker` only; the API stays responsive. That's the real reason to
split a service — not "microservices are modern."

## Communication styles (and where we use each)
- **Browser ↔ API: REST + WebSocket** — public, cacheable, ubiquitous tooling.
- **Service ↔ service, synchronous: gRPC** — worker → `graph_service`. Binary
  Protobuf, HTTP/2 multiplexing, schema-first contracts, low latency for
  high-volume internal calls.
- **Service ↔ service, asynchronous: Kafka** — the outbox stream. Decouples
  producers from consumers, buffers load, enables replay.

The rule of thumb: **synchronous (REST/gRPC) when the caller needs an answer
now; asynchronous (Kafka) when you want decoupling, buffering, and fan-out.**

## REST vs gRPC vs messaging

| | REST/JSON | gRPC/Protobuf | Messaging (Kafka) |
|---|---|---|---|
| Shape | request/response | request/response (+streaming) | publish/subscribe |
| Encoding | text JSON | binary, compact | any |
| Contract | OpenAPI (loose) | `.proto` (strict, codegen) | schema/registry |
| Best for | public APIs, browsers | internal high-volume RPC | decoupling, fan-out, replay |
| Weakness | verbose, no streaming ergonomics | not browser-native, binary to debug | eventual consistency, ordering care |

We use **all three**, each where it fits — and naming that explicitly is a strong
interview signal.

## Monolith vs microservices (have an opinion)
Start as a **modular monolith**; split out a service only when it has a *different
scaling profile* or *failure domain*. Premature microservices buy you distributed
transactions, network failures, and ops overhead before you have the scale to
justify them. Our split is justified precisely because the worker's load is
spiky and independent of the API's.

## Service discovery, contracts, resilience
- **Discovery**: DNS/service mesh in k8s (`graph-service:50051`); we use Compose
  DNS locally.
- **Contracts**: `.proto` files are the source of truth for gRPC; OpenAPI for REST.
- **Resilience**: timeouts, retries with backoff + jitter, circuit breakers,
  bulkheads. Async (Kafka) gives backpressure for free — the log buffers.

## Interview soundbites

- "I split services by **scaling profile and failure domain**, not by fashion —
  the worker scales on cascade load without touching the API."
- "**gRPC internally, REST externally, Kafka for decoupling** — three transports,
  each where it's strongest."
- "I'd start as a **modular monolith** and extract services when a piece earns it."

## Likely follow-ups

- *"Why gRPC for graph_service but REST for the API?"* → Internal, high-volume,
  schema-first, binary-fast vs. public, browser-native, cacheable.
- *"How do services find each other?"* → DNS/service mesh; contracts via proto/OpenAPI.
- *"Downside of microservices here?"* → Distributed failures and no cross-service
  ACID — which is exactly why we use the outbox/saga-style patterns (Ch. 10).


---

# Chapter 10 — Messaging Patterns

## In our system
Kafka is the spine: outbox rows → `task.schedule.changed` → workers →
`project.changed` → fan-out + projector. Keyed by `project_id` for ordering;
consumer groups for parallelism; DLQ topics for poison messages.

## Kafka vs RabbitMQ (the classic question)

| | Kafka (log) | RabbitMQ (queue/broker) |
|---|---|---|
| Model | append-only **log**; consumers track offsets | smart broker routes messages to queues |
| Replay | yes — rewind to any offset | no — once acked, it's gone |
| Ordering | per-partition (by key) | per-queue, weaker under competing consumers |
| Routing | dumb broker, smart consumer | rich (exchanges, topic/headers, priorities) |
| Throughput | very high, sequential disk | high, lower than Kafka at scale |
| Fan-out | many consumer groups read same log | needs bindings/copies |
| Best for | event streaming, replay, ordered-by-key, CDC sinks | task queues, complex routing, per-message priority/TTL |

**We chose Kafka** because we need: ordering by `project_id`, replay (a new
projector can rebuild from the log), and fan-out (fan-out *and* projector read the
same `project.changed`). RabbitMQ would win if we needed per-message priorities or
intricate routing and didn't care about replay.

## The patterns we used (and the ones to name)

- **Transactional Outbox** (ours) — atomic DB-change-plus-event; the antidote to
  dual-write. The single most important pattern in the system.
- **CDC (Change Data Capture)** — Debezium tails the WAL to produce events.
- **CQRS** — separate write model (Postgres) from read model (OpenSearch via
  `projector`); reads scale independently of writes. (Read side deferred.)
- **Event-Carried State Transfer vs. thin events** — our `project.changed` carried
  *all* the state (and blew up at 50k); the fix is **thin events** ("project X
  changed, go fetch") or chunked deltas.
- **Pub/Sub** — multiple consumer groups (workers, fan-out, projector).
- **Dead-Letter Queue** — poison messages routed to `<topic>.dlq`.
- **Idempotent consumer** — dedup inbox (`processed_event`).

## Saga vs. our outbox (distributed transactions)
A **saga** coordinates a multi-service business transaction as a sequence of local
transactions, each with a **compensating action** to undo on failure
(choreography via events, or orchestration via a central coordinator). You reach
for it when a workflow spans *multiple services' databases* and you can't use a
single ACID transaction (e.g., "reserve inventory → charge card → ship," undo each
on failure).

**Our cascade is *not* a saga** — it's a single service's local transaction
(cascade write + outbox) replayed idempotently. We didn't need compensations
because each step is within one Postgres tx and the work is idempotent. The
interview point: *know the difference and don't over-apply saga.* If we later
added "reschedule → notify billing → update external calendar," that cross-service
workflow would become a saga, and the outbox would be how each step emits its
events reliably.

## Delivery semantics (say the myth out loud)
- **At-most-once** — fire and forget; may lose.
- **At-least-once** — retried; may duplicate → **must be idempotent**.
- **Exactly-once** — only *within* Kafka (transactions/Kafka Streams). **Across
  external systems (Postgres/OpenSearch) it's a myth**; you get *effectively-once*
  via idempotency + dedup (Ch. 4).

## Interview soundbites

- "Kafka is a **replayable log**, RabbitMQ is a **smart queue** — I picked Kafka
  for ordering-by-key, replay, and multi-consumer fan-out."
- "My core pattern is the **transactional outbox + CDC** — atomic event emission
  without dual-write."
- "A **saga** is for multi-service workflows with compensations; my cascade is a
  single-service idempotent transaction, so it isn't one — but the outbox is how
  saga steps would emit events."

## Likely follow-ups

- *"How do you replay after a bug?"* → Reset the consumer group offset / rebuild
  the projector from the `project.changed` log.
- *"Hot partition (one giant project)?"* → That project serializes (by design);
  bound/chunk its events; others are unaffected.
- *"When would you actually use a saga here?"* → A cross-service workflow
  (billing, external calendar) needing compensations.


---

# Chapter 11 — Design Patterns

> Don't recite the Gang of Four. Point at where a pattern *actually lives* in the
> system and why. Below, each pattern is tied to a real file.

## Patterns actually in our code

- **Singleton (lazy)** — `wf_core/db.py` uses `@lru_cache(maxsize=1)` on
  `get_engine()`/`get_async_engine()` so the SQLAlchemy engine (and its connection
  pool) is created **once per process, on first use**. Lazy so importing
  `wf_core` never requires a DB driver. This is the idiomatic Python singleton —
  not a global, but a memoized factory.
- **Factory** — the same `get_*_engine()` functions are factories that build a
  configured resource; `SessionLocal()` manufactures sessions.
- **Strategy** — `DepType` (FS/SS/FF/SF) selects the constraint formula in
  `cascade.py`'s `_earliest_start`; each dependency type is an interchangeable
  strategy for computing a successor's start.
- **Unit of Work** — `session_scope()` / `async_session_scope()` wrap a
  transaction (commit on success, rollback on error); the cascade's many writes
  + outbox + dedup row are one unit.
- **Repository-ish** — services go through `wf_core.models` + session helpers
  rather than raw SQL, centralizing data access.
- **Template Method** — every book chapter (and each service's
  consume→process→commit loop) follows a fixed skeleton with varying steps.
- **Observer / Pub-Sub** — Kafka consumer groups + Redis pub/sub are the
  distributed form of Observer.
- **Adapter** — `telemetry.py` adapts our payload dict into an OTel propagation
  carrier; the cascade adapts ORM rows into pure `TaskNode`/`DepEdge` values.

## Patterns to *name* but used at the architecture level
- **Transactional Outbox**, **CQRS**, **Saga**, **Circuit Breaker**, **Bulkhead**,
  **Sidecar** (a service mesh/OTel collector), **Strangler Fig** (how you migrate a
  monolith) — these are "architecture patterns," more interview-relevant than GoF.

## Dependency Injection
FastAPI's `Depends(get_session)` / `Depends(current_customer_id)` is DI — the
framework injects a per-request session and the tenant. It keeps handlers testable
(swap the dependency) and decouples them from construction.

## Monolith vs microservices vs modular monolith
- **Monolith**: one deployable; simplest ops; scales as a unit.
- **Microservices**: independent scaling/failure domains; cost = distributed
  failures, no cross-service ACID, ops overhead.
- **Modular monolith** (recommended default): clean module boundaries inside one
  deployable; extract a service only when it earns it (different scaling profile).

Our repo is structured for either: `libs/wf_core` is the shared module; `services/*`
are independently deployable but share that library.

## 12-Factor highlights we follow
- **Config in the environment** (`wf_core/config.py` reads env; defaults overridable).
- **Backing services as attached resources** (DB/Kafka/Redis by URL).
- **Stateless processes** (API/workers hold no local state; state is in
  Postgres/Kafka/Redis) — which is what makes horizontal scaling trivial.
- **Disposability** (fast startup, graceful shutdown; the launcher manages procs).

## Interview soundbites

- "Our singleton is a **memoized factory** (`lru_cache`) for the DB engine — lazy,
  so importing the package doesn't require a database."
- "`DepType` is a textbook **Strategy** — each dependency type plugs a different
  constraint formula into the same cascade loop."
- "At the architecture level the patterns that matter are **outbox, CQRS, saga,
  circuit breaker** — not the GoF catalogue."

## Likely follow-ups

- *"Show me a real strategy/factory in your code."* → `cascade.py` `_earliest_start`
  per `DepType`; `db.py` `get_async_engine`.
- *"Singleton is an anti-pattern though?"* → A global mutable singleton is; a lazy
  memoized factory of a connection pool is the right call (you want exactly one).
- *"Monolith or microservices for a greenfield?"* → Modular monolith first; split
  on demonstrated need.


---

# Chapter 12 — Caching

## In our system
We use **Redis** today for **pub/sub fan-out** (Ch. 6), and the design uses
**OpenSearch as a CQRS read model** (a *cache* of denormalized, query-optimized
documents — deferred stub). Plus a Postgres-level cache: the **denormalized
rollup** `project.percent_complete` is effectively a materialized cache of an
aggregate. So "caching" here spans three layers, which is the nuanced answer.

## The caching layers (name the hierarchy)
1. **Client / CDN** — static assets, cacheable GETs (`Cache-Control`, ETags). Our
   `If-Match`/version is the same family (conditional requests).
2. **Application cache (Redis)** — hot keys, sessions, rate-limit counters,
   pub/sub. Sub-ms reads, offloads the DB.
3. **Read model / materialized views (CQRS)** — OpenSearch documents or
   denormalized Postgres columns; not a TTL cache but a *purpose-built read copy*.
4. **DB buffer cache** — Postgres `shared_buffers` (we bumped to 1GB in perf
   testing) caches hot pages in RAM.

## Caching strategies
- **Cache-aside (lazy)**: app checks cache, on miss reads DB and populates. Most
  common; risk = thundering herd on a cold key (mitigate: request coalescing /
  locks).
- **Read-through / write-through**: the cache layer owns DB access; writes update
  cache + DB synchronously (consistent, slower writes).
- **Write-behind**: write to cache, flush to DB async (fast, risk of loss).
- **Refresh-ahead**: proactively refresh hot keys before expiry.

Our **CQRS read model is write-through-ish via the event stream**: a write commits
to Postgres, and the `project.changed` event updates the read model — eventual
consistency, not synchronous.

## Invalidation (the hard problem)
> "There are only two hard things in CS: cache invalidation and naming things."
- **TTL** — simplest; bounded staleness.
- **Event-driven invalidation** — our model: the same `project.changed` events that
  drive fan-out also invalidate/update the read model. This is the clean way: the
  event stream is the single source of "what changed."
- **Versioned keys** — include the entity `version` in the cache key so a stale
  read is naturally bypassed.

## What we deliberately do *not* cache
The **write path / source of truth** (Postgres). Caching writes is where
corruption lives. We cache *reads* and *fan-out*, never the authoritative state.

## Alternatives & trade-offs
| Need | Tool | Note |
|---|---|---|
| Pub/sub, hot keys, counters | **Redis** (ours) | in-memory, sub-ms; persistence optional |
| Durable streams w/ consumer tracking | Redis **Streams** | when pub/sub's fire-and-forget isn't enough |
| Dashboard/search reads | **OpenSearch** read model | denormalized, full-text, aggregations |
| Edge/static | CDN | for assets and cacheable GETs |

## Interview soundbites

- "Caching here is **three layers**: Redis for fan-out/hot keys, an OpenSearch
  **CQRS read model**, and denormalized rollups in Postgres."
- "I invalidate the read model **off the same event stream** that drives real-time
  fan-out — one source of truth for 'what changed.'"
- "I never cache the **write path**; only reads and fan-out. The DB stays
  authoritative."

## Likely follow-ups

- *"Cache-aside vs write-through?"* → Aside for general hot reads; write-through
  (event-driven) for the read model where consistency matters more.
- *"How do you avoid stale Gantt data?"* → Event-driven read-model updates +
  `seq` gap-detection + version in the response.
- *"Thundering herd on a cold popular key?"* → Request coalescing / a short lock /
  refresh-ahead.


---

# Chapter 13 — Datastores (polyglot persistence)

## In our system
- **Postgres** — the source of truth (relational, ACID, the write model).
- **Kafka** — durable event log (not a DB, but the system-of-record for *events*).
- **Redis** — in-memory pub/sub + (future) hot-key cache.
- **OpenSearch** — CQRS read model for search/dashboards (deferred stub).
- **Neo4j** — graph store behind `graph_service` for subgraph traversal (deferred
  stub).

This is **polyglot persistence**: each store does what it's best at. The interview
skill is justifying *why each*, not defaulting to one.

## When to use what

| Store | Data shape | Strengths | Use it when | Avoid when |
|---|---|---|---|---|
| **Postgres** (RDBMS) | relational, transactional | ACID, joins, constraints, JSONB | source of truth, money, integrity | massive horizontal write scale beyond one master |
| **MongoDB** (document) | self-contained documents | flexible schema, easy horizontal sharding | whole-object reads, schemaless, few joins | rich multi-entity transactions/joins |
| **Elasticsearch / OpenSearch** | inverted index | full-text, aggregations, fast filtered reads | search, dashboards, log analytics | source of truth (it isn't durable truth) |
| **Neo4j** (graph) | nodes + edges | cheap deep traversal, pathfinding | "all transitive successors," recommendations | simple tabular CRUD |
| **Redis** (KV/in-mem) | keys, structures | sub-ms, pub/sub, TTL | cache, counters, fan-out, locks | durable primary store |
| **Kafka** (log) | append-only events | replay, ordering, fan-out | event backbone, CDC | random-access queries |

## Postgres vs MongoDB (the FAQ)
We chose **Postgres** because the domain is **highly relational** (hierarchies,
dependency graphs, assignments) and demands **ACID + constraints** (no corrupt
schedules). We still get document flexibility via **JSONB** for custom fields
(Ch. 2) — "Postgres as a document store when you need it." Mongo would win if
objects were self-contained, rarely joined, and you wanted built-in horizontal
sharding from day one. *Don't reach for Mongo to avoid schema design.*

## Why Neo4j is in the design (and why it's a stub today)
The cascade needs "all transitive successors of task X in dependency order." In
Postgres that's a recursive CTE or loading the whole project; at extreme depth a
**graph DB traverses faster**. But at our tested scale, loading from Postgres is
fine — so `graph_service`/Neo4j is **deferred until graph depth justifies it**.
Honest sequencing beats premature complexity.

## CAP / PACELC (have the one-liner)
- **CAP**: under a network **P**artition you choose **C** or **A**. Postgres (single
  primary) is **CP**; Cassandra/Dynamo lean **AP**.
- **PACELC**: even without partitions (**E**lse), you trade **L**atency vs
  **C**onsistency. Our split is exactly this: the **write model is CP** (strong),
  the **read model/fan-out is AP-ish** (eventually consistent for lower latency).

## Sharding & scaling Postgres
Vertical first (Aurora/bigger box) → **read replicas** for read scale →
**partitioning** (`hour` by date, big tables by `customer_id`) → **sharding** by
`customer_id` (tenant-aligned, so a shard holds whole tenants and cross-shard
queries are rare). The tenant-leading keys from Ch. 2 make this migration natural.

## Interview soundbites

- "**Polyglot persistence**: Postgres for truth, OpenSearch for search, Neo4j for
  graph traversal, Redis for fan-out, Kafka for the event log — each where it's best."
- "I use **Postgres with JSONB** rather than Mongo because the domain is
  relational and needs ACID; JSONB gives schema flexibility where I want it."
- "Consistency-wise it's **PACELC**: the write model is strong/CP, the read model
  is eventually consistent for latency."

## Likely follow-ups

- *"Why not Mongo for everything?"* → Relational integrity + joins + ACID; JSONB
  covers the flexible bits.
- *"When does Neo4j actually pay off?"* → Deep/wide transitive traversals where a
  recursive SQL query gets expensive — our pathological mega-cascade.
- *"How do you shard Postgres?"* → By `customer_id` (tenant-aligned); keys are
  already tenant-leading.


---

# Chapter 14 — Webhooks at Scale

> The prompt explicitly mentions firing webhooks. This is the same eventing
> backbone pointed at *external* consumers — and external HTTP is the unreliable,
> adversarial part, so it needs extra care.

## How it slots into our design
We already have the hard part — a durable, ordered event log (`project.changed`,
etc.). Firing webhooks is **another consumer group**:

```
project.changed (Kafka)
   → webhook-dispatcher (consumer group)
       → look up subscriptions for (tenant, event_type)
       → POST to each subscriber URL with retries/backoff
       → record delivery attempt; non-2xx → retry queue → DLQ
```

No new infrastructure — webhooks are a **fan-out consumer** on the existing log.
That reuse is the headline answer.

## The hard parts of webhooks (external HTTP is hostile)
1. **Delivery reliability** — subscribers are down, slow, or flaky. Use
   **at-least-once with retries**: exponential backoff **+ jitter**, capped
   attempts, then a **DLQ** + alert. Never retry synchronously in the request path.
2. **Idempotency for the receiver** — at-least-once means duplicates. Send an
   **idempotency key / event ID** (and a monotonic `seq`) so receivers dedup.
   This is the *same* dedup-inbox idea (Ch. 4) pushed to the partner.
3. **Ordering** — partners often want per-resource order. Partition the dispatch
   by resource (e.g., `project_id`) just like internally; or include `seq` and let
   them reorder.
4. **Security** — **HMAC-sign** each payload (shared secret), include a timestamp
   to prevent replay, deliver over TLS, and support secret rotation. Optionally
   mTLS / allow-listed source IPs.
5. **Back-pressure & isolation** — one slow partner must not stall others: **per-
   subscriber concurrency limits / circuit breakers**, and isolate a failing
   endpoint (bulkhead) so it can't exhaust the dispatcher.
6. **Poison endpoints** — auto-disable a subscription after N consecutive failures;
   notify the owner; expose a redelivery API.

## At 10M events/day
~116/s of source events, fanned to *each* subscription → multiply by subscribers.
The log buffers bursts; the dispatcher scales horizontally (more consumers across
partitions). Slow endpoints are the real limiter, so **bounded per-endpoint
concurrency + async I/O** matter more than raw throughput. Outbound calls are
I/O-bound → an async dispatcher (like our async API) holds many in-flight POSTs
cheaply.

## Build vs buy
- **Build** on our Kafka log (full control, no per-event cost).
- **Buy / use a library**: **Svix** (managed webhooks-as-a-service — retries,
  signing, dashboards, partner-facing UI) or **Hookdeck**. For a product exposing
  webhooks to many external partners, buying the *delivery/observability* layer is
  often worth it; you still produce events from your own log.
- **Local dev**: **ngrok** to route a cloud webhook to your laptop; a dashboard
  (Svix/RequestBin) to inspect payloads and partner responses (200 vs 500).

## Interview soundbites

- "Webhooks are just **another consumer group on the existing event log** — the
  durable, ordered backbone is already built."
- "External HTTP is hostile, so: **at-least-once with backoff+jitter+DLQ,
  HMAC-signed payloads, idempotency keys, and per-subscriber circuit breakers** so
  one slow partner can't stall the rest."
- "I'd consider **Svix** to buy the delivery/observability layer while still
  producing events from my own Kafka log."

## Likely follow-ups

- *"A subscriber is down for an hour — then what?"* → Retries with backoff, then
  DLQ; auto-disable after N failures; redelivery API when they recover. The log
  retains events for replay.
- *"How does the partner avoid double-processing?"* → Idempotency key + `seq`; they
  dedup — same pattern as our internal dedup inbox.
- *"How do you prove you sent it / they got it?"* → Persist delivery attempts +
  response codes (the Svix/observability view); expose it to partners.


---

# Chapter 15 — Infrastructure & Cloud

## In our system (local)
Everything runs via Docker Compose + a local launcher (`main.py`): Postgres
(Homebrew, `wal_level=logical`), Kafka (KRaft, no Zookeeper), Debezium/Kafka
Connect, Redis, Jaeger, Kafka-UI, RedisInsight. App services run as processes
(api with multiple uvicorn workers; a worker fleet; ws-fanout). This is a faithful
*shape* of production, scaled down.

## Local → production mapping (AWS as example)

| Local | AWS managed | Why |
|---|---|---|
| Postgres (Homebrew) | **Aurora PostgreSQL** | managed HA, read replicas, autoscaling storage |
| Kafka (KRaft) | **MSK** (or Confluent Cloud) | managed brokers, scaling, durability |
| Debezium on Connect | **MSK Connect** | managed CDC connectors |
| Redis | **ElastiCache (Redis)** | managed, clustered |
| OpenSearch | **Amazon OpenSearch Service** | managed search/read model |
| App processes | **EKS** (k8s) pods / ECS / Fargate | orchestration, autoscaling, rollouts |
| Jaeger/metrics | **Datadog** / Grafana Cloud / AWS X-Ray | managed observability |
| Object storage | **S3** | attachments, backups, data lake |
| Secrets/config | **Secrets Manager / SSM** | no secrets in images (12-factor) |

The point isn't AWS specifically — it's that **every component has a managed
equivalent**, and you'd buy the undifferentiated heavy lifting (brokers, DB HA,
search) and keep your own code in containers.

## Kubernetes & autoscaling
- **Deployments** for stateless services (api, worker, ws-fanout) — trivially
  horizontally scalable because they hold no local state (12-factor).
- **HPA** (Horizontal Pod Autoscaler) on CPU/latency for the API.
- **KEDA** for the workers — scale on **Kafka consumer lag** (the right signal:
  scale workers when the cascade backlog grows, scale to zero when idle). This is
  the cloud-native version of "the worker tier never bottlenecked, just add more."
- **StatefulSets / managed services** for stores (or use the managed cloud ones).
- **Service mesh** (Istio/Linkerd) for mTLS, retries, and traffic shaping; the
  OTel collector runs as a **sidecar** or DaemonSet.

## Resource right-sizing (tie to our numbers)
We measured ~582 moves/s on one box with the worker tier idle. So for 10M/day
(~116/s avg): a few small API pods behind an HPA, a handful of worker pods behind
KEDA on lag, a modest Aurora instance + a read replica, a small MSK cluster with
enough partitions for cascade parallelism. **Over-provisioned for the target** —
the spend goes to HA/redundancy, not raw throughput.

## Cost & reliability levers
- **Multi-AZ** for Aurora/MSK/ElastiCache (HA).
- **Partitions** sized for peak cascade concurrency, not average QPS.
- **Autoscale on the right metric** (lag for workers, latency/CPU for API).
- **Spot/Fargate** for stateless workers to cut cost.

## Interview soundbites

- "Stateless services + **KEDA scaling workers on Kafka consumer lag** — scale on
  the backlog signal, not CPU."
- "Every local component has a **managed equivalent** (Aurora, MSK, ElastiCache,
  OpenSearch); I buy the undifferentiated parts and keep my code in **EKS**."
- "Sizing is driven by our measured ~582/s headroom — for 10M/day the spend is
  about **HA and partitions**, not throughput."

## Likely follow-ups

- *"How do you autoscale the workers?"* → KEDA on consumer lag; scale to zero when
  idle, burst when a cascade storm hits.
- *"Self-host Kafka or managed?"* → Managed (MSK/Confluent) unless you have a
  platform team; brokers are undifferentiated heavy lifting.
- *"How do you do zero-downtime deploys with a schema change?"* → Expand/contract
  migrations + rolling deploys (Ch. 16).


---

# Chapter 16 — CI/CD & Operations

## In our system
We have the building blocks ops needs: **Alembic migrations** (`0001`, `0002`) for
versioned schema, **tests** (`tests/` — contract, concurrency, worker, e2e) and a
**perf harness** (`tests/perf/`) for load regression, **OpenTelemetry/Jaeger** for
tracing, and **health endpoints** on every service. A pipeline wires these
together.

## A CI/CD pipeline for this system
**CI (on every PR):**
1. Lint/format/type-check.
2. **Unit tests** — the pure `cascade.py` (no infra) runs in milliseconds.
3. **Integration tests** — spin Postgres (+ Kafka) in the CI runner (Compose /
   Testcontainers); run `tests/test_worker_handler.py`, `test_concurrency.py`.
4. Build container images per service; tag by git SHA.
5. (Periodically) run the **perf harness** as a regression gate on throughput.

**CD (on merge to main):**
1. Push images to a registry.
2. **Run DB migrations** as a gated step (`alembic upgrade head`) *before* rolling
   the new code — using **expand/contract** so old and new code both work against
   the intermediate schema.
3. **Progressive rollout**: rolling update → **canary** (small % of traffic) →
   watch SLOs → promote or auto-rollback.

## Zero-downtime schema changes (expand/contract)
Never make a breaking change in one step:
- **Expand**: add the new column/table (nullable/defaulted), deploy code that
  writes both old and new.
- **Migrate**: backfill.
- **Contract**: switch reads to new, then drop the old — in a *later* deploy.

Our additive migrations (`0002` just adds `processed_event`) are expand-safe. The
`version_id_col` addition needed no schema change at all (the column existed).

## Observability-driven operations (SLOs)
- **SLIs**: API p99 latency, error rate, **consumer lag**, **replication-slot
  lag**, DLQ rate, cascade end-to-end latency.
- **SLOs** + error budgets drive alerting; page on lag/slot-lag/DLQ growth before
  users feel it.
- **Dashboards**: Datadog or Grafana over the OTel metrics; traces in Jaeger/Tempo;
  Kafka-UI/RedisInsight for component drill-down (Ch. 7).

## Deployment strategies (know the trade-offs)
| Strategy | What | When |
|---|---|---|
| Rolling | replace pods gradually | default for stateless services |
| Blue-green | full parallel env, switch traffic | instant rollback, 2× resources |
| Canary | small % first, watch metrics, promote | risk-averse, needs good SLIs |

## Operational runbooks (the stuff that wins senior interviews)
- **Worker lag climbing** → scale workers (KEDA), check for a poison message / a
  mega-cascade hogging a partition.
- **Debezium connector FAILED** → check connector status (we *did* this — the 8MB
  message), fix payload size, restart task; reset slot only as last resort.
- **DLQ filling** → inspect poison messages, fix + replay.
- **Replication slot lag growing** → Debezium down/slow → WAL piles up → disk
  risk; alert early.

## Interview soundbites

- "Migrations run as a **gated CD step with expand/contract** so deploys are
  zero-downtime; my additive migrations are already expand-safe."
- "I alert on **consumer lag, slot lag, and DLQ rate** — leading indicators, not
  just CPU."
- "The pure cascade function gives **fast unit tests**; integration tests use
  Testcontainers; the perf harness is a **throughput regression gate**."

## Likely follow-ups

- *"How do you roll back a bad deploy?"* → Canary catches it on SLOs → auto-
  rollback; blue-green for instant switch; migrations are backward-compatible so
  code rollback is safe.
- *"How do you test the cascade without Kafka?"* → It's a pure function + a
  handler tested against Postgres directly (we did exactly this).
- *"What pages you at 3am?"* → Growing consumer/slot lag or DLQ — the eventing
  system falling behind.


---

# Chapter 17 — Mock Interview: The Main Question

> **"Design an eventing system that handles 10M changes per day, fires webhooks to
> external partners, and updates a real-time UI. Walk me through it."**

This is a 35–45 minute whiteboard. Here is a structured answer that maps directly
onto what we built. Use the headers as your spoken outline.

---

## 1. Clarify & scope (2–3 min — *don't skip this*)
> "Let me pin down requirements first.
> - **What's a 'change'?** A user edits an entity (in our case a task's dates),
>   which can **cascade** to dependent entities. So one change can fan out.
> - **10M/day** — is that user actions or total events including fan-out? I'll
>   assume ~10M source changes/day ≈ **116/s average, ~1k/s peak**.
> - **Consistency**: must the editor see their change immediately? (Yes.) Must
>   dependents update instantly? (Eventually, within ~1s, is fine.)
> - **Webhooks**: at-least-once to partners, signed, with retries?
> - **Multi-tenant SaaS?** (Yes.)
> - **Ordering**: do changes to the same entity need to be ordered? (Yes.)"

Stating these *reframes the problem*: the hard part is **fan-out + real-time +
correctness**, not raw QPS.

## 2. Back-of-envelope (1–2 min)
> "116/s avg, ~1k/s peak. Each change is a small row + an event, a few KB of WAL →
> single-digit MB/s, trivial for one Postgres. 10M rows/day × ~1KB ≈ 10GB/day —
> fine with partitioning. So storage and steady QPS aren't the constraint. The
> constraint is a **single change rescheduling tens of thousands of dependents**,
> and getting that to every connected browser and every webhook subscriber
> **without blocking the user**."

## 3. API & data model (4–5 min)
> "Browser talks **REST for commands, WebSocket for live updates**. The write is:
> `PATCH /tasks/{id}/schedule` with `If-Match: <version>` for optimistic
> concurrency. It returns **200 with the edited task's new state in ~tens of ms**;
> dependents arrive later over WebSocket. Edited task = strongly consistent,
> dependents = eventually consistent.
>
> Data lives in **Postgres** (relational, ACID — schedules can't corrupt),
> multi-tenant with `customer_id` leading every index, **UUIDv7** keys for
> index-friendly uniqueness, **JSONB** for custom fields, and a `version` column
> per row for locking."

## 4. The eventing core (8–10 min — *spend your time here*)
> "The naive 'update DB then publish to Kafka' is a **dual-write bug** — a crash
> between them loses or duplicates events. Instead I use the **transactional
> outbox**: the API writes the domain change **and** an `outbox` row in *one*
> Postgres transaction. Then **Debezium** does **CDC** off the WAL and publishes
> outbox rows to **Kafka**.
>
> I key every event by **`project_id`** so all of a project's changes land on one
> **partition** → consumed **in order** by one worker. That's my concurrency
> control for cascades: per-project serialization, parallel across projects.
>
> A **worker fleet** (consumer group) does the cascade: load the affected
> subgraph, run a **pure constraint-propagation function**, write the new dates
> back **version-guarded**, and emit a `project.changed` event — all in one
> transaction with a **dedup-inbox** row.
>
> On delivery semantics: **true exactly-once across Kafka and Postgres is a
> myth** — Kafka transactions only cover Kafka-to-Kafka. I get **effectively-
> once** = outbox + idempotent producer + dedup inbox + idempotent (deterministic)
> cascade."

*(Draw: API → Postgres+outbox → Debezium → Kafka(task.schedule.changed) → workers
→ Kafka(project.changed) → consumers.)*

## 5. Real-time UI (3 min)
> "`project.changed` is consumed by a **fan-out bridge** that publishes to **Redis
> pub/sub**; every API instance subscribes and pushes to its locally-held
> WebSockets. This solves the mismatch that *the instance consuming the partition
> isn't the one holding the socket*. Each event carries a monotonic **`seq`**; the
> client refetches if it detects a gap."

## 6. Webhooks (3 min)
> "Webhooks are **just another consumer group** on `project.changed` — the durable
> log is already built. A dispatcher looks up subscriptions and POSTs with
> **at-least-once + exponential backoff + jitter**, **HMAC-signed** payloads, an
> **idempotency key** so partners dedup, **per-subscriber circuit breakers** so one
> slow partner can't stall others, and a **DLQ** for poison endpoints. I might buy
> Svix for the delivery/observability layer."

## 7. Concurrency & correctness (3 min)
> "Two managers editing one task: **optimistic locking** via a version column in
> the UPDATE's WHERE clause — first commit wins, second gets 409. (App-level checks
> are a TOCTOU race that loses updates — the version must be in the DB write.)
> Worker-vs-human mid-cascade: the guarded write fails, the offset isn't
> committed, **Kafka redelivers**, the deterministic cascade recomputes — self-
> healing, no locks. Cascade-vs-cascade: solved by partition ordering."

## 8. Scale & failure (3 min)
> "Stateless API and workers scale horizontally; **workers autoscale on Kafka
> consumer lag (KEDA)**. Postgres scales vertically → read replicas → partition →
> shard by `customer_id`. Failure modes: a worker crash → redelivery + idempotency;
> Debezium lag → watch the replication slot; a poison message → DLQ; a **mega-
> cascade** is bounded to one project and **its event payload must be chunked** —
> I learned that the hard way when a 50k-change event exceeded Kafka's message
> limit and killed the connector."

## 9. Numbers (close strong)
> "I actually built and load-tested this: **~582 changes/s sustained on one
> laptop** with the worker tier at **~0 lag** — ~5× the 10M/day target — and a
> **50,000-task cascade in 5.8s while the API still answered in 45ms**. So the
> architecture is comfortably over the target; the engineering effort goes into
> correctness and bounding pathological cascades."

---

## Likely interviewer follow-ups (with crisp answers)

- **"Why Kafka over RabbitMQ?"** → Replayable log, ordering by key, multi-consumer
  fan-out (workers + UI + webhooks read the same log). Rabbit wins for complex
  routing/priorities.
- **"How do you guarantee ordering?"** → Partition by `project_id`; order is
  per-partition; one consumer per partition.
- **"How do you avoid losing an event?"** → Outbox (atomic with the write) + CDC;
  offset committed only after the consumer's tx commits.
- **"How do you avoid double-processing?"** → Dedup inbox + deterministic cascade
  (reprocessing → zero changes).
- **"What if a cascade is 1M tasks?"** → Still one ordered unit for that project;
  chunk the emitted event; accept longer eventual consistency for that project;
  others unaffected.
- **"How does the UI stay correct over a flaky connection?"** → `seq` cursor +
  gap-detection → refetch the snapshot; Postgres is the source of truth.
- **"Webhook partner is down for an hour?"** → Backoff retries → DLQ → auto-disable
  → redelivery API; the log retains events for replay.
- **"How do you debug 'my change didn't show up'?"** → One **trace ID propagated
  through Kafka**; Jaeger shows which hop failed; then Kafka-UI/RedisInsight to
  drill in.
- **"Single Postgres is your SPOF / bottleneck?"** → Aurora multi-AZ + replicas;
  partition; shard by tenant; the outbox is in the same DB so it stays atomic.


---

# Chapter 18 — Rapid-Fire Q&A

Short, out-loud answers. Each ties back to our system where possible.

### Messaging & events
1. **Kafka vs RabbitMQ?** Kafka = replayable log, ordering-by-key, multi-consumer
   fan-out. Rabbit = smart broker, rich routing/priorities, no replay. We chose Kafka.
2. **At-least-once vs exactly-once?** At-least-once + idempotency is the practical
   answer. Exactly-once exists only *within* Kafka; across Postgres/OpenSearch it's
   a myth → **effectively-once** (outbox + idempotent producer + dedup inbox).
3. **How to guarantee ordering in Kafka?** Per-partition only; key by the entity
   you need ordered (we key by `project_id`).
4. **Transactional outbox — why?** Atomic "DB write + event" in one tx; kills the
   dual-write bug.
5. **CDC?** Change Data Capture — read the DB's WAL (Debezium) to emit events
   without app dual-writes.
6. **Dead-letter queue?** Where poison messages go after N failed retries; inspect,
   fix, replay.
7. **Idempotent consumer?** Dedup by event ID in the same tx as the side effect
   (our `processed_event`), so redelivery is a no-op.
8. **Hot partition?** One key dominates (a giant project). It serializes by design;
   bound/chunk its work; consider sub-keying if truly pathological.

### APIs & communication
9. **REST vs gRPC?** REST = public, browser-native, cacheable. gRPC = internal,
   binary/Protobuf, HTTP/2, schema-first, low-latency RPC.
10. **REST vs GraphQL?** GraphQL = flexible client-driven queries, but heavier
    subscriptions/ops. REST when the write surface is small (ours).
11. **WebSocket vs SSE vs polling?** WS = bidirectional, real-time (ours, for
    Gantt). SSE = one-way push, auto-reconnect, simpler. Polling = last resort.
12. **Sync vs async API runtime?** Async (event loop) for I/O-bound + many
    connections (WebSockets force it); sync (threadpool) for CPU-bound simple work.
13. **Idempotency for APIs?** Idempotency-Key header; safe retries; we use
    `If-Match`/version for conditional writes.
14. **How to version an API?** URI version (`/api/v1`), additive changes,
    expand/contract for breaking ones.

### Data & consistency
15. **SQL vs NoSQL?** SQL for relational + ACID (ours); NoSQL (document/KV/wide-
    column) for schemaless, horizontal scale, simple access patterns.
16. **Postgres vs Mongo?** Relational integrity + joins + ACID + JSONB flexibility
    vs. self-contained documents + easy sharding. We chose Postgres + JSONB.
17. **CAP?** Under partition, choose Consistency or Availability. Write model CP,
    read model AP.
18. **PACELC?** Even without a partition, trade Latency vs Consistency — our
    strong-write / eventual-read split.
19. **Optimistic vs pessimistic locking?** Optimistic (version in WHERE) for low
    contention, no held locks; pessimistic (`SELECT FOR UPDATE`) for hot rows.
20. **How to shard a relational DB?** By a tenant-aligned key (`customer_id`) so
    shards hold whole tenants; cross-shard queries stay rare.
21. **Elasticsearch/OpenSearch — when?** Search, aggregations, dashboards — a read
    model, never the source of truth.
22. **Graph DB — when?** Deep/wide transitive traversals (our cascade subgraph)
    where recursive SQL gets expensive.

### Caching
23. **Cache-aside vs write-through?** Aside = app populates on miss (common);
    write-through = cache+DB updated together (consistent, slower writes).
24. **Cache invalidation?** TTL, event-driven (ours — off the event stream), or
    versioned keys.
25. **What not to cache?** The write path / source of truth.

### Patterns & architecture
26. **Singleton in practice?** A lazy memoized factory (`lru_cache`) for the DB
    engine — exactly one pool per process.
27. **Strategy?** Our `DepType` (FS/SS/FF/SF) picks the constraint formula.
28. **Saga?** Multi-service workflow with compensating actions; orchestration or
    choreography. Our cascade isn't one (single-service, idempotent).
29. **CQRS?** Separate write model (Postgres) from read model (OpenSearch); scale
    reads independently.
30. **Monolith vs microservices?** Modular monolith first; split on scaling/failure
    need. We split the worker because its load is spiky and independent.
31. **Circuit breaker / bulkhead?** Stop calling a failing dependency / isolate it
    so it can't exhaust shared resources — used in webhook dispatch.

### Infra, scale, ops
32. **How to autoscale workers?** KEDA on **Kafka consumer lag** — scale on
    backlog, not CPU.
33. **k8s primitives?** Deployments (stateless), HPA, KEDA, StatefulSets, Services,
    service mesh sidecars.
34. **Zero-downtime schema change?** Expand/contract migrations + rolling deploy.
35. **Deployment strategies?** Rolling (default), blue-green (instant rollback),
    canary (watch SLOs).
36. **What do you monitor in an eventing system?** Consumer lag, replication-slot
    lag, DLQ rate, p99 latency, error rate, cascade end-to-end latency.
37. **The three pillars of observability?** Traces, metrics, logs — correlated by
    trace ID.
38. **How does a trace cross Kafka?** Inject the W3C `traceparent` into the message
    payload; the consumer resumes the span as a child.
39. **Webhook reliability?** At-least-once + backoff/jitter + DLQ + HMAC signing +
    idempotency keys + per-subscriber circuit breakers.
40. **Biggest risk in this design?** A pathological mega-cascade and the oversized
    event it emits — bound it and chunk the payload (we hit this for real).


---

# Appendix — Glossary & Decision Log

## The numbers (memorize these)

| Metric | Value |
|---|---|
| Target | 10M changes/day ≈ **116/s avg**, ~1k/s peak |
| Sustained throughput (1 laptop, all services co-located) | **~582 moves/s**, worker lag ~0 |
| Cascaded task-writes/s | ≈ **5,800/s** |
| Mega-cascade | **50,000 tasks in 5.81s = 8,602 tasks/s** |
| API write latency | **45ms** |
| Single API process ceiling | ~190 req/s (1 core) |

## Decision log (every choice + the why)

These were real decisions made while building the system. In an interview, "I
chose X over Y because Z" is far stronger than reciting X.

| Decision | Chose | Over | Why |
|---|---|---|---|
| Primary key | **UUIDv7** | UUIDv4 / bigint | time-ordered → index-friendly + globally unique for events |
| Custom fields | **JSONB + generated cols** | EAV table | no self-join explosion; flexible + queryable |
| Tenancy | **shared tables, `customer_id`-leading indexes** | DB-per-tenant | scales to many tenants; isolate later by partition |
| API protocol | **REST + WebSocket** | GraphQL / SSE | small write surface; WS needed for live Gantt |
| API runtime | **async FastAPI** | sync threadpool | I/O-bound + holds WebSocket connections |
| Cascade response | **200 now + WS ripple** | 202 / inline | instant edit confirm; dependents eventually consistent |
| DB → broker | **outbox + Debezium CDC** | direct publish | kills the dual-write bug |
| Broker | **Kafka** | RabbitMQ | replay, ordering-by-key, multi-consumer fan-out |
| Ordering | **partition by `project_id`** | global lock | per-project serialization = cascade concurrency control |
| Delivery | **effectively-once** (idempotent + dedup) | Kafka EOS transactions | EOS can't span Postgres/OpenSearch |
| Locking | **`version_id_col`** (DB WHERE) | app-level version check | app check is a TOCTOU race → lost updates |
| Worker conflict | **optimistic + Kafka redelivery** | pessimistic `FOR UPDATE` | self-heals, no held locks |
| Real-time fan-out | **Redis pub/sub bridge** | per-instance Kafka group / sticky routing | broadcast solves "consumer ≠ socket holder" |
| Cascade compute | **pure function** (`cascade.py`) | logic inside the consumer | unit-testable with zero infra |
| Subgraph load | **Postgres now**, Neo4j later | Neo4j now | avoid premature complexity until depth justifies it |
| Read model | **OpenSearch (deferred)** | build now | core write/cascade path works without it |
| Tracing | **OpenTelemetry → Jaeger** | vendor agent | vendor-neutral; swap exporter for prod |

## Bugs we found (war stories)

- **Optimistic-lock TOCTOU**: version check in Python → lost updates; fixed with
  `version_id_col` (version in the SQL WHERE).
- **`redis.asyncio` `listen()` hangs on redis 8.0**: switched to
  `get_message(timeout=)` + a resilient reconnect loop.
- **Debezium EventRouter rejected `created_at`** ("not INT64"): dropped the
  event-timestamp mapping.
- **8MB mega-cascade message killed the Debezium task**: bumped `max.message.bytes`
  as a band-aid; real fix is to **chunk the cascade event**.
- **Connection-pool starvation**: default ~5 async connections queued 300
  requests (p99 6s); raised pool to 40+20, `max_connections` to 300.
- **UTC drift**: pinned every DB connection to UTC so the wire format is consistent.

## Glossary

- **Outbox pattern** — write domain change + event in one DB tx; relay to broker.
- **CDC** — Change Data Capture; emit events by tailing the DB WAL (Debezium).
- **CQRS** — separate write model from read model.
- **Saga** — multi-service transaction via local steps + compensations.
- **Effectively-once** — at-least-once delivery made safe by idempotency + dedup.
- **Optimistic locking** — version column in the UPDATE's WHERE; first writer wins.
- **Backpressure** — letting a buffer (Kafka log) absorb load the consumer can't yet handle.
- **Consumer lag** — how far behind a consumer group is on a topic (key health metric).
- **PACELC** — if Partition: C-or-A; Else: Latency-or-Consistency.
- **HPA / KEDA** — k8s autoscalers (CPU/latency / event-source lag).
- **DLQ** — dead-letter queue for poison messages.
- **traceparent** — W3C header carrying trace context across service/queue boundaries.

## Repo map (where to point during the interview)

| Concept | File |
|---|---|
| Schema, tenancy, version col | `libs/wf_core/wf_core/models.py` |
| UUIDv7 | `libs/wf_core/wf_core/ids.py` |
| Async + sync DB engines | `libs/wf_core/wf_core/db.py` |
| Outbox / dedup tables | `models.py` (`OutboxEvent`, `ProcessedEvent`) |
| API + cascade contract | `services/api/src/main.py`, `docs/api-contract.md` |
| WebSocket manager | `services/api/src/ws.py` |
| Pure cascade algorithm | `services/worker/src/cascade.py` |
| Worker (Kafka + dedup + redelivery) | `services/worker/src/{handler,consumer}.py` |
| Debezium outbox connector | `debezium/register-outbox.json` |
| Redis fan-out bridge | `services/ws_fanout/src/main.py` |
| Trace propagation | `libs/wf_core/wf_core/telemetry.py` |
| Event-flow design | `docs/event-flow.md` |
| Perf harness | `tests/perf/` |
| Correctness tests | `tests/test_*.py` |


---

