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
