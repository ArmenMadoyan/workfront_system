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
