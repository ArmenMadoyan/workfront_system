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
