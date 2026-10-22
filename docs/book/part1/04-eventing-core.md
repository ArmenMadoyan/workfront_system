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
