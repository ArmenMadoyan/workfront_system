# Event Flow (Task 3) — Kafka + the Worker Fleet

The nervous system: connects "a write happened" → "the cascade was computed" →
"the browser and read-model find out", preserving per-project ordering.

## Topology

```
API / worker write ──tx──▶ Postgres + outbox            (atomic, no dual-write)
        │
   Debezium (WAL CDC, idempotent producer, outbox event-router SMT)
        ▼
  task.schedule.changed   (key = project_id)
        │
   worker-fleet  (consumer group; one partition = ordered per project)
        ├─ gRPC → graph_service.GetAffectedSubgraph(task)
        ├─ cascade.py
        └─ ONE tx: UPDATE downstream (version-guarded)
                   + processed_event (dedup)
                   + outbox(project.changed)
        │
   Debezium
        ▼
  project.changed   (key = project_id)
        ├─ ws-fanout consumer ──▶ Redis pub/sub  ──▶ every api instance ──▶ WebSocket
        └─ projector       ──▶ OpenSearch (idempotent upsert by id+version)
```

## Topics

| topic | kind | key | partitions | cleanup | producer | consumers |
|---|---|---|---|---|---|---|
| `task.schedule.changed` | event | `project_id` | 12–48 | delete 7d | Debezium (outbox) | worker-fleet |
| `project.changed` | event | `project_id` | 12–48 | delete 7d | Debezium (outbox) | ws-fanout, projector |
| `task.batch.create` | command | `project_id` | 12–48 | delete 7d | api / agent (phase 2) | worker-fleet |
| `*.dlq` | event | orig key | 3 | delete 14d | failed consumers | ops |

**Partition key = `project_id`** is the core invariant: all of a project's events
land on one partition → one ordered consumer → cascades serialized per project.
Parallelism is across projects. Partition count caps concurrent-cascade throughput.

## Exactly-once = effectively-once (and why)

Kafka transactional EOS only covers **Kafka→Kafka**. Our sinks are external
(Postgres, OpenSearch) and Debezium CDC is at-least-once, so end-to-end
exactly-once is assembled from:

1. **Transactional outbox** — atomic "DB change + event" (producer side).
2. **Idempotent producers** — `enable.idempotence=true`, `acks=all` on Connect.
3. **Dedup inbox** (`processed_event`) — `(consumer_group, event_id)` checked +
   inserted in the SAME tx as the side effects; a redelivery is skipped.
4. **Idempotent sinks** — cascade is deterministic + version-guarded; projector
   upserts OpenSearch with external version = `task.version`.

## Worker node algorithm (per message)

```
1. read task.schedule.changed            (manual offset commit, AFTER success)
2. BEGIN tx
3.   if processed_event has (worker, event_id): COMMIT, ack, return   # dedup
4.   subgraph = graph_service.GetAffectedSubgraph(task_id)            # gRPC
5.   changes  = cascade(subgraph)                                     # pure
6.   UPDATE each downstream SET dates, version+1 WHERE id=? AND version=?
7.   INSERT processed_event(worker, event_id)
8.   INSERT outbox(project.changed, key=project_id, payload=changes)
9. COMMIT tx  →  commit Kafka offset
   on error: retry w/ backoff; after N → task.schedule.changed.dlq
```

## WebSocket fan-out (Redis)

`project.changed` → a `ws-fanout` consumer group bridges Kafka → Redis
(`PUBLISH project:{id} <event>`). Every `api` instance `SUBSCRIBE`s and pushes to
its locally-held sockets. This solves "the instance consuming the partition isn't
the one holding the socket": Redis broadcasts to all instances.

## Local vs prod

| concern | local/dev | prod |
|---|---|---|
| CDC relay | Debezium on Kafka Connect (compose) | Debezium on managed Connect |
| Kafka | single-broker KRaft | multi-broker, RF=3 |
| Redis | single node | clustered / managed |
| DLQ | topic + log | topic + alerting |