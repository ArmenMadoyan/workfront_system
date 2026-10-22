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
