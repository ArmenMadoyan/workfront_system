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
