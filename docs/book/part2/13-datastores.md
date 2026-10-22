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
