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
