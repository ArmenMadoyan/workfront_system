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
