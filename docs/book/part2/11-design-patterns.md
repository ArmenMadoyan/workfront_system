# Chapter 11 — Design Patterns

> Don't recite the Gang of Four. Point at where a pattern *actually lives* in the
> system and why. Below, each pattern is tied to a real file.

## Patterns actually in our code

- **Singleton (lazy)** — `wf_core/db.py` uses `@lru_cache(maxsize=1)` on
  `get_engine()`/`get_async_engine()` so the SQLAlchemy engine (and its connection
  pool) is created **once per process, on first use**. Lazy so importing
  `wf_core` never requires a DB driver. This is the idiomatic Python singleton —
  not a global, but a memoized factory.
- **Factory** — the same `get_*_engine()` functions are factories that build a
  configured resource; `SessionLocal()` manufactures sessions.
- **Strategy** — `DepType` (FS/SS/FF/SF) selects the constraint formula in
  `cascade.py`'s `_earliest_start`; each dependency type is an interchangeable
  strategy for computing a successor's start.
- **Unit of Work** — `session_scope()` / `async_session_scope()` wrap a
  transaction (commit on success, rollback on error); the cascade's many writes
  + outbox + dedup row are one unit.
- **Repository-ish** — services go through `wf_core.models` + session helpers
  rather than raw SQL, centralizing data access.
- **Template Method** — every book chapter (and each service's
  consume→process→commit loop) follows a fixed skeleton with varying steps.
- **Observer / Pub-Sub** — Kafka consumer groups + Redis pub/sub are the
  distributed form of Observer.
- **Adapter** — `telemetry.py` adapts our payload dict into an OTel propagation
  carrier; the cascade adapts ORM rows into pure `TaskNode`/`DepEdge` values.

## Patterns to *name* but used at the architecture level
- **Transactional Outbox**, **CQRS**, **Saga**, **Circuit Breaker**, **Bulkhead**,
  **Sidecar** (a service mesh/OTel collector), **Strangler Fig** (how you migrate a
  monolith) — these are "architecture patterns," more interview-relevant than GoF.

## Dependency Injection
FastAPI's `Depends(get_session)` / `Depends(current_customer_id)` is DI — the
framework injects a per-request session and the tenant. It keeps handlers testable
(swap the dependency) and decouples them from construction.

## Monolith vs microservices vs modular monolith
- **Monolith**: one deployable; simplest ops; scales as a unit.
- **Microservices**: independent scaling/failure domains; cost = distributed
  failures, no cross-service ACID, ops overhead.
- **Modular monolith** (recommended default): clean module boundaries inside one
  deployable; extract a service only when it earns it (different scaling profile).

Our repo is structured for either: `libs/wf_core` is the shared module; `services/*`
are independently deployable but share that library.

## 12-Factor highlights we follow
- **Config in the environment** (`wf_core/config.py` reads env; defaults overridable).
- **Backing services as attached resources** (DB/Kafka/Redis by URL).
- **Stateless processes** (API/workers hold no local state; state is in
  Postgres/Kafka/Redis) — which is what makes horizontal scaling trivial.
- **Disposability** (fast startup, graceful shutdown; the launcher manages procs).

## Interview soundbites

- "Our singleton is a **memoized factory** (`lru_cache`) for the DB engine — lazy,
  so importing the package doesn't require a database."
- "`DepType` is a textbook **Strategy** — each dependency type plugs a different
  constraint formula into the same cascade loop."
- "At the architecture level the patterns that matter are **outbox, CQRS, saga,
  circuit breaker** — not the GoF catalogue."

## Likely follow-ups

- *"Show me a real strategy/factory in your code."* → `cascade.py` `_earliest_start`
  per `DepType`; `db.py` `get_async_engine`.
- *"Singleton is an anti-pattern though?"* → A global mutable singleton is; a lazy
  memoized factory of a connection pool is the right call (you want exactly one).
- *"Monolith or microservices for a greenfield?"* → Modular monolith first; split
  on demonstrated need.
