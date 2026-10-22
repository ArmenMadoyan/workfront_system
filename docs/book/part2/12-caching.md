# Chapter 12 — Caching

## In our system
We use **Redis** today for **pub/sub fan-out** (Ch. 6), and the design uses
**OpenSearch as a CQRS read model** (a *cache* of denormalized, query-optimized
documents — deferred stub). Plus a Postgres-level cache: the **denormalized
rollup** `project.percent_complete` is effectively a materialized cache of an
aggregate. So "caching" here spans three layers, which is the nuanced answer.

## The caching layers (name the hierarchy)
1. **Client / CDN** — static assets, cacheable GETs (`Cache-Control`, ETags). Our
   `If-Match`/version is the same family (conditional requests).
2. **Application cache (Redis)** — hot keys, sessions, rate-limit counters,
   pub/sub. Sub-ms reads, offloads the DB.
3. **Read model / materialized views (CQRS)** — OpenSearch documents or
   denormalized Postgres columns; not a TTL cache but a *purpose-built read copy*.
4. **DB buffer cache** — Postgres `shared_buffers` (we bumped to 1GB in perf
   testing) caches hot pages in RAM.

## Caching strategies
- **Cache-aside (lazy)**: app checks cache, on miss reads DB and populates. Most
  common; risk = thundering herd on a cold key (mitigate: request coalescing /
  locks).
- **Read-through / write-through**: the cache layer owns DB access; writes update
  cache + DB synchronously (consistent, slower writes).
- **Write-behind**: write to cache, flush to DB async (fast, risk of loss).
- **Refresh-ahead**: proactively refresh hot keys before expiry.

Our **CQRS read model is write-through-ish via the event stream**: a write commits
to Postgres, and the `project.changed` event updates the read model — eventual
consistency, not synchronous.

## Invalidation (the hard problem)
> "There are only two hard things in CS: cache invalidation and naming things."
- **TTL** — simplest; bounded staleness.
- **Event-driven invalidation** — our model: the same `project.changed` events that
  drive fan-out also invalidate/update the read model. This is the clean way: the
  event stream is the single source of "what changed."
- **Versioned keys** — include the entity `version` in the cache key so a stale
  read is naturally bypassed.

## What we deliberately do *not* cache
The **write path / source of truth** (Postgres). Caching writes is where
corruption lives. We cache *reads* and *fan-out*, never the authoritative state.

## Alternatives & trade-offs
| Need | Tool | Note |
|---|---|---|
| Pub/sub, hot keys, counters | **Redis** (ours) | in-memory, sub-ms; persistence optional |
| Durable streams w/ consumer tracking | Redis **Streams** | when pub/sub's fire-and-forget isn't enough |
| Dashboard/search reads | **OpenSearch** read model | denormalized, full-text, aggregations |
| Edge/static | CDN | for assets and cacheable GETs |

## Interview soundbites

- "Caching here is **three layers**: Redis for fan-out/hot keys, an OpenSearch
  **CQRS read model**, and denormalized rollups in Postgres."
- "I invalidate the read model **off the same event stream** that drives real-time
  fan-out — one source of truth for 'what changed.'"
- "I never cache the **write path**; only reads and fan-out. The DB stays
  authoritative."

## Likely follow-ups

- *"Cache-aside vs write-through?"* → Aside for general hot reads; write-through
  (event-driven) for the read model where consistency matters more.
- *"How do you avoid stale Gantt data?"* → Event-driven read-model updates +
  `seq` gap-detection + version in the response.
- *"Thundering herd on a cold popular key?"* → Request coalescing / a short lock /
  refresh-ahead.
