# Chapter 3 — The API Layer (REST + WebSocket, async)

## In our system

Two halves that share one vocabulary (`services/api/src/{main,schemas,ws}.py`,
spec in `docs/api-contract.md`):

- **REST** — the browser *asks* (create project, load Gantt, move a task).
- **WebSocket** — the backend *tells* (the downstream tasks a cascade moved).

The browser **never polls** for cascade results: it does the move over REST, gets
the authoritative answer for *that* task, and the ripple arrives over WS.

### The cascade response contract (the crux)
```
PATCH /api/v1/tasks/{id}/schedule    If-Match: <version>
  → 200 with the moved task's NEW dates + new version     (instant: "A is done")
  → 409 if the client's version is stale
... downstream tasks arrive later as ONE batched schedule.cascade WS event
```
So the **edited task is synchronous truth; its dependents are async deltas.** This
is the consistency split from Chapter 1, expressed as an API contract.

### `seq`: the cursor that ties REST and WS together
Every WS envelope carries a per-project monotonic `seq`. `GET /gantt` returns the
current `seq`; the client ignores lower-seq events and **refetches if it sees a
gap**. Our test proved it: a client that connected late got `subscribed seq 3`
(it had missed 3 events) — exactly the signal to resync. This is how a
real-time UI stays correct over an imperfect connection.

### Why the API is async
FastAPI gives two execution models:
- **`def` (sync)** handlers run in a **threadpool** — one thread parked per
  in-flight request while it waits on the DB.
- **`async def`** handlers run on the **event loop** — thousands of in-flight
  awaits per process, no thread each.

We chose **async** because the API is I/O-bound (it mostly waits on Postgres) and
— the clincher — **it also holds the WebSocket connections**, which is precisely
an event-loop job. psycopg 3 does async, so no new driver; `wf_core/db.py` exposes
both a sync engine (worker/migrations) and an async engine (API). The hard rule:
inside `async def` you must never make a blocking call, or you stall the whole loop.

## The general pattern

- **Command/query over REST, server push over WebSocket** for collaborative UIs.
- **Optimistic-concurrency on writes** via `If-Match`/`ETag`-style version headers.
- **Cursor + gap-detection** to reconcile a snapshot (REST) with a live stream (WS).
- **Async runtime end-to-end** when the service holds many long-lived connections.

## Alternatives & trade-offs

| Choice | We picked | Alternative | When the alternative wins |
|---|---|---|---|
| Protocol | REST + WebSocket | GraphQL + subscriptions | Many heterogeneous clients needing flexible field selection; willing to own subscription scaling |
| Protocol | REST + WebSocket | REST + SSE | One-way push only, want auto-reconnect & simplicity (no client→server channel) |
| Runtime | async (event loop) | sync (threadpool) | CPU-bound endpoints, simple code, no long-lived connections |
| Cascade response | 200 now + WS ripple | 202, everything via WS | Purer CQRS, but the UI has nothing to confirm synchronously |
| Cascade response | 200 now + WS ripple | 200 with full cascade inline | Tiny, bounded fan-out only |

**GraphQL note for the room:** GraphQL shines for read flexibility and many
client shapes, but subscriptions are operationally heavier and you still need the
same eventing backend underneath. We used REST because our write surface is small
and the real complexity is the async pipeline, not query flexibility. FastAPI's
`/docs` (Swagger) is our interactive playground — the GraphiQL equivalent.

## Interview soundbites

- "The edited task returns **200 in ~45ms**; its dependents stream back over a
  **single batched WebSocket event** — that's the consistency split as a contract."
- "I made the API **async** primarily because it owns the WebSocket fleet; with
  psycopg 3 the same process does async REST and async DB with no extra driver."
- "A monotonic **`seq` per project** lets a reconnecting client detect a gap and
  refetch — real-time correctness without a perfect socket."

## Likely follow-ups

- *"sync `def` in FastAPI isn't single-threaded though?"* → Correct — it's a
  threadpool; the real axis is thread-per-request vs. event-loop concurrency, and
  WebSockets force the event loop.
- *"How does the client avoid showing stale data after reconnect?"* → `seq` gap
  detection → refetch `/gantt`.
- *"Why not block and return the whole cascade?"* → A 50k cascade is 5.8s; you
  can't hold an HTTP request that long, and it would serialize unrelated load.
