# Chapter 6 — Real-time Fan-out

## In our system

The cascade's results must reach every browser watching that project — live. The
non-obvious problem: **the API runs many instances, and a Kafka consumer group
gives each partition to *one* instance, but a user's WebSocket might be connected
to a *different* instance.** "Who consumes the event" ≠ "who holds the socket."

Our solution (`services/api/src/ws.py`, `services/ws_fanout/`, API Redis lifespan):

```
project.changed (Kafka)
   → ws-fanout consumer group  (bridges Kafka → Redis)
       → PUBLISH project:{id}  (Redis pub/sub)
           → EVERY api instance is SUBSCRIBEd  → push to its local sockets
```

Redis pub/sub has **broadcast** semantics — every API instance receives every
`project:{id}` message and pushes to whichever local sockets are subscribed to
that project. That sidesteps the partition-vs-socket mismatch entirely. The
`ConnectionManager` holds per-project "rooms"; `push_local` sends a pre-formed
event (the `seq` came from the producer, not re-assigned).

`seq` here is the **Kafka offset** (monotonic per partition = per project, since
keyed), which feeds the gap-detection from Chapter 3.

### A real bug we hit
`redis.asyncio`'s `pubsub.listen()` **hangs/times out on redis 8.0**. We switched
to `get_message(timeout=...)` polling inside a **resilient reconnect loop** — a
single transient must not permanently disable fan-out. (First version caught the
exception once and returned, killing fan-out forever; the reconnect loop fixed it.)

## The general pattern

Decouple "consume the durable stream" from "deliver to connection holders":
- A small **bridge** consumes Kafka (durable, replayable) and republishes to a
  **broadcast bus** (Redis pub/sub) that all stateless front-ends subscribe to.
- Front-ends keep an **in-memory room registry** of local connections.
- A **cursor (`seq`)** lets clients detect missed messages and resync from the
  source of truth.

## Alternatives & trade-offs

| Approach | How | Pro | Con |
|---|---|---|---|
| **Redis pub/sub** (ours) | bridge Kafka→Redis, all instances subscribe | simple broadcast, no routing | every instance sees every event (fine at moderate scale); pub/sub is fire-and-forget |
| **Per-instance Kafka group** | each API instance is its own consumer group | no Redis | every instance reads *all* of Kafka; wasteful at scale |
| **Sticky routing** | hash sockets so a project's clients land on the partition's consumer | most efficient (no broadcast) | complex: consistent hashing, rebalancing, connection draining |
| **Dedicated push service** (e.g. Centrifugo, Ably, API GW WebSockets) | offload fan-out entirely | scales connections, less code | another system / vendor, cost |

At very high connection counts you'd move from Redis pub/sub to **Redis Streams**
(durable, consumer-tracked) or a managed push tier, and shard channels.

## Interview soundbites

- "The subtle bug is that **the instance consuming the partition isn't the one
  holding the socket** — so I bridge Kafka to **Redis pub/sub** and every API
  instance broadcasts to its local connections."
- "`seq` is the Kafka offset; clients use it for **gap detection** and refetch on
  a miss — fan-out is best-effort, the snapshot is the source of truth."
- "Fan-out must be **self-healing**: a reconnect loop, not a one-shot subscriber."

## Likely follow-ups

- *"Redis pub/sub drops messages if a subscriber is down — isn't that a problem?"*
  → Yes, it's fire-and-forget; correctness comes from `seq` gap-detection +
  refetch, and from Postgres being the source of truth. For stronger delivery use
  Redis Streams.
- *"How does this scale to 1M concurrent sockets?"* → Shard channels, move to a
  dedicated push tier, and stop broadcasting every event to every instance
  (sticky routing or topic sharding).
- *"Why not push straight from the worker?"* → The worker doesn't know which API
  instance holds the socket; the broadcast bus is exactly the decoupling.
