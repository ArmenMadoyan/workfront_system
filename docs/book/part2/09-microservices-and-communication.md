# Chapter 9 — Microservices & Communication

## In our system

Services and *why each is its own deployable*:
- **api** (async FastAPI) — request fan-in + WebSocket connections.
- **worker fleet** — CPU/IO-heavy cascade compute; scales independently of the API.
- **ws-fanout** — bridges Kafka → Redis for real-time delivery.
- **projector** — builds the read model (deferred stub).
- **graph_service** — gRPC subgraph provider over Neo4j (deferred stub).
- Shared code (models, db, events, telemetry) lives in a library `libs/wf_core`,
  *imported* by services — not a network call.

The split is driven by **independent scaling and failure isolation**: a cascade
spike scales `worker` only; the API stays responsive. That's the real reason to
split a service — not "microservices are modern."

## Communication styles (and where we use each)
- **Browser ↔ API: REST + WebSocket** — public, cacheable, ubiquitous tooling.
- **Service ↔ service, synchronous: gRPC** — worker → `graph_service`. Binary
  Protobuf, HTTP/2 multiplexing, schema-first contracts, low latency for
  high-volume internal calls.
- **Service ↔ service, asynchronous: Kafka** — the outbox stream. Decouples
  producers from consumers, buffers load, enables replay.

The rule of thumb: **synchronous (REST/gRPC) when the caller needs an answer
now; asynchronous (Kafka) when you want decoupling, buffering, and fan-out.**

## REST vs gRPC vs messaging

| | REST/JSON | gRPC/Protobuf | Messaging (Kafka) |
|---|---|---|---|
| Shape | request/response | request/response (+streaming) | publish/subscribe |
| Encoding | text JSON | binary, compact | any |
| Contract | OpenAPI (loose) | `.proto` (strict, codegen) | schema/registry |
| Best for | public APIs, browsers | internal high-volume RPC | decoupling, fan-out, replay |
| Weakness | verbose, no streaming ergonomics | not browser-native, binary to debug | eventual consistency, ordering care |

We use **all three**, each where it fits — and naming that explicitly is a strong
interview signal.

## Monolith vs microservices (have an opinion)
Start as a **modular monolith**; split out a service only when it has a *different
scaling profile* or *failure domain*. Premature microservices buy you distributed
transactions, network failures, and ops overhead before you have the scale to
justify them. Our split is justified precisely because the worker's load is
spiky and independent of the API's.

## Service discovery, contracts, resilience
- **Discovery**: DNS/service mesh in k8s (`graph-service:50051`); we use Compose
  DNS locally.
- **Contracts**: `.proto` files are the source of truth for gRPC; OpenAPI for REST.
- **Resilience**: timeouts, retries with backoff + jitter, circuit breakers,
  bulkheads. Async (Kafka) gives backpressure for free — the log buffers.

## Interview soundbites

- "I split services by **scaling profile and failure domain**, not by fashion —
  the worker scales on cascade load without touching the API."
- "**gRPC internally, REST externally, Kafka for decoupling** — three transports,
  each where it's strongest."
- "I'd start as a **modular monolith** and extract services when a piece earns it."

## Likely follow-ups

- *"Why gRPC for graph_service but REST for the API?"* → Internal, high-volume,
  schema-first, binary-fast vs. public, browser-native, cacheable.
- *"How do services find each other?"* → DNS/service mesh; contracts via proto/OpenAPI.
- *"Downside of microservices here?"* → Distributed failures and no cross-service
  ACID — which is exactly why we use the outbox/saga-style patterns (Ch. 10).
