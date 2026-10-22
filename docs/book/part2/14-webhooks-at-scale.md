# Chapter 14 — Webhooks at Scale

> The prompt explicitly mentions firing webhooks. This is the same eventing
> backbone pointed at *external* consumers — and external HTTP is the unreliable,
> adversarial part, so it needs extra care.

## How it slots into our design
We already have the hard part — a durable, ordered event log (`project.changed`,
etc.). Firing webhooks is **another consumer group**:

```
project.changed (Kafka)
   → webhook-dispatcher (consumer group)
       → look up subscriptions for (tenant, event_type)
       → POST to each subscriber URL with retries/backoff
       → record delivery attempt; non-2xx → retry queue → DLQ
```

No new infrastructure — webhooks are a **fan-out consumer** on the existing log.
That reuse is the headline answer.

## The hard parts of webhooks (external HTTP is hostile)
1. **Delivery reliability** — subscribers are down, slow, or flaky. Use
   **at-least-once with retries**: exponential backoff **+ jitter**, capped
   attempts, then a **DLQ** + alert. Never retry synchronously in the request path.
2. **Idempotency for the receiver** — at-least-once means duplicates. Send an
   **idempotency key / event ID** (and a monotonic `seq`) so receivers dedup.
   This is the *same* dedup-inbox idea (Ch. 4) pushed to the partner.
3. **Ordering** — partners often want per-resource order. Partition the dispatch
   by resource (e.g., `project_id`) just like internally; or include `seq` and let
   them reorder.
4. **Security** — **HMAC-sign** each payload (shared secret), include a timestamp
   to prevent replay, deliver over TLS, and support secret rotation. Optionally
   mTLS / allow-listed source IPs.
5. **Back-pressure & isolation** — one slow partner must not stall others: **per-
   subscriber concurrency limits / circuit breakers**, and isolate a failing
   endpoint (bulkhead) so it can't exhaust the dispatcher.
6. **Poison endpoints** — auto-disable a subscription after N consecutive failures;
   notify the owner; expose a redelivery API.

## At 10M events/day
~116/s of source events, fanned to *each* subscription → multiply by subscribers.
The log buffers bursts; the dispatcher scales horizontally (more consumers across
partitions). Slow endpoints are the real limiter, so **bounded per-endpoint
concurrency + async I/O** matter more than raw throughput. Outbound calls are
I/O-bound → an async dispatcher (like our async API) holds many in-flight POSTs
cheaply.

## Build vs buy
- **Build** on our Kafka log (full control, no per-event cost).
- **Buy / use a library**: **Svix** (managed webhooks-as-a-service — retries,
  signing, dashboards, partner-facing UI) or **Hookdeck**. For a product exposing
  webhooks to many external partners, buying the *delivery/observability* layer is
  often worth it; you still produce events from your own log.
- **Local dev**: **ngrok** to route a cloud webhook to your laptop; a dashboard
  (Svix/RequestBin) to inspect payloads and partner responses (200 vs 500).

## Interview soundbites

- "Webhooks are just **another consumer group on the existing event log** — the
  durable, ordered backbone is already built."
- "External HTTP is hostile, so: **at-least-once with backoff+jitter+DLQ,
  HMAC-signed payloads, idempotency keys, and per-subscriber circuit breakers** so
  one slow partner can't stall the rest."
- "I'd consider **Svix** to buy the delivery/observability layer while still
  producing events from my own Kafka log."

## Likely follow-ups

- *"A subscriber is down for an hour — then what?"* → Retries with backoff, then
  DLQ; auto-disable after N failures; redelivery API when they recover. The log
  retains events for replay.
- *"How does the partner avoid double-processing?"* → Idempotency key + `seq`; they
  dedup — same pattern as our internal dedup inbox.
- *"How do you prove you sent it / they got it?"* → Persist delivery attempts +
  response codes (the Svix/observability view); expose it to partners.
