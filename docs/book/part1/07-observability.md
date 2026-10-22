# Chapter 7 — Observability (the God View)

> "If you debug a system this complex by reading raw terminal logs, you lose your
> mind in five minutes." A request bounces API → Postgres → Debezium → Kafka →
> worker → Redis → WebSocket. You need to *see* where it died.

## In our system

### Distributed tracing across the async boundary (the interesting part)
`libs/wf_core/wf_core/telemetry.py` sets up **OpenTelemetry** exporting to
**Jaeger** (OTLP). The hard problem: one logical request crosses a **message
queue**, so the trace must survive Kafka.

The mechanism is **W3C trace-context propagation**:
1. The API has an active span (auto-created by `FastAPIInstrumentor`).
2. Before writing the outbox row, it injects the `traceparent` into the payload:
   `payload["_trace"] = inject_trace({})`.
3. Debezium carries the payload through Kafka unchanged.
4. The worker calls `extract_context(payload["_trace"])` and starts
   `worker.cascade` **with that as parent**.

**Proven:** one trace ID spanned both services:
```
trace b6ba89…   (services: workfront-api, workfront-worker)
  workfront-api    → PATCH /api/v1/tasks/{id}/schedule
  workfront-worker → worker.cascade          ← resumed AFTER Kafka
```
So when a user says *"my date didn't update,"* you paste the trace ID into Jaeger
and see exactly which hop was slow or threw.

### Component UIs (jump in once the trace localizes the failure)
We run, in `docker-compose.yml`:
- **Jaeger** (`:16686`) — the waterfall / God View.
- **Kafka UI** (`:8080`) — browse `task.schedule.changed` JSON, consumer-group
  lag, and **Debezium connector health**.
- **RedisInsight** (`:5540`) — inspect keys, watch `project:*` pub/sub live.
- **Swagger** (`:8000/docs`) — the REST playground.

This literally found a real bug: the Debezium **connector status** showed `task:
FAILED` (the 8MB mega-cascade message) — invisible in app logs, obvious in the UI.

## The three pillars (name them)
- **Traces** — one request across services (OpenTelemetry → Jaeger/Tempo).
- **Metrics** — rates/latency/saturation; the killer metric here is **consumer
  lag** (are workers keeping up?) and **replication-slot lag** (is Debezium
  behind?). Export via OTel/Prometheus → Grafana/Datadog.
- **Logs** — structured, **correlated by trace ID** so logs join the trace.

## Alternatives & trade-offs

| Concern | We used | Alternatives | Note |
|---|---|---|---|
| Tracing backend | Jaeger (local) | Grafana Tempo, Datadog APM, Honeycomb | OTel is vendor-neutral — swap the exporter, not the code |
| Instrumentation | OpenTelemetry | vendor agents | OTel avoids lock-in; auto-instrumentation for FastAPI/SQLAlchemy |
| Metrics+dashboards | (would add) Prometheus+Grafana | Datadog, CloudWatch | Datadog = managed, all-in-one, pricey; Prom+Grafana = self-host |
| Kafka UI | provectus Kafka-UI | Redpanda Console, Confluent CP | feature parity for browsing/lag |

## Interview soundbites

- "I propagate the **W3C `traceparent` through the Kafka message** so the worker's
  span is a *child* of the API span — one trace ID for the whole pipeline,
  including the async hop."
- "The metric that matters most in an eventing system is **consumer lag** — it
  tells you the workers are falling behind before users notice."
- "OpenTelemetry keeps it **vendor-neutral**: Jaeger locally, swap the exporter
  for Datadog/Tempo in prod without touching app code."

## Likely follow-ups

- *"How does the trace cross Kafka if the worker is a different process?"* →
  Inject `traceparent` into the message payload; extract it and set it as the
  parent context — standard context propagation over a custom carrier.
- *"User says it didn't update — walk me through debugging."* → Trace ID in
  Jaeger → see which hop failed; if it's Kafka, Kafka-UI for lag/connector
  status; if fan-out, RedisInsight for the `project:*` publish. (This is the
  whole Chapter, and the question your prompt asked.)
- *"What do you alert on?"* → Consumer lag, slot lag, DLQ rate, p99 latency,
  error rate — SLO-based (Ch. 16).
