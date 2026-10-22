# Chapter 16 — CI/CD & Operations

## In our system
We have the building blocks ops needs: **Alembic migrations** (`0001`, `0002`) for
versioned schema, **tests** (`tests/` — contract, concurrency, worker, e2e) and a
**perf harness** (`tests/perf/`) for load regression, **OpenTelemetry/Jaeger** for
tracing, and **health endpoints** on every service. A pipeline wires these
together.

## A CI/CD pipeline for this system
**CI (on every PR):**
1. Lint/format/type-check.
2. **Unit tests** — the pure `cascade.py` (no infra) runs in milliseconds.
3. **Integration tests** — spin Postgres (+ Kafka) in the CI runner (Compose /
   Testcontainers); run `tests/test_worker_handler.py`, `test_concurrency.py`.
4. Build container images per service; tag by git SHA.
5. (Periodically) run the **perf harness** as a regression gate on throughput.

**CD (on merge to main):**
1. Push images to a registry.
2. **Run DB migrations** as a gated step (`alembic upgrade head`) *before* rolling
   the new code — using **expand/contract** so old and new code both work against
   the intermediate schema.
3. **Progressive rollout**: rolling update → **canary** (small % of traffic) →
   watch SLOs → promote or auto-rollback.

## Zero-downtime schema changes (expand/contract)
Never make a breaking change in one step:
- **Expand**: add the new column/table (nullable/defaulted), deploy code that
  writes both old and new.
- **Migrate**: backfill.
- **Contract**: switch reads to new, then drop the old — in a *later* deploy.

Our additive migrations (`0002` just adds `processed_event`) are expand-safe. The
`version_id_col` addition needed no schema change at all (the column existed).

## Observability-driven operations (SLOs)
- **SLIs**: API p99 latency, error rate, **consumer lag**, **replication-slot
  lag**, DLQ rate, cascade end-to-end latency.
- **SLOs** + error budgets drive alerting; page on lag/slot-lag/DLQ growth before
  users feel it.
- **Dashboards**: Datadog or Grafana over the OTel metrics; traces in Jaeger/Tempo;
  Kafka-UI/RedisInsight for component drill-down (Ch. 7).

## Deployment strategies (know the trade-offs)
| Strategy | What | When |
|---|---|---|
| Rolling | replace pods gradually | default for stateless services |
| Blue-green | full parallel env, switch traffic | instant rollback, 2× resources |
| Canary | small % first, watch metrics, promote | risk-averse, needs good SLIs |

## Operational runbooks (the stuff that wins senior interviews)
- **Worker lag climbing** → scale workers (KEDA), check for a poison message / a
  mega-cascade hogging a partition.
- **Debezium connector FAILED** → check connector status (we *did* this — the 8MB
  message), fix payload size, restart task; reset slot only as last resort.
- **DLQ filling** → inspect poison messages, fix + replay.
- **Replication slot lag growing** → Debezium down/slow → WAL piles up → disk
  risk; alert early.

## Interview soundbites

- "Migrations run as a **gated CD step with expand/contract** so deploys are
  zero-downtime; my additive migrations are already expand-safe."
- "I alert on **consumer lag, slot lag, and DLQ rate** — leading indicators, not
  just CPU."
- "The pure cascade function gives **fast unit tests**; integration tests use
  Testcontainers; the perf harness is a **throughput regression gate**."

## Likely follow-ups

- *"How do you roll back a bad deploy?"* → Canary catches it on SLOs → auto-
  rollback; blue-green for instant switch; migrations are backward-compatible so
  code rollback is safe.
- *"How do you test the cascade without Kafka?"* → It's a pure function + a
  handler tested against Postgres directly (we did exactly this).
- *"What pages you at 3am?"* → Growing consumer/slot lag or DLQ — the eventing
  system falling behind.
