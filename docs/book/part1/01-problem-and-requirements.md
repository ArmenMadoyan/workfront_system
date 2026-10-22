# Chapter 1 — Problem & Requirements

> The first five minutes of any system-design interview are about *scoping*, not
> architecture. Whoever defines the requirements controls the design.

## In our system

We built a Workfront-style **work-management** backend. The domain hierarchy:

```
Portfolio → Program → Project → Task (tree) / Issue
                          └── Predecessor (dependency edges: FS/SS/FF/SF + lag)
```

The hard, defining feature is the **dependency cascade**: when a task's dates
move, every downstream task (its transitive successors) must be rescheduled,
respecting dependency type and lag. One drag on a Gantt chart can ripple to
thousands of tasks. That single requirement is what forces almost every
architectural decision in this book.

## Dissecting "10M"

Adobe's prompt is usually phrased "handle 10M *something* per day." **Always
convert the headline number to a rate, then find the real stressor.**

- **10M requests/day** = 10,000,000 / 86,400 ≈ **116 requests/sec average**.
- Real traffic isn't flat; assume a **5–10× peak** → **~600–1,200/sec peak**.
- That is *not* a scary number. A single well-indexed Postgres + a few app
  instances handle it. Saying this out loud signals seniority: *"10M/day is
  ~116/s average — the interesting problem isn't raw QPS."*

The actual stressors hide behind the headline:

| Dimension | Why it hurts |
|---|---|
| **Fan-out** | One edit → 10k–50k dependent task updates. A *single* request becomes a huge unit of work. |
| **Data volume** | 10M tasks means cascade queries and indexes must stay fast at scale. |
| **Real-time** | The Gantt must update live for every connected user, so writes must propagate to browsers. |
| **Ordering & correctness** | Concurrent edits + cascades must not corrupt schedules. |

We **measured** these on one laptop: steady throughput peaked at ~582 schedule
moves/s (well past 116/s), but a **50,000-task cascade took 5.81s** — confirming
that fan-out, not QPS, is the design driver.

## Functional vs. non-functional requirements

**Functional** (what it does)
- CRUD on portfolios/programs/projects/tasks/issues.
- Add/remove dependency edges between tasks.
- Reschedule a task → cascade to dependents.
- Real-time Gantt updates for connected clients.
- (Later) fire webhooks to external partners on changes.

**Non-functional** (how well)
- **Scale**: 10M changes/day; projects up to tens of thousands of tasks.
- **Latency**: an interactive edit must feel instant (the *origin* task confirms
  in tens of ms; the ripple can be eventually consistent over ~1s).
- **Correctness**: no lost updates under concurrency; no double-applied cascades.
- **Availability**: a slow/large cascade for one project must not block others.
- **Multi-tenant**: it's SaaS — strict tenant isolation.
- **Observability**: trace one request across all hops.

## The capacity-estimation muscle (do this out loud)

> 10M writes/day ≈ 116/s avg, ~1k/s peak. Each write is a small row + an outbox
> row ≈ a few KB of WAL. ~1k/s × a few KB = single-digit MB/s of WAL — trivial
> for one Postgres. Storage: 10M tasks × ~1KB ≈ 10GB + indexes — also small.
> So the bottleneck is **not** storage or steady QPS; it's **cascade fan-out
> latency** and **keeping the read/real-time side consistent**. That reframing
> is the whole design.

## Alternatives & trade-offs (scoping choices)

- **Synchronous cascade** (compute the whole ripple inside the request) vs.
  **asynchronous** (confirm the origin, ripple later). We chose async — a 50k
  cascade can't block an HTTP request for 6s. Sync is simpler and fine *only* if
  fan-out is provably tiny.
- **Strong vs. eventual consistency** for the ripple. We chose: **origin task =
  strongly consistent** (read-your-write), **dependents = eventually consistent**
  (arrive via WebSocket in ~1s). This is the single most important
  consistency decision and it falls straight out of the latency requirement.

## Interview soundbites

- "First, 10M/day is ~116/s average — raw QPS isn't the hard part. The hard part
  is **fan-out**: one edit can reschedule 50,000 tasks."
- "I'll make the **edited task strongly consistent and its dependents eventually
  consistent** — that's what lets the API answer in 45ms while a huge cascade
  runs in the background."
- "This is multi-tenant SaaS, so tenancy is a *data-model* concern from row one,
  not an afterthought."

## Likely follow-ups

- *"What if a cascade touches 1M tasks?"* → It's still one ordered unit per
  project (Ch. 4–5); you bound it, chunk the emitted event (Ch. 4's real bug),
  and accept longer eventual-consistency for that project while others are
  unaffected.
- *"Where would you push back on the requirement?"* → On synchronous cascade and
  on exactly-once (Ch. 4) — both are usually misframed.
