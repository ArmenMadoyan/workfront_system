# API Contract (Task 2)

Frontend ↔ backend communication for the Workfront core.

- **REST** — the browser asks the backend to do/fetch something (sync request/response).
- **WebSocket** — the backend tells the browser what changed (server→client push).
- The `api` gateway is **async** (FastAPI async + async SQLAlchemy over psycopg 3).

## Conventions

| Thing | Rule |
|---|---|
| IDs | UUIDv7 strings |
| Datetimes | ISO-8601 UTC (`...Z`) |
| Durations | minutes (int) |
| Tenant | `customer_id` from auth token (dev: `X-Customer-Id` header) |
| Concurrency | `version` per task; schedule moves require `If-Match: <version>` |
| Errors | standard HTTP status + JSON body |

## REST endpoints (`/api/v1`)

| Method | Path | Body | Notes |
|---|---|---|---|
| POST | `/customers` | `{name}` | dev bootstrap |
| POST | `/projects` | `{name}` | → `ProjectOut` (201) |
| GET | `/projects/{id}` | — | → `ProjectOut` |
| GET | `/projects/{id}/gantt` | — | → `GanttOut` (tasks + deps + `seq`) |
| POST | `/projects/{id}/tasks` | `TaskCreate` | → `TaskOut` (201) |
| PATCH | `/tasks/{id}` | `TaskUpdate` | name/status edits |
| PATCH | `/tasks/{id}/schedule` | `TaskScheduleUpdate` | **cascade trigger**; `If-Match` required |
| DELETE | `/tasks/{id}` | — | 204 |
| POST | `/predecessors` | `PredecessorCreate` | add dependency edge |

### The cascade flow

```
1. UI optimistically moves task A.
2. PATCH /tasks/A/schedule {planned_start, planned_completion}   If-Match: <version>
3. 200 → A's authoritative new dates + new version            (A is "done")
   409 → version conflict; UI refetches
4. downstream tasks pushed over WebSocket as ONE schedule.cascade event
```

## WebSocket (`/ws`)

### client → server
```jsonc
{ "action": "subscribe",   "project_id": "019e…" }
{ "action": "unsubscribe", "project_id": "019e…" }
```

### server → client (shared envelope)
```jsonc
{ "type": "...", "project_id": "019e…", "seq": 10472, "ts": "2026-…Z", "data": { } }
```

`seq` is **monotonic per project**. `GET /gantt` returns the current `seq`; the
client ignores lower-seq events and **refetches the Gantt if it sees a gap**.

### event types

| type | data |
|---|---|
| `subscribed` | `{}` (ack; `seq` is the resume cursor) |
| `schedule.cascade` | `{origin_task_id, changes:[{task_id, planned_start, planned_completion, version}]}` |
| `task.created` | full `TaskOut` |
| `task.updated` | `{task_id, fields, version}` |
| `task.deleted` | `{task_id}` |
| `dependency.changed` | `{predecessor_id, successor_id, op}` |
| `error` | `{detail}` |

## Scaling note

The WS fan-out and `seq` source are in-process today. With multiple `api`
replicas they move to a shared bus (Redis pub/sub or the read-model stream) and
the cascade producer becomes the `worker` (off Kafka) instead of the inline dev
path. **The wire contract above does not change.**