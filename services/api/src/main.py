"""API gateway (async) — Task 2 contract.

REST for commands/queries; WebSocket for real-time Gantt pushes.

Cascade contract: PATCH /schedule updates the moved task synchronously (200 +
authoritative dates/version) and the downstream ripple arrives over WebSocket
as one batched `schedule.cascade` event.

DEV NOTE: the cascade is computed inline here using the pure worker algorithm so
the contract is demonstrable end-to-end without Kafka. In production the move
only writes the outbox row; the worker consumes it and the fan-out emits the
WebSocket event. The public contract is identical either way.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from services.worker.src import cascade as casc
from wf_core import models
from wf_core.config import settings
from wf_core.db import async_session_scope

log = logging.getLogger("api")

from .schemas import (
    GanttOut,
    PredecessorCreate,
    PredecessorOut,
    ProjectCreate,
    ProjectOut,
    ScheduleChangeOut,
    TaskCreate,
    TaskOut,
    TaskScheduleUpdate,
    TaskUpdate,
    WSClientMessage,
)
from .ws import manager


async def _redis_subscriber() -> None:
    """Subscribe to project:* and push cascade events to local WebSockets.

    Resilient: if Redis isn't up (pure local mode), we log and skip — the api
    still serves REST/WS, with the inline-cascade path covering the demo.
    """
    try:
        import redis.asyncio as aioredis
    except ImportError:
        log.warning("redis not installed; WS fan-out disabled")
        return
    try:
        r = aioredis.from_url(settings.redis_url)
        pubsub = r.pubsub()
        await pubsub.psubscribe("project:*")
        log.info("subscribed to redis project:* for WS fan-out")
        async for msg in pubsub.listen():
            if msg.get("type") != "pmessage":
                continue
            event = json.loads(msg["data"])
            await manager.push_local(str(event.get("project_id")), event)
    except Exception as exc:  # noqa: BLE001
        log.warning("redis fan-out unavailable: %s", exc)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_redis_subscriber())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="workfront-api", version="0.2.0", lifespan=lifespan)


# --- dependencies -------------------------------------------------------------
async def get_session() -> AsyncSession:
    async with async_session_scope() as s:
        yield s


async def current_customer_id(x_customer_id: uuid.UUID = Header(alias="X-Customer-Id")) -> uuid.UUID:
    """In production this comes from the auth token, not a header."""
    return x_customer_id


@app.get("/health")
async def health() -> dict:
    return {"service": "api", "status": "ok"}


# ============================================================================
# Customers (dev bootstrap; real systems create tenants out of band)
# ============================================================================
@app.post("/api/v1/customers")
async def create_customer(body: ProjectCreate, s: AsyncSession = Depends(get_session)) -> dict:
    c = models.Customer(name=body.name)
    s.add(c)
    await s.flush()
    return {"id": str(c.id), "name": c.name}


# ============================================================================
# Projects
# ============================================================================
@app.post("/api/v1/projects", response_model=ProjectOut, status_code=201)
async def create_project(
    body: ProjectCreate,
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> models.Project:
    p = models.Project(customer_id=customer_id, name=body.name)
    s.add(p)
    await s.flush()
    return p


@app.get("/api/v1/projects/{project_id}", response_model=ProjectOut)
async def get_project(
    project_id: uuid.UUID,
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> models.Project:
    p = await s.get(models.Project, project_id)
    if not p or p.customer_id != customer_id:
        raise HTTPException(404, "project not found")
    return p


@app.get("/api/v1/projects/{project_id}/gantt", response_model=GanttOut)
async def get_gantt(
    project_id: uuid.UUID,
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> GanttOut:
    p = await s.get(models.Project, project_id)
    if not p or p.customer_id != customer_id:
        raise HTTPException(404, "project not found")
    tasks = (await s.scalars(select(models.Task).where(models.Task.project_id == project_id))).all()
    task_ids = {t.id for t in tasks}
    deps = (
        await s.scalars(
            select(models.Predecessor).where(models.Predecessor.successor_id.in_(task_ids or {None}))
        )
    ).all()
    return GanttOut(
        project_id=project_id,
        seq=manager.current_seq(str(project_id)),
        tasks=[TaskOut.model_validate(t, from_attributes=True) for t in tasks],
        dependencies=[PredecessorOut.model_validate(d, from_attributes=True) for d in deps],
    )


# ============================================================================
# Tasks
# ============================================================================
@app.post("/api/v1/projects/{project_id}/tasks", response_model=TaskOut, status_code=201)
async def create_task(
    project_id: uuid.UUID,
    body: TaskCreate,
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> models.Task:
    p = await s.get(models.Project, project_id)
    if not p or p.customer_id != customer_id:
        raise HTTPException(404, "project not found")
    t = models.Task(
        customer_id=customer_id,
        project_id=project_id,
        name=body.name,
        work_required_minutes=body.work_required_minutes,
        parent_id=body.parent_id,
        planned_start=body.planned_start,
        planned_completion=body.planned_completion,
    )
    s.add(t)
    await s.flush()
    await manager.broadcast(
        str(project_id), "task.created", TaskOut.model_validate(t, from_attributes=True).model_dump(mode="json")
    )
    return t


@app.patch("/api/v1/tasks/{task_id}", response_model=TaskOut)
async def update_task(
    task_id: uuid.UUID,
    body: TaskUpdate,
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> models.Task:
    t = await _load_task(s, task_id, customer_id)
    if body.name is not None:
        t.name = body.name
    if body.status is not None:
        t.status = body.status
    t.version += 1
    await s.flush()
    await manager.broadcast(
        str(t.project_id),
        "task.updated",
        {"task_id": str(t.id), "fields": body.model_dump(exclude_none=True), "version": t.version},
    )
    return t


@app.patch("/api/v1/tasks/{task_id}/schedule", response_model=TaskOut)
async def reschedule_task(
    task_id: uuid.UUID,
    body: TaskScheduleUpdate,
    if_match: int | None = Header(default=None, alias="If-Match"),
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> models.Task:
    if if_match is None:
        raise HTTPException(428, "If-Match: <version> required for schedule moves")
    t = await _load_task(s, task_id, customer_id)
    if t.version != if_match:
        raise HTTPException(409, f"version conflict: have {t.version}, sent {if_match}")

    # 1. move the task (authoritative) + bump version
    t.planned_start = body.planned_start
    t.planned_completion = body.planned_completion
    t.version += 1

    # 2. write the outbox row (same tx) — the durable cascade trigger
    s.add(
        models.OutboxEvent(
            customer_id=customer_id,
            aggregate_type="task",
            aggregate_id=t.id,
            event_type="task.schedule.changed",
            partition_key=t.project_id,
            payload={
                "task_id": str(t.id),
                "planned_start": body.planned_start.isoformat(),
                "planned_completion": body.planned_completion.isoformat(),
                "version": t.version,
            },
        )
    )

    # 3. cascade. INLINE_CASCADE=true (local, no Kafka) computes it here and
    #    pushes over WS directly. In the full pipeline this is false: the worker
    #    consumes the outbox event and the ripple returns via Redis fan-out.
    if settings.inline_cascade:
        changes = await _run_cascade(s, t)
        await s.flush()
        await manager.broadcast(
            str(t.project_id),
            "schedule.cascade",
            {"origin_task_id": str(t.id), "changes": [c.model_dump(mode="json") for c in changes]},
        )
    return t


@app.delete("/api/v1/tasks/{task_id}", status_code=204)
async def delete_task(
    task_id: uuid.UUID,
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> None:
    t = await _load_task(s, task_id, customer_id)
    project_id = t.project_id
    await s.delete(t)
    await manager.broadcast(str(project_id), "task.deleted", {"task_id": str(task_id)})


# ============================================================================
# Dependencies
# ============================================================================
@app.post("/api/v1/predecessors", response_model=PredecessorOut, status_code=201)
async def add_predecessor(
    body: PredecessorCreate,
    customer_id: uuid.UUID = Depends(current_customer_id),
    s: AsyncSession = Depends(get_session),
) -> models.Predecessor:
    succ = await _load_task(s, body.successor_id, customer_id)
    edge = models.Predecessor(
        customer_id=customer_id,
        predecessor_id=body.predecessor_id,
        successor_id=body.successor_id,
        type=models.DepType(body.type),
        lag_minutes=body.lag_minutes,
    )
    s.add(edge)
    await s.flush()
    await manager.broadcast(
        str(succ.project_id),
        "dependency.changed",
        {"predecessor_id": str(body.predecessor_id), "successor_id": str(body.successor_id), "op": "added"},
    )
    return edge


# ============================================================================
# WebSocket
# ============================================================================
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await manager.connect(ws)
    try:
        while True:
            raw = await ws.receive_json()
            try:
                msg = WSClientMessage.model_validate(raw)
            except ValidationError as e:
                await ws.send_json({"type": "error", "data": {"detail": e.errors()}})
                continue
            pid = str(msg.project_id)
            if msg.action == "subscribe":
                manager.subscribe(pid, ws)
                await ws.send_json(
                    {
                        "type": "subscribed",
                        "project_id": pid,
                        "seq": manager.current_seq(pid),
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "data": {},
                    }
                )
            else:
                manager.unsubscribe(pid, ws)
    except WebSocketDisconnect:
        manager.disconnect(ws)


# --- helpers ------------------------------------------------------------------
async def _load_task(s: AsyncSession, task_id: uuid.UUID, customer_id: uuid.UUID) -> models.Task:
    t = await s.get(models.Task, task_id)
    if not t or t.customer_id != customer_id:
        raise HTTPException(404, "task not found")
    return t


_DEP = {m: casc.DepType(m.value) for m in models.DepType}


async def _run_cascade(s: AsyncSession, origin: models.Task) -> list[ScheduleChangeOut]:
    """Recompute downstream dates with the pure algorithm and persist them."""
    tasks = (
        await s.scalars(select(models.Task).where(models.Task.project_id == origin.project_id))
    ).all()
    by_id = {str(t.id): t for t in tasks}
    nodes = {
        tid: casc.TaskNode(task_id=tid, start=t.planned_start, finish=t.planned_completion)
        for tid, t in by_id.items()
        if t.planned_start and t.planned_completion
    }
    if str(origin.id) not in nodes:
        return []
    task_ids = set(by_id)
    edges_rows = (
        await s.scalars(
            select(models.Predecessor).where(models.Predecessor.successor_id.in_(task_ids))
        )
    ).all()
    edges = [
        casc.DepEdge(
            predecessor_id=str(e.predecessor_id),
            successor_id=str(e.successor_id),
            type=_DEP[e.type],
            lag=casc.timedelta(minutes=e.lag_minutes),
        )
        for e in edges_rows
        if str(e.predecessor_id) in nodes and str(e.successor_id) in nodes
    ]

    out: list[ScheduleChangeOut] = []
    for ch in casc.cascade(str(origin.id), nodes, edges):
        t = by_id[ch.task_id]
        t.planned_start = ch.new_start
        t.planned_completion = ch.new_finish
        t.version += 1
        out.append(
            ScheduleChangeOut(
                task_id=t.id,
                planned_start=ch.new_start,
                planned_completion=ch.new_finish,
                version=t.version,
            )
        )
    return out