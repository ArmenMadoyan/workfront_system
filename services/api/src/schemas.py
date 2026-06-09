"""API contract schemas (Task 2).

The single source of truth for the frontend<->backend wire format:
  * REST request/response bodies
  * WebSocket client messages and server event envelopes

Conventions: IDs are UUIDv7 strings, datetimes are ISO-8601 UTC, durations are
minutes (int), every task carries a `version` for optimistic concurrency.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# ============================================================================
# REST — requests
# ============================================================================
class ProjectCreate(BaseModel):
    name: str


class TaskCreate(BaseModel):
    name: str
    work_required_minutes: int = 0
    parent_id: uuid.UUID | None = None
    planned_start: datetime | None = None
    planned_completion: datetime | None = None


class TaskUpdate(BaseModel):
    """Non-schedule edits (name, status, ...). Schedule moves use TaskScheduleUpdate."""

    name: str | None = None
    status: str | None = None


class TaskScheduleUpdate(BaseModel):
    """A drag on the Gantt. Triggers the cascade. Guarded by If-Match: <version>."""

    planned_start: datetime
    planned_completion: datetime


class PredecessorCreate(BaseModel):
    predecessor_id: uuid.UUID
    successor_id: uuid.UUID
    type: Literal["FS", "SS", "FF", "SF"] = "FS"
    lag_minutes: int = 0


# ============================================================================
# REST — responses
# ============================================================================
class ProjectOut(BaseModel):
    id: uuid.UUID
    name: str
    status: str
    planned_start: datetime | None = None
    planned_completion: datetime | None = None
    percent_complete: int
    version: int


class TaskOut(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    parent_id: uuid.UUID | None = None
    name: str
    status: str
    planned_start: datetime | None = None
    planned_completion: datetime | None = None
    work_required_minutes: int
    percent_complete: int
    version: int


class PredecessorOut(BaseModel):
    id: uuid.UUID
    predecessor_id: uuid.UUID
    successor_id: uuid.UUID
    type: str
    lag_minutes: int


class GanttOut(BaseModel):
    """The initial Gantt load. `seq` is the WS cursor the client resumes from."""

    project_id: uuid.UUID
    seq: int
    tasks: list[TaskOut]
    dependencies: list[PredecessorOut]


# ============================================================================
# WebSocket — client -> server
# ============================================================================
class WSClientMessage(BaseModel):
    action: Literal["subscribe", "unsubscribe"]
    project_id: uuid.UUID


# ============================================================================
# WebSocket — server -> client
# ============================================================================
class ScheduleChangeOut(BaseModel):
    task_id: uuid.UUID
    planned_start: datetime
    planned_completion: datetime
    version: int


class CascadeData(BaseModel):
    origin_task_id: uuid.UUID
    changes: list[ScheduleChangeOut]


class WSEvent(BaseModel):
    """Every server->client push shares this envelope."""

    type: Literal[
        "schedule.cascade",
        "task.created",
        "task.updated",
        "task.deleted",
        "dependency.changed",
        "subscribed",
        "error",
    ]
    project_id: uuid.UUID
    seq: int = Field(description="monotonic per project — clients detect gaps and refetch")
    ts: datetime
    data: dict