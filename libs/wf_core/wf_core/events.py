"""Event and command schemas that travel over Kafka.

These are the wire contracts between services. The outbox `payload` column
stores the serialized form of these.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class TaskScheduleChanged(BaseModel):
    """EVENT (fact, past tense): a task's dates moved. Triggers a cascade."""

    event_id: str
    task_id: str
    project_id: str  # = Kafka partition key (ordering per project)
    task_version: int  # optimistic concurrency
    new_planned_start: datetime
    new_planned_completion: datetime
    actor_id: str
    occurred_at: datetime


class TaskBatchCreate(BaseModel):
    """COMMAND (intent, imperative): create a batch of tasks + dependencies.

    Emitted by the API (or, later, the AI agent) and applied by the worker.
    """

    command_id: str  # idempotency key
    project_id: str
    tasks: list[dict]
    predecessors: list[dict]
    actor_id: str