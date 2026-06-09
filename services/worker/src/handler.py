"""Worker core: process one `task.schedule.changed` event.

Transport-free so it can be unit-tested against Postgres directly. The Kafka
consumer (consumer.py) is a thin loop around this. Everything here runs inside a
single Postgres transaction (the caller's session_scope):

  dedup → load subgraph → cascade → version-guarded writes
        → dedup marker → outbox(project.changed)

DEV NOTE: the subgraph is loaded straight from Postgres. In production this
becomes a gRPC call to graph_service (Neo4j). The cascade math is identical.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from wf_core import models

from . import cascade as casc

WORKER_GROUP = "worker-fleet"
_DEP = {m: casc.DepType(m.value) for m in models.DepType}


def already_processed(s: Session, group: str, event_id: uuid.UUID) -> bool:
    return s.get(models.ProcessedEvent, (group, event_id)) is not None


def process_schedule_changed(
    s: Session,
    event_id: uuid.UUID,
    task_id: uuid.UUID,
    group: str = WORKER_GROUP,
) -> list[dict]:
    """Cascade downstream dates after `task_id` moved. Idempotent + dedup-guarded.

    The origin task's own new dates were already committed by the API; this only
    recomputes its dependents. Returns the list of changed-task payloads (also
    emitted as a project.changed outbox row).
    """
    if already_processed(s, group, event_id):
        return []  # redelivery — skip

    origin = s.get(models.Task, task_id)
    if origin is None:
        s.add(models.ProcessedEvent(consumer_group=group, event_id=event_id))
        return []

    tasks = s.scalars(
        select(models.Task).where(models.Task.project_id == origin.project_id)
    ).all()
    by_id = {str(t.id): t for t in tasks}
    nodes = {
        tid: casc.TaskNode(task_id=tid, start=t.planned_start, finish=t.planned_completion)
        for tid, t in by_id.items()
        if t.planned_start and t.planned_completion
    }

    changes: list[dict] = []
    if str(origin.id) in nodes:
        ids = set(by_id)
        edge_rows = s.scalars(
            select(models.Predecessor).where(models.Predecessor.successor_id.in_(ids))
        ).all()
        edges = [
            casc.DepEdge(
                predecessor_id=str(e.predecessor_id),
                successor_id=str(e.successor_id),
                type=_DEP[e.type],
                lag=timedelta(minutes=e.lag_minutes),
            )
            for e in edge_rows
            if str(e.predecessor_id) in nodes and str(e.successor_id) in nodes
        ]
        for ch in casc.cascade(str(origin.id), nodes, edges):
            t = by_id[ch.task_id]
            t.planned_start = ch.new_start
            t.planned_completion = ch.new_finish
            t.version += 1
            changes.append(
                {
                    "task_id": ch.task_id,
                    "planned_start": ch.new_start.isoformat(),
                    "planned_completion": ch.new_finish.isoformat(),
                    "version": t.version,
                }
            )

    # dedup marker + downstream event — same tx as the writes above
    s.add(models.ProcessedEvent(consumer_group=group, event_id=event_id))
    if changes:
        s.add(
            models.OutboxEvent(
                customer_id=origin.customer_id,
                aggregate_type="project",
                aggregate_id=origin.project_id,
                event_type="project.changed",
                partition_key=origin.project_id,
                payload={"origin_task_id": str(origin.id), "changes": changes},
            )
        )
    return changes
