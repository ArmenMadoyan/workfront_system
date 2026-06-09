"""Task 3 — worker cascade handler against real Postgres (no Kafka needed).

Run:  PYTHONPATH=.:libs/wf_core DATABASE_URL=... python tests/test_worker_handler.py
Needs: Postgres only.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import select

from services.worker.src.handler import WORKER_GROUP, already_processed, process_schedule_changed
from wf_core import models
from wf_core.db import session_scope

D = lambda s: datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def main() -> None:
    with session_scope() as s:
        c = models.Customer(name="WT")
        s.add(c)
        s.flush()
        p = models.Project(customer_id=c.id, name="P")
        s.add(p)
        s.flush()
        # A already "moved" to 2026-07-05 (the API committed this); B is downstream
        A = models.Task(customer_id=c.id, project_id=p.id, name="A",
                        planned_start=D("2026-07-05T09:00:00"), planned_completion=D("2026-07-05T17:00:00"))
        B = models.Task(customer_id=c.id, project_id=p.id, name="B",
                        planned_start=D("2026-07-01T17:00:00"), planned_completion=D("2026-07-02T01:00:00"))
        s.add_all([A, B])
        s.flush()
        s.add(models.Predecessor(customer_id=c.id, predecessor_id=A.id, successor_id=B.id, type=models.DepType.FS))
        s.flush()
        cid, aid, bid, pid = c.id, A.id, B.id, p.id

    evt = uuid.uuid4()
    try:
        with session_scope() as s:
            changes = process_schedule_changed(s, evt, aid)
            print("cascade changes:", changes)
            assert len(changes) == 1 and changes[0]["task_id"] == str(bid)
            assert changes[0]["planned_start"] == "2026-07-05T17:00:00+00:00", changes[0]["planned_start"]

        with session_scope() as s:
            b = s.get(models.Task, bid)
            print("B persisted:", b.planned_start.isoformat(), "ver", b.version)
            assert b.planned_start == D("2026-07-05T17:00:00") and b.version == 2
            ob = s.scalars(
                select(models.OutboxEvent).where(
                    models.OutboxEvent.partition_key == pid,
                    models.OutboxEvent.event_type == "project.changed",
                )
            ).all()
            print("project.changed outbox rows (this project):", len(ob))
            assert len(ob) == 1
            assert already_processed(s, WORKER_GROUP, evt), "dedup row missing"

        with session_scope() as s:
            again = process_schedule_changed(s, evt, aid)
            print("reprocess (dedup):", again)
            assert again == []

        print("\nWORKER HANDLER PASSED ✅ (cascade + version bump + outbox + dedup)")
    finally:
        with session_scope() as s:
            s.query(models.Customer).filter_by(id=cid).delete()
            s.query(models.ProcessedEvent).filter_by(event_id=evt).delete()


if __name__ == "__main__":
    main()
