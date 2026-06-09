"""Worker Kafka consumer — thin transport around handler.process_schedule_changed.

Reads `task.schedule.changed` (one ordered partition per project), runs the
cascade in one Postgres tx, commits the offset only on success, DLQs poison
messages. Exactly-once-in-practice = idempotent producer + dedup inbox (handler)
+ version-guarded writes.

Debezium outbox event-router contract:
  * message key   = project_id   (partition_key column)
  * header "id"    = outbox row id  -> our dedup event_id
  * value (JSON)   = the outbox payload  (contains task_id, ...)
"""

from __future__ import annotations

import json
import logging
import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition
from sqlalchemy.orm.exc import StaleDataError

from wf_core.config import settings
from wf_core.db import session_scope

from .handler import process_schedule_changed

log = logging.getLogger("worker")

TOPIC = settings.topic_schedule_changed
DLQ = f"{TOPIC}.dlq"


def _consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_brokers,
            "group.id": "worker-fleet",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )


def _producer() -> Producer:
    return Producer(
        {"bootstrap.servers": settings.kafka_brokers, "enable.idempotence": True, "acks": "all"}
    )


def _header(msg, key: str) -> str | None:
    for k, v in msg.headers() or []:
        if k == key:
            return v.decode() if isinstance(v, (bytes, bytearray)) else v
    return None


def _parse(msg) -> tuple[uuid.UUID, uuid.UUID]:
    payload = json.loads(msg.value())
    event_id = uuid.UUID(_header(msg, "id") or payload["event_id"])  # outbox row id
    task_id = uuid.UUID(payload["task_id"])
    return event_id, task_id


def run() -> None:
    consumer = _consumer()
    consumer.subscribe([TOPIC])
    dlq = _producer()
    log.info("worker-fleet consuming %s", TOPIC)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue
            try:
                event_id, task_id = _parse(msg)
                with session_scope() as s:
                    changes = process_schedule_changed(s, event_id, task_id)
                consumer.commit(msg)
                log.info("processed event %s -> %d downstream changes", event_id, len(changes))
            except StaleDataError:
                # a human edited a task mid-cascade and won. Don't commit the
                # offset; seek back so the next poll recomputes against fresh state.
                log.warning("version conflict for key %s; re-delivering for recompute", msg.key())
                consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                time.sleep(0.2)
            except Exception:
                log.exception("processing failed; routing to %s", DLQ)
                dlq.produce(DLQ, key=msg.key(), value=msg.value(), headers=msg.headers())
                dlq.flush()
                consumer.commit(msg)  # advance past the poison message
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run()
