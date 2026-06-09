"""ws-fanout — bridges `project.changed` (Kafka) -> Redis pub/sub.

Solves "the api instance consuming the partition isn't the one holding the
socket": this single consumer group reads the durable Kafka stream and
broadcasts each event to Redis channel `project:{id}`. Every api instance
SUBSCRIBEs and pushes to its locally-held WebSockets.

`seq` = Kafka offset (monotonic per partition = per project, since keyed by
project_id) — this is the cursor clients use for gap detection.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import redis
from confluent_kafka import Consumer

from wf_core.config import settings

log = logging.getLogger("ws-fanout")
TOPIC = settings.topic_project_changed


def run() -> None:
    r = redis.Redis.from_url(settings.redis_url)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_brokers,
            "group.id": "ws-fanout",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([TOPIC])
    log.info("ws-fanout bridging %s -> redis", TOPIC)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue
            project_id = msg.key().decode() if msg.key() else None
            data = json.loads(msg.value())
            event = {
                "type": "schedule.cascade",
                "project_id": project_id,
                "seq": msg.offset(),
                "ts": datetime.now(timezone.utc).isoformat(),
                "data": data,
            }
            r.publish(f"project:{project_id}", json.dumps(event))
            consumer.commit(msg)
            log.info("fanned out project %s seq %s", project_id, msg.offset())
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run()
