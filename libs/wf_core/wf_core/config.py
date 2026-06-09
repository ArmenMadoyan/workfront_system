"""Environment-driven configuration shared by every service."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://admin:supersecretpassword@postgres-db:5432/workfront_core",
    )
    kafka_brokers: str = os.getenv("KAFKA_BROKERS", "kafka:9092")
    graph_service_addr: str = os.getenv("GRAPH_SERVICE_ADDR", "graph-service:50051")
    graph_db_url: str = os.getenv("GRAPH_DB_URL", "bolt://graph-db:7687")
    graph_db_user: str = os.getenv("GRAPH_DB_USER", "neo4j")
    graph_db_password: str = os.getenv("GRAPH_DB_PASSWORD", "supersecretpassword")
    opensearch_url: str = os.getenv("OPENSEARCH_URL", "http://search-index:9200")
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379")

    # behaviour flags
    inline_cascade: bool = os.getenv("INLINE_CASCADE", "true").lower() == "true"

    # Kafka topics
    topic_schedule_changed: str = os.getenv("TOPIC_SCHEDULE_CHANGED", "task.schedule.changed")
    topic_project_changed: str = os.getenv("TOPIC_PROJECT_CHANGED", "project.changed")


settings = Settings()