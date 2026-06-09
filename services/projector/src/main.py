"""projector (dev placeholder).

Real implementation: consumes Kafka and maintains the OpenSearch read model
(CQRS). For the initial local setup it exposes a health endpoint.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="workfront-projector", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"service": "projector", "status": "stub", "transport": "kafka->opensearch (todo)"}