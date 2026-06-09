"""worker (dev placeholder).

Real implementation: a Kafka consumer (ordered per project_id) that runs the
pure cascade.py algorithm and writes results back to Postgres. For the initial
local setup it exposes a health endpoint so the launcher can run it on its port.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="workfront-worker", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"service": "worker", "status": "stub", "transport": "kafka (todo)"}