"""graph_service (dev placeholder).

Real implementation: a gRPC server over Neo4j answering GetAffectedSubgraph
(see proto/scheduling.proto). For the initial local setup it exposes a health
endpoint so the launcher can run it on its own port.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="workfront-graph-service", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"service": "graph_service", "status": "stub", "transport": "grpc (todo)"}