"""WebSocket connection + subscription manager (project "rooms").

Server->client pushes share the WSEvent envelope and carry a per-project
monotonic `seq` so clients can detect gaps and refetch the Gantt.

NOTE (scaling): this is in-process. With multiple api replicas the `seq`
source and fan-out move to a shared bus (e.g. Redis pub/sub or the read-model
stream); the public WS contract stays identical.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from fastapi import WebSocket

from .schemas import WSEvent


class ConnectionManager:
    def __init__(self) -> None:
        self._rooms: dict[str, set[WebSocket]] = defaultdict(set)
        self._seq: dict[str, int] = defaultdict(int)

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()

    def subscribe(self, project_id: str, ws: WebSocket) -> None:
        self._rooms[project_id].add(ws)

    def unsubscribe(self, project_id: str, ws: WebSocket) -> None:
        self._rooms[project_id].discard(ws)

    def disconnect(self, ws: WebSocket) -> None:
        for subs in self._rooms.values():
            subs.discard(ws)

    def current_seq(self, project_id: str) -> int:
        return self._seq[project_id]

    def _next_seq(self, project_id: str) -> int:
        self._seq[project_id] += 1
        return self._seq[project_id]

    async def push_local(self, project_id: str, event: dict) -> None:
        """Send a fully-formed event (e.g. from Redis fan-out) to local sockets.

        Unlike `broadcast`, this does NOT assign a seq — the producer (ws-fanout)
        already set it from the Kafka offset.
        """
        dead: list[WebSocket] = []
        for ws in list(self._rooms.get(project_id, ())):
            try:
                await ws.send_json(event)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def broadcast(self, project_id: str, type_: str, data: dict) -> WSEvent:
        event = WSEvent(
            type=type_,
            project_id=project_id,
            seq=self._next_seq(project_id),
            ts=datetime.now(timezone.utc),
            data=data,
        )
        payload = event.model_dump(mode="json")
        dead: list[WebSocket] = []
        for ws in list(self._rooms.get(project_id, ())):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)
        return event


manager = ConnectionManager()