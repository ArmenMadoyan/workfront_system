"""Pure constraint-propagation algorithm — the heart of the cascade engine.

No I/O, no framework, no infra. Given the changed (root) task with its NEW
dates already applied, plus the affected subgraph, it computes the new dates
for every downstream task. This is the piece you unit-test exhaustively.

Dependency semantics (lag may be negative):
    FS  successor.start  >= predecessor.finish + lag
    SS  successor.start  >= predecessor.start  + lag
    FF  successor.finish >= predecessor.finish + lag
    SF  successor.finish >= predecessor.start  + lag

Task duration (finish - start) is preserved; the task slides as a whole.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


class DepType(str, Enum):
    FS = "FS"
    SS = "SS"
    FF = "FF"
    SF = "SF"


@dataclass
class TaskNode:
    task_id: str
    start: datetime
    finish: datetime

    @property
    def duration(self) -> timedelta:
        return self.finish - self.start


@dataclass(frozen=True)
class DepEdge:
    predecessor_id: str
    successor_id: str
    type: DepType
    lag: timedelta = timedelta()


@dataclass(frozen=True)
class ScheduleChange:
    task_id: str
    new_start: datetime
    new_finish: datetime


class DependencyCycleError(ValueError):
    """Raised when the affected subgraph contains a cycle (non-schedulable)."""


def cascade(
    root_id: str,
    nodes: dict[str, TaskNode],
    edges: list[DepEdge],
) -> list[ScheduleChange]:
    """Recompute downstream task dates after `root_id` moved.

    `nodes` must already contain `root_id` with its NEW dates. Returns only the
    tasks whose dates actually changed (root excluded), in topological order.
    """
    successors: dict[str, list[DepEdge]] = defaultdict(list)
    predecessors: dict[str, list[DepEdge]] = defaultdict(list)
    for e in edges:
        successors[e.predecessor_id].append(e)
        predecessors[e.successor_id].append(e)

    # 1. Affected set = every transitive successor of the root.
    affected: set[str] = set()
    stack = [root_id]
    while stack:
        cur = stack.pop()
        for e in successors[cur]:
            if e.successor_id not in affected:
                affected.add(e.successor_id)
                stack.append(e.successor_id)

    # 2. Topological order over the affected nodes (Kahn), edges within `affected`.
    indegree: dict[str, int] = {n: 0 for n in affected}
    for e in edges:
        if e.predecessor_id in affected and e.successor_id in affected:
            indegree[e.successor_id] += 1
    queue = deque(n for n, d in indegree.items() if d == 0)
    topo: list[str] = []
    while queue:
        n = queue.popleft()
        topo.append(n)
        for e in successors[n]:
            if e.successor_id in affected:
                indegree[e.successor_id] -= 1
                if indegree[e.successor_id] == 0:
                    queue.append(e.successor_id)
    if len(topo) != len(affected):
        raise DependencyCycleError("cycle detected in affected subgraph")

    # 3. Recompute each affected task from ALL its predecessors (updated in place).
    changes: list[ScheduleChange] = []
    for tid in topo:
        node = nodes[tid]
        new_start = _earliest_start(node, predecessors[tid], nodes)
        if new_start is None or new_start == node.start:
            continue
        node.start, node.finish = new_start, new_start + node.duration
        changes.append(ScheduleChange(tid, node.start, node.finish))
    return changes


def _earliest_start(
    node: TaskNode,
    preds: list[DepEdge],
    nodes: dict[str, TaskNode],
) -> datetime | None:
    """Binding start = max over all predecessor constraints. None if unconstrained."""
    candidates: list[datetime] = []
    for e in preds:
        p = nodes[e.predecessor_id]
        if e.type is DepType.FS:
            candidates.append(p.finish + e.lag)
        elif e.type is DepType.SS:
            candidates.append(p.start + e.lag)
        elif e.type is DepType.FF:
            candidates.append(p.finish + e.lag - node.duration)
        elif e.type is DepType.SF:
            candidates.append(p.start + e.lag - node.duration)
    return max(candidates) if candidates else None