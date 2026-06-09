"""API gateway (dev) — FastAPI over the local Postgres truth.

Real endpoints backed by wf_core models. This is the only fully-wired service
for the initial setup; the others are health stubs until their transports
(gRPC, Kafka) land.
"""

from __future__ import annotations

import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import select

from wf_core import models
from wf_core.db import session_scope

app = FastAPI(title="workfront-api", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"service": "api", "status": "ok"}


# --- customers ----------------------------------------------------------------
class CustomerIn(BaseModel):
    name: str


@app.post("/customers")
def create_customer(body: CustomerIn) -> dict:
    with session_scope() as s:
        c = models.Customer(name=body.name)
        s.add(c)
        s.flush()
        return {"id": str(c.id), "name": c.name}


@app.get("/customers")
def list_customers() -> list[dict]:
    with session_scope() as s:
        rows = s.scalars(select(models.Customer)).all()
        return [{"id": str(c.id), "name": c.name} for c in rows]


# --- projects -----------------------------------------------------------------
class ProjectIn(BaseModel):
    customer_id: uuid.UUID
    name: str


@app.post("/projects")
def create_project(body: ProjectIn) -> dict:
    with session_scope() as s:
        if not s.get(models.Customer, body.customer_id):
            raise HTTPException(404, "customer not found")
        p = models.Project(customer_id=body.customer_id, name=body.name)
        s.add(p)
        s.flush()
        return {"id": str(p.id), "name": p.name, "status": p.status}


@app.get("/projects")
def list_projects(customer_id: uuid.UUID) -> list[dict]:
    with session_scope() as s:
        rows = s.scalars(
            select(models.Project).where(models.Project.customer_id == customer_id)
        ).all()
        return [{"id": str(p.id), "name": p.name, "status": p.status} for p in rows]


# --- tasks --------------------------------------------------------------------
class TaskIn(BaseModel):
    customer_id: uuid.UUID
    project_id: uuid.UUID
    name: str
    work_required_minutes: int = 0
    parent_id: uuid.UUID | None = None


@app.post("/tasks")
def create_task(body: TaskIn) -> dict:
    with session_scope() as s:
        if not s.get(models.Project, body.project_id):
            raise HTTPException(404, "project not found")
        t = models.Task(
            customer_id=body.customer_id,
            project_id=body.project_id,
            name=body.name,
            work_required_minutes=body.work_required_minutes,
            parent_id=body.parent_id,
        )
        s.add(t)
        s.flush()
        return {"id": str(t.id), "name": t.name, "version": t.version}


@app.get("/tasks")
def list_tasks(project_id: uuid.UUID) -> list[dict]:
    with session_scope() as s:
        rows = s.scalars(
            select(models.Task).where(models.Task.project_id == project_id)
        ).all()
        return [
            {"id": str(t.id), "name": t.name, "status": t.status, "version": t.version}
            for t in rows
        ]