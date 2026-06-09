"""SQLAlchemy models = the source-of-truth schema for the Workfront core.

Design rules (see Task 1):
  * Multi-tenant: every table carries `customer_id`; it leads every index and
    is the partition key for the large tables.
  * UUIDv7 primary keys (time-ordered -> index-friendly).
  * Custom fields live in `custom_attributes` JSONB; promote hot fields to
    generated columns when you need to filter/range on them.
  * Optimistic concurrency via `version` on mutable hot tables.
  * Denormalized rollups (`percent_complete`) on project for fast reads.

Alembic autogenerates migrations from this metadata (migrations/env.py).
"""

from __future__ import annotations

import enum
import uuid
from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from wf_core.ids import uuid7


class Base(DeclarativeBase):
    pass


class DepType(str, enum.Enum):
    FS = "FS"  # finish-to-start
    SS = "SS"  # start-to-start
    FF = "FF"  # finish-to-finish
    SF = "SF"  # start-to-finish


# --- column helpers -----------------------------------------------------------
def _pk() -> Mapped[uuid.UUID]:
    return mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid7)


def _tenant_fk() -> Mapped[uuid.UUID]:
    return mapped_column(
        UUID(as_uuid=True), ForeignKey("customer.id", ondelete="CASCADE"), nullable=False
    )


def _fk(target: str, *, nullable: bool, ondelete: str) -> Mapped:
    return mapped_column(
        UUID(as_uuid=True), ForeignKey(target, ondelete=ondelete), nullable=nullable
    )


def _created() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now())


def _updated() -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


# ============================================================================
# Tenancy & org
# ============================================================================
class Customer(Base):
    __tablename__ = "customer"

    id: Mapped[uuid.UUID] = _pk()
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    created_at: Mapped[datetime] = _created()


class User(Base):
    __tablename__ = "user"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = _created()

    __table_args__ = (UniqueConstraint("customer_id", "email", name="uq_user_customer_email"),)


class Team(Base):
    __tablename__ = "team"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    parent_id: Mapped[uuid.UUID | None] = _fk("team.id", nullable=True, ondelete="SET NULL")

    __table_args__ = (Index("ix_team_customer", "customer_id"),)


class Group(Base):
    __tablename__ = "group"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    parent_id: Mapped[uuid.UUID | None] = _fk("group.id", nullable=True, ondelete="SET NULL")

    __table_args__ = (Index("ix_group_customer", "customer_id"),)


class Role(Base):
    __tablename__ = "role"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    name: Mapped[str] = mapped_column(String(256), nullable=False)

    __table_args__ = (Index("ix_role_customer", "customer_id"),)


# ============================================================================
# Work hierarchy (the spine)
# ============================================================================
class Portfolio(Base):
    __tablename__ = "portfolio"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    custom_attributes: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (Index("ix_portfolio_customer", "customer_id"),)


class Program(Base):
    __tablename__ = "program"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    portfolio_id: Mapped[uuid.UUID | None] = _fk("portfolio.id", nullable=True, ondelete="SET NULL")
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (Index("ix_program_customer_portfolio", "customer_id", "portfolio_id"),)


class Project(Base):
    __tablename__ = "project"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    portfolio_id: Mapped[uuid.UUID | None] = _fk("portfolio.id", nullable=True, ondelete="SET NULL")
    program_id: Mapped[uuid.UUID | None] = _fk("program.id", nullable=True, ondelete="SET NULL")
    owner_id: Mapped[uuid.UUID | None] = _fk("user.id", nullable=True, ondelete="SET NULL")

    name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="planning", nullable=False)
    planned_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    planned_completion: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    percent_complete: Mapped[int] = mapped_column(Integer, default=0, nullable=False)  # rollup

    version: Mapped[int] = mapped_column(BigInteger, default=1, nullable=False)
    custom_attributes: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    tasks: Mapped[list["Task"]] = relationship(back_populates="project")

    __table_args__ = (
        Index("ix_project_customer_program", "customer_id", "program_id"),
        Index("ix_project_customer_status", "customer_id", "status"),
    )


class Task(Base):
    __tablename__ = "task"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    project_id: Mapped[uuid.UUID] = _fk("project.id", nullable=False, ondelete="CASCADE")
    parent_id: Mapped[uuid.UUID | None] = _fk("task.id", nullable=True, ondelete="CASCADE")

    name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="new", nullable=False)
    planned_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    planned_completion: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    work_required_minutes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    calendar_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    percent_complete: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    version: Mapped[int] = mapped_column(BigInteger, default=1, nullable=False)  # cascade guard
    custom_attributes: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    project: Mapped["Project"] = relationship(back_populates="tasks")

    __table_args__ = (
        Index("ix_task_customer_project", "customer_id", "project_id"),
        Index("ix_task_customer_parent", "customer_id", "parent_id"),
        Index("ix_task_customer_status_due", "customer_id", "status", "planned_completion"),
    )


class Issue(Base):
    """Workfront 'OPTASK' — bug / request / change tied to a project."""

    __tablename__ = "issue"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    project_id: Mapped[uuid.UUID] = _fk("project.id", nullable=False, ondelete="CASCADE")
    name: Mapped[str] = mapped_column(String(512), nullable=False)
    type: Mapped[str] = mapped_column(String(32), default="issue", nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="new", nullable=False)
    custom_attributes: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = _created()
    updated_at: Mapped[datetime] = _updated()

    __table_args__ = (Index("ix_issue_customer_project_status", "customer_id", "project_id", "status"),)


# ============================================================================
# Dependencies & assignment
# ============================================================================
class Predecessor(Base):
    """A dependency edge between two tasks (the cascade DAG)."""

    __tablename__ = "predecessor"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    predecessor_id: Mapped[uuid.UUID] = _fk("task.id", nullable=False, ondelete="CASCADE")
    successor_id: Mapped[uuid.UUID] = _fk("task.id", nullable=False, ondelete="CASCADE")
    type: Mapped[DepType] = mapped_column(SAEnum(DepType), default=DepType.FS, nullable=False)
    lag_minutes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    __table_args__ = (
        Index("ix_predecessor_customer_pred", "customer_id", "predecessor_id"),
        Index("ix_predecessor_customer_succ", "customer_id", "successor_id"),
        UniqueConstraint("predecessor_id", "successor_id", name="uq_predecessor_edge"),
    )


class Assignment(Base):
    """M:N join between a work item and a user/role/team, with its own attributes."""

    __tablename__ = "assignment"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    task_id: Mapped[uuid.UUID] = _fk("task.id", nullable=False, ondelete="CASCADE")
    assigned_to_id: Mapped[uuid.UUID] = _fk("user.id", nullable=False, ondelete="CASCADE")
    role_id: Mapped[uuid.UUID | None] = _fk("role.id", nullable=True, ondelete="SET NULL")
    team_id: Mapped[uuid.UUID | None] = _fk("team.id", nullable=True, ondelete="SET NULL")
    assignment_percent: Mapped[int] = mapped_column(Integer, default=100, nullable=False)

    __table_args__ = (
        Index("ix_assignment_customer_task", "customer_id", "task_id"),
        Index("ix_assignment_customer_user", "customer_id", "assigned_to_id"),
    )


# ============================================================================
# Time
# ============================================================================
class Hour(Base):
    """Logged time — the highest-volume table. Range-partition by logged_date at scale."""

    __tablename__ = "hour"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    task_id: Mapped[uuid.UUID] = _fk("task.id", nullable=False, ondelete="CASCADE")
    user_id: Mapped[uuid.UUID] = _fk("user.id", nullable=False, ondelete="CASCADE")
    logged_date: Mapped[date] = mapped_column(Date, nullable=False)
    minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[str | None] = mapped_column(String(1024))
    created_at: Mapped[datetime] = _created()

    __table_args__ = (
        Index("ix_hour_customer_task", "customer_id", "task_id"),
        Index("ix_hour_customer_user_date", "customer_id", "user_id", "logged_date"),
    )


class Timesheet(Base):
    __tablename__ = "timesheet"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    user_id: Mapped[uuid.UUID] = _fk("user.id", nullable=False, ondelete="CASCADE")
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="open", nullable=False)

    __table_args__ = (Index("ix_timesheet_customer_user", "customer_id", "user_id"),)


# ============================================================================
# Infra: transactional outbox
# ============================================================================
class OutboxEvent(Base):
    """Written in the SAME tx as the domain change. A CDC relay (Debezium) tails
    this table and publishes rows to Kafka -> no dual-write, no lost cascades."""

    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = _pk()
    customer_id: Mapped[uuid.UUID] = _tenant_fk()
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)  # e.g. "task"
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    partition_key: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)  # project_id
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = _created()

    __table_args__ = (Index("ix_outbox_created_at", "created_at"),)