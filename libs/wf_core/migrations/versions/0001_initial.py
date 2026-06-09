"""initial schema: tenancy, work spine, dependencies, time, outbox

Revision ID: 0001_initial
Revises:
Create Date: 2026-06-09

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB()
dep_type = sa.Enum("FS", "SS", "FF", "SF", name="deptype")


def _tenant_fk():
    return sa.Column("customer_id", UUID, sa.ForeignKey("customer.id", ondelete="CASCADE"), nullable=False)


def upgrade() -> None:
    # --- tenancy & org -------------------------------------------------------
    op.create_table(
        "customer",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "user",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("customer_id", "email", name="uq_user_customer_email"),
    )

    op.create_table(
        "team",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("parent_id", UUID, sa.ForeignKey("team.id", ondelete="SET NULL")),
    )
    op.create_index("ix_team_customer", "team", ["customer_id"])

    op.create_table(
        "group",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("parent_id", UUID, sa.ForeignKey("group.id", ondelete="SET NULL")),
    )
    op.create_index("ix_group_customer", "group", ["customer_id"])

    op.create_table(
        "role",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("name", sa.String(256), nullable=False),
    )
    op.create_index("ix_role_customer", "role", ["customer_id"])

    # --- work spine ----------------------------------------------------------
    op.create_table(
        "portfolio",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("custom_attributes", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_portfolio_customer", "portfolio", ["customer_id"])

    op.create_table(
        "program",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("portfolio_id", UUID, sa.ForeignKey("portfolio.id", ondelete="SET NULL")),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_program_customer_portfolio", "program", ["customer_id", "portfolio_id"])

    op.create_table(
        "project",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("portfolio_id", UUID, sa.ForeignKey("portfolio.id", ondelete="SET NULL")),
        sa.Column("program_id", UUID, sa.ForeignKey("program.id", ondelete="SET NULL")),
        sa.Column("owner_id", UUID, sa.ForeignKey("user.id", ondelete="SET NULL")),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="planning"),
        sa.Column("planned_start", sa.DateTime(timezone=True)),
        sa.Column("planned_completion", sa.DateTime(timezone=True)),
        sa.Column("percent_complete", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default="1"),
        sa.Column("custom_attributes", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_project_customer_program", "project", ["customer_id", "program_id"])
    op.create_index("ix_project_customer_status", "project", ["customer_id", "status"])

    op.create_table(
        "task",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("project_id", UUID, sa.ForeignKey("project.id", ondelete="CASCADE"), nullable=False),
        sa.Column("parent_id", UUID, sa.ForeignKey("task.id", ondelete="CASCADE")),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="new"),
        sa.Column("planned_start", sa.DateTime(timezone=True)),
        sa.Column("planned_completion", sa.DateTime(timezone=True)),
        sa.Column("work_required_minutes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("calendar_id", UUID),
        sa.Column("percent_complete", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default="1"),
        sa.Column("custom_attributes", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_task_customer_project", "task", ["customer_id", "project_id"])
    op.create_index("ix_task_customer_parent", "task", ["customer_id", "parent_id"])
    op.create_index("ix_task_customer_status_due", "task", ["customer_id", "status", "planned_completion"])

    op.create_table(
        "issue",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("project_id", UUID, sa.ForeignKey("project.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(512), nullable=False),
        sa.Column("type", sa.String(32), nullable=False, server_default="issue"),
        sa.Column("status", sa.String(32), nullable=False, server_default="new"),
        sa.Column("custom_attributes", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_issue_customer_project_status", "issue", ["customer_id", "project_id", "status"])

    # --- dependencies & assignment ------------------------------------------
    op.create_table(
        "predecessor",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("predecessor_id", UUID, sa.ForeignKey("task.id", ondelete="CASCADE"), nullable=False),
        sa.Column("successor_id", UUID, sa.ForeignKey("task.id", ondelete="CASCADE"), nullable=False),
        sa.Column("type", dep_type, nullable=False, server_default="FS"),
        sa.Column("lag_minutes", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("predecessor_id", "successor_id", name="uq_predecessor_edge"),
    )
    op.create_index("ix_predecessor_customer_pred", "predecessor", ["customer_id", "predecessor_id"])
    op.create_index("ix_predecessor_customer_succ", "predecessor", ["customer_id", "successor_id"])

    op.create_table(
        "assignment",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("task_id", UUID, sa.ForeignKey("task.id", ondelete="CASCADE"), nullable=False),
        sa.Column("assigned_to_id", UUID, sa.ForeignKey("user.id", ondelete="CASCADE"), nullable=False),
        sa.Column("role_id", UUID, sa.ForeignKey("role.id", ondelete="SET NULL")),
        sa.Column("team_id", UUID, sa.ForeignKey("team.id", ondelete="SET NULL")),
        sa.Column("assignment_percent", sa.Integer(), nullable=False, server_default="100"),
    )
    op.create_index("ix_assignment_customer_task", "assignment", ["customer_id", "task_id"])
    op.create_index("ix_assignment_customer_user", "assignment", ["customer_id", "assigned_to_id"])

    # --- time ----------------------------------------------------------------
    op.create_table(
        "hour",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("task_id", UUID, sa.ForeignKey("task.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", UUID, sa.ForeignKey("user.id", ondelete="CASCADE"), nullable=False),
        sa.Column("logged_date", sa.Date(), nullable=False),
        sa.Column("minutes", sa.Integer(), nullable=False),
        sa.Column("description", sa.String(1024)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_hour_customer_task", "hour", ["customer_id", "task_id"])
    op.create_index("ix_hour_customer_user_date", "hour", ["customer_id", "user_id", "logged_date"])

    op.create_table(
        "timesheet",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("user_id", UUID, sa.ForeignKey("user.id", ondelete="CASCADE"), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="open"),
    )
    op.create_index("ix_timesheet_customer_user", "timesheet", ["customer_id", "user_id"])

    # --- outbox --------------------------------------------------------------
    op.create_table(
        "outbox",
        sa.Column("id", UUID, primary_key=True),
        _tenant_fk(),
        sa.Column("aggregate_type", sa.String(64), nullable=False),
        sa.Column("aggregate_id", UUID, nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("partition_key", UUID, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_outbox_created_at", "outbox", ["created_at"])


def downgrade() -> None:
    for table in (
        "outbox", "timesheet", "hour", "assignment", "predecessor",
        "issue", "task", "project", "program", "portfolio",
        "role", "group", "team", "user", "customer",
    ):
        op.drop_table(table)
    dep_type.drop(op.get_bind(), checkfirst=True)