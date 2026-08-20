from __future__ import annotations

from datetime import datetime

from pydantic import JsonValue
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from chattocodex.orchestrator_backend.database.orm_base import OrmBase


class SchemaMigrationRecord(OrmBase):
    """Record the canonical Python-defined database schema version."""

    __tablename__ = "chattocodex_schema_migrations"

    version: Mapped[str] = mapped_column(Text, primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsProjectionModel(OrmBase):
    """Persist the complete Control Panel operations snapshot."""

    __tablename__ = "operations_projection"
    __table_args__ = (
        CheckConstraint("singleton_id = 1", name="operations_projection_singleton"),
    )

    singleton_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[JsonValue] = mapped_column(JSONB, nullable=False)
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsProjectModel(OrmBase):
    """Persist one normalized project row for operations queries."""

    __tablename__ = "operations_projects"

    project_id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    root: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_project_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_project_label: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_project_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    connector_ready: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    binding_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    desired_capacity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    spawn_needed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provisioning_pending: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsRuntimeGroupModel(OrmBase):
    """Persist one recoverable runtime-group projection for a project."""

    __tablename__ = "operations_runtime_groups"

    project_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("operations_projects.project_id", ondelete="CASCADE"),
        primary_key=True,
    )
    runtime_group_key: Mapped[str] = mapped_column(Text, primary_key=True)
    initialized: Mapped[bool] = mapped_column(Boolean, nullable=False)
    topology_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    idle: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    waking: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claiming: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    busy: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsWorkerModel(OrmBase):
    """Persist one Worker wake-target row in the operations projection."""

    __tablename__ = "operations_workers"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "runtime_group_key"],
            [
                "operations_runtime_groups.project_id",
                "operations_runtime_groups.runtime_group_key",
            ],
            ondelete="CASCADE",
            name="operations_workers_project_id_runtime_group_key_fkey",
        ),
        Index(
            "operations_workers_project_runtime_group_idx",
            "project_id",
            "runtime_group_key",
        ),
        Index("operations_workers_status_idx", "status"),
    )

    wake_target_id: Mapped[str] = mapped_column(Text, primary_key=True)
    project_id: Mapped[str] = mapped_column(Text, nullable=False)
    runtime_group_key: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    wake_revision: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    worker_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsRunModel(OrmBase):
    """Persist one recent orchestration run for operations queries."""

    __tablename__ = "operations_runs"
    __table_args__ = (
        Index("operations_runs_project_idx", "project_id"),
        Index("operations_runs_status_idx", "status"),
        Index("operations_runs_updated_idx", "run_updated_at"),
    )

    run_id: Mapped[str] = mapped_column(Text, primary_key=True)
    project_id: Mapped[str] = mapped_column(Text, nullable=False)
    project_name: Mapped[str] = mapped_column(Text, nullable=False)
    goal: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    requested_parallelism: Mapped[int] = mapped_column(Integer, nullable=False)
    run_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsBrokerStateModel(OrmBase):
    """Persist the Broker-authoritative aggregate counters."""

    __tablename__ = "operations_broker_state"
    __table_args__ = (
        CheckConstraint("singleton_id = 1", name="operations_broker_state_singleton"),
    )

    singleton_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    term: Mapped[int] = mapped_column(BigInteger, nullable=False)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    queued_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ready_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claimed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    idle_slot_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    busy_slot_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsBrokerNamespaceModel(OrmBase):
    """Persist one Broker namespace aggregate projection."""

    __tablename__ = "operations_broker_namespaces"

    project_id: Mapped[str] = mapped_column(Text, primary_key=True)
    queued_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ready_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claimed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    idle_slot_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    busy_slot_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class OperationsBrokerConsumerGroupModel(OrmBase):
    """Persist one Broker consumer-group directory projection."""

    __tablename__ = "operations_broker_consumer_groups"
    __table_args__ = (
        Index("operations_broker_consumer_groups_namespace_idx", "namespace_id"),
    )

    group_id: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace_id: Mapped[str] = mapped_column(Text, nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    group_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    consumer_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
