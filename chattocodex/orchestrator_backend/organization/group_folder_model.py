from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from chattocodex.orchestrator_backend.database.orm_base import OrmBase


class GroupFolderModel(OrmBase):
    """SQLAlchemy persistence model for the optional project organization tree."""

    __tablename__ = "group_folders"
    __table_args__ = (
        CheckConstraint(
            "char_length(btrim(name)) > 0", name="group_folders_name_nonblank"
        ),
        CheckConstraint("order_index >= 0", name="group_folders_order_nonnegative"),
        Index(
            "group_folders_sibling_name_uq",
            "project_id",
            "parent_id",
            "name",
            unique=True,
            postgresql_nulls_not_distinct=True,
        ),
        Index(
            "group_folders_project_tree_idx",
            "project_id",
            "parent_id",
            "order_index",
            "folder_id",
        ),
    )

    folder_id: Mapped[UUID] = mapped_column(
        postgresql.UUID(as_uuid=True),
        primary_key=True,
    )
    project_id: Mapped[str] = mapped_column(String(128), nullable=False)
    parent_id: Mapped[UUID | None] = mapped_column(
        postgresql.UUID(as_uuid=True),
        ForeignKey("group_folders.folder_id", ondelete="CASCADE"),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    order_index: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
