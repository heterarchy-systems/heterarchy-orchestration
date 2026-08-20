from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True, slots=True, kw_only=True)
class GroupFolder:
    """Validated internal representation of one project-scoped organization folder."""

    folder_id: UUID
    project_id: str
    parent_id: UUID | None
    name: str
    order_index: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class GroupFolderTreeNode:
    """Immutable tree node assembled from one flat authoritative folder query."""

    folder: GroupFolder
    children: tuple[GroupFolderTreeNode, ...]
