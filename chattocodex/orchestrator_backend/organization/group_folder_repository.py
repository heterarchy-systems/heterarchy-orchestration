from __future__ import annotations

from collections.abc import Callable
from types import TracebackType
from typing import Protocol, Self
from uuid import UUID

from chattocodex.orchestrator_backend.organization.group_folder_dto import GroupFolder


class GroupFolderRepository(Protocol):
    """Persistence port for project-scoped organization folders."""

    async def create(self, folder: GroupFolder) -> GroupFolder:
        """Create. Args: folder. Returns: GroupFolder."""
        ...

    async def get(self, folder_id: UUID) -> GroupFolder | None:
        """Get. Args: folder_id. Returns: GroupFolder | None."""
        ...

    async def list_for_project(self, project_id: str) -> tuple[GroupFolder, ...]:
        """List for project. Args: project_id. Returns: tuple[GroupFolder, ...]."""
        ...

    async def next_order_index(
        self,
        project_id: str,
        parent_id: UUID | None,
    ) -> int:
        """Next order index. Args: project_id, parent_id. Returns: int."""
        ...

    async def update_placement(
        self,
        folder_id: UUID,
        parent_id: UUID | None,
        order_index: int,
    ) -> GroupFolder | None:
        """Update placement. Args: folder_id, parent_id, order_index. Returns: GroupFolder | None."""
        ...

    async def rename(self, folder_id: UUID, name: str) -> GroupFolder | None:
        """Rename. Args: folder_id, name. Returns: GroupFolder | None."""
        ...

    async def delete(self, folder_id: UUID) -> bool:
        """Delete. Args: folder_id. Returns: bool."""
        ...


class GroupFolderUnitOfWork(Protocol):
    """Transaction boundary shared by one GroupFolder application use case."""

    folders: GroupFolderRepository

    async def __aenter__(self) -> Self:
        """Enter this asynchronous context manager. Args: None. Returns: Self."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit this asynchronous context manager. Args: exc_type, exc_value, traceback. Returns: None."""
        ...

    async def commit(self) -> None:
        """Commit. Args: None. Returns: None."""
        ...


GroupFolderUnitOfWorkFactory = Callable[[], GroupFolderUnitOfWork]
