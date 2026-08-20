from __future__ import annotations

from types import TracebackType
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from chattocodex.orchestrator_backend.database.sqlalchemy_runtime import (
    SqlAlchemyResourcesProvider,
    resolve_sqlalchemy_resources,
)
from chattocodex.orchestrator_backend.organization.group_folder_dto import GroupFolder
from chattocodex.orchestrator_backend.organization.group_folder_exceptions import (
    GroupFolderNameConflictError,
    GroupFolderStorageError,
)
from chattocodex.orchestrator_backend.organization.group_folder_model import (
    GroupFolderModel,
)
from chattocodex.orchestrator_backend.organization.group_folder_repository import (
    GroupFolderRepository,
)

_ORDER_INDEX_GAP = 1_000


class SqlAlchemyGroupFolderRepository:
    """SQLAlchemy adapter using one externally owned AsyncSession transaction."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize this object. Args: session. Returns: None."""
        self._session = session

    async def create(self, folder: GroupFolder) -> GroupFolder:
        """Create. Args: folder. Returns: GroupFolder."""
        model = GroupFolderModel(
            folder_id=folder.folder_id,
            project_id=folder.project_id,
            parent_id=folder.parent_id,
            name=folder.name,
            order_index=folder.order_index,
            created_at=folder.created_at,
            updated_at=folder.updated_at,
        )
        self._session.add(model)
        await self._flush_with_conflict_mapping()
        return _to_domain(model)

    async def get(self, folder_id: UUID) -> GroupFolder | None:
        """Get. Args: folder_id. Returns: GroupFolder | None."""
        model = await self._session.get(GroupFolderModel, folder_id)
        return None if model is None else _to_domain(model)

    async def list_for_project(self, project_id: str) -> tuple[GroupFolder, ...]:
        """List for project. Args: project_id. Returns: tuple[GroupFolder, ...]."""
        statement = (
            select(GroupFolderModel)
            .where(GroupFolderModel.project_id == project_id)
            .order_by(
                GroupFolderModel.parent_id.asc().nullsfirst(),
                GroupFolderModel.order_index.asc(),
                GroupFolderModel.folder_id.asc(),
            )
        )
        result = await self._session.scalars(statement)
        return tuple(_to_domain(model) for model in result)

    async def next_order_index(
        self,
        project_id: str,
        parent_id: UUID | None,
    ) -> int:
        """Next order index. Args: project_id, parent_id. Returns: int."""
        parent_predicate = (
            GroupFolderModel.parent_id.is_(None)
            if parent_id is None
            else GroupFolderModel.parent_id == parent_id
        )
        statement = select(
            func.coalesce(func.max(GroupFolderModel.order_index), 0)
        ).where(
            GroupFolderModel.project_id == project_id,
            parent_predicate,
        )
        current_max = await self._session.scalar(statement)
        return int(current_max or 0) + _ORDER_INDEX_GAP

    async def update_placement(
        self,
        folder_id: UUID,
        parent_id: UUID | None,
        order_index: int,
    ) -> GroupFolder | None:
        """Update placement. Args: folder_id, parent_id, order_index. Returns: GroupFolder | None."""
        model = await self._session.get(GroupFolderModel, folder_id)
        if model is None:
            return None
        model.parent_id = parent_id
        model.order_index = order_index
        await self._flush_with_conflict_mapping()
        await self._session.refresh(model)
        return _to_domain(model)

    async def rename(self, folder_id: UUID, name: str) -> GroupFolder | None:
        """Rename. Args: folder_id, name. Returns: GroupFolder | None."""
        model = await self._session.get(GroupFolderModel, folder_id)
        if model is None:
            return None
        model.name = name
        await self._flush_with_conflict_mapping()
        await self._session.refresh(model)
        return _to_domain(model)

    async def delete(self, folder_id: UUID) -> bool:
        """Delete. Args: folder_id. Returns: bool."""
        model = await self._session.get(GroupFolderModel, folder_id)
        if model is None:
            return False
        await self._session.delete(model)
        await self._flush_with_conflict_mapping()
        return True

    async def _flush_with_conflict_mapping(self) -> None:
        """Flush with conflict mapping. Args: None. Returns: None."""
        try:
            await self._session.flush()
        except IntegrityError as error:
            raise GroupFolderNameConflictError(
                "A sibling GroupFolder already uses this name or placement."
            ) from error
        except SQLAlchemyError as error:
            raise GroupFolderStorageError(
                "PostgreSQL GroupFolder persistence failed."
            ) from error


class SqlAlchemyGroupFolderUnitOfWork:
    """Open one AsyncSession per application use case and own commit/rollback."""

    def __init__(
        self,
        resources_provider: SqlAlchemyResourcesProvider,
    ) -> None:
        """Initialize this object. Args: resources_provider. Returns: None."""
        self._resources_provider = resources_provider
        self._session: AsyncSession | None = None
        self._folders: GroupFolderRepository | None = None

    @property
    def folders(self) -> GroupFolderRepository:
        """Folders. Args: None. Returns: GroupFolderRepository."""
        if self._folders is None:
            raise RuntimeError("GroupFolder unit of work has not been entered")
        return self._folders

    async def __aenter__(self) -> SqlAlchemyGroupFolderUnitOfWork:
        """Enter this asynchronous context manager. Args: None. Returns: SqlAlchemyGroupFolderUnitOfWork."""
        resources = await resolve_sqlalchemy_resources(
            provider=self._resources_provider
        )
        self._session = resources.session_factory()
        self._folders = SqlAlchemyGroupFolderRepository(session=self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit this asynchronous context manager. Args: exc_type, exc_value, traceback. Returns: None."""
        del exc_type, exc_value, traceback
        session = self._require_session()
        try:
            if session.in_transaction():
                await session.rollback()
        finally:
            await session.close()
            self._session = None
            self._folders = None

    async def commit(self) -> None:
        """Commit. Args: None. Returns: None."""
        session = self._require_session()
        try:
            await session.commit()
        except SQLAlchemyError as error:
            await session.rollback()
            raise GroupFolderStorageError(
                "PostgreSQL GroupFolder transaction commit failed."
            ) from error

    def _require_session(self) -> AsyncSession:
        """Require session. Args: None. Returns: AsyncSession."""
        if self._session is None:
            raise RuntimeError("GroupFolder unit of work has not been entered")
        return self._session


def _to_domain(model: GroupFolderModel) -> GroupFolder:
    """To domain. Args: model. Returns: GroupFolder."""
    return GroupFolder(
        folder_id=model.folder_id,
        project_id=model.project_id,
        parent_id=model.parent_id,
        name=model.name,
        order_index=model.order_index,
        created_at=model.created_at,
        updated_at=model.updated_at,
    )
