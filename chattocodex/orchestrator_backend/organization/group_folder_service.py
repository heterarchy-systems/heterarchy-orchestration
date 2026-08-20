from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.organization.group_folder_dto import (
    GroupFolder,
    GroupFolderTreeNode,
)
from chattocodex.orchestrator_backend.organization.group_folder_exceptions import (
    GroupFolderNameConflictError,
    GroupFolderStorageError,
)
from chattocodex.orchestrator_backend.organization.group_folder_repository import (
    GroupFolderUnitOfWorkFactory,
)

_MAX_FOLDER_DEPTH = 32
_MAX_FOLDER_NAME_LENGTH = 255


class GroupFolderService:
    """Manage the optional project organization tree above persistent Teams."""

    def __init__(self, unit_of_work_factory: GroupFolderUnitOfWorkFactory) -> None:
        """Initialize this object. Args: unit_of_work_factory. Returns: None."""
        self._unit_of_work_factory = unit_of_work_factory

    async def create_folder(
        self,
        project_id: str,
        name: str,
        parent_id: UUID | None,
    ) -> GroupFolder:
        """Create folder. Args: project_id, name, parent_id. Returns: GroupFolder."""
        normalized_name = _normalize_name(name)
        resource_id = f"{project_id}:{parent_id or 'root'}:{normalized_name}"
        with _repository_error_boundary(
            operation="group_folder_create",
            resource_id=resource_id,
        ):
            async with self._unit_of_work_factory() as unit_of_work:
                if parent_id is not None:
                    parent = await unit_of_work.folders.get(folder_id=parent_id)
                    _require_project_folder(
                        folder=parent,
                        project_id=project_id,
                        operation="group_folder_create",
                        resource_id=str(parent_id),
                    )
                order_index = await unit_of_work.folders.next_order_index(
                    project_id=project_id,
                    parent_id=parent_id,
                )
                now = datetime.now(UTC)
                created = await unit_of_work.folders.create(
                    folder=GroupFolder(
                        folder_id=uuid4(),
                        project_id=project_id,
                        parent_id=parent_id,
                        name=normalized_name,
                        order_index=order_index,
                        created_at=now,
                        updated_at=now,
                    )
                )
                await unit_of_work.commit()
                return created

    async def get_folder(self, project_id: str, folder_id: UUID) -> GroupFolder:
        """Get folder. Args: project_id, folder_id. Returns: GroupFolder."""
        with _repository_error_boundary(
            operation="group_folder_get",
            resource_id=str(folder_id),
        ):
            async with self._unit_of_work_factory() as unit_of_work:
                folder = await unit_of_work.folders.get(folder_id=folder_id)
                return _require_project_folder(
                    folder=folder,
                    project_id=project_id,
                    operation="group_folder_get",
                    resource_id=str(folder_id),
                )

    async def get_tree(self, project_id: str) -> tuple[GroupFolderTreeNode, ...]:
        """Get tree. Args: project_id. Returns: tuple[GroupFolderTreeNode, ...]."""
        with _repository_error_boundary(
            operation="group_folder_tree",
            resource_id=project_id,
        ):
            async with self._unit_of_work_factory() as unit_of_work:
                folders = await unit_of_work.folders.list_for_project(
                    project_id=project_id
                )
        return _build_tree(project_id=project_id, folders=folders)

    async def rename_folder(
        self,
        project_id: str,
        folder_id: UUID,
        name: str,
    ) -> GroupFolder:
        """Rename folder. Args: project_id, folder_id, name. Returns: GroupFolder."""
        normalized_name = _normalize_name(name)
        with _repository_error_boundary(
            operation="group_folder_rename",
            resource_id=str(folder_id),
        ):
            async with self._unit_of_work_factory() as unit_of_work:
                existing = await unit_of_work.folders.get(folder_id=folder_id)
                _require_project_folder(
                    folder=existing,
                    project_id=project_id,
                    operation="group_folder_rename",
                    resource_id=str(folder_id),
                )
                renamed = await unit_of_work.folders.rename(
                    folder_id=folder_id,
                    name=normalized_name,
                )
                if renamed is None:
                    raise ResourceNotFoundError(
                        operation="group_folder_rename",
                        resource_id=str(folder_id),
                    )
                await unit_of_work.commit()
                return renamed

    async def move_folder(
        self,
        project_id: str,
        folder_id: UUID,
        parent_id: UUID | None,
    ) -> GroupFolder:
        """Move folder. Args: project_id, folder_id, parent_id. Returns: GroupFolder."""
        if parent_id == folder_id:
            raise InvalidTransitionError(
                operation="group_folder_move",
                resource_id=str(folder_id),
                message="A GroupFolder cannot be moved below itself.",
            )
        with _repository_error_boundary(
            operation="group_folder_move",
            resource_id=str(folder_id),
        ):
            async with self._unit_of_work_factory() as unit_of_work:
                existing = _require_project_folder(
                    folder=await unit_of_work.folders.get(folder_id=folder_id),
                    project_id=project_id,
                    operation="group_folder_move",
                    resource_id=str(folder_id),
                )
                if parent_id is not None:
                    _require_project_folder(
                        folder=await unit_of_work.folders.get(folder_id=parent_id),
                        project_id=project_id,
                        operation="group_folder_move",
                        resource_id=str(parent_id),
                    )
                folders = await unit_of_work.folders.list_for_project(
                    project_id=project_id
                )
                descendant_ids = _descendant_ids(
                    root_id=existing.folder_id,
                    folders=folders,
                )
                if parent_id in descendant_ids:
                    raise InvalidTransitionError(
                        operation="group_folder_move",
                        resource_id=str(folder_id),
                        message="A GroupFolder cannot be moved below one of its descendants.",
                    )
                order_index = await unit_of_work.folders.next_order_index(
                    project_id=project_id,
                    parent_id=parent_id,
                )
                moved = await unit_of_work.folders.update_placement(
                    folder_id=folder_id,
                    parent_id=parent_id,
                    order_index=order_index,
                )
                if moved is None:
                    raise ResourceNotFoundError(
                        operation="group_folder_move",
                        resource_id=str(folder_id),
                    )
                await unit_of_work.commit()
                return moved

    async def reorder_folder(
        self,
        project_id: str,
        folder_id: UUID,
        order_index: int,
    ) -> GroupFolder:
        """Reorder folder. Args: project_id, folder_id, order_index. Returns: GroupFolder."""
        if order_index < 0:
            raise InvalidTransitionError(
                operation="group_folder_reorder",
                resource_id=str(folder_id),
                message="GroupFolder order_index must not be negative.",
            )
        with _repository_error_boundary(
            operation="group_folder_reorder",
            resource_id=str(folder_id),
        ):
            async with self._unit_of_work_factory() as unit_of_work:
                existing = _require_project_folder(
                    folder=await unit_of_work.folders.get(folder_id=folder_id),
                    project_id=project_id,
                    operation="group_folder_reorder",
                    resource_id=str(folder_id),
                )
                reordered = await unit_of_work.folders.update_placement(
                    folder_id=folder_id,
                    parent_id=existing.parent_id,
                    order_index=order_index,
                )
                if reordered is None:
                    raise ResourceNotFoundError(
                        operation="group_folder_reorder",
                        resource_id=str(folder_id),
                    )
                await unit_of_work.commit()
                return reordered

    async def delete_folder(self, project_id: str, folder_id: UUID) -> None:
        """Delete folder. Args: project_id, folder_id. Returns: None."""
        with _repository_error_boundary(
            operation="group_folder_delete",
            resource_id=str(folder_id),
        ):
            async with self._unit_of_work_factory() as unit_of_work:
                _require_project_folder(
                    folder=await unit_of_work.folders.get(folder_id=folder_id),
                    project_id=project_id,
                    operation="group_folder_delete",
                    resource_id=str(folder_id),
                )
                deleted = await unit_of_work.folders.delete(folder_id=folder_id)
                if not deleted:
                    raise ResourceNotFoundError(
                        operation="group_folder_delete",
                        resource_id=str(folder_id),
                    )
                await unit_of_work.commit()


def _normalize_name(name: str) -> str:
    """Normalize name. Args: name. Returns: str."""
    normalized = " ".join(name.split())
    if not normalized:
        raise ValueError("GroupFolder name must not be blank")
    if len(normalized) > _MAX_FOLDER_NAME_LENGTH:
        raise ValueError(
            f"GroupFolder name must contain at most {_MAX_FOLDER_NAME_LENGTH} characters"
        )
    return normalized


def _require_project_folder(
    folder: GroupFolder | None,
    project_id: str,
    operation: str,
    resource_id: str,
) -> GroupFolder:
    """Require project folder. Args: folder, project_id, operation, resource_id. Returns: GroupFolder."""
    if folder is None or folder.project_id != project_id:
        raise ResourceNotFoundError(operation=operation, resource_id=resource_id)
    return folder


def _descendant_ids(
    root_id: UUID,
    folders: tuple[GroupFolder, ...],
) -> frozenset[UUID]:
    """Descendant ids. Args: root_id, folders. Returns: frozenset[UUID]."""
    children_by_parent: dict[UUID, list[UUID]] = defaultdict(list)
    for folder in folders:
        if folder.parent_id is not None:
            children_by_parent[folder.parent_id].append(folder.folder_id)

    descendants: set[UUID] = set()
    pending = list(children_by_parent.get(root_id, ()))
    while pending:
        candidate = pending.pop()
        if candidate in descendants:
            raise StatePersistenceError(
                operation="group_folder_descendants",
                resource_id=str(root_id),
                message="GroupFolder persistence contains a cycle.",
            )
        descendants.add(candidate)
        pending.extend(children_by_parent.get(candidate, ()))
    return frozenset(descendants)


def _build_tree(
    project_id: str,
    folders: tuple[GroupFolder, ...],
) -> tuple[GroupFolderTreeNode, ...]:
    """Build tree. Args: project_id, folders. Returns: tuple[GroupFolderTreeNode, ...]."""
    folder_ids = {folder.folder_id for folder in folders}
    children_by_parent: dict[UUID | None, list[GroupFolder]] = defaultdict(list)
    for folder in folders:
        if folder.parent_id is not None and folder.parent_id not in folder_ids:
            raise StatePersistenceError(
                operation="group_folder_tree",
                resource_id=str(folder.folder_id),
                message="GroupFolder persistence contains an orphaned parent reference.",
            )
        children_by_parent[folder.parent_id].append(folder)
    for children in children_by_parent.values():
        children.sort(key=lambda child: (child.order_index, child.folder_id))

    visited: set[UUID] = set()

    def build(parent_id: UUID | None, depth: int) -> tuple[GroupFolderTreeNode, ...]:
        """Build. Args: parent_id, depth. Returns: tuple[GroupFolderTreeNode, ...]."""
        if depth > _MAX_FOLDER_DEPTH:
            raise StatePersistenceError(
                operation="group_folder_tree",
                resource_id=project_id,
                message=f"GroupFolder tree exceeds the {_MAX_FOLDER_DEPTH}-level limit.",
            )
        nodes: list[GroupFolderTreeNode] = []
        for folder in children_by_parent.get(parent_id, ()):
            if folder.folder_id in visited:
                raise StatePersistenceError(
                    operation="group_folder_tree",
                    resource_id=str(folder.folder_id),
                    message="GroupFolder persistence contains a cycle.",
                )
            visited.add(folder.folder_id)
            nodes.append(
                GroupFolderTreeNode(
                    folder=folder,
                    children=build(folder.folder_id, depth + 1),
                )
            )
        return tuple(nodes)

    tree = build(None, 1)
    if len(visited) != len(folders):
        raise StatePersistenceError(
            operation="group_folder_tree",
            resource_id=project_id,
            message="GroupFolder persistence contains unreachable nodes.",
        )
    return tree


@contextmanager
def _repository_error_boundary(
    operation: str,
    resource_id: str,
) -> Iterator[None]:
    """Repository error boundary. Args: operation, resource_id. Returns: Iterator[None]."""
    try:
        yield
    except GroupFolderNameConflictError as error:
        raise ResourceAlreadyExistsError(
            operation=operation,
            resource_id=resource_id,
        ) from error
    except GroupFolderStorageError as error:
        raise StatePersistenceError(
            operation=operation,
            resource_id=resource_id,
            message=str(error),
        ) from error
