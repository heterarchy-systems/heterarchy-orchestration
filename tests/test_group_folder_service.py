from __future__ import annotations

import asyncio
from dataclasses import replace
from types import TracebackType
from uuid import UUID

import pytest

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    ResourceAlreadyExistsError,
)
from chattocodex.orchestrator_backend.organization.group_folder_dto import GroupFolder
from chattocodex.orchestrator_backend.organization.group_folder_exceptions import (
    GroupFolderNameConflictError,
)
from chattocodex.orchestrator_backend.organization.group_folder_repository import (
    GroupFolderRepository,
)
from chattocodex.orchestrator_backend.organization.group_folder_service import (
    GroupFolderService,
)

_PROJECT_ID = "prj_folder-e2e_1a2b3c4d"


class FakeGroupFolderRepository:
    def __init__(self, *, folders: dict[UUID, GroupFolder]) -> None:
        self._folders = folders

    async def create(self, *, folder: GroupFolder) -> GroupFolder:
        self._require_unique_sibling(
            project_id=folder.project_id,
            parent_id=folder.parent_id,
            name=folder.name,
            excluded_id=None,
        )
        self._folders[folder.folder_id] = folder
        return folder

    async def get(self, *, folder_id: UUID) -> GroupFolder | None:
        return self._folders.get(folder_id)

    async def list_for_project(self, *, project_id: str) -> tuple[GroupFolder, ...]:
        return tuple(
            sorted(
                (
                    folder
                    for folder in self._folders.values()
                    if folder.project_id == project_id
                ),
                key=lambda folder: (
                    str(folder.parent_id or ""),
                    folder.order_index,
                    folder.folder_id,
                ),
            )
        )

    async def next_order_index(
        self,
        *,
        project_id: str,
        parent_id: UUID | None,
    ) -> int:
        sibling_orders = [
            folder.order_index
            for folder in self._folders.values()
            if folder.project_id == project_id and folder.parent_id == parent_id
        ]
        return max(sibling_orders, default=0) + 1_000

    async def update_placement(
        self,
        *,
        folder_id: UUID,
        parent_id: UUID | None,
        order_index: int,
    ) -> GroupFolder | None:
        current = self._folders.get(folder_id)
        if current is None:
            return None
        self._require_unique_sibling(
            project_id=current.project_id,
            parent_id=parent_id,
            name=current.name,
            excluded_id=folder_id,
        )
        updated = replace(
            current,
            parent_id=parent_id,
            order_index=order_index,
        )
        self._folders[folder_id] = updated
        return updated

    async def rename(self, *, folder_id: UUID, name: str) -> GroupFolder | None:
        current = self._folders.get(folder_id)
        if current is None:
            return None
        self._require_unique_sibling(
            project_id=current.project_id,
            parent_id=current.parent_id,
            name=name,
            excluded_id=folder_id,
        )
        updated = replace(current, name=name)
        self._folders[folder_id] = updated
        return updated

    async def delete(self, *, folder_id: UUID) -> bool:
        if folder_id not in self._folders:
            return False
        pending = [folder_id]
        deleted: set[UUID] = set()
        while pending:
            current = pending.pop()
            deleted.add(current)
            pending.extend(
                folder.folder_id
                for folder in self._folders.values()
                if folder.parent_id == current and folder.folder_id not in deleted
            )
        for deleted_id in deleted:
            self._folders.pop(deleted_id, None)
        return True

    def _require_unique_sibling(
        self,
        *,
        project_id: str,
        parent_id: UUID | None,
        name: str,
        excluded_id: UUID | None,
    ) -> None:
        conflict = any(
            folder.folder_id != excluded_id
            and folder.project_id == project_id
            and folder.parent_id == parent_id
            and folder.name == name
            for folder in self._folders.values()
        )
        if conflict:
            raise GroupFolderNameConflictError(name)


class FakeGroupFolderUnitOfWork:
    def __init__(self, *, repository: GroupFolderRepository) -> None:
        self.folders = repository
        self.commit_count = 0

    async def __aenter__(self) -> FakeGroupFolderUnitOfWork:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback

    async def commit(self) -> None:
        self.commit_count += 1


def _service() -> GroupFolderService:
    folders: dict[UUID, GroupFolder] = {}
    repository = FakeGroupFolderRepository(folders=folders)
    return GroupFolderService(
        unit_of_work_factory=lambda: FakeGroupFolderUnitOfWork(repository=repository)
    )


def test_group_folder_create_build_tree_move_and_delete() -> None:
    async def scenario() -> None:
        service = _service()
        research = await service.create_folder(
            project_id=_PROJECT_ID,
            name=" Research ",
            parent_id=None,
        )
        market = await service.create_folder(
            project_id=_PROJECT_ID,
            name="Market Team",
            parent_id=research.folder_id,
        )
        engineering = await service.create_folder(
            project_id=_PROJECT_ID,
            name="Engineering",
            parent_id=None,
        )

        tree = await service.get_tree(project_id=_PROJECT_ID)
        assert tuple(node.folder.name for node in tree) == ("Research", "Engineering")
        assert tree[0].children[0].folder.folder_id == market.folder_id

        moved = await service.move_folder(
            project_id=_PROJECT_ID,
            folder_id=market.folder_id,
            parent_id=engineering.folder_id,
        )
        assert moved.parent_id == engineering.folder_id

        await service.delete_folder(
            project_id=_PROJECT_ID,
            folder_id=engineering.folder_id,
        )
        remaining = await service.get_tree(project_id=_PROJECT_ID)
        assert tuple(node.folder.folder_id for node in remaining) == (
            research.folder_id,
        )

    asyncio.run(scenario())


def test_group_folder_rejects_descendant_cycle() -> None:
    async def scenario() -> None:
        service = _service()
        parent = await service.create_folder(
            project_id=_PROJECT_ID,
            name="Parent",
            parent_id=None,
        )
        child = await service.create_folder(
            project_id=_PROJECT_ID,
            name="Child",
            parent_id=parent.folder_id,
        )

        with pytest.raises(InvalidTransitionError):
            await service.move_folder(
                project_id=_PROJECT_ID,
                folder_id=parent.folder_id,
                parent_id=child.folder_id,
            )

    asyncio.run(scenario())


def test_group_folder_maps_duplicate_sibling_to_domain_error() -> None:
    async def scenario() -> None:
        service = _service()
        await service.create_folder(
            project_id=_PROJECT_ID,
            name="Research",
            parent_id=None,
        )
        with pytest.raises(ResourceAlreadyExistsError):
            await service.create_folder(
                project_id=_PROJECT_ID,
                name="Research",
                parent_id=None,
            )

    asyncio.run(scenario())
