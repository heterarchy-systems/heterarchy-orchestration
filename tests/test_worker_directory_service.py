import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.persistence.version_control.git_worktree_repository import (
    GitWorktreeRepository,
)
from chattocodex.orchestrator_backend.repository.project_path_resolver import (
    ProjectPathResolver,
)
from chattocodex.orchestrator_backend.repository.repository_exceptions import (
    ProjectPathDeniedError,
    UnsupportedRepositoryFileError,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_mutation_service import (
    WorkerDirectoryMutationService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_query_service import (
    WorkerDirectoryQueryService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_file_mutation_service import (
    WorkerFileMutationService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_workspace_access_service import (
    WorkerWorkspaceAccessService,
)
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.orchestrator_backend.version_control.worker_git_worktree_service import (
    WorkerGitWorktreeService,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.shared.element_collect.repository_enums import RepositoryEntryKind
from tests.broker_support import build_broker_service, claim_broker_task


@dataclass(frozen=True, slots=True)
class _DirectoryRuntime:
    query: WorkerDirectoryQueryService
    mutation: WorkerDirectoryMutationService
    files: WorkerFileMutationService
    run_id: str
    worker_id: str
    worker_handle: str
    worktree_root: Path


def _git(cwd: Path, args: tuple[str, ...]) -> None:
    subprocess.run(
        ("git", "-C", str(cwd), *args),
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )


def _build_runtime(tmp_path: Path) -> _DirectoryRuntime:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    _git(project_root, ("init",))
    _git(project_root, ("config", "user.email", "test@example.com"))
    _git(project_root, ("config", "user.name", "Test User"))
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'sample'\n",
        encoding="utf-8",
    )
    _git(project_root, ("add", "."))
    _git(project_root, ("commit", "-m", "initial"))

    state_root = tmp_path / "state"
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = TaskStateRepository(state_root=state_root)
    locks = EntityLockRegistry()
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_service,
        command_runner=GitCommandRunner(),
    )
    broker_service = build_broker_service(
        state_root=state_root,
        workspace_root=workspace_root,
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=locks,
    )
    project = workspace_service.list_projects()[0]
    run = run_service.create_run(
        goal="Directory MCP coverage", project_id=project.project_id
    )
    claim = claim_broker_task(
        service=broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=project.project_id,
        objective="Mutate isolated directories",
        role="writer",
        label="directory-worker",
    )
    worker = claim.assignment.worker
    worker_handle = claim.assignment.worker_handle
    worktree_repository = GitWorktreeRepository(state_root=state_root)
    worktree = WorkerGitWorktreeService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        worktree_repository=worktree_repository,
        command_runner=GitCommandRunner(),
        lock_registry=locks,
    ).prepare(
        run_id=run.state.run_id,
        worker_id=worker.worker_id,
        worker_handle=worker_handle,
    )
    access = WorkerWorkspaceAccessService(
        worker_repository=worker_repository,
        worktree_repository=worktree_repository,
        path_resolver=ProjectPathResolver(),
    )
    return _DirectoryRuntime(
        query=WorkerDirectoryQueryService(access_service=access),
        mutation=WorkerDirectoryMutationService(
            access_service=access,
            lock_registry=locks,
        ),
        files=WorkerFileMutationService(
            access_service=access,
            lock_registry=locks,
        ),
        run_id=run.state.run_id,
        worker_id=worker.worker_id,
        worker_handle=worker_handle,
        worktree_root=Path(worktree.worktree_root).resolve(),
    )


def test_worker_directory_create_list_stat_move_delete(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    created = runtime.mutation.create_directory(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        "notes",
        False,
    )
    assert created.relative_path == "notes"

    listing = runtime.query.list_directory(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        ".",
    )
    assert "notes" in {entry.name for entry in listing.entries}
    assert ".git" not in {entry.name for entry in listing.entries}

    info = runtime.query.stat_path(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        "notes",
    )
    assert info.kind is RepositoryEntryKind.DIRECTORY
    assert info.size_bytes is None

    moved = runtime.mutation.move_directory(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        "notes",
        "archive",
    )
    assert moved.relative_path == "archive"
    runtime.mutation.delete_directory(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        "archive",
        False,
    )
    assert not (runtime.worktree_root / "archive").exists()


def test_worker_directory_recursive_delete_requires_explicit_opt_in(
    tmp_path: Path,
) -> None:
    runtime = _build_runtime(tmp_path)
    runtime.mutation.create_directory(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        "tree/child",
        True,
    )
    runtime.files.create_file(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        "tree/child/value.txt",
        "value\n",
    )

    with pytest.raises(UnsupportedRepositoryFileError):
        runtime.mutation.delete_directory(
            runtime.run_id,
            runtime.worker_id,
            runtime.worker_handle,
            "tree",
            False,
        )
    assert (runtime.worktree_root / "tree" / "child" / "value.txt").is_file()

    runtime.mutation.delete_directory(
        runtime.run_id,
        runtime.worker_id,
        runtime.worker_handle,
        "tree",
        True,
    )
    assert not (runtime.worktree_root / "tree").exists()


@pytest.mark.parametrize("relative_path", ("../escape", ".git", ".git/config"))
def test_worker_directory_rejects_reserved_or_escaping_paths(
    tmp_path: Path,
    relative_path: str,
) -> None:
    runtime = _build_runtime(tmp_path)
    with pytest.raises(ProjectPathDeniedError):
        runtime.mutation.create_directory(
            runtime.run_id,
            runtime.worker_id,
            runtime.worker_handle,
            relative_path,
            True,
        )


def test_worker_directory_rejects_symlink_traversal_and_root_delete(
    tmp_path: Path,
) -> None:
    runtime = _build_runtime(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (runtime.worktree_root / "link").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ProjectPathDeniedError):
        runtime.mutation.create_directory(
            runtime.run_id,
            runtime.worker_id,
            runtime.worker_handle,
            "link/escaped",
            True,
        )
    with pytest.raises(ProjectPathDeniedError):
        runtime.mutation.delete_directory(
            runtime.run_id,
            runtime.worker_id,
            runtime.worker_handle,
            ".",
            True,
        )
    assert not (outside / "escaped").exists()
