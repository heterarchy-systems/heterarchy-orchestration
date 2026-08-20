import subprocess
from pathlib import Path

import pytest

from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.orchestrator_backend.version_control.git_exceptions import (
    GitWorktreeError,
)
from chattocodex.orchestrator_backend.version_control.worker_git_worktree_service import (
    WorkerGitWorktreeService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.persistence.version_control.git_worktree_repository import (
    GitWorktreeRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from tests.broker_support import build_broker_service, claim_broker_task


def _git(*, cwd: Path, args: tuple[str, ...]) -> str:
    result = subprocess.run(
        ("git", "-C", str(cwd), *args),
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.strip()


def _create_git_project(tmp_path: Path) -> tuple[Path, Path]:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    _git(cwd=project_root, args=("init",))
    _git(cwd=project_root, args=("config", "user.email", "test@example.com"))
    _git(cwd=project_root, args=("config", "user.name", "Test User"))
    (project_root / "README.md").write_text("base\n", encoding="utf-8")
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'sample'\n",
        encoding="utf-8",
    )
    _git(cwd=project_root, args=("add", "."))
    _git(cwd=project_root, args=("commit", "-m", "initial"))
    return workspace_root, project_root


def _build_services(
    *,
    tmp_path: Path,
    workspace_root: Path,
) -> tuple[RunOrchestrationService, BrokerService, WorkerGitWorktreeService]:
    state_root = tmp_path / "state"
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = TaskStateRepository(state_root=state_root)
    locks = EntityLockRegistry()
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    return (
        RunOrchestrationService(
            run_repository=run_repository,
            workspace_project_service=workspace_service,
            command_runner=GitCommandRunner(),
        ),
        build_broker_service(
            state_root=state_root,
            workspace_root=workspace_root,
            run_repository=run_repository,
            worker_repository=worker_repository,
            task_repository=task_repository,
            lock_registry=locks,
        ),
        WorkerGitWorktreeService(
            run_repository=run_repository,
            worker_repository=worker_repository,
            worktree_repository=GitWorktreeRepository(state_root=state_root),
            command_runner=GitCommandRunner(),
            lock_registry=locks,
        ),
    )


def test_two_workers_receive_isolated_worktrees_from_same_base(tmp_path: Path) -> None:
    workspace_root, project_root = _create_git_project(tmp_path)
    run_service, broker_service, worktree_service = _build_services(
        tmp_path=tmp_path,
        workspace_root=workspace_root,
    )
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    created_run = run_service.create_run(
        goal="Parallel write isolation", project_id=project.project_id
    )
    claims = [
        claim_broker_task(
            service=broker_service,
            run_id=created_run.state.run_id,
            run_handle=created_run.run_handle,
            project_id=project.project_id,
            objective=f"Write lane {index}",
            role=f"writer-{index}",
            label=f"consumer-{index}",
        )
        for index in range(1, 3)
    ]

    prepared = [
        worktree_service.prepare(
            run_id=created_run.state.run_id,
            worker_id=claim.assignment.worker.worker_id,
            worker_handle=claim.assignment.worker_handle,
        )
        for claim in claims
    ]

    assert prepared[0].base_commit == prepared[1].base_commit
    assert prepared[0].worktree_root != prepared[1].worktree_root
    assert prepared[0].base_commit == _git(cwd=project_root, args=("rev-parse", "HEAD"))

    first_root = Path(prepared[0].worktree_root)
    second_root = Path(prepared[1].worktree_root)
    (first_root / "README.md").write_text("worker-one\n", encoding="utf-8")
    (second_root / "README.md").write_text("worker-two\n", encoding="utf-8")

    assert (first_root / "README.md").read_text(encoding="utf-8") == "worker-one\n"
    assert (second_root / "README.md").read_text(encoding="utf-8") == "worker-two\n"
    assert (project_root / "README.md").read_text(encoding="utf-8") == "base\n"

    restarted = _build_services(tmp_path=tmp_path, workspace_root=workspace_root)[2]
    restored = restarted.prepare(
        run_id=created_run.state.run_id,
        worker_id=claims[0].assignment.worker.worker_id,
        worker_handle=claims[0].assignment.worker_handle,
    )
    assert restored.worktree_root == prepared[0].worktree_root
    assert (Path(restored.worktree_root) / "README.md").read_text(
        encoding="utf-8"
    ) == "worker-one\n"


def test_later_worker_uses_original_run_base_after_repository_advances(
    tmp_path: Path,
) -> None:
    workspace_root, project_root = _create_git_project(tmp_path)
    run_service, broker_service, worktree_service = _build_services(
        tmp_path=tmp_path,
        workspace_root=workspace_root,
    )
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    created_run = run_service.create_run(
        goal="Stable base", project_id=project.project_id
    )
    first = claim_broker_task(
        service=broker_service,
        run_id=created_run.state.run_id,
        run_handle=created_run.run_handle,
        project_id=project.project_id,
        objective="First write",
        role="first",
        label="consumer-first",
    )
    second = claim_broker_task(
        service=broker_service,
        run_id=created_run.state.run_id,
        run_handle=created_run.run_handle,
        project_id=project.project_id,
        objective="Second write",
        role="second",
        label="consumer-second",
    )

    first_workspace = worktree_service.prepare(
        run_id=created_run.state.run_id,
        worker_id=first.assignment.worker.worker_id,
        worker_handle=first.assignment.worker_handle,
    )
    (project_root / "later.txt").write_text("later\n", encoding="utf-8")
    _git(cwd=project_root, args=("add", "later.txt"))
    _git(cwd=project_root, args=("commit", "-m", "later"))
    assert (
        _git(cwd=project_root, args=("rev-parse", "HEAD"))
        != first_workspace.base_commit
    )

    second_workspace = worktree_service.prepare(
        run_id=created_run.state.run_id,
        worker_id=second.assignment.worker.worker_id,
        worker_handle=second.assignment.worker_handle,
    )
    assert second_workspace.base_commit == first_workspace.base_commit
    assert not (Path(second_workspace.worktree_root) / "later.txt").exists()


def test_first_worktree_prepare_rejects_dirty_repository(tmp_path: Path) -> None:
    workspace_root, project_root = _create_git_project(tmp_path)
    run_service, broker_service, worktree_service = _build_services(
        tmp_path=tmp_path,
        workspace_root=workspace_root,
    )
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    created_run = run_service.create_run(
        goal="Dirty safety", project_id=project.project_id
    )
    claim = claim_broker_task(
        service=broker_service,
        run_id=created_run.state.run_id,
        run_handle=created_run.run_handle,
        project_id=project.project_id,
        objective="Dirty write",
        role="writer",
        label="consumer-dirty",
    )
    (project_root / "dirty.txt").write_text("dirty\n", encoding="utf-8")

    with pytest.raises(GitWorktreeError, match="uncommitted or untracked"):
        worktree_service.prepare(
            run_id=created_run.state.run_id,
            worker_id=claim.assignment.worker.worker_id,
            worker_handle=claim.assignment.worker_handle,
        )
