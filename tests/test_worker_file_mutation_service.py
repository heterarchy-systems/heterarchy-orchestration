import hashlib
import subprocess
import threading
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
)
from chattocodex.orchestrator_backend.repository.worker_workspace.repository_write_exceptions import (
    ContentHashConflictError,
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
from tests.broker_support import build_broker_service, claim_broker_task


def _git(*, cwd: Path, args: tuple[str, ...]) -> None:
    subprocess.run(
        ("git", "-C", str(cwd), *args),
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )


def _setup_two_writers(
    tmp_path: Path,
) -> tuple[WorkerFileMutationService, str, list[tuple[str, str, Path]], Path]:
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
    worktree_repository = GitWorktreeRepository(state_root=state_root)
    worktree_service = WorkerGitWorktreeService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        worktree_repository=worktree_repository,
        command_runner=GitCommandRunner(),
        lock_registry=locks,
    )
    mutation_service = WorkerFileMutationService(
        access_service=WorkerWorkspaceAccessService(
            worker_repository=worker_repository,
            worktree_repository=worktree_repository,
            path_resolver=ProjectPathResolver(),
        ),
        lock_registry=locks,
    )

    project = workspace_service.list_projects()[0]
    created_run = run_service.create_run(
        goal="Isolated file mutation", project_id=project.project_id
    )
    workers: list[tuple[str, str, Path]] = []
    for index in range(1, 3):
        claim = claim_broker_task(
            service=broker_service,
            run_id=created_run.state.run_id,
            run_handle=created_run.run_handle,
            project_id=project.project_id,
            objective=f"Mutate lane {index}",
            role=f"writer-{index}",
            label=f"consumer-{index}",
        )
        workspace = worktree_service.prepare(
            run_id=created_run.state.run_id,
            worker_id=claim.assignment.worker.worker_id,
            worker_handle=claim.assignment.worker_handle,
        )
        workers.append(
            (
                claim.assignment.worker.worker_id,
                claim.assignment.worker_handle,
                Path(workspace.worktree_root),
            )
        )
    return mutation_service, created_run.state.run_id, workers, project_root


def test_worker_file_replace_and_create_stay_inside_each_worktree(
    tmp_path: Path,
) -> None:
    service, run_id, workers, project_root = _setup_two_writers(tmp_path)
    base_sha = hashlib.sha256(b"base\n").hexdigest()

    first_id, first_handle, first_root = workers[0]
    second_id, second_handle, second_root = workers[1]
    first_replace = service.replace_file(
        run_id=run_id,
        worker_id=first_id,
        worker_handle=first_handle,
        relative_path="README.md",
        expected_sha256=base_sha,
        content="worker-one\n",
    )
    second_replace = service.replace_file(
        run_id=run_id,
        worker_id=second_id,
        worker_handle=second_handle,
        relative_path="README.md",
        expected_sha256=base_sha,
        content="worker-two\n",
    )
    created = service.create_file(
        run_id=run_id,
        worker_id=first_id,
        worker_handle=first_handle,
        relative_path="src/new_module.py",
        content="VALUE = 1\n",
    )

    assert first_replace.sha256 == hashlib.sha256(b"worker-one\n").hexdigest()
    assert second_replace.sha256 == hashlib.sha256(b"worker-two\n").hexdigest()
    assert created.relative_path == "src/new_module.py"
    assert (first_root / "README.md").read_text(encoding="utf-8") == "worker-one\n"
    assert (second_root / "README.md").read_text(encoding="utf-8") == "worker-two\n"
    assert (first_root / "src/new_module.py").read_text(
        encoding="utf-8"
    ) == "VALUE = 1\n"
    assert not (second_root / "src/new_module.py").exists()
    assert (project_root / "README.md").read_text(encoding="utf-8") == "base\n"
    assert not (project_root / "src/new_module.py").exists()


def test_worker_file_replace_rejects_stale_hash_without_mutation(
    tmp_path: Path,
) -> None:
    service, run_id, workers, _project_root = _setup_two_writers(tmp_path)
    worker_id, worker_handle, worktree_root = workers[0]

    with pytest.raises(ContentHashConflictError):
        service.replace_file(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            relative_path="README.md",
            expected_sha256="0" * 64,
            content="should-not-write\n",
        )

    assert (worktree_root / "README.md").read_text(encoding="utf-8") == "base\n"


def test_worker_file_mutation_blocks_git_and_symlink_escape(tmp_path: Path) -> None:
    service, run_id, workers, _project_root = _setup_two_writers(tmp_path)
    worker_id, worker_handle, worktree_root = workers[0]
    outside = tmp_path / "outside.txt"
    outside.write_text("outside\n", encoding="utf-8")
    (worktree_root / "escape.txt").symlink_to(outside)
    (worktree_root / "broken-link.txt").symlink_to(tmp_path / "missing-target.txt")

    with pytest.raises(ProjectPathDeniedError):
        service.create_file(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            relative_path=".git/config-copy",
            content="denied\n",
        )

    with pytest.raises(ProjectPathDeniedError):
        service.replace_file(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            relative_path="escape.txt",
            expected_sha256=hashlib.sha256(b"outside\n").hexdigest(),
            content="denied\n",
        )
    assert outside.read_text(encoding="utf-8") == "outside\n"

    with pytest.raises(ProjectPathDeniedError):
        service.create_file(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            relative_path="broken-link.txt",
            content="denied\n",
        )


def test_worker_file_mutations_for_different_workers_do_not_share_lifecycle_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, run_id, workers, _project_root = _setup_two_writers(tmp_path)
    first_id, first_handle, first_root = workers[0]
    second_id, second_handle, second_root = workers[1]
    first_worker_checked = threading.Event()
    allow_first_worker = threading.Event()
    second_worker_complete = threading.Event()
    original_require_workspace = service._access_service.require_workspace

    def pause_first_worker(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        operation: str,
        require_mutable: bool,
    ) -> Path:
        worktree_root = original_require_workspace(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            operation=operation,
            require_mutable=require_mutable,
        )
        if worker_id == first_id:
            first_worker_checked.set()
            assert allow_first_worker.wait(timeout=5)
        return worktree_root

    monkeypatch.setattr(
        service._access_service,
        "require_workspace",
        pause_first_worker,
    )
    thread_errors: list[BaseException] = []

    def mutate_first_worker() -> None:
        try:
            service.create_file(
                run_id=run_id,
                worker_id=first_id,
                worker_handle=first_handle,
                relative_path="first.txt",
                content="first\n",
            )
        except BaseException as error:
            thread_errors.append(error)

    def mutate_second_worker() -> None:
        try:
            service.create_file(
                run_id=run_id,
                worker_id=second_id,
                worker_handle=second_handle,
                relative_path="second.txt",
                content="second\n",
            )
        except BaseException as error:
            thread_errors.append(error)
        finally:
            second_worker_complete.set()

    first_thread = threading.Thread(target=mutate_first_worker)
    second_thread = threading.Thread(target=mutate_second_worker)
    first_thread.start()
    assert first_worker_checked.wait(timeout=5)
    second_thread.start()
    second_finished_while_first_blocked = second_worker_complete.wait(timeout=2)
    allow_first_worker.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert second_finished_while_first_blocked
    assert thread_errors == []
    assert (first_root / "first.txt").read_text(encoding="utf-8") == "first\n"
    assert (second_root / "second.txt").read_text(encoding="utf-8") == "second\n"
