import hashlib
import subprocess
import threading
from pathlib import Path

import pytest

from chattocodex.broker.broker_service import BrokerService
from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.git.git_exceptions import GitWorktreeError
from chattocodex.git.git_result_commit_runner import GitResultCommitRunner
from chattocodex.git.worker_git_worktree_service import WorkerGitWorktreeService
from chattocodex.git.worker_result_commit_service import WorkerResultCommitService
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_exceptions import InvalidTransitionError
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.persistence.git_worktree_repository import GitWorktreeRepository
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.persistence.worker_result_repository import WorkerResultRepository
from chattocodex.repository.project_path_resolver import ProjectPathResolver
from chattocodex.repository.worker_file_mutation_service import (
    WorkerFileMutationService,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService
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


def _create_project(tmp_path: Path) -> tuple[Path, Path, str]:
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
    return (
        workspace_root,
        project_root,
        _git(cwd=project_root, args=("rev-parse", "HEAD")),
    )


def _build_runtime(
    *, tmp_path: Path, workspace_root: Path
) -> tuple[
    RunOrchestrationService,
    BrokerService,
    WorkerGitWorktreeService,
    WorkerFileMutationService,
    WorkerResultCommitService,
    GitWorktreeRepository,
]:
    state_root = tmp_path / "state"
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = TaskStateRepository(state_root=state_root)
    worktree_repository = GitWorktreeRepository(state_root=state_root)
    result_repository = WorkerResultRepository(state_root=state_root)
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
            worktree_repository=worktree_repository,
            command_runner=GitCommandRunner(),
            lock_registry=locks,
        ),
        WorkerFileMutationService(
            worker_repository=worker_repository,
            worktree_repository=worktree_repository,
            path_resolver=ProjectPathResolver(),
            lock_registry=locks,
        ),
        WorkerResultCommitService(
            worker_repository=worker_repository,
            worktree_repository=worktree_repository,
            result_repository=result_repository,
            commit_runner=GitResultCommitRunner(),
            lock_registry=locks,
        ),
        worktree_repository,
    )


def test_two_worker_results_are_distinct_commits_with_same_parent(
    tmp_path: Path,
) -> None:
    workspace_root, project_root, base_commit = _create_project(tmp_path)
    (
        run_service,
        broker_service,
        worktree_service,
        mutation_service,
        result_service,
        _worktree_repository,
    ) = _build_runtime(tmp_path=tmp_path, workspace_root=workspace_root)
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    run = run_service.create_run(
        goal="Commit worker results", project_id=project.project_id
    )
    workers: list[tuple[str, str, Path]] = []
    for index in range(1, 3):
        claim = claim_broker_task(
            service=broker_service,
            run_id=run.state.run_id,
            run_handle=run.run_handle,
            project_id=project.project_id,
            objective=f"Write result {index}",
            role=f"writer-{index}",
            label=f"consumer-{index}",
        )
        worker = claim.assignment.worker
        worker_handle = claim.assignment.worker_handle
        workspace = worktree_service.prepare(
            run_id=run.state.run_id,
            worker_id=worker.worker_id,
            worker_handle=worker_handle,
        )
        mutation_service.replace_file(
            run_id=run.state.run_id,
            worker_id=worker.worker_id,
            worker_handle=worker_handle,
            relative_path="README.md",
            expected_sha256=hashlib.sha256(b"base\n").hexdigest(),
            content=f"worker-{index}\n",
        )
        workers.append((worker.worker_id, worker_handle, Path(workspace.worktree_root)))

    hooks_root = project_root / ".git" / "hooks"
    hooks_root.mkdir(exist_ok=True)
    pre_commit = hooks_root / "pre-commit"
    pre_commit.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
    pre_commit.chmod(0o755)

    results = [
        result_service.commit(
            run_id=run.state.run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
            summary=f"worker {index} result",
        )
        for index, (worker_id, worker_handle, _worktree_root) in enumerate(
            workers, start=1
        )
    ]

    assert {result.base_commit for result in results} == {base_commit}
    assert len({result.commit_sha for result in results}) == 2
    assert {result.changed_paths for result in results} == {("README.md",)}
    assert _git(cwd=project_root, args=("rev-parse", "HEAD")) == base_commit
    assert (project_root / "README.md").read_text(encoding="utf-8") == "base\n"
    for result, (_worker_id, _worker_handle, worktree_root) in zip(
        results, workers, strict=True
    ):
        assert _git(cwd=worktree_root, args=("rev-parse", "HEAD")) == result.commit_sha
        assert _git(cwd=worktree_root, args=("rev-parse", "HEAD^")) == base_commit
        assert _git(cwd=worktree_root, args=("status", "--porcelain=v1")) == ""

    listed = result_service.list_for_run(run_id=run.state.run_id)
    assert {result.commit_sha for result in listed} == {
        result.commit_sha for result in results
    }

    first_worker_id, first_worker_handle, _ = workers[0]
    repeated = result_service.commit(
        run_id=run.state.run_id,
        worker_id=first_worker_id,
        worker_handle=first_worker_handle,
        summary="worker 1 result",
    )
    assert repeated.commit_sha == results[0].commit_sha

    with pytest.raises(InvalidTransitionError):
        mutation_service.create_file(
            run_id=run.state.run_id,
            worker_id=first_worker_id,
            worker_handle=first_worker_handle,
            relative_path="after-finalize.txt",
            content="denied\n",
        )


def test_result_commit_rejects_workspace_without_changes(tmp_path: Path) -> None:
    workspace_root, _project_root, _base_commit = _create_project(tmp_path)
    run_service, broker_service, worktree_service, _mutation, result_service, _repo = (
        _build_runtime(tmp_path=tmp_path, workspace_root=workspace_root)
    )
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    run = run_service.create_run(goal="No changes", project_id=project.project_id)
    claim = claim_broker_task(
        service=broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=project.project_id,
        objective="Prepare empty result",
        role="writer",
        label="consumer-empty",
    )
    worker = claim.assignment.worker
    worker_handle = claim.assignment.worker_handle
    worktree_service.prepare(
        run_id=run.state.run_id,
        worker_id=worker.worker_id,
        worker_handle=worker_handle,
    )

    with pytest.raises(GitWorktreeError, match="no changes"):
        result_service.commit(
            run_id=run.state.run_id,
            worker_id=worker.worker_id,
            worker_handle=worker_handle,
            summary="empty result",
        )


def test_result_commit_waits_for_in_flight_worker_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_root, _project_root, _base_commit = _create_project(tmp_path)
    (
        run_service,
        broker_service,
        worktree_service,
        mutation_service,
        result_service,
        _worktree_repository,
    ) = _build_runtime(tmp_path=tmp_path, workspace_root=workspace_root)
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    run = run_service.create_run(
        goal="Linearize mutation and result commit",
        project_id=project.project_id,
    )
    claim = claim_broker_task(
        service=broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=project.project_id,
        objective="Race mutation against result commit",
        role="writer",
        label="consumer-race",
    )
    worker = claim.assignment.worker
    worker_handle = claim.assignment.worker_handle
    workspace = worktree_service.prepare(
        run_id=run.state.run_id,
        worker_id=worker.worker_id,
        worker_handle=worker_handle,
    )
    mutation_service.create_file(
        run_id=run.state.run_id,
        worker_id=worker.worker_id,
        worker_handle=worker_handle,
        relative_path="initial.txt",
        content="initial\n",
    )

    preliminary_check_complete = threading.Event()
    allow_mutation = threading.Event()
    commit_complete = threading.Event()
    original_require_workspace = mutation_service._require_worker_workspace

    def pause_after_preliminary_check(
        *, run_id: str, worker_id: str, worker_handle: str
    ) -> Path:
        worktree_root = original_require_workspace(
            run_id=run_id,
            worker_id=worker_id,
            worker_handle=worker_handle,
        )
        preliminary_check_complete.set()
        assert allow_mutation.wait(timeout=5)
        return worktree_root

    monkeypatch.setattr(
        mutation_service,
        "_require_worker_workspace",
        pause_after_preliminary_check,
    )
    mutation_errors: list[BaseException] = []
    commit_errors: list[BaseException] = []
    committed_results = []

    def mutate() -> None:
        try:
            mutation_service.create_file(
                run_id=run.state.run_id,
                worker_id=worker.worker_id,
                worker_handle=worker_handle,
                relative_path="late.txt",
                content="late\n",
            )
        except BaseException as error:
            mutation_errors.append(error)

    def commit() -> None:
        try:
            committed_results.append(
                result_service.commit(
                    run_id=run.state.run_id,
                    worker_id=worker.worker_id,
                    worker_handle=worker_handle,
                    summary="linearized result",
                )
            )
        except BaseException as error:
            commit_errors.append(error)
        finally:
            commit_complete.set()

    mutation_thread = threading.Thread(target=mutate)
    commit_thread = threading.Thread(target=commit)
    mutation_thread.start()
    assert preliminary_check_complete.wait(timeout=5)
    commit_thread.start()
    commit_finished_before_mutation = commit_complete.wait(timeout=1)
    allow_mutation.set()
    mutation_thread.join(timeout=5)
    commit_thread.join(timeout=5)

    assert not mutation_thread.is_alive()
    assert not commit_thread.is_alive()
    assert not commit_finished_before_mutation
    assert mutation_errors == []
    assert commit_errors == []
    assert len(committed_results) == 1
    assert set(committed_results[0].changed_paths) == {"initial.txt", "late.txt"}
    worktree_root = Path(workspace.worktree_root)
    assert _git(cwd=worktree_root, args=("status", "--porcelain=v1")) == ""
