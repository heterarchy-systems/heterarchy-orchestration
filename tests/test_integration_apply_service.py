import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.git.git_exceptions import GitWorktreeError
from chattocodex.git.git_result_dto import WorkerResultCommit
from chattocodex.integration.git_integration_runner import GitIntegrationRunner
from chattocodex.integration.integration_apply_service import IntegrationApplyService
from chattocodex.integration.integration_preview_service import (
    IntegrationPreviewService,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import RunStatus, TaskStatus
from chattocodex.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    StatePersistenceError,
)
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.orchestration.worker_execution_service import WorkerExecutionService
from chattocodex.persistence.broker_repository import BrokerTaskRepository
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.persistence.run_control_state_store import RunControlStateStore
from chattocodex.persistence.worker_result_repository import WorkerResultRepository
from chattocodex.run_control.run_control_identity import digest_capability
from chattocodex.run_control.run_control_state import (
    WorkflowRoundState,
    WorkflowState,
    WorkflowStatus,
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


def _create_repo(tmp_path: Path) -> tuple[Path, Path, str]:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    _git(cwd=project_root, args=("init",))
    _git(cwd=project_root, args=("config", "user.email", "test@example.com"))
    _git(cwd=project_root, args=("config", "user.name", "Test User"))
    (project_root / "README.md").write_text("base\n", encoding="utf-8")
    _git(cwd=project_root, args=("add", "README.md"))
    _git(cwd=project_root, args=("commit", "-m", "base"))
    return (
        workspace_root,
        project_root,
        _git(cwd=project_root, args=("rev-parse", "HEAD")),
    )


def _create_result_commit(
    *,
    project_root: Path,
    worktree_root: Path,
    base_commit: str,
    relative_path: str,
    content: str,
    message: str,
) -> str:
    _git(
        cwd=project_root,
        args=("worktree", "add", "--detach", str(worktree_root), base_commit),
    )
    target = worktree_root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    _git(cwd=worktree_root, args=("add", "--all"))
    tree_sha = _git(cwd=worktree_root, args=("write-tree",))
    commit_sha = _git(
        cwd=worktree_root,
        args=("commit-tree", tree_sha, "-p", base_commit, "-m", message),
    )
    _git(cwd=project_root, args=("worktree", "remove", "--force", str(worktree_root)))
    return commit_sha


class _Runtime:
    def __init__(self, *, tmp_path: Path) -> None:
        self.workspace_root, self.project_root, self.base_commit = _create_repo(
            tmp_path
        )
        self.state_root = tmp_path / "state"
        self.run_repository = RunStateRepository(state_root=self.state_root)
        self.worker_repository = WorkerStateRepository(state_root=self.state_root)
        self.task_repository = TaskStateRepository(state_root=self.state_root)
        self.broker_task_repository = BrokerTaskRepository(state_root=self.state_root)
        self.result_repository = WorkerResultRepository(state_root=self.state_root)
        self.locks = EntityLockRegistry()
        workspace_service = WorkspaceProjectService(workspace_root=self.workspace_root)
        self.project = workspace_service.list_projects()[0]
        self.run_service = RunOrchestrationService(
            run_repository=self.run_repository,
            workspace_project_service=workspace_service,
            command_runner=GitCommandRunner(),
        )
        self.worker_service = WorkerExecutionService(
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            lock_registry=self.locks,
        )
        self.broker_service = build_broker_service(
            state_root=self.state_root,
            workspace_root=self.workspace_root,
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            lock_registry=self.locks,
        )
        self.created_run = self.run_service.create_run(
            goal="Integrate two worker results",
            project_id=self.project.project_id,
        )
        self.apply_service = IntegrationApplyService(
            run_repository=self.run_repository,
            task_repository=self.task_repository,
            broker_task_repository=self.broker_task_repository,
            result_repository=self.result_repository,
            preview_service=IntegrationPreviewService(
                result_repository=self.result_repository
            ),
            runner=GitIntegrationRunner(command_runner=GitCommandRunner()),
            integration_root=self.state_root / "integrations",
            lock_registry=self.locks,
        )

    def add_worker_result(
        self,
        *,
        index: int,
        relative_path: str,
        content: str,
        complete_task: bool = True,
    ) -> str:
        claim = claim_broker_task(
            service=self.broker_service,
            run_id=self.created_run.state.run_id,
            run_handle=self.created_run.run_handle,
            project_id=self.project.project_id,
            objective=f"Create {relative_path}",
            role=f"worker-{index}",
            label=f"consumer-{index}",
        )
        worker = claim.assignment.worker
        task = claim.assignment.task
        worker_handle = claim.assignment.worker_handle
        if complete_task:
            self.worker_service.update_task(
                run_id=self.created_run.state.run_id,
                worker_id=worker.worker_id,
                worker_handle=worker_handle,
                task_id=task.task_id,
                expected_revision=0,
                status=TaskStatus.COMPLETED,
                progress_summary="done",
                result="result commit ready",
            )
            self.broker_service.finish_assignment(
                slot_id=claim.slot.state.slot_id,
                slot_handle=claim.slot.slot_handle,
            )
        commit_sha = _create_result_commit(
            project_root=self.project_root,
            worktree_root=self.state_root / f"result-{index}",
            base_commit=self.base_commit,
            relative_path=relative_path,
            content=content,
            message=f"worker {index}",
        )
        self.result_repository.save(
            result=WorkerResultCommit(
                run_id=self.created_run.state.run_id,
                worker_id=worker.worker_id,
                base_commit=self.base_commit,
                commit_sha=commit_sha,
                changed_paths=(relative_path,),
                summary=f"worker {index}",
                created_at=datetime.now(UTC),
            )
        )
        return commit_sha


def test_integration_apply_fast_forwards_clean_project(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.add_worker_result(index=1, relative_path="alpha.txt", content="alpha\n")
    runtime.add_worker_result(index=2, relative_path="beta.txt", content="beta\n")

    result = runtime.apply_service.apply(
        run_id=runtime.created_run.state.run_id,
        run_handle=runtime.created_run.run_handle,
    )

    assert result.base_commit == runtime.base_commit
    assert result.result_count == 2
    assert result.changed_paths == ("alpha.txt", "beta.txt")
    assert (
        _git(cwd=runtime.project_root, args=("rev-parse", "HEAD"))
        == result.integration_commit
    )
    assert (
        _git(cwd=runtime.project_root, args=("rev-parse", "HEAD^"))
        == runtime.base_commit
    )
    assert (runtime.project_root / "alpha.txt").read_text() == "alpha\n"
    assert (runtime.project_root / "beta.txt").read_text() == "beta\n"
    assert _git(cwd=runtime.project_root, args=("status", "--porcelain")) == ""
    assert not (
        runtime.state_root / "integrations" / runtime.created_run.state.run_id
    ).exists()
    completed_run = runtime.run_repository.read(run_id=runtime.created_run.state.run_id)
    assert completed_run.status is RunStatus.COMPLETED
    assert completed_run.revision == 1


def test_integration_apply_is_idempotent_after_run_completion(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.add_worker_result(index=1, relative_path="alpha.txt", content="alpha\n")
    runtime.add_worker_result(index=2, relative_path="beta.txt", content="beta\n")

    first = runtime.apply_service.apply(
        run_id=runtime.created_run.state.run_id,
        run_handle=runtime.created_run.run_handle,
    )
    first_head = _git(cwd=runtime.project_root, args=("rev-parse", "HEAD"))
    second = runtime.apply_service.apply(
        run_id=runtime.created_run.state.run_id,
        run_handle=runtime.created_run.run_handle,
    )

    assert second.integration_commit == first.integration_commit
    assert _git(cwd=runtime.project_root, args=("rev-parse", "HEAD")) == first_head
    assert _git(cwd=runtime.project_root, args=("rev-list", "--count", "HEAD")) == "2"
    completed_run = runtime.run_repository.read(run_id=runtime.created_run.state.run_id)
    assert completed_run.status is RunStatus.COMPLETED
    assert completed_run.revision == 1


def test_integration_apply_recovers_after_run_state_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.add_worker_result(index=1, relative_path="alpha.txt", content="alpha\n")
    runtime.add_worker_result(index=2, relative_path="beta.txt", content="beta\n")
    original_write = runtime.run_repository.write
    failed_once = False

    def fail_first_write(*, state):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise StatePersistenceError(
                operation="write_run",
                resource_id=state.run_id,
                message="Injected run state persistence failure.",
            )
        return original_write(state=state)

    monkeypatch.setattr(runtime.run_repository, "write", fail_first_write)
    with pytest.raises(StatePersistenceError, match="Injected"):
        runtime.apply_service.apply(
            run_id=runtime.created_run.state.run_id,
            run_handle=runtime.created_run.run_handle,
        )

    applied_head = _git(cwd=runtime.project_root, args=("rev-parse", "HEAD"))
    assert applied_head != runtime.base_commit
    stale_run = runtime.run_repository.read(run_id=runtime.created_run.state.run_id)
    assert stale_run.status is RunStatus.ACTIVE
    assert stale_run.revision == 0

    recovered = runtime.apply_service.apply(
        run_id=runtime.created_run.state.run_id,
        run_handle=runtime.created_run.run_handle,
    )
    assert recovered.integration_commit == applied_head
    assert _git(cwd=runtime.project_root, args=("rev-parse", "HEAD")) == applied_head
    completed_run = runtime.run_repository.read(run_id=runtime.created_run.state.run_id)
    assert completed_run.status is RunStatus.COMPLETED
    assert completed_run.revision == 1


def test_integration_apply_rejects_project_head_drift(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.add_worker_result(index=1, relative_path="alpha.txt", content="alpha\n")
    (runtime.project_root / "drift.txt").write_text("drift\n", encoding="utf-8")
    _git(cwd=runtime.project_root, args=("add", "drift.txt"))
    _git(cwd=runtime.project_root, args=("commit", "-m", "drift"))
    drift_head = _git(cwd=runtime.project_root, args=("rev-parse", "HEAD"))

    with pytest.raises(GitWorktreeError, match="HEAD moved"):
        runtime.apply_service.apply(
            run_id=runtime.created_run.state.run_id,
            run_handle=runtime.created_run.run_handle,
        )

    assert _git(cwd=runtime.project_root, args=("rev-parse", "HEAD")) == drift_head
    assert not (runtime.project_root / "alpha.txt").exists()


def test_integration_apply_rejects_incomplete_broker_tasks(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.add_worker_result(
        index=1,
        relative_path="alpha.txt",
        content="alpha\n",
        complete_task=False,
    )

    with pytest.raises(InvalidTransitionError, match="Broker Tasks must be completed"):
        runtime.apply_service.apply(
            run_id=runtime.created_run.state.run_id,
            run_handle=runtime.created_run.run_handle,
        )

    assert (
        _git(cwd=runtime.project_root, args=("rev-parse", "HEAD"))
        == runtime.base_commit
    )
    assert not (runtime.project_root / "alpha.txt").exists()


def test_integration_apply_requires_main_review_gate_approval(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.add_worker_result(index=1, relative_path="alpha.txt", content="alpha\n")
    runtime.add_worker_result(index=2, relative_path="beta.txt", content="beta\n")

    run_id = runtime.created_run.state.run_id
    broker_tasks = runtime.broker_task_repository.list_for_run(run_id=run_id)
    now = datetime.now(UTC)
    state_store = RunControlStateStore(state_root=runtime.state_root)
    waiting = state_store.create(
        state=WorkflowState(
            schema_version=1,
            workflow_id="wf_aaaaaaaaaaaaaaaaaaaaaaaa",
            workflow_handle_digest=digest_capability("wfh_test-review-gate-capability"),
            goal="Coordinate integration approval",
            project_id=runtime.project.project_id,
            review_instructions="Review worker output before integration.",
            max_rework_rounds=2,
            rework_rounds=0,
            status=WorkflowStatus.WAITING_MAIN_REVIEW,
            rounds=(
                WorkflowRoundState(
                    round_number=1,
                    run_id=run_id,
                    task_keys=("alpha", "beta"),
                    broker_task_ids=tuple(task.broker_task_id for task in broker_tasks),
                    created_at=now,
                ),
            ),
            last_review_summary=None,
            revision=0,
            created_at=now,
            updated_at=now,
        )
    )
    guarded = IntegrationApplyService(
        run_repository=runtime.run_repository,
        task_repository=runtime.task_repository,
        broker_task_repository=runtime.broker_task_repository,
        result_repository=runtime.result_repository,
        preview_service=IntegrationPreviewService(
            result_repository=runtime.result_repository
        ),
        runner=GitIntegrationRunner(command_runner=GitCommandRunner()),
        integration_root=runtime.state_root / "guarded-integrations",
        lock_registry=runtime.locks,
        run_control_state_store=state_store,
    )

    with pytest.raises(InvalidTransitionError, match="Main Review Gate"):
        guarded.apply(
            run_id=run_id,
            run_handle=runtime.created_run.run_handle,
        )

    state_store.write(
        state=WorkflowState(
            schema_version=waiting.schema_version,
            workflow_id=waiting.workflow_id,
            workflow_handle_digest=waiting.workflow_handle_digest,
            goal=waiting.goal,
            project_id=waiting.project_id,
            review_instructions=waiting.review_instructions,
            max_rework_rounds=waiting.max_rework_rounds,
            rework_rounds=waiting.rework_rounds,
            status=WorkflowStatus.COMPLETED,
            rounds=waiting.rounds,
            last_review_summary="Approved after inspecting results and tests.",
            revision=waiting.revision + 1,
            created_at=waiting.created_at,
            updated_at=datetime.now(UTC),
        )
    )
    applied = guarded.apply(
        run_id=run_id,
        run_handle=runtime.created_run.run_handle,
    )
    assert applied.result_count == 2
