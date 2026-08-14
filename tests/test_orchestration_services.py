import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier

import pytest

from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestration.orchestration_exceptions import (
    AssignmentMismatchError,
    CapabilityDeniedError,
    InvalidTransitionError,
    RevisionConflictError,
)
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.orchestration.worker_execution_service import WorkerExecutionService
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService
from tests.broker_support import build_broker_service, claim_broker_task


class _Runtime:
    def __init__(self, *, tmp_path: Path) -> None:
        self.state_root = tmp_path / "state"
        self.workspace_root = tmp_path / "workspace"
        project_root = self.workspace_root / "sample-project"
        project_root.mkdir(parents=True, exist_ok=True)
        (project_root / "pyproject.toml").write_text(
            "[project]\nname = 'sample-project'\nversion = '0.1.0'\n"
        )
        if not (project_root / ".git").exists():
            for args in (
                ("init",),
                ("config", "user.email", "test@example.com"),
                ("config", "user.name", "Test User"),
                ("add", "."),
                ("commit", "-m", "initial"),
            ):
                subprocess.run(
                    ("git", "-C", str(project_root), *args),
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
        workspace_service = WorkspaceProjectService(workspace_root=self.workspace_root)
        self.project_id = workspace_service.list_projects()[0].project_id
        self.run_repository = RunStateRepository(state_root=self.state_root)
        self.worker_repository = WorkerStateRepository(state_root=self.state_root)
        self.task_repository = TaskStateRepository(state_root=self.state_root)
        self.locks = EntityLockRegistry()
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


def test_broker_assignment_creates_active_execution_identity_and_survives_restart(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.run_service.create_run(
        goal="Broker execution primitives",
        project_id=runtime.project_id,
    )
    claim = claim_broker_task(
        service=runtime.broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=runtime.project_id,
        objective="Inspect persistence boundaries",
        role="backend",
        label="generic-consumer",
    )

    context = runtime.worker_service.get_worker_context(
        run_id=run.state.run_id,
        worker_id=claim.assignment.worker.worker_id,
        worker_handle=claim.assignment.worker_handle,
    )
    assert context.worker.role == "backend"
    assert context.tasks == (claim.assignment.task,)

    completed = runtime.worker_service.update_task(
        run_id=run.state.run_id,
        worker_id=context.worker.worker_id,
        worker_handle=claim.assignment.worker_handle,
        task_id=context.tasks[0].task_id,
        expected_revision=0,
        status=TaskStatus.COMPLETED,
        progress_summary="done",
        result="review complete",
    )
    assert completed.status is TaskStatus.COMPLETED
    runtime.broker_service.finish_assignment(
        slot_id=claim.slot.state.slot_id,
        slot_handle=claim.slot.slot_handle,
    )

    restarted = _Runtime(tmp_path=tmp_path)
    restored = restarted.worker_service.get_worker_context(
        run_id=run.state.run_id,
        worker_id=context.worker.worker_id,
        worker_handle=claim.assignment.worker_handle,
    )
    assert restored.tasks[0].status is TaskStatus.COMPLETED
    assert restored.tasks[0].result == "review complete"

    run_json = (
        runtime.state_root / "runs" / run.state.run_id / "orchestration.json"
    ).read_text()
    worker_json = (
        runtime.state_root
        / "runs"
        / run.state.run_id
        / "workers"
        / f"{context.worker.worker_id}.json"
    ).read_text()
    assert run.run_handle not in run_json
    assert claim.assignment.worker_handle not in worker_json


def test_task_update_enforces_capability_assignment_and_terminal_result(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.run_service.create_run(
        goal="Isolation", project_id=runtime.project_id
    )
    first = claim_broker_task(
        service=runtime.broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=runtime.project_id,
        objective="First task",
        role="first",
        label="consumer-first",
    )
    second = claim_broker_task(
        service=runtime.broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=runtime.project_id,
        objective="Second task",
        role="second",
        label="consumer-second",
    )

    with pytest.raises(CapabilityDeniedError):
        runtime.worker_service.get_worker_context(
            run_id=run.state.run_id,
            worker_id=first.assignment.worker.worker_id,
            worker_handle="wkh_invalid-invalid-invalid-invalid-invalid-invalid",
        )

    with pytest.raises(AssignmentMismatchError):
        runtime.worker_service.update_task(
            run_id=run.state.run_id,
            worker_id=first.assignment.worker.worker_id,
            worker_handle=first.assignment.worker_handle,
            task_id=second.assignment.task.task_id,
            expected_revision=0,
            status=TaskStatus.COMPLETED,
            progress_summary="wrong owner",
            result="must fail",
        )

    with pytest.raises(InvalidTransitionError, match="require a result"):
        runtime.worker_service.update_task(
            run_id=run.state.run_id,
            worker_id=first.assignment.worker.worker_id,
            worker_handle=first.assignment.worker_handle,
            task_id=first.assignment.task.task_id,
            expected_revision=0,
            status=TaskStatus.COMPLETED,
            progress_summary="missing result",
            result=None,
        )


def test_concurrent_task_update_allows_exactly_one_revision_winner(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.run_service.create_run(
        goal="Concurrent update", project_id=runtime.project_id
    )
    claim = claim_broker_task(
        service=runtime.broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=runtime.project_id,
        objective="Race one task revision",
        role="concurrent-worker",
        label="consumer-race",
    )
    barrier = Barrier(2)

    def update(result: str) -> str:
        barrier.wait()
        try:
            runtime.worker_service.update_task(
                run_id=run.state.run_id,
                worker_id=claim.assignment.worker.worker_id,
                worker_handle=claim.assignment.worker_handle,
                task_id=claim.assignment.task.task_id,
                expected_revision=0,
                status=TaskStatus.COMPLETED,
                progress_summary=result,
                result=result,
            )
        except RevisionConflictError:
            return "conflict"
        return "completed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(update, ("result-a", "result-b")))

    assert sorted(outcomes) == ["completed", "conflict"]
    persisted = runtime.task_repository.read(
        run_id=run.state.run_id,
        task_id=claim.assignment.task.task_id,
    )
    assert persisted.status is TaskStatus.COMPLETED
    assert persisted.revision == 1
    assert persisted.result in {"result-a", "result-b"}


def test_task_repository_write_rejects_a_stale_expected_revision(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.run_service.create_run(
        goal="Repository Task CAS", project_id=runtime.project_id
    )
    claim = claim_broker_task(
        service=runtime.broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=runtime.project_id,
        objective="Reject stale repository writes",
        role="concurrent-worker",
        label="repository-cas-consumer",
    )
    original = claim.assignment.task
    winner = replace(
        original,
        status=TaskStatus.BLOCKED,
        revision=1,
        progress_summary="first writer",
    )
    stale = replace(
        original,
        status=TaskStatus.COMPLETED,
        revision=1,
        progress_summary="stale writer",
        result="must not persist",
    )

    persisted = runtime.task_repository.write(
        state=winner,
        expected_revision=0,
    )
    with pytest.raises(RevisionConflictError) as conflict:
        runtime.task_repository.write(
            state=stale,
            expected_revision=0,
        )

    assert conflict.value.current_revision == 1
    assert (
        runtime.task_repository.read(
            run_id=run.state.run_id,
            task_id=original.task_id,
        )
        == persisted
    )
