import subprocess
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import Barrier, Event, current_thread

import pytest

from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskStatus,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    TaskState,
    TaskStatus,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    RevisionConflictError,
)
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.orchestration.worker_execution_service import (
    WorkerExecutionService,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_report_repository import (
    WorkerReportRepository,
)
from chattocodex.orchestrator_backend.report.worker_report_service import (
    WorkerReportService,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from tests.broker_support import build_broker_service, claim_broker_task


class _RecoveryRaceLockRegistry(EntityLockRegistry):
    def __init__(self, *, recovery_progress: Event) -> None:
        super().__init__()
        self._recovery_progress = recovery_progress

    @contextmanager
    def hold(self, *, entity_key: str) -> Iterator[None]:
        if current_thread().name.startswith("recovery") and entity_key.startswith(
            "task:"
        ):
            self._recovery_progress.set()
        with super().hold(entity_key=entity_key):
            yield


class _RecoveryRaceTaskRepository(TaskStateRepository):
    def __init__(
        self,
        *,
        state_root: Path,
        completion_write_started: Event,
        recovery_progress: Event,
    ) -> None:
        super().__init__(state_root=state_root)
        self._completion_write_started = completion_write_started
        self._recovery_progress = recovery_progress

    def write(self, *, state: TaskState, expected_revision: int) -> TaskState:
        if state.status is TaskStatus.COMPLETED:
            self._completion_write_started.set()
            if not self._recovery_progress.wait(timeout=5):
                raise AssertionError(
                    "Recovery did not reach the Task mutation boundary."
                )
        elif state.status is TaskStatus.FAILED:
            self._recovery_progress.set()
        return super().write(state=state, expected_revision=expected_revision)


class _FailingCompletionTaskRepository(TaskStateRepository):
    def write(self, *, state: TaskState, expected_revision: int) -> TaskState:
        if state.status is TaskStatus.COMPLETED:
            current = self.read(run_id=state.run_id, task_id=state.task_id)
            raise RevisionConflictError(
                operation="write_task",
                resource_id=state.task_id,
                current_revision=current.revision,
            )
        return super().write(state=state, expected_revision=expected_revision)


def _git(*, cwd: Path, args: tuple[str, ...]) -> None:
    subprocess.run(
        ("git", "-C", str(cwd), *args),
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_research_worker_submits_report_without_git_worktree_and_main_reads_it(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    _git(cwd=project_root, args=("init",))
    _git(cwd=project_root, args=("config", "user.email", "test@example.com"))
    _git(cwd=project_root, args=("config", "user.name", "Test User"))
    (project_root / "README.md").write_text("base\n", encoding="utf-8")
    _git(cwd=project_root, args=("add", "README.md"))
    _git(cwd=project_root, args=("commit", "-m", "base"))

    state_root = tmp_path / "state"
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = TaskStateRepository(state_root=state_root)
    report_repository = WorkerReportRepository(state_root=state_root)
    locks = EntityLockRegistry()
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project_id = workspace_service.list_projects()[0].project_id
    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_service,
        command_runner=GitCommandRunner(),
    )
    report_service = WorkerReportService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        report_repository=report_repository,
        lock_registry=locks,
    )
    broker_service = build_broker_service(
        state_root=state_root,
        workspace_root=workspace_root,
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=locks,
    )

    run = run_service.create_run(
        goal="Research current approaches", project_id=project_id
    )
    claim = claim_broker_task(
        service=broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=project_id,
        objective="Research three approaches and cite sources",
        role="research",
        label="research-consumer",
    )
    worker = claim.assignment.worker
    task = claim.assignment.task
    worker_handle = claim.assignment.worker_handle

    report = report_service.submit(
        run_id=run.state.run_id,
        worker_id=worker.worker_id,
        worker_handle=worker_handle,
        task_id=task.task_id,
        expected_revision=0,
        title="Research findings",
        summary="Approach B is the best fit.",
        content="Compared A, B, and C. B has the strongest evidence and simplest integration.",
        references=("https://example.com/a", "paper:example-b"),
    )

    assert report.task_id == task.task_id
    assert report.references == ("https://example.com/a", "paper:example-b")
    completed = task_repository.read(run_id=run.state.run_id, task_id=task.task_id)
    assert completed.status is TaskStatus.COMPLETED
    assert completed.revision == 1
    assert completed.result == f"worker_report:{task.task_id}"
    assert report_service.list_for_run(run_id=run.state.run_id) == (report,)
    assert not (state_root / "runs" / run.state.run_id / "worktrees").exists()

    finished = broker_service.finish_assignment(
        slot_id=claim.slot.state.slot_id,
        slot_handle=claim.slot.slot_handle,
    )
    assert finished.broker_task.status.value == "completed"

    retried = report_service.submit(
        run_id=run.state.run_id,
        worker_id=worker.worker_id,
        worker_handle=worker_handle,
        task_id=task.task_id,
        expected_revision=0,
        title="Research findings",
        summary="Approach B is the best fit.",
        content="Compared A, B, and C. B has the strongest evidence and simplest integration.",
        references=("https://example.com/a", "paper:example-b"),
    )
    assert retried == report

    with pytest.raises(ResourceAlreadyExistsError):
        report_service.submit(
            run_id=run.state.run_id,
            worker_id=worker.worker_id,
            worker_handle=worker_handle,
            task_id=task.task_id,
            expected_revision=1,
            title="Changed research",
            summary="Different payload",
            content="A conflicting second report must not overwrite the first report.",
            references=(),
        )


def test_task_update_and_report_submission_share_one_task_cas_boundary(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    _git(cwd=project_root, args=("init",))
    _git(cwd=project_root, args=("config", "user.email", "test@example.com"))
    _git(cwd=project_root, args=("config", "user.name", "Test User"))
    (project_root / "README.md").write_text("base\n", encoding="utf-8")
    _git(cwd=project_root, args=("add", "README.md"))
    _git(cwd=project_root, args=("commit", "-m", "base"))

    state_root = tmp_path / "state"
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = TaskStateRepository(state_root=state_root)
    report_repository = WorkerReportRepository(state_root=state_root)
    locks = EntityLockRegistry()
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project_id = workspace_service.list_projects()[0].project_id
    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_service,
        command_runner=GitCommandRunner(),
    )
    worker_service = WorkerExecutionService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=locks,
    )
    report_service = WorkerReportService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        report_repository=report_repository,
        lock_registry=locks,
    )
    broker_service = build_broker_service(
        state_root=state_root,
        workspace_root=workspace_root,
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=locks,
    )
    run = run_service.create_run(goal="Race Task writers", project_id=project_id)
    claim = claim_broker_task(
        service=broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=project_id,
        objective="Complete through exactly one Task mutation path",
        role="research",
        label="race-consumer",
    )
    task = claim.assignment.task
    worker = claim.assignment.worker
    barrier = Barrier(2)

    def update_task() -> str:
        barrier.wait()
        try:
            worker_service.update_task(
                run_id=run.state.run_id,
                worker_id=worker.worker_id,
                worker_handle=claim.assignment.worker_handle,
                task_id=task.task_id,
                expected_revision=0,
                status=TaskStatus.COMPLETED,
                progress_summary="Task update won.",
                result="task-update-result",
            )
        except RevisionConflictError:
            return "task-conflict"
        return "task-won"

    def submit_report() -> str:
        barrier.wait()
        try:
            report_service.submit(
                run_id=run.state.run_id,
                worker_id=worker.worker_id,
                worker_handle=claim.assignment.worker_handle,
                task_id=task.task_id,
                expected_revision=0,
                title="Race report",
                summary="Report submission won.",
                content="One immutable report.",
                references=(),
            )
        except RevisionConflictError:
            return "report-conflict"
        return "report-won"

    with ThreadPoolExecutor(max_workers=2) as executor:
        task_future = executor.submit(update_task)
        report_future = executor.submit(submit_report)
        outcomes = {task_future.result(), report_future.result()}

    persisted_task = task_repository.read(
        run_id=run.state.run_id,
        task_id=task.task_id,
    )
    persisted_report = report_repository.read(
        run_id=run.state.run_id,
        task_id=task.task_id,
    )
    assert persisted_task.status is TaskStatus.COMPLETED
    assert persisted_task.revision == 1
    if "task-won" in outcomes:
        assert outcomes == {"task-won", "report-conflict"}
        assert persisted_task.result == "task-update-result"
        assert persisted_report is None
    else:
        assert outcomes == {"task-conflict", "report-won"}
        assert persisted_task.result == f"worker_report:{task.task_id}"
        assert persisted_report is not None
        assert persisted_report.task_id == task.task_id


def test_report_submission_and_assignment_recovery_share_canonical_task_lock(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    _git(cwd=project_root, args=("init",))
    _git(cwd=project_root, args=("config", "user.email", "test@example.com"))
    _git(cwd=project_root, args=("config", "user.name", "Test User"))
    (project_root / "README.md").write_text("base\n", encoding="utf-8")
    _git(cwd=project_root, args=("add", "README.md"))
    _git(cwd=project_root, args=("commit", "-m", "base"))

    state_root = tmp_path / "state"
    completion_write_started = Event()
    recovery_progress = Event()
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = _RecoveryRaceTaskRepository(
        state_root=state_root,
        completion_write_started=completion_write_started,
        recovery_progress=recovery_progress,
    )
    report_repository = WorkerReportRepository(state_root=state_root)
    locks = _RecoveryRaceLockRegistry(recovery_progress=recovery_progress)
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project_id = workspace_service.list_projects()[0].project_id
    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_service,
        command_runner=GitCommandRunner(),
    )
    report_service = WorkerReportService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        report_repository=report_repository,
        lock_registry=locks,
    )
    broker_service = build_broker_service(
        state_root=state_root,
        workspace_root=workspace_root,
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=locks,
    )
    run = run_service.create_run(goal="Race report and recovery", project_id=project_id)
    claim = claim_broker_task(
        service=broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=project_id,
        objective="Persist either one report completion or one recovered failure",
        role="research",
        label="race-consumer",
    )
    task = claim.assignment.task
    worker = claim.assignment.worker

    with (
        ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="report"
        ) as report_executor,
        ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="recovery"
        ) as recovery_executor,
    ):
        report_future = report_executor.submit(
            report_service.submit,
            run_id=run.state.run_id,
            worker_id=worker.worker_id,
            worker_handle=claim.assignment.worker_handle,
            task_id=task.task_id,
            expected_revision=0,
            title="Race report",
            summary="The report path acquired the Task lock first.",
            content="The recovery path must observe the completed Task afterward.",
            references=(),
        )
        assert completion_write_started.wait(timeout=5)
        recovery_future = recovery_executor.submit(
            broker_service.recover_abandoned_assignment,
            slot_id=claim.slot.state.slot_id,
            reason="deterministic race test",
        )
        report = report_future.result(timeout=5)
        recovered_slot = recovery_future.result(timeout=5)

    persisted_task = task_repository.read(
        run_id=run.state.run_id,
        task_id=task.task_id,
    )
    persisted_report = report_repository.read(
        run_id=run.state.run_id,
        task_id=task.task_id,
    )
    snapshot = broker_service.get_run_status(run_id=run.state.run_id)

    assert report == persisted_report
    assert persisted_task.status is TaskStatus.COMPLETED
    assert persisted_task.result == f"worker_report:{task.task_id}"
    assert recovered_slot.status is WorkerSlotStatus.IDLE
    assert snapshot.broker_tasks[0].status is BrokerTaskStatus.COMPLETED
    assert (persisted_report is not None) == (
        persisted_task.status is TaskStatus.COMPLETED
        and persisted_task.result == f"worker_report:{task.task_id}"
    )


def test_new_report_is_rolled_back_when_task_completion_write_fails(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    _git(cwd=project_root, args=("init",))
    _git(cwd=project_root, args=("config", "user.email", "test@example.com"))
    _git(cwd=project_root, args=("config", "user.name", "Test User"))
    (project_root / "README.md").write_text("base\n", encoding="utf-8")
    _git(cwd=project_root, args=("add", "README.md"))
    _git(cwd=project_root, args=("commit", "-m", "base"))

    state_root = tmp_path / "state"
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = _FailingCompletionTaskRepository(state_root=state_root)
    report_repository = WorkerReportRepository(state_root=state_root)
    locks = EntityLockRegistry()
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project_id = workspace_service.list_projects()[0].project_id
    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_service,
        command_runner=GitCommandRunner(),
    )
    report_service = WorkerReportService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        report_repository=report_repository,
        lock_registry=locks,
    )
    broker_service = build_broker_service(
        state_root=state_root,
        workspace_root=workspace_root,
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=locks,
    )
    run = run_service.create_run(
        goal="Rollback failed report saga", project_id=project_id
    )
    claim = claim_broker_task(
        service=broker_service,
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        project_id=project_id,
        objective="Leave no report unless Task completion commits",
        role="research",
        label="rollback-consumer",
    )
    task = claim.assignment.task
    worker = claim.assignment.worker

    with pytest.raises(RevisionConflictError):
        report_service.submit(
            run_id=run.state.run_id,
            worker_id=worker.worker_id,
            worker_handle=claim.assignment.worker_handle,
            task_id=task.task_id,
            expected_revision=0,
            title="Rollback report",
            summary="This completion write is injected to fail.",
            content="The report file must be removed conditionally.",
            references=(),
        )

    persisted_task = task_repository.read(
        run_id=run.state.run_id,
        task_id=task.task_id,
    )
    assert persisted_task.status is TaskStatus.IN_PROGRESS
    assert persisted_task.revision == 0
    assert (
        report_repository.read(
            run_id=run.state.run_id,
            task_id=task.task_id,
        )
        is None
    )
