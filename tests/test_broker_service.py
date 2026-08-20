import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

import chattocodex.orchestrator_backend.broker.broker_service as broker_service_module
from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskStatus,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    TaskStatus,
    WorkerStatus,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.orchestration.worker_execution_service import (
    WorkerExecutionService,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from tests.broker_support import FakeWorkBrokerGateway


class _Runtime:
    def __init__(self, *, tmp_path: Path) -> None:
        self.state_root = tmp_path / "state"
        self.workspace_root = tmp_path / "workspace"
        project_root = self.workspace_root / "sample-project"
        project_root.mkdir(parents=True)
        (project_root / "pyproject.toml").write_text(
            "[project]\nname = 'sample-project'\nversion = '0.1.0'\n"
        )
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

        self.workspace_service = WorkspaceProjectService(
            workspace_root=self.workspace_root
        )
        self.project_id = self.workspace_service.list_projects()[0].project_id
        self.run_repository = RunStateRepository(state_root=self.state_root)
        self.worker_repository = WorkerStateRepository(state_root=self.state_root)
        self.task_repository = TaskStateRepository(state_root=self.state_root)
        self.broker_task_repository = BrokerTaskRepository(state_root=self.state_root)
        self.slot_repository = WorkerSlotRepository(state_root=self.state_root)
        self.work_broker_gateway = FakeWorkBrokerGateway()
        locks = EntityLockRegistry()
        self.run_service = RunOrchestrationService(
            run_repository=self.run_repository,
            workspace_project_service=self.workspace_service,
            command_runner=GitCommandRunner(),
        )
        self.worker_service = WorkerExecutionService(
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            lock_registry=locks,
        )
        self.broker_service = BrokerService(
            work_broker_gateway=self.work_broker_gateway,
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            broker_task_repository=self.broker_task_repository,
            slot_repository=self.slot_repository,
            workspace_project_service=self.workspace_service,
            lock_registry=locks,
        )

    def create_run(self):
        return self.run_service.create_run(
            goal="Broker consumer-group test",
            project_id=self.project_id,
        )


def _complete_assignment(runtime: _Runtime, *, assignment) -> None:
    runtime.worker_service.update_task(
        run_id=assignment.run.run_id,
        worker_id=assignment.worker.worker_id,
        worker_handle=assignment.worker_handle,
        task_id=assignment.task.task_id,
        expected_revision=assignment.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="Broker assignment completed.",
        result=f"done:{assignment.broker_task.role}",
    )


def test_generic_slot_reuses_same_logical_broker_consumer_for_dynamic_roles(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    created_slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="generic-chat-1",
    )

    first = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Review product scope.",
        role="research",
        depends_on=(),
    )
    assignment_one = runtime.broker_service.claim_task(
        slot_id=created_slot.state.slot_id,
        slot_handle=created_slot.slot_handle,
    )
    assert assignment_one is not None
    assert assignment_one.broker_task.broker_task_id == first.broker_task_id
    assert assignment_one.worker.role == "research"
    assert assignment_one.worker.status is WorkerStatus.ACTIVE
    assert assignment_one.task.status is TaskStatus.IN_PROGRESS
    assert assignment_one.slot.status is WorkerSlotStatus.BUSY
    assert created_slot.state.broker_consumer_id != created_slot.state.slot_id
    assert (
        runtime.work_broker_gateway.lease_member_id(
            run_id=run.state.run_id,
            task_id=first.broker_task_id,
        )
        == created_slot.state.broker_consumer_id
    )

    _complete_assignment(runtime, assignment=assignment_one)
    released_one = runtime.broker_service.finish_assignment(
        slot_id=created_slot.state.slot_id,
        slot_handle=created_slot.slot_handle,
    )
    assert released_one.broker_task.status is BrokerTaskStatus.COMPLETED
    assert released_one.slot.status is WorkerSlotStatus.IDLE

    second = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Review persistence boundaries.",
        role="backend",
        depends_on=(),
    )
    assignment_two = runtime.broker_service.claim_task(
        slot_id=created_slot.state.slot_id,
        slot_handle=created_slot.slot_handle,
    )
    assert assignment_two is not None
    assert assignment_two.broker_task.broker_task_id == second.broker_task_id
    assert assignment_two.slot.slot_id == assignment_one.slot.slot_id
    assert (
        assignment_two.slot.broker_consumer_id
        == assignment_one.slot.broker_consumer_id
        == created_slot.state.broker_consumer_id
    )
    assert assignment_two.worker.worker_id != assignment_one.worker.worker_id
    assert assignment_two.worker.role == "backend"

    slot_json = (
        runtime.state_root / "broker" / "slots" / f"{created_slot.state.slot_id}.json"
    ).read_text()
    assert created_slot.slot_handle not in slot_json


def test_dependency_blocks_downstream_until_prerequisite_finishes(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    slot_one = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="consumer-a",
    )
    slot_two = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="consumer-b",
    )
    prerequisite = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Collect evidence.",
        role="analyst",
        depends_on=(),
    )
    downstream = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Synthesize the evidence.",
        role="architect",
        depends_on=(prerequisite.broker_task_id,),
    )

    assignment = runtime.broker_service.claim_task(
        slot_id=slot_one.state.slot_id,
        slot_handle=slot_one.slot_handle,
    )
    assert assignment is not None
    assert assignment.broker_task.broker_task_id == prerequisite.broker_task_id

    assert (
        runtime.broker_service.claim_task(
            slot_id=slot_two.state.slot_id,
            slot_handle=slot_two.slot_handle,
        )
        is None
    )

    _complete_assignment(runtime, assignment=assignment)
    runtime.broker_service.finish_assignment(
        slot_id=slot_one.state.slot_id,
        slot_handle=slot_one.slot_handle,
    )

    downstream_assignment = runtime.broker_service.claim_task(
        slot_id=slot_two.state.slot_id,
        slot_handle=slot_two.slot_handle,
    )
    assert downstream_assignment is not None
    assert downstream_assignment.broker_task.broker_task_id == downstream.broker_task_id
    assert downstream_assignment.worker.role == "architect"


def test_slot_capability_and_busy_state_prevent_double_claim(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label=None,
    )
    runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="First task.",
        role=None,
        depends_on=(),
    )
    runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Second task.",
        role=None,
        depends_on=(),
    )

    with pytest.raises(CapabilityDeniedError):
        runtime.broker_service.claim_task(
            slot_id=slot.state.slot_id,
            slot_handle="slh_invalid-invalid-invalid-invalid-invalid-invalid",
        )

    assert (
        runtime.broker_service.claim_task(
            slot_id=slot.state.slot_id,
            slot_handle=slot.slot_handle,
        )
        is not None
    )
    with pytest.raises(InvalidTransitionError):
        runtime.broker_service.claim_task(
            slot_id=slot.state.slot_id,
            slot_handle=slot.slot_handle,
        )


def test_reclaim_slot_rotates_capability_and_rejects_old_handle(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="persistent-worker",
    )

    reclaimed = runtime.broker_service.reclaim_slot_for_wake(
        slot_id=slot.state.slot_id,
        project_id=runtime.project_id,
    )

    assert reclaimed.state.slot_id == slot.state.slot_id
    assert reclaimed.state.revision == slot.state.revision + 1
    assert reclaimed.slot_handle != slot.slot_handle
    with pytest.raises(CapabilityDeniedError):
        runtime.broker_service.get_slot(
            slot_id=slot.state.slot_id,
            slot_handle=slot.slot_handle,
        )
    assert (
        runtime.broker_service.get_slot(
            slot_id=reclaimed.state.slot_id,
            slot_handle=reclaimed.slot_handle,
        )
        == reclaimed.state
    )


def test_cross_project_reclaim_leaves_slot_unchanged(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    other_root = runtime.workspace_root / "other-project"
    other_root.mkdir()
    (other_root / "pyproject.toml").write_text(
        "[project]\nname = 'other-project'\nversion = '0.1.0'\n"
    )
    other_project_id = next(
        project.project_id
        for project in runtime.workspace_service.list_projects()
        if project.project_id != runtime.project_id
    )
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="project-owned-worker",
    )

    with pytest.raises(InvalidTransitionError):
        runtime.broker_service.reclaim_slot_for_wake(
            slot_id=slot.state.slot_id,
            project_id=other_project_id,
        )

    assert (
        runtime.broker_service.get_slot(
            slot_id=slot.state.slot_id,
            slot_handle=slot.slot_handle,
        )
        == slot.state
    )


def test_reclaim_serializes_capability_rotation_before_stale_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="racing-worker",
    )
    published = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Exercise claim and wake-reclaim serialization.",
        role="race-test",
        depends_on=(),
    )

    reclaim_holds_locks = Event()
    allow_reclaim = Event()
    claim_reached_task_selection = Event()
    allow_claim = Event()
    original_new_slot_handle = broker_service_module._new_slot_handle
    claim_service = runtime.broker_service._assignment_claim_service
    original_next_ready_task = claim_service._next_ready_task

    def paused_new_slot_handle() -> str:
        reclaim_holds_locks.set()
        assert allow_reclaim.wait(timeout=5)
        return original_new_slot_handle()

    def paused_next_ready_task(*, project_id: str, run_id: str | None = None):
        claim_reached_task_selection.set()
        assert allow_claim.wait(timeout=5)
        return original_next_ready_task(project_id=project_id, run_id=run_id)

    monkeypatch.setattr(
        broker_service_module,
        "_new_slot_handle",
        paused_new_slot_handle,
    )
    monkeypatch.setattr(
        claim_service,
        "_next_ready_task",
        paused_next_ready_task,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        reclaim_future = pool.submit(
            runtime.broker_service.reclaim_slot_for_wake,
            slot_id=slot.state.slot_id,
            project_id=runtime.project_id,
        )
        assert reclaim_holds_locks.wait(timeout=5)
        claim_future = pool.submit(
            runtime.broker_service.claim_task,
            slot_id=slot.state.slot_id,
            slot_handle=slot.slot_handle,
        )
        stale_claim_entered_transition = claim_reached_task_selection.wait(timeout=0.5)
        allow_reclaim.set()
        reclaimed = reclaim_future.result(timeout=5)
        allow_claim.set()
        with pytest.raises(CapabilityDeniedError):
            claim_future.result(timeout=5)

    assert not stale_claim_entered_transition
    with pytest.raises(CapabilityDeniedError):
        runtime.broker_service.get_slot(
            slot_id=slot.state.slot_id,
            slot_handle=slot.slot_handle,
        )
    assert (
        runtime.broker_service.get_slot(
            slot_id=reclaimed.state.slot_id,
            slot_handle=reclaimed.slot_handle,
        )
        == reclaimed.state
    )

    assignment = runtime.broker_service.claim_task(
        slot_id=reclaimed.state.slot_id,
        slot_handle=reclaimed.slot_handle,
    )
    assert assignment is not None
    assert assignment.broker_task.broker_task_id == published.broker_task_id
    assert assignment.slot.slot_handle_digest == reclaimed.state.slot_handle_digest


def test_claim_serializes_assignment_before_competing_reclaim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    registered = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="claim-first-worker",
    )
    current = runtime.broker_service.reclaim_slot_for_wake(
        slot_id=registered.state.slot_id,
        project_id=runtime.project_id,
    )
    published = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Exercise claim-first serialization against wake reclaim.",
        role="race-test",
        depends_on=(),
    )

    claim_holds_locks = Event()
    allow_claim = Event()
    reclaim_started = Event()
    claim_service = runtime.broker_service._assignment_claim_service
    original_next_ready_task = claim_service._next_ready_task

    def paused_next_ready_task(*, project_id: str, run_id: str | None = None):
        claim_holds_locks.set()
        assert allow_claim.wait(timeout=5)
        return original_next_ready_task(project_id=project_id, run_id=run_id)

    def reclaim_after_claim_starts():
        reclaim_started.set()
        return runtime.broker_service.reclaim_slot_for_wake(
            slot_id=current.state.slot_id,
            project_id=runtime.project_id,
        )

    monkeypatch.setattr(
        claim_service,
        "_next_ready_task",
        paused_next_ready_task,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        claim_future = pool.submit(
            runtime.broker_service.claim_task,
            slot_id=current.state.slot_id,
            slot_handle=current.slot_handle,
        )
        assert claim_holds_locks.wait(timeout=5)
        reclaim_future = pool.submit(reclaim_after_claim_starts)
        assert reclaim_started.wait(timeout=5)
        assert not reclaim_future.done()

        allow_claim.set()
        assignment = claim_future.result(timeout=5)
        with pytest.raises(InvalidTransitionError):
            reclaim_future.result(timeout=5)

    assert assignment is not None
    assert assignment.broker_task.broker_task_id == published.broker_task_id
    assert (
        runtime.broker_service.get_slot(
            slot_id=current.state.slot_id,
            slot_handle=current.slot_handle,
        )
        == assignment.slot
    )
    with pytest.raises(CapabilityDeniedError):
        runtime.broker_service.get_slot(
            slot_id=registered.state.slot_id,
            slot_handle=registered.slot_handle,
        )

    broker_tasks = runtime.broker_task_repository.list_for_run(run_id=run.state.run_id)
    workers = runtime.worker_repository.list_for_run(run_id=run.state.run_id)
    tasks = runtime.task_repository.list_for_run(run_id=run.state.run_id)
    assert broker_tasks == (assignment.broker_task,)
    assert workers == (assignment.worker,)
    assert tasks == (assignment.task,)
    assert assignment.slot.status is WorkerSlotStatus.BUSY
    assert (
        assignment.slot.current_broker_task_id == assignment.broker_task.broker_task_id
    )
    assert assignment.slot.current_worker_id == assignment.worker.worker_id
    assert assignment.slot.current_task_id == assignment.task.task_id


def test_recover_abandoned_assignment_requeues_nonterminal_work(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="abandoned-consumer",
    )
    published = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Recover this work after the consumer disappears.",
        role="recovery-test",
        depends_on=(),
    )
    abandoned = runtime.broker_service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    assert abandoned is not None

    recovered_slot = runtime.broker_service.recover_abandoned_assignment(
        slot_id=slot.state.slot_id,
        reason="consumer lost",
    )

    old_task = runtime.task_repository.read(
        run_id=abandoned.run.run_id,
        task_id=abandoned.task.task_id,
    )
    broker_task = runtime.broker_task_repository.read(
        broker_task_id=published.broker_task_id
    )
    assert old_task.status is TaskStatus.FAILED
    assert old_task.progress_summary is not None
    assert "consumer lost" in old_task.progress_summary
    assert broker_task.status is BrokerTaskStatus.QUEUED
    assert broker_task.assigned_slot_id is None
    assert broker_task.assigned_worker_id is None
    assert broker_task.assigned_task_id is None
    assert recovered_slot.status is WorkerSlotStatus.IDLE
    runtime.work_broker_gateway.expire_task(
        run_id=run.state.run_id,
        task_id=published.broker_task_id,
    )

    replacement_slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="replacement-consumer",
    )
    replacement = runtime.broker_service.claim_task(
        slot_id=replacement_slot.state.slot_id,
        slot_handle=replacement_slot.slot_handle,
    )
    assert replacement is not None
    assert replacement.broker_task.broker_task_id == published.broker_task_id
    assert replacement.worker.worker_id != abandoned.worker.worker_id
    assert replacement.task.task_id != abandoned.task.task_id


@pytest.mark.parametrize("failing_repository", ["broker_task", "slot"])
def test_recover_abandoned_assignment_converges_after_partial_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_repository: str,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="abandoned-consumer",
    )
    published = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Converge recovery after a partial persistence failure.",
        role="recovery-test",
        depends_on=(),
    )
    abandoned = runtime.broker_service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    assert abandoned is not None

    repository = (
        runtime.broker_task_repository
        if failing_repository == "broker_task"
        else runtime.slot_repository
    )
    original_write = repository.write
    should_fail = True

    def fail_once(*, state):
        nonlocal should_fail
        if should_fail:
            should_fail = False
            raise StatePersistenceError(
                operation="injected_recovery_write",
                resource_id=published.broker_task_id,
                message="Injected partial-write failure.",
            )
        return original_write(state=state)

    monkeypatch.setattr(repository, "write", fail_once)

    with pytest.raises(StatePersistenceError):
        runtime.broker_service.recover_abandoned_assignment(
            slot_id=slot.state.slot_id,
            reason="consumer lost",
        )

    partial_task = runtime.task_repository.read(
        run_id=abandoned.run.run_id,
        task_id=abandoned.task.task_id,
    )
    partial_broker_task = runtime.broker_task_repository.read(
        broker_task_id=published.broker_task_id
    )
    partial_slot = runtime.slot_repository.read(slot_id=slot.state.slot_id)
    assert partial_task.status is TaskStatus.FAILED
    assert partial_slot.status is WorkerSlotStatus.BUSY
    if failing_repository == "broker_task":
        assert partial_broker_task.status is BrokerTaskStatus.CLAIMED
    else:
        assert partial_broker_task.status is BrokerTaskStatus.QUEUED

    recovered_slot = runtime.broker_service.recover_abandoned_assignment(
        slot_id=slot.state.slot_id,
        reason="consumer lost",
    )

    recovered_broker_task = runtime.broker_task_repository.read(
        broker_task_id=published.broker_task_id
    )
    assert recovered_broker_task.status is BrokerTaskStatus.QUEUED
    assert recovered_broker_task.assigned_slot_id is None
    assert recovered_broker_task.assigned_worker_id is None
    assert recovered_broker_task.assigned_task_id is None
    assert recovered_slot.status is WorkerSlotStatus.IDLE
    runtime.work_broker_gateway.expire_task(
        run_id=run.state.run_id,
        task_id=published.broker_task_id,
    )

    replacement_slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="replacement-consumer",
    )
    replacement = runtime.broker_service.claim_task(
        slot_id=replacement_slot.state.slot_id,
        slot_handle=replacement_slot.slot_handle,
    )
    assert replacement is not None
    assert replacement.broker_task.broker_task_id == published.broker_task_id
    assert replacement.task.task_id != abandoned.task.task_id


def test_recover_abandoned_assignment_preserves_terminal_result(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="completed-but-unreleased",
    )
    published = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Complete before the physical Worker disappears.",
        role="terminal-recovery",
        depends_on=(),
    )
    assignment = runtime.broker_service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    assert assignment is not None
    _complete_assignment(runtime, assignment=assignment)

    recovered_slot = runtime.broker_service.recover_abandoned_assignment(
        slot_id=slot.state.slot_id,
        reason="worker disappeared after completion",
    )

    broker_task = runtime.broker_task_repository.read(
        broker_task_id=published.broker_task_id
    )
    assert broker_task.status is BrokerTaskStatus.COMPLETED
    assert recovered_slot.status is WorkerSlotStatus.IDLE
    assert (
        runtime.broker_service.claim_task(
            slot_id=slot.state.slot_id,
            slot_handle=slot.slot_handle,
        )
        is None
    )


def test_recover_abandoned_terminal_assignment_converges_after_slot_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.create_run()
    slot = runtime.broker_service.register_slot(
        project_id=runtime.project_id,
        label="completed-but-unreleased",
    )
    published = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Complete before a recoverable slot persistence failure.",
        role="terminal-recovery",
        depends_on=(),
    )
    assignment = runtime.broker_service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    assert assignment is not None
    _complete_assignment(runtime, assignment=assignment)

    original_write = runtime.slot_repository.write
    should_fail = True

    def fail_once(*, state):
        nonlocal should_fail
        if should_fail:
            should_fail = False
            raise StatePersistenceError(
                operation="injected_recovery_write",
                resource_id=slot.state.slot_id,
                message="Injected slot-write failure.",
            )
        return original_write(state=state)

    monkeypatch.setattr(runtime.slot_repository, "write", fail_once)

    with pytest.raises(StatePersistenceError):
        runtime.broker_service.recover_abandoned_assignment(
            slot_id=slot.state.slot_id,
            reason="worker disappeared after completion",
        )

    partial_broker_task = runtime.broker_task_repository.read(
        broker_task_id=published.broker_task_id
    )
    assert partial_broker_task.status is BrokerTaskStatus.COMPLETED
    assert (
        runtime.slot_repository.read(slot_id=slot.state.slot_id).status
        is WorkerSlotStatus.BUSY
    )

    recovered_slot = runtime.broker_service.recover_abandoned_assignment(
        slot_id=slot.state.slot_id,
        reason="worker disappeared after completion",
    )

    assert recovered_slot.status is WorkerSlotStatus.IDLE
    assert (
        runtime.broker_task_repository.read(
            broker_task_id=published.broker_task_id
        ).revision
        == partial_broker_task.revision
    )
