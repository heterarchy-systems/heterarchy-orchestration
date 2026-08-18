import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

from chattocodex.broker.broker_dto import WorkerSlotStatus
from chattocodex.broker.broker_service import BrokerService
from chattocodex.facade.facade_dto import DelegationTaskSpec
from chattocodex.facade.facade_service import ChatFacadeService
from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.orchestration.worker_execution_service import WorkerExecutionService
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.persistence.worker_wake_capability_repository import (
    WorkerWakeCapabilityRepository,
)
from chattocodex.wake.wake_dto import WakeTargetStatus
from chattocodex.wake.wake_exceptions import WakeTargetConflictError
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.wake.worker_wake_capability_dto import WorkerWakeCapabilityStatus
from chattocodex.wake.worker_wake_capability_service import (
    WorkerWakeCapabilityService,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService
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

        workspace_service = WorkspaceProjectService(workspace_root=self.workspace_root)
        self.project_id = workspace_service.list_projects()[0].project_id
        self.run_repository = RunStateRepository(state_root=self.state_root)
        self.worker_repository = WorkerStateRepository(state_root=self.state_root)
        self.task_repository = TaskStateRepository(state_root=self.state_root)
        broker_task_repository = BrokerTaskRepository(state_root=self.state_root)
        slot_repository = WorkerSlotRepository(state_root=self.state_root)
        locks = EntityLockRegistry(lock_directory=self.state_root / "locks")
        run_service = RunOrchestrationService(
            run_repository=self.run_repository,
            workspace_project_service=workspace_service,
            command_runner=GitCommandRunner(),
        )
        self.worker_service = WorkerExecutionService(
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            lock_registry=locks,
        )
        self.broker_service = BrokerService(
            work_broker_gateway=FakeWorkBrokerGateway(),
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            broker_task_repository=broker_task_repository,
            slot_repository=slot_repository,
            workspace_project_service=workspace_service,
            lock_registry=locks,
        )
        self.wake_presence_service = WakePresenceService(
            repository=WakePresenceRepository(state_root=self.state_root)
        )
        self.wake_target_repository = WakeTargetRepository(state_root=self.state_root)
        self.wake_target_service = WakeTargetService(
            repository=self.wake_target_repository,
            presence_service=self.wake_presence_service,
            lock_registry=locks,
        )
        self.worker_wake_capability_service = WorkerWakeCapabilityService(
            repository=WorkerWakeCapabilityRepository(state_root=self.state_root),
            lock_registry=locks,
            physical_target_exists=lambda wake_target_id: (
                self.wake_target_repository.exists(wake_target_id=wake_target_id)
            ),
        )
        self.facade = ChatFacadeService(
            run_service=run_service,
            broker_service=self.broker_service,
            wake_target_service=self.wake_target_service,
            workspace_project_service=workspace_service,
            worker_wake_capability_service=self.worker_wake_capability_service,
        )


def _active_wake_target(*, runtime: _Runtime, number: int) -> str:
    wake_target_id = f"wtgt_{number:032x}"
    runtime.wake_target_service.register_target(
        wake_target_id=wake_target_id,
        project_id=runtime.project_id,
    )
    runtime.wake_presence_service.heartbeat(wake_target_id=wake_target_id)
    return wake_target_id


def _wake_revision(*, runtime: _Runtime, wake_target_id: str) -> int:
    return runtime.wake_target_service.get_target(
        wake_target_id=wake_target_id
    ).wake_revision


def test_delegate_work_builds_named_dag_and_auto_selects_single_project(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    started = runtime.facade.delegate_work(
        goal="Review and synthesize",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="synthesis",
                role="architect",
                objective="Synthesize findings.",
                depends_on=("research", "security"),
            ),
            DelegationTaskSpec(
                key="research",
                role="researcher",
                objective="Collect evidence.",
                depends_on=(),
            ),
            DelegationTaskSpec(
                key="security",
                role="security-reviewer",
                objective="Review security.",
                depends_on=(),
            ),
        ),
    )

    tasks = {task.key: task.broker_task for task in started.tasks}
    assert started.run.state.project_id == runtime.project_id
    assert tasks["synthesis"].depends_on == (
        tasks["research"].broker_task_id,
        tasks["security"].broker_task_id,
    )
    status = runtime.facade.delegation_status(run_id=started.run.state.run_id).snapshot
    assert status.ready_count == 2
    assert status.queued_count == 3


def test_registered_wake_targets_are_woken_and_dag_successor_wakes_after_release(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    target_one = _active_wake_target(runtime=runtime, number=1)
    target_two = _active_wake_target(runtime=runtime, number=2)
    started = runtime.facade.delegate_work(
        goal="Parallel review then synthesis",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="review-a",
                role="review-a",
                objective="Review A.",
                depends_on=(),
            ),
            DelegationTaskSpec(
                key="review-b",
                role="review-b",
                objective="Review B.",
                depends_on=(),
            ),
            DelegationTaskSpec(
                key="synthesis",
                role="synthesizer",
                objective="Synthesize.",
                depends_on=("review-a", "review-b"),
            ),
        ),
    )
    assert (
        runtime.wake_target_service.get_target(wake_target_id=target_one).status
        is WakeTargetStatus.WAKING
    )
    assert (
        runtime.wake_target_service.get_target(wake_target_id=target_two).status
        is WakeTargetStatus.WAKING
    )

    first = runtime.facade.work(
        project_id=None,
        slot_id=None,
        slot_handle=None,
        label=None,
        wake_target_id=target_one,
        wake_revision=_wake_revision(runtime=runtime, wake_target_id=target_one),
    )
    second = runtime.facade.work(
        project_id=None,
        slot_id=None,
        slot_handle=None,
        label=None,
        wake_target_id=target_two,
        wake_revision=_wake_revision(runtime=runtime, wake_target_id=target_two),
    )
    assert first.assignment is not None
    assert second.assignment is not None
    assert (
        runtime.wake_target_service.get_target(wake_target_id=target_one).status
        is WakeTargetStatus.BUSY
    )
    assert (
        runtime.wake_target_service.get_target(wake_target_id=target_two).status
        is WakeTargetStatus.BUSY
    )

    for work in (first, second):
        assert work.assignment is not None
        runtime.worker_service.update_task(
            run_id=work.assignment.run.run_id,
            worker_id=work.assignment.worker.worker_id,
            worker_handle=work.assignment.worker_handle,
            task_id=work.assignment.task.task_id,
            expected_revision=work.assignment.task.revision,
            status=TaskStatus.COMPLETED,
            progress_summary="done",
            result="done",
        )
        runtime.facade.work_done(
            slot_id=work.slot.slot_id,
            slot_handle=work.slot_handle,
        )

    status = runtime.facade.delegation_status(run_id=started.run.state.run_id).snapshot
    assert status.ready_count == 1
    target_states = {
        target_id: runtime.wake_target_service.get_target(wake_target_id=target_id)
        for target_id in (target_one, target_two)
    }
    assert (
        sum(state.status is WakeTargetStatus.WAKING for state in target_states.values())
        == 1
    )
    assert (
        sum(state.status is WakeTargetStatus.IDLE for state in target_states.values())
        == 1
    )


def test_delegate_work_rejects_cycle_before_creating_run(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    with pytest.raises(InvalidTransitionError, match="cycle"):
        runtime.facade.delegate_work(
            goal="Invalid",
            project_id=None,
            tasks=(
                DelegationTaskSpec(
                    key="a",
                    role=None,
                    objective="A",
                    depends_on=("b",),
                ),
                DelegationTaskSpec(
                    key="b",
                    role=None,
                    objective="B",
                    depends_on=("a",),
                ),
            ),
        )
    assert runtime.run_repository.list_all() == ()


def test_fresh_work_requires_registered_wake_target(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.facade.delegate_work(
        goal="Delegated only",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="review",
                role="reviewer",
                objective="Review.",
                depends_on=(),
            ),
        ),
    )

    with pytest.raises(
        InvalidTransitionError, match="server-issued Worker Wake delivery"
    ):
        runtime.facade.work(
            project_id=None,
            slot_id=None,
            slot_handle=None,
            label="main-chat-must-not-self-claim",
        )


def test_prepare_scheduled_work_claims_without_physical_wake_and_rejects_replay(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    started = runtime.facade.prepare_scheduled_work(
        goal="Scheduled one-worker probe",
        project_id=runtime.project_id,
        requested_parallelism=1,
        tasks=(
            DelegationTaskSpec(
                key="probe",
                role="scheduler-worker",
                objective="Return a deterministic probe result.",
                depends_on=(),
            ),
        ),
    )
    assert runtime.wake_target_repository.list_all() == ()
    assert len(started.wake_capabilities) == 1
    capability = started.wake_capabilities[0]
    assert capability.status is WorkerWakeCapabilityStatus.ISSUED

    work = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=None,
        slot_handle=None,
        label="scheduled-worker-1",
        wake_target_id=capability.wake_target_id,
        wake_revision=capability.wake_revision,
    )
    assert work.registered is True
    assert work.assignment is not None
    assert work.assignment.run.run_id == started.run.state.run_id
    assert runtime.wake_target_repository.list_all() == ()
    assert runtime.worker_wake_capability_service.owns_slot(slot_id=work.slot.slot_id)

    with pytest.raises(CapabilityDeniedError):
        runtime.facade.work(
            project_id=runtime.project_id,
            slot_id=None,
            slot_handle=None,
            label="scheduled-worker-replay",
            wake_target_id=capability.wake_target_id,
            wake_revision=capability.wake_revision,
        )

    runtime.worker_service.update_task(
        run_id=work.assignment.run.run_id,
        worker_id=work.assignment.worker.worker_id,
        worker_handle=work.assignment.worker_handle,
        task_id=work.assignment.task.task_id,
        expected_revision=work.assignment.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="scheduler probe complete",
        result="SCHEDULED_WORKER_E2E_PASS",
    )
    done = runtime.facade.work_done(
        slot_id=work.slot.slot_id,
        slot_handle=work.slot_handle,
    )
    assert done.slot.status is WorkerSlotStatus.IDLE
    assert runtime.wake_target_repository.list_all() == ()
    status = runtime.facade.delegation_status(run_id=started.run.state.run_id).snapshot
    assert status.completed_count == 1
    assert status.is_complete is True


def test_work_auto_registers_resumes_and_reuses_same_slot_for_new_role(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    wake_target_id = _active_wake_target(runtime=runtime, number=3)
    started = runtime.facade.delegate_work(
        goal="Two roles",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="first",
                role="researcher",
                objective="First task.",
                depends_on=(),
            ),
            DelegationTaskSpec(
                key="second",
                role="reviewer",
                objective="Second task.",
                depends_on=("first",),
            ),
        ),
    )

    first = runtime.facade.work(
        project_id=None,
        slot_id=None,
        slot_handle=None,
        label="generic-chat",
        wake_target_id=wake_target_id,
        wake_revision=_wake_revision(runtime=runtime, wake_target_id=wake_target_id),
    )
    assert first.registered is True
    assert first.assignment is not None
    assert first.assignment.worker.role == "researcher"
    first_worker_handle = first.assignment.worker_handle

    resumed = runtime.facade.work(
        project_id=None,
        slot_id=first.slot.slot_id,
        slot_handle=first.slot_handle,
        label=None,
    )
    assert resumed.resumed is True
    assert resumed.assignment is not None
    assert resumed.assignment.task.task_id == first.assignment.task.task_id
    assert resumed.assignment.worker_handle != first_worker_handle

    completed = runtime.worker_service.update_task(
        run_id=resumed.assignment.run.run_id,
        worker_id=resumed.assignment.worker.worker_id,
        worker_handle=resumed.assignment.worker_handle,
        task_id=resumed.assignment.task.task_id,
        expected_revision=resumed.assignment.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="done",
        result="first result",
    )
    assert completed.status is TaskStatus.COMPLETED
    done = runtime.facade.work_done(
        slot_id=resumed.slot.slot_id,
        slot_handle=resumed.slot_handle,
    )
    assert done.slot.status is WorkerSlotStatus.IDLE

    second = runtime.facade.work(
        project_id=None,
        slot_id=resumed.slot.slot_id,
        slot_handle=resumed.slot_handle,
        label=None,
    )
    assert second.registered is False
    assert second.assignment is not None
    assert second.assignment.worker.role == "reviewer"
    assert second.slot.slot_id == first.slot.slot_id
    status = runtime.facade.delegation_status(run_id=started.run.state.run_id).snapshot
    assert status.claimed_count == 1


def test_fresh_wake_reclaims_the_target_bound_slot_without_browser_slot_state(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    wake_target_id = _active_wake_target(runtime=runtime, number=33)
    runtime.facade.delegate_work(
        goal="First run",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="first",
                role="accessibility reviewer",
                objective="First task.",
                depends_on=(),
            ),
        ),
    )
    first = runtime.facade.work(
        project_id=None,
        slot_id=None,
        slot_handle=None,
        label="persistent-chat",
        wake_target_id=wake_target_id,
        wake_revision=_wake_revision(runtime=runtime, wake_target_id=wake_target_id),
    )
    assert first.assignment is not None
    runtime.worker_service.update_task(
        run_id=first.assignment.run.run_id,
        worker_id=first.assignment.worker.worker_id,
        worker_handle=first.assignment.worker_handle,
        task_id=first.assignment.task.task_id,
        expected_revision=first.assignment.task.revision,
        status=TaskStatus.COMPLETED,
        progress_summary="done",
        result="first result",
    )
    runtime.facade.work_done(
        slot_id=first.slot.slot_id,
        slot_handle=first.slot_handle,
    )

    runtime.facade.delegate_work(
        goal="Second run",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="second",
                role="test reviewer",
                objective="Second task.",
                depends_on=(),
            ),
        ),
    )
    second = runtime.facade.work(
        project_id=None,
        slot_id=None,
        slot_handle=None,
        label=None,
        wake_target_id=wake_target_id,
        wake_revision=_wake_revision(runtime=runtime, wake_target_id=wake_target_id),
    )

    assert second.registered is False
    assert second.assignment is not None
    assert second.assignment.worker.role == "test reviewer"
    assert second.slot.slot_id == first.slot.slot_id
    assert second.slot_handle != first.slot_handle
    assert len(tuple((runtime.state_root / "broker" / "slots").glob("*.json"))) == 1


def test_slot_resume_requeues_assignment_when_connector_changes_after_preflight(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    epoch = [0]
    runtime.wake_target_service.attach_connector_epoch_resolver(
        lambda _project_id: epoch[0]
    )
    wake_target_id = "wtgt_00000000000000000000000000000035"
    runtime.wake_target_service.register_target(
        wake_target_id=wake_target_id,
        project_id=runtime.project_id,
        registered_connector_epoch=0,
    )
    runtime.wake_presence_service.heartbeat(wake_target_id=wake_target_id)
    started = runtime.facade.delegate_work(
        goal="Resume race",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="task",
                role="reviewer",
                objective="Review.",
                depends_on=(),
            ),
        ),
    )
    initial = runtime.facade.work(
        project_id=None,
        slot_id=None,
        slot_handle=None,
        label="persistent-chat",
        wake_target_id=wake_target_id,
        wake_revision=_wake_revision(runtime=runtime, wake_target_id=wake_target_id),
    )
    assert initial.assignment is not None
    runtime.broker_service.recover_abandoned_assignment(
        slot_id=initial.slot.slot_id,
        reason="Arrange an idle reusable Slot for the deterministic resume race.",
    )
    runtime.wake_target_service.release_slot(slot_id=initial.slot.slot_id)

    acquired = Event()
    allow_refresh = Event()
    original_acquire = runtime.broker_service.acquire_task

    def acquire_then_pause(*, slot_id: str, slot_handle: str):
        result = original_acquire(slot_id=slot_id, slot_handle=slot_handle)
        acquired.set()
        assert allow_refresh.wait(timeout=2)
        return result

    runtime.broker_service.acquire_task = acquire_then_pause  # type: ignore[method-assign]
    with ThreadPoolExecutor(max_workers=1) as executor:
        resumed = executor.submit(
            runtime.facade.work,
            project_id=None,
            slot_id=initial.slot.slot_id,
            slot_handle=initial.slot_handle,
            label=None,
        )
        assert acquired.wait(timeout=2)
        epoch[0] = 1
        allow_refresh.set()
        with pytest.raises(WakeTargetConflictError, match="different connector epoch"):
            resumed.result(timeout=2)

    snapshot = runtime.broker_service.get_run_status(run_id=started.run.state.run_id)
    assert snapshot.ready_count == 1
    assert snapshot.claimed_count == 0
    assert snapshot.busy_slot_count == 0
    assert snapshot.idle_slot_count == 1


def test_concurrent_same_wake_revision_claims_once_without_orphan_slot(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    wake_target_id = _active_wake_target(runtime=runtime, number=34)
    runtime.facade.delegate_work(
        goal="Exactly once",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="first",
                role="first",
                objective="First.",
                depends_on=(),
            ),
            DelegationTaskSpec(
                key="second",
                role="second",
                objective="Second.",
                depends_on=(),
            ),
        ),
    )
    revision = _wake_revision(runtime=runtime, wake_target_id=wake_target_id)

    def claim():
        return runtime.facade.work(
            project_id=None,
            slot_id=None,
            slot_handle=None,
            label="persistent-chat",
            wake_target_id=wake_target_id,
            wake_revision=revision,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(lambda _index: _capture(claim), range(2)))

    successes = tuple(value for value in outcomes if not isinstance(value, Exception))
    failures = tuple(value for value in outcomes if isinstance(value, Exception))
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], WakeTargetConflictError)
    assert len(tuple((runtime.state_root / "broker" / "slots").glob("*.json"))) == 1
    snapshot = runtime.broker_service.get_run_status(
        run_id=successes[0].assignment.run.run_id
    )
    assert snapshot.claimed_count == 1
    assert snapshot.ready_count == 1


def _capture(callback):
    try:
        return callback()
    except Exception as error:
        return error


def test_work_auto_releases_terminal_assignment_if_worker_forgot_work_done(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    wake_target_id = _active_wake_target(runtime=runtime, number=4)
    runtime.facade.delegate_work(
        goal="Recover forgotten release",
        project_id=None,
        tasks=(
            DelegationTaskSpec(
                key="one",
                role="one",
                objective="One.",
                depends_on=(),
            ),
            DelegationTaskSpec(
                key="two",
                role="two",
                objective="Two.",
                depends_on=(),
            ),
        ),
    )
    first = runtime.facade.work(
        project_id=None,
        slot_id=None,
        slot_handle=None,
        label=None,
        wake_target_id=wake_target_id,
        wake_revision=_wake_revision(runtime=runtime, wake_target_id=wake_target_id),
    )
    assert first.assignment is not None
    runtime.worker_service.update_task(
        run_id=first.assignment.run.run_id,
        worker_id=first.assignment.worker.worker_id,
        worker_handle=first.assignment.worker_handle,
        task_id=first.assignment.task.task_id,
        expected_revision=0,
        status=TaskStatus.COMPLETED,
        progress_summary="done",
        result="done",
    )

    recovered = runtime.facade.work(
        project_id=None,
        slot_id=first.slot.slot_id,
        slot_handle=first.slot_handle,
        label=None,
    )
    assert recovered.released_previous is True
    assert recovered.assignment is not None
    assert recovered.assignment.broker_task.broker_task_id != (
        first.assignment.broker_task.broker_task_id
    )
