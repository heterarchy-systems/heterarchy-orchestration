import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from unittest.mock import patch

from chattocodex.orchestrator_backend.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskStatus,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
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
from chattocodex.orchestrator_backend.persistence.wake.wake_presence_repository import (
    WakePresenceRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetStatus
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.worker_assignment_lease_reconciler import (
    WorkerAssignmentLeaseReconciler,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from tests.broker_support import FakeWorkBrokerGateway


class _Clock:
    def __init__(self) -> None:
        self._now = datetime(2026, 8, 11, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, *, seconds: int) -> None:
        self._now += timedelta(seconds=seconds)


def _workspace(tmp_path: Path) -> Path:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname='sample-project'\nversion='0.1.0'\n",
        encoding="utf-8",
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
    return workspace_root


def test_assignment_lease_requeues_work_when_busy_chat_never_returns(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    workspace_service = WorkspaceProjectService(workspace_root=_workspace(tmp_path))
    project_id = workspace_service.list_projects()[0].project_id
    run_repository = RunStateRepository(state_root=state_root)
    worker_repository = WorkerStateRepository(state_root=state_root)
    task_repository = TaskStateRepository(state_root=state_root)
    broker_task_repository = BrokerTaskRepository(state_root=state_root)
    slot_repository = WorkerSlotRepository(state_root=state_root)
    wake_target_repository = WakeTargetRepository(state_root=state_root)
    wake_presence_repository = WakePresenceRepository(state_root=state_root)
    locks = EntityLockRegistry()
    clock = _Clock()

    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_service,
        command_runner=GitCommandRunner(),
    )
    run = run_service.create_run(
        goal="Assignment lease recovery", project_id=project_id
    )
    work_broker_gateway = FakeWorkBrokerGateway()
    broker_service = BrokerService(
        work_broker_gateway=work_broker_gateway,
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        broker_task_repository=broker_task_repository,
        slot_repository=slot_repository,
        workspace_project_service=workspace_service,
        lock_registry=locks,
        now_provider=clock,
    )
    published = broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Recover this in-progress assignment.",
        role="recovery-worker",
        depends_on=(),
    )
    slot = broker_service.register_slot(project_id=project_id, label="lost-worker")
    assignment = broker_service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    assert assignment is not None

    presence = WakePresenceService(
        repository=wake_presence_repository,
        now_provider=clock,
    )
    wake_service = WakeTargetService(
        repository=wake_target_repository,
        presence_service=presence,
        lock_registry=locks,
        now_provider=clock,
    )
    target_id = f"wtgt_{91:032x}"
    wake_service.register_target(wake_target_id=target_id, project_id=project_id)
    wake_service.bind_target(
        wake_target_id=target_id,
        project_id=project_id,
        slot_id=slot.state.slot_id,
        has_assignment=True,
    )
    recovery = AbandonedAssignmentRecovery(
        broker_task_repository=broker_task_repository,
        slot_repository=slot_repository,
        task_repository=task_repository,
        lock_registry=locks,
        now_provider=clock,
    )
    reconciler = WorkerAssignmentLeaseReconciler(
        wake_target_repository=wake_target_repository,
        wake_presence_repository=wake_presence_repository,
        abandoned_assignment_recovery=recovery,
        wake_target_service=wake_service,
        lock_registry=locks,
        now_provider=clock,
    )

    clock.advance(seconds=121)
    entered_recovery = Event()
    resume_recovery = Event()
    original_recover = recovery.recover

    def pause_before_locked_revalidation(**kwargs):
        entered_recovery.set()
        assert resume_recovery.wait(timeout=2)
        return original_recover(**kwargs)

    with (
        patch.object(recovery, "recover", side_effect=pause_before_locked_revalidation),
        ThreadPoolExecutor(max_workers=1) as executor,
    ):
        future = executor.submit(reconciler.reconcile_project, project_id=project_id)
        assert entered_recovery.wait(timeout=2)
        presence.heartbeat(wake_target_id=target_id)
        resume_recovery.set()
        assert future.result(timeout=2) == ()

    assert (
        slot_repository.read(slot_id=slot.state.slot_id).status is WorkerSlotStatus.BUSY
    )
    assert (
        wake_target_repository.read(wake_target_id=target_id).status
        is WakeTargetStatus.BUSY
    )

    clock.advance(seconds=121)
    callback_entered = Event()
    competing_lock_started = Event()
    competing_lock_acquired = Event()
    contender_future = None

    def verify_callback_holds_slot_lock(**kwargs):
        nonlocal contender_future
        original_after_recovery = kwargs["after_recovery"]

        def checked_after_recovery(recovered_slot):
            nonlocal contender_future
            callback_entered.set()
            contender_future = executor.submit(
                _acquire_slot_lock,
                locks,
                slot.state.slot_id,
                competing_lock_started,
                competing_lock_acquired,
            )
            assert competing_lock_started.wait(timeout=2)
            assert not competing_lock_acquired.wait(timeout=0)
            original_after_recovery(recovered_slot)

        kwargs["after_recovery"] = checked_after_recovery
        return original_recover(**kwargs)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with patch.object(
            recovery, "recover", side_effect=verify_callback_holds_slot_lock
        ):
            recovered = reconciler.reconcile_project(project_id=project_id)
        assert callback_entered.is_set()
        assert competing_lock_acquired.wait(timeout=2)
        assert contender_future is not None
        contender_future.result(timeout=2)

    assert recovered == (slot.state.slot_id,)
    old_task = task_repository.read(
        run_id=assignment.run.run_id,
        task_id=assignment.task.task_id,
    )
    broker_task = broker_task_repository.read(broker_task_id=published.broker_task_id)
    recovered_slot = slot_repository.read(slot_id=slot.state.slot_id)
    recovered_target = wake_target_repository.read(wake_target_id=target_id)
    assert old_task.status is TaskStatus.FAILED
    assert broker_task.status is BrokerTaskStatus.QUEUED
    assert recovered_slot.status is WorkerSlotStatus.IDLE
    assert recovered_target.status is WakeTargetStatus.IDLE
    work_broker_gateway.expire_task(
        run_id=run.state.run_id,
        task_id=published.broker_task_id,
    )

    replacement_slot = broker_service.register_slot(
        project_id=project_id,
        label="replacement-worker",
    )
    replacement = broker_service.claim_task(
        slot_id=replacement_slot.state.slot_id,
        slot_handle=replacement_slot.slot_handle,
    )
    assert replacement is not None
    assert replacement.broker_task.broker_task_id == published.broker_task_id
    assert replacement.worker.worker_id != assignment.worker.worker_id
    assert replacement.task.task_id != assignment.task.task_id


def _acquire_slot_lock(
    locks: EntityLockRegistry,
    slot_id: str,
    started: Event,
    acquired: Event,
) -> None:
    started.set()
    with locks.hold(entity_key=f"broker:slot:{slot_id}"):
        acquired.set()
