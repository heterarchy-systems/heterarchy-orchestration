import subprocess
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event

import pytest
from starlette.testclient import TestClient

from chattocodex.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.broker.broker_dto import BrokerTaskStatus, WorkerSlotStatus
from chattocodex.broker.broker_service import BrokerService
from chattocodex.consumer_group.capacity_decision import CapacityLimits
from chattocodex.consumer_group.consumer_group_coordinator import (
    ConsumerGroupCoordinator,
)
from chattocodex.control_panel.control_panel_routes import create_control_panel_app
from chattocodex.control_panel.control_panel_service import ControlPanelService
from chattocodex.facade.facade_dto import DelegationTaskSpec, WorkerWork
from chattocodex.facade.facade_service import ChatFacadeService
from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestration.orchestration_exceptions import ResourceNotFoundError
from chattocodex.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestration.worker_execution_service import WorkerExecutionService
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.consumer_group_state_store import ConsumerGroupStateStore
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.persistence.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.persistence.runtime_config_repository import RuntimeConfigRepository
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.persistence.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.persistence.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.project_runtime.project_binding_service import ProjectBindingService
from chattocodex.provisioning.provisioning_dto import (
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)
from chattocodex.provisioning.provisioning_exceptions import (
    ProvisioningConflictError,
    ProvisioningStaleCallbackError,
)
from chattocodex.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.schemas.auth_schemas import RuntimeConfigRecord
from chattocodex.schemas.project_binding_schemas import ProjectBindingAdapterMetadata
from chattocodex.wake.wake_control_service import WakeControlService
from chattocodex.wake.wake_dto import WakeTargetRetirement, WakeTargetStatus
from chattocodex.wake.wake_exceptions import WakeTargetConflictError
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.worker_runtime.broker_ready_capacity_projection import (
    BrokerReadyCapacityProjection,
)
from chattocodex.worker_runtime.worker_assignment_lease_reconciler import (
    WorkerAssignmentLeaseReconciler,
)
from chattocodex.worker_runtime.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class _Clock:
    def __init__(self) -> None:
        self._now = datetime(2026, 8, 13, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, *, seconds: int) -> None:
        self._now += timedelta(seconds=seconds)


class _PauseBeforeActiveRepository(WorkerProvisioningRepository):
    def __init__(self, *, state_root: Path) -> None:
        super().__init__(state_root=state_root)
        self.finalization_started = Event()
        self.release_finalization = Event()

    def update(
        self,
        *,
        updater: Callable[[ProvisioningStoreState], ProvisioningStoreState],
    ) -> ProvisioningStoreState:
        if updater.__name__ == "register":
            self.finalization_started.set()
            assert self.release_finalization.wait(timeout=3)
        return super().update(updater=updater)


class _Runtime:
    def __init__(self, *, tmp_path: Path) -> None:
        self.clock = _Clock()
        self.state_root = tmp_path / "state"
        workspace_root = tmp_path / "workspace"
        self.workspace_root = workspace_root
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

        self.workspace = WorkspaceProjectService(workspace_root=workspace_root)
        self.project_id = self.workspace.list_projects()[0].project_id
        self.locks = EntityLockRegistry(lock_directory=tmp_path / "locks")
        self.run_repository = RunStateRepository(state_root=self.state_root)
        self.worker_repository = WorkerStateRepository(state_root=self.state_root)
        self.task_repository = TaskStateRepository(state_root=self.state_root)
        self.broker_task_repository = BrokerTaskRepository(state_root=self.state_root)
        self.slot_repository = WorkerSlotRepository(state_root=self.state_root)
        self.wake_target_repository = WakeTargetRepository(state_root=self.state_root)
        self.wake_presence_repository = WakePresenceRepository(
            state_root=self.state_root
        )
        self.wake_presence = WakePresenceService(
            repository=self.wake_presence_repository,
            now_provider=self.clock,
        )
        self.runtime_presence = WorkerRuntimePresenceTracker(
            store=WorkerRuntimePresenceStore(state_root=self.state_root),
            now_provider=self.clock,
        )
        self.wake_service = WakeTargetService(
            repository=self.wake_target_repository,
            presence_service=self.wake_presence,
            runtime_presence_tracker=self.runtime_presence,
            lock_registry=self.locks,
            now_provider=self.clock,
        )
        self.binding_service = ProjectBindingService(
            repository=ProjectBindingRepository(state_root=self.state_root),
            workspace_project_service=self.workspace,
            lock_registry=self.locks,
            now_provider=self.clock,
        )
        self.binding_service.upsert_binding(
            project_id=self.project_id,
            metadata=ProjectBindingAdapterMetadata(
                provider="chatgpt",
                project_ref="sample-project-ref",
                project_path="/g/sample-project-ref",
                app_ref="plugin_sample",
                app_label="ChatToCodex",
            ),
        )
        self.provisioning_repository = WorkerProvisioningRepository(
            state_root=self.state_root
        )
        self.provisioning = WorkerProvisioningService(
            repository=self.provisioning_repository,
            project_binding_service=self.binding_service,
            wake_target_service=self.wake_service,
            lock_registry=self.locks,
            now_provider=self.clock,
        )
        run_service = RunOrchestrationService(
            run_repository=self.run_repository,
            workspace_project_service=self.workspace,
            command_runner=GitCommandRunner(),
        )
        broker_service = BrokerService(
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            broker_task_repository=self.broker_task_repository,
            slot_repository=self.slot_repository,
            workspace_project_service=self.workspace,
            lock_registry=self.locks,
            now_provider=self.clock,
        )
        self.worker_service = WorkerExecutionService(
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            lock_registry=self.locks,
        )
        self.assignment_reconciler = WorkerAssignmentLeaseReconciler(
            wake_target_repository=self.wake_target_repository,
            wake_presence_repository=self.wake_presence_repository,
            abandoned_assignment_recovery=AbandonedAssignmentRecovery(
                broker_task_repository=self.broker_task_repository,
                slot_repository=self.slot_repository,
                task_repository=self.task_repository,
                lock_registry=self.locks,
                now_provider=self.clock,
            ),
            wake_target_service=self.wake_service,
            lock_registry=self.locks,
            now_provider=self.clock,
        )
        self.group = ConsumerGroupCoordinator(
            state_store=ConsumerGroupStateStore(state_root=self.state_root),
            wake_target_repository=self.wake_target_repository,
            wake_presence_service=self.wake_presence,
            runtime_presence_tracker=self.runtime_presence,
            wake_target_service=self.wake_service,
            ready_capacity_projection=BrokerReadyCapacityProjection(
                broker_task_repository=self.broker_task_repository,
                run_repository=self.run_repository,
            ),
            assignment_lease_reconciler=self.assignment_reconciler,
            lock_registry=self.locks,
            capacity_limits=CapacityLimits(
                project_max_workers=2,
                global_max_workers=2,
            ),
            pending_provisioning_count=self.provisioning.pending_provisioning_count,
            on_capacity_decision=self.provisioning.on_capacity_decision,
            now_provider=self.clock,
        )
        self.provisioning.attach_consumer_group_coordinator(self.group)
        self.facade = ChatFacadeService(
            run_service=run_service,
            broker_service=broker_service,
            wake_target_service=self.wake_service,
            workspace_project_service=self.workspace,
            consumer_group_coordinator=self.group,
        )

    def delegate(self, *, roles: tuple[str, str]) -> None:
        self.facade.delegate_work(
            goal="Two independent reviews",
            tasks=tuple(
                DelegationTaskSpec(
                    key=f"review-{index}",
                    objective=f"Review from the {role} perspective",
                    role=role,
                    depends_on=(),
                )
                for index, role in enumerate(roles, start=1)
            ),
            requested_parallelism=2,
            project_id=self.project_id,
        )

    def register_admitted_worker(self, *, conversation_id: str) -> str:
        admitted = self.provisioning.admit_next()
        assert admitted is not None
        assert admitted.wake_target_id is not None
        assert admitted.admission_revision is not None
        active = self.provisioning.register_callback(
            request_id=admitted.request_id,
            conversation_id=conversation_id,
            wake_target_id=admitted.wake_target_id,
            admission_revision=admitted.admission_revision,
        )
        assert active.conversation_id == conversation_id
        assert active.wake_target_id is not None
        assert (
            active.wake_target_id
            in self.group.reconcile_membership(project_id=self.project_id).member_ids
        )
        return active.wake_target_id

    def claim(self, *, wake_target_id: str, label: str) -> WorkerWork:
        target = self.wake_service.get_target(wake_target_id=wake_target_id)
        assert target.status is WakeTargetStatus.WAKING
        work = self.facade.work(
            project_id=self.project_id,
            slot_id=None,
            slot_handle=None,
            label=label,
            wake_target_id=wake_target_id,
            wake_revision=target.wake_revision,
        )
        assert work.assignment is not None
        return work

    def complete(self, *, work: WorkerWork) -> None:
        assert work.assignment is not None
        assignment = work.assignment
        self.worker_service.update_task(
            run_id=assignment.run.run_id,
            worker_id=assignment.worker.worker_id,
            worker_handle=assignment.worker_handle,
            task_id=assignment.task.task_id,
            expected_revision=assignment.task.revision,
            status=TaskStatus.COMPLETED,
            progress_summary="completed",
            result="completed",
        )
        done = self.facade.work_done(
            slot_id=work.slot.slot_id,
            slot_handle=work.slot_handle,
        )
        assert done.slot.status is WorkerSlotStatus.IDLE

    def provision_two_idle_workers(self) -> tuple[str, str]:
        self.delegate(roles=("security-reviewer", "data-integrity-reviewer"))
        target_a = self.register_admitted_worker(conversation_id="conversation-a")
        self.clock.advance(seconds=12)
        target_b = self.register_admitted_worker(conversation_id="conversation-b")
        for target_id, label in ((target_a, "worker-a"), (target_b, "worker-b")):
            self.complete(work=self.claim(wake_target_id=target_id, label=label))
        return target_a, target_b

    def rebind_connector(self, *, app_ref: str, app_label: str) -> None:
        binding = self.binding_service.get_binding(project_id=self.project_id)
        rebound = self.binding_service.upsert_binding(
            project_id=self.project_id,
            expected_revision=binding.revision,
            metadata=ProjectBindingAdapterMetadata(
                provider="chatgpt",
                project_ref="sample-project-ref",
                project_path="/g/sample-project-ref",
                app_ref=app_ref,
                app_label=app_label,
            ),
        )
        assert rebound.revision > binding.revision
        self.delegate(roles=("architecture-reviewer", "test-reviewer"))


def test_n2_provisioning_then_second_run_reuses_the_same_consumers(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    assert (
        runtime.group.reconcile_membership(project_id=runtime.project_id).member_ids
        == ()
    )
    assert runtime.wake_target_repository.list_all() == ()

    first_roles = ("security-reviewer", "data-integrity-reviewer")
    runtime.delegate(roles=first_roles)
    demands, initial_requests = runtime.provisioning.list_status()
    assert demands[0].spawn_needed == 2
    assert len(initial_requests) == 2

    target_a = runtime.register_admitted_worker(conversation_id="conversation-a")
    assert runtime.provisioning.admit_next() is None
    runtime.clock.advance(seconds=11)
    assert runtime.provisioning.admit_next() is None
    runtime.clock.advance(seconds=1)
    target_b = runtime.register_admitted_worker(conversation_id="conversation-b")

    assert target_a != target_b
    assert runtime.group.reconcile_membership(
        project_id=runtime.project_id
    ).member_ids == (target_a, target_b)
    first_work = {
        target_id: runtime.claim(wake_target_id=target_id, label=label)
        for target_id, label in ((target_a, "worker-a"), (target_b, "worker-b"))
    }
    assert {
        work.assignment.broker_task.role
        for work in first_work.values()
        if work.assignment is not None
    } == set(first_roles)
    assert (
        len(
            {
                work.assignment.broker_task.broker_task_id
                for work in first_work.values()
                if work.assignment is not None
            }
        )
        == 2
    )

    for work in first_work.values():
        runtime.complete(work=work)
    assert tuple(
        runtime.wake_service.get_target(wake_target_id=target_id).status
        for target_id in (target_a, target_b)
    ) == (WakeTargetStatus.IDLE, WakeTargetStatus.IDLE)

    first_request_ids = {
        request.request_id for request in runtime.provisioning.list_status()[1]
    }
    target_slots = {
        target_id: runtime.wake_service.get_target(
            wake_target_id=target_id
        ).bound_slot_id
        for target_id in (target_a, target_b)
    }
    second_roles = ("architecture-reviewer", "test-reviewer")
    runtime.delegate(roles=second_roles)

    second_work = {
        target_id: runtime.claim(wake_target_id=target_id, label="ignored-on-reuse")
        for target_id in (target_a, target_b)
    }
    assert {
        work.assignment.broker_task.role
        for work in second_work.values()
        if work.assignment is not None
    } == set(second_roles)
    assert {
        target_id: work.slot.slot_id for target_id, work in second_work.items()
    } == target_slots
    assert {
        request.request_id for request in runtime.provisioning.list_status()[1]
    } == first_request_ids
    assert runtime.provisioning.pending_provisioning_count(runtime.project_id) == 0
    assert {
        target.wake_target_id for target in runtime.wake_target_repository.list_all()
    } == {target_a, target_b}


def test_retiring_unbound_unclaimed_workers_reserves_exact_replacements(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.delegate(roles=("architecture-reviewer", "test-reviewer"))
    target_a = runtime.register_admitted_worker(conversation_id="conversation-a")
    runtime.clock.advance(seconds=12)
    target_b = runtime.register_admitted_worker(conversation_id="conversation-b")
    assert all(
        runtime.wake_service.get_target(wake_target_id=target_id).bound_slot_id is None
        for target_id in (target_a, target_b)
    )
    before_request_ids = {
        request.request_id for request in runtime.provisioning.list_status()[1]
    }
    control = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(
            state_root=runtime.state_root
        ),
        wake_target_service=runtime.wake_service,
        wake_presence_service=runtime.wake_presence,
        runtime_presence_tracker=runtime.runtime_presence,
        assignment_lease_reconciler=runtime.assignment_reconciler,
        consumer_group_coordinator=runtime.group,
        worker_provisioning_service=runtime.provisioning,
        lock_registry=runtime.locks,
    )

    for target_id in (target_a, target_b):
        current = runtime.wake_target_repository.read(wake_target_id=target_id)
        retired = control.retire_unclaimed_target(
            wake_target_id=target_id,
            expected_revision=current.revision,
        )
        assert retired.wake_target_id == target_id
        assert not runtime.wake_target_repository.exists(wake_target_id=target_id)
        assert not runtime.wake_presence_repository.exists(wake_target_id=target_id)

    snapshot = runtime.group.reconcile_project(project_id=runtime.project_id)
    demands, requests = runtime.provisioning.list_status()
    replacement_requests = [
        request for request in requests if request.request_id not in before_request_ids
    ]

    assert snapshot.group.member_ids == ()
    assert snapshot.capacity_decision.recoverable_consumer_count == 0
    assert snapshot.capacity_decision.capacity_deficit == 2
    assert snapshot.capacity_decision.pending_provisioning_count == 2
    assert snapshot.capacity_decision.spawn_needed == 0
    assert demands[0].spawn_needed == 2
    assert len(replacement_requests) == 2
    retired_requests = {
        request.wake_target_id: request
        for request in requests
        if request.wake_target_id in {target_a, target_b}
    }
    assert set(retired_requests) == {target_a, target_b}
    assert {
        (request.status, request.failure_reason)
        for request in retired_requests.values()
    } == {(ProvisioningRequestStatus.CANCELLED, "runtime_ownership_lost")}
    assert runtime.provisioning.admit_next() is None


def test_batch_retirement_of_two_active_idle_targets_reconciles_once_and_reserves_two_replacements(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.delegate(roles=("architecture-reviewer", "test-reviewer"))
    target_a = runtime.register_admitted_worker(conversation_id="conversation-batch-a")
    runtime.clock.advance(seconds=12)
    target_b = runtime.register_admitted_worker(conversation_id="conversation-batch-b")
    target_ids = (target_a, target_b)
    runtime.wake_service.sync_ready_capacity(
        project_id=runtime.project_id,
        ready_count=0,
    )
    assert all(
        runtime.wake_service.get_target(wake_target_id=target_id).status
        is WakeTargetStatus.IDLE
        for target_id in target_ids
    )
    before_request_ids = {
        request.request_id for request in runtime.provisioning.list_status()[1]
    }
    reconciled_projects: list[str] = []
    original_reconcile = runtime.group._reconcile_project_locked

    def count_reconcile(*, project_id: str):
        reconciled_projects.append(project_id)
        return original_reconcile(project_id=project_id)

    runtime.group._reconcile_project_locked = count_reconcile  # type: ignore[method-assign]
    control = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(
            state_root=runtime.state_root
        ),
        wake_target_service=runtime.wake_service,
        wake_presence_service=runtime.wake_presence,
        runtime_presence_tracker=runtime.runtime_presence,
        assignment_lease_reconciler=runtime.assignment_reconciler,
        consumer_group_coordinator=runtime.group,
        worker_provisioning_service=runtime.provisioning,
        lock_registry=runtime.locks,
    )
    retirements = tuple(
        WakeTargetRetirement(
            wake_target_id=target_id,
            expected_revision=runtime.wake_service.get_target(
                wake_target_id=target_id
            ).revision,
        )
        for target_id in target_ids
    )

    retired_ids = control.retire_unclaimed_targets_batch(
        project_id=runtime.project_id,
        targets=retirements,
    )

    assert retired_ids == target_ids
    assert reconciled_projects == [runtime.project_id]
    assert all(
        not runtime.wake_target_repository.exists(wake_target_id=target_id)
        for target_id in target_ids
    )
    demands, requests = runtime.provisioning.list_status()
    replacements = tuple(
        request for request in requests if request.request_id not in before_request_ids
    )
    assert len(replacements) == 2
    assert demands[0].spawn_needed == 2
    assert {
        (request.status, request.failure_reason)
        for request in requests
        if request.wake_target_id in target_ids
    } == {(ProvisioningRequestStatus.CANCELLED, "runtime_ownership_lost")}

    provisioning_before_replay = runtime.provisioning_repository.read_state()
    group_before_replay = ConsumerGroupStateStore(state_root=runtime.state_root).read(
        project_id=runtime.project_id
    )
    replayed_ids = control.retire_unclaimed_targets_batch(
        project_id=runtime.project_id,
        targets=retirements,
    )

    assert replayed_ids == target_ids
    assert runtime.provisioning_repository.read_state() == provisioning_before_replay
    assert (
        ConsumerGroupStateStore(state_root=runtime.state_root).read(
            project_id=runtime.project_id
        )
        == group_before_replay
    )
    assert reconciled_projects == [runtime.project_id, runtime.project_id]


@pytest.mark.parametrize(
    "invalid_case",
    ("duplicate", "cross-project", "stale", "bound", "non-idle"),
)
def test_batch_retirement_prevalidates_every_target_before_any_mutation(
    tmp_path: Path,
    invalid_case: str,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.delegate(roles=("architecture-reviewer", "test-reviewer"))
    target_a = runtime.register_admitted_worker(conversation_id="conversation-valid")
    runtime.clock.advance(seconds=12)
    target_b = runtime.register_admitted_worker(conversation_id="conversation-invalid")
    runtime.wake_service.sync_ready_capacity(
        project_id=runtime.project_id,
        ready_count=0,
    )
    before_targets = {
        target_id: runtime.wake_service.get_target(wake_target_id=target_id)
        for target_id in (target_a, target_b)
    }
    second_target = before_targets[target_b]
    if invalid_case == "cross-project":
        runtime.wake_target_repository.write(
            state=replace(
                second_target,
                project_id="prj_foreign_12345678",
            )
        )
        second_target = runtime.wake_service.get_target(wake_target_id=target_b)
    elif invalid_case == "bound":
        runtime.wake_service.bind_target(
            wake_target_id=target_b,
            project_id=runtime.project_id,
            slot_id="slot_111111111111111111111111",
            has_assignment=False,
        )
        second_target = runtime.wake_service.get_target(wake_target_id=target_b)
    elif invalid_case == "non-idle":
        runtime.wake_service.sync_ready_capacity(
            project_id=runtime.project_id,
            ready_count=1,
        )
        target_b = next(
            target_id
            for target_id in (target_a, target_b)
            if runtime.wake_service.get_target(wake_target_id=target_id).status
            is WakeTargetStatus.WAKING
        )
        target_a = next(
            target_id for target_id in before_targets if target_id != target_b
        )
        second_target = runtime.wake_service.get_target(wake_target_id=target_b)

    retirements = (
        WakeTargetRetirement(
            wake_target_id=target_a,
            expected_revision=runtime.wake_service.get_target(
                wake_target_id=target_a
            ).revision,
        ),
        WakeTargetRetirement(
            wake_target_id=target_b,
            expected_revision=(
                second_target.revision + 1
                if invalid_case == "stale"
                else second_target.revision
            ),
        ),
    )
    if invalid_case == "duplicate":
        retirements = (retirements[0], retirements[0])
    provisioning_before = runtime.provisioning_repository.read_state()
    reconcile_calls: list[str] = []
    runtime.group.reconcile_project = (  # type: ignore[method-assign]
        lambda *, project_id: reconcile_calls.append(project_id)
    )
    control = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(
            state_root=runtime.state_root
        ),
        wake_target_service=runtime.wake_service,
        wake_presence_service=runtime.wake_presence,
        runtime_presence_tracker=runtime.runtime_presence,
        consumer_group_coordinator=runtime.group,
        worker_provisioning_service=runtime.provisioning,
        lock_registry=runtime.locks,
    )

    with pytest.raises(
        (ValueError, ProvisioningConflictError, WakeTargetConflictError)
    ):
        control.retire_unclaimed_targets_batch(
            project_id=runtime.project_id,
            targets=retirements,
        )

    assert runtime.wake_target_repository.exists(wake_target_id=target_a)
    assert runtime.wake_target_repository.exists(wake_target_id=target_b)
    assert runtime.provisioning_repository.read_state() == provisioning_before
    assert reconcile_calls == []


def test_retirement_wins_post_membership_pre_active_gap_without_resurrection(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.delegate(roles=("architecture-reviewer", "test-reviewer"))
    admitted = runtime.provisioning.admit_next()
    assert admitted is not None
    assert admitted.wake_target_id is not None
    assert admitted.admission_revision is not None

    pausing_repository = _PauseBeforeActiveRepository(state_root=runtime.state_root)
    runtime.provisioning._repository = pausing_repository
    control = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(
            state_root=runtime.state_root
        ),
        wake_target_service=runtime.wake_service,
        wake_presence_service=runtime.wake_presence,
        runtime_presence_tracker=runtime.runtime_presence,
        assignment_lease_reconciler=runtime.assignment_reconciler,
        consumer_group_coordinator=runtime.group,
        worker_provisioning_service=runtime.provisioning,
        lock_registry=runtime.locks,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        registration = pool.submit(
            runtime.provisioning.register_callback,
            request_id=admitted.request_id,
            conversation_id="conversation-racing-retirement",
            wake_target_id=admitted.wake_target_id,
            admission_revision=admitted.admission_revision,
        )
        assert pausing_repository.finalization_started.wait(timeout=3)
        target = runtime.wake_service.get_target(wake_target_id=admitted.wake_target_id)
        retired = control.retire_unclaimed_target(
            wake_target_id=admitted.wake_target_id,
            expected_revision=target.revision,
        )
        pausing_repository.release_finalization.set()
        with pytest.raises(ProvisioningStaleCallbackError):
            registration.result(timeout=3)

    assert retired.wake_target_id == admitted.wake_target_id
    with pytest.raises(ResourceNotFoundError):
        runtime.wake_service.get_target(wake_target_id=admitted.wake_target_id)
    persisted = pausing_repository.read_request(request_id=admitted.request_id)
    assert persisted.status is ProvisioningRequestStatus.CANCELLED
    assert persisted.failure_reason == "runtime_ownership_lost"
    assert pausing_repository.read_admission().in_flight_request_id is None

    restarted = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=runtime.state_root),
        minimum_admission_interval=timedelta(0),
    )
    snapshot_before_replay = restarted._repository.read_state()
    replayed = restarted.retire_runtime_ownership(
        wake_target_id=admitted.wake_target_id
    )
    replayed_again = restarted.retire_runtime_ownership(
        wake_target_id=admitted.wake_target_id
    )
    assert replayed == persisted
    assert replayed_again == persisted
    assert restarted._repository.read_state() == snapshot_before_replay


def test_batch_retirement_wins_post_membership_pre_active_gap_without_resurrection(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.delegate(roles=("architecture-reviewer", "test-reviewer"))
    admitted = runtime.provisioning.admit_next()
    assert admitted is not None
    assert admitted.wake_target_id is not None
    assert admitted.admission_revision is not None

    pausing_repository = _PauseBeforeActiveRepository(state_root=runtime.state_root)
    runtime.provisioning._repository = pausing_repository
    control = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(
            state_root=runtime.state_root
        ),
        wake_target_service=runtime.wake_service,
        wake_presence_service=runtime.wake_presence,
        runtime_presence_tracker=runtime.runtime_presence,
        assignment_lease_reconciler=runtime.assignment_reconciler,
        consumer_group_coordinator=runtime.group,
        worker_provisioning_service=runtime.provisioning,
        lock_registry=runtime.locks,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        registration = pool.submit(
            runtime.provisioning.register_callback,
            request_id=admitted.request_id,
            conversation_id="conversation-racing-batch-retirement",
            wake_target_id=admitted.wake_target_id,
            admission_revision=admitted.admission_revision,
        )
        assert pausing_repository.finalization_started.wait(timeout=3)
        target = runtime.wake_service.get_target(wake_target_id=admitted.wake_target_id)
        retired_ids = control.retire_unclaimed_targets_batch(
            project_id=runtime.project_id,
            targets=(
                WakeTargetRetirement(
                    wake_target_id=admitted.wake_target_id,
                    expected_revision=target.revision,
                ),
            ),
        )
        pausing_repository.release_finalization.set()
        with pytest.raises(ProvisioningStaleCallbackError):
            registration.result(timeout=3)

    assert retired_ids == (admitted.wake_target_id,)
    with pytest.raises(ResourceNotFoundError):
        runtime.wake_service.get_target(wake_target_id=admitted.wake_target_id)
    persisted = pausing_repository.read_request(request_id=admitted.request_id)
    assert persisted.status is ProvisioningRequestStatus.CANCELLED
    assert persisted.failure_reason == "runtime_ownership_lost"
    assert pausing_repository.read_admission().in_flight_request_id is None

    snapshot_before_replay = pausing_repository.read_state()
    replayed_ids = control.retire_unclaimed_targets_batch(
        project_id=runtime.project_id,
        targets=(
            WakeTargetRetirement(
                wake_target_id=admitted.wake_target_id,
                expected_revision=target.revision,
            ),
        ),
    )
    assert replayed_ids == retired_ids
    assert pausing_repository.read_state() == snapshot_before_replay


def test_active_registration_replay_retires_missing_runtime_target(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.delegate(roles=("architecture-reviewer", "test-reviewer"))
    admitted = runtime.provisioning.admit_next()
    assert admitted is not None
    assert admitted.wake_target_id is not None
    assert admitted.admission_revision is not None
    active = runtime.provisioning.register_callback(
        request_id=admitted.request_id,
        conversation_id="conversation-active-replay",
        wake_target_id=admitted.wake_target_id,
        admission_revision=admitted.admission_revision,
    )
    runtime.wake_target_repository.delete(wake_target_id=admitted.wake_target_id)

    with pytest.raises(
        ProvisioningConflictError,
        match="no longer has a recoverable runtime target",
    ):
        runtime.provisioning.register_callback(
            request_id=active.request_id,
            conversation_id=active.conversation_id or "",
            wake_target_id=active.wake_target_id or "",
            admission_revision=active.admission_revision or 0,
        )

    repaired = runtime.provisioning_repository.read_request(
        request_id=active.request_id
    )
    assert repaired.status is ProvisioningRequestStatus.CANCELLED
    assert repaired.failure_reason == "runtime_ownership_lost"


def test_failed_provisioning_retirement_keeps_wake_target_retryable(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.delegate(roles=("accessibility-reviewer",))
    target_id = runtime.register_admitted_worker(conversation_id="conversation-a")
    target = runtime.wake_target_repository.read(wake_target_id=target_id)

    class _FailingProvisioningService:
        def retire_runtime_ownership(self, *, wake_target_id: str) -> None:
            assert wake_target_id == target_id
            raise RuntimeError("injected provisioning persistence failure")

    control = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(
            state_root=runtime.state_root
        ),
        wake_target_service=runtime.wake_service,
        wake_presence_service=runtime.wake_presence,
        runtime_presence_tracker=runtime.runtime_presence,
        consumer_group_coordinator=runtime.group,
        worker_provisioning_service=_FailingProvisioningService(),  # type: ignore[arg-type]
        lock_registry=runtime.locks,
    )

    try:
        control.retire_unclaimed_target(
            wake_target_id=target_id,
            expected_revision=target.revision,
        )
    except RuntimeError as error:
        assert str(error) == "injected provisioning persistence failure"
    else:
        raise AssertionError("Injected lifecycle failure must abort retirement")

    assert runtime.wake_target_repository.read(wake_target_id=target_id) == target
    assert runtime.wake_presence_repository.exists(wake_target_id=target_id)
    request = next(
        request
        for request in runtime.provisioning.list_status()[1]
        if request.wake_target_id == target_id
    )
    assert request.status == ProvisioningRequestStatus.ACTIVE


def test_app_ref_rebind_excludes_old_consumers_from_membership(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.provision_two_idle_workers()

    runtime.rebind_connector(
        app_ref="plugin_replacement", app_label="Replacement Connector"
    )

    membership = runtime.group.reconcile_membership(project_id=runtime.project_id)

    assert membership.member_ids == ()


def test_app_ref_rebind_reports_zero_recoverable_consumers(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.provision_two_idle_workers()

    runtime.rebind_connector(
        app_ref="plugin_replacement", app_label="Replacement Connector"
    )

    snapshot = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert snapshot.capacity_decision.recoverable_consumer_count == 0


def test_app_ref_rebind_recovers_busy_assignment_and_exposes_replacement_capacity(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    target_a, _target_b = runtime.provision_two_idle_workers()
    runtime.delegate(roles=("active-reviewer", "replacement-reviewer"))
    work = runtime.claim(wake_target_id=target_a, label="old-connector-worker")
    assert work.assignment is not None
    old_assignment = work.assignment

    runtime.rebind_connector(
        app_ref="plugin_replacement", app_label="Replacement Connector"
    )

    target = runtime.wake_target_repository.read(wake_target_id=target_a)
    slot = runtime.slot_repository.read(slot_id=work.slot.slot_id)
    task = runtime.task_repository.read(
        run_id=old_assignment.run.run_id,
        task_id=old_assignment.task.task_id,
    )
    broker_task = runtime.broker_task_repository.read(
        broker_task_id=old_assignment.broker_task.broker_task_id
    )
    snapshot = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert target.status is WakeTargetStatus.IDLE
    assert slot.status is WorkerSlotStatus.IDLE
    assert task.status is TaskStatus.FAILED
    assert broker_task.status is BrokerTaskStatus.QUEUED
    assert snapshot.capacity_decision is not None
    assert snapshot.capacity_decision.recoverable_consumer_count == 0
    assert snapshot.capacity_decision.capacity_deficit == 2
    assert snapshot.capacity_decision.pending_provisioning_count == 2


def test_register_route_rejects_stale_epoch_renewal_and_recovers_busy_assignment(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    target_a, _target_b = runtime.provision_two_idle_workers()
    runtime.delegate(roles=("active-reviewer", "replacement-reviewer"))
    work = runtime.claim(wake_target_id=target_a, label="old-connector-worker")
    assert work.assignment is not None
    assignment = work.assignment
    presence_before = runtime.wake_presence_repository.read(wake_target_id=target_a)
    binding = runtime.binding_service.get_binding(project_id=runtime.project_id)
    rebound = runtime.binding_service.upsert_binding(
        project_id=runtime.project_id,
        expected_revision=binding.revision,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="sample-project-ref",
            project_path="/g/sample-project-ref",
            app_ref="plugin_replacement",
            app_label="Replacement Connector",
        ),
    )
    assert rebound.connector_epoch == binding.connector_epoch + 1
    runtime.clock.advance(seconds=30)
    assert (
        runtime.wake_target_repository.read(wake_target_id=target_a).status
        is WakeTargetStatus.BUSY
    )
    assert (
        runtime.slot_repository.read(slot_id=work.slot.slot_id).status
        is WorkerSlotStatus.BUSY
    )
    assert (
        runtime.task_repository.read(
            run_id=assignment.run.run_id,
            task_id=assignment.task.task_id,
        ).status
        is TaskStatus.IN_PROGRESS
    )
    assert (
        runtime.broker_task_repository.read(
            broker_task_id=assignment.broker_task.broker_task_id
        ).status
        is BrokerTaskStatus.CLAIMED
    )
    assert runtime.provisioning.pending_provisioning_count(runtime.project_id) == 0
    RuntimeConfigRepository(state_root=runtime.state_root).save(
        record=RuntimeConfigRecord(
            workspace_root=str(runtime.workspace_root.resolve()),
            public_url=None,
            oauth_enabled=False,
        )
    )
    wake_control = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(
            state_root=runtime.state_root
        ),
        wake_target_service=runtime.wake_service,
        wake_presence_service=runtime.wake_presence,
        runtime_presence_tracker=runtime.runtime_presence,
        lock_registry=runtime.locks,
        assignment_lease_reconciler=runtime.assignment_reconciler,
        consumer_group_coordinator=runtime.group,
    )
    app = create_control_panel_app(
        service=ControlPanelService(state_root=runtime.state_root),
        wake_service=wake_control,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/wake/targets/register",
            json={"wake_target_id": target_a, "project_id": runtime.project_id},
        )

    assert response.status_code == 200
    assert response.json()["target"]["status"] == "idle"
    assert (
        runtime.wake_presence_repository.read(wake_target_id=target_a)
        == presence_before
    )
    assert (
        runtime.slot_repository.read(slot_id=work.slot.slot_id).status
        is WorkerSlotStatus.IDLE
    )
    assert (
        runtime.task_repository.read(
            run_id=assignment.run.run_id,
            task_id=assignment.task.task_id,
        ).status
        is TaskStatus.FAILED
    )
    assert (
        runtime.broker_task_repository.read(
            broker_task_id=assignment.broker_task.broker_task_id
        ).status
        is BrokerTaskStatus.QUEUED
    )
    demands, _requests = runtime.provisioning.list_status()
    assert demands[0].spawn_needed == 2
    assert runtime.provisioning.pending_provisioning_count(runtime.project_id) == 2


def test_app_ref_rebind_reserves_replacement_spawn_deficit(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.provision_two_idle_workers()
    old_request_ids = {
        request.request_id for request in runtime.provisioning.list_status()[1]
    }

    runtime.rebind_connector(
        app_ref="plugin_replacement", app_label="Replacement Connector"
    )

    demands, requests = runtime.provisioning.list_status()

    assert demands[0].spawn_needed == 2
    replacement_requests = tuple(
        request for request in requests if request.request_id not in old_request_ids
    )
    assert len(replacement_requests) == 2
    assert {request.adapter_metadata.app_ref for request in replacement_requests} == {
        "plugin_replacement"
    }


def test_app_label_change_with_same_app_ref_keeps_consumers_reusable(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    targets = runtime.provision_two_idle_workers()
    request_ids = {
        request.request_id for request in runtime.provisioning.list_status()[1]
    }

    runtime.rebind_connector(app_ref="plugin_sample", app_label="Renamed Connector")

    for target_id in targets:
        runtime.complete(
            work=runtime.claim(wake_target_id=target_id, label="ignored-on-reuse")
        )
    assert {
        request.request_id for request in runtime.provisioning.list_status()[1]
    } == request_ids
