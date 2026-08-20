import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from chattocodex.orchestrator_backend.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskStatus,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.facade.facade_service import ChatFacadeService
from chattocodex.orchestrator_backend.facade.worker_work_service import (
    WorkerWorkService,
)
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import RunStatus
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
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
from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_group_state_store import (
    WorkerRuntimeGroupStateStore,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetStatus
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.capacity.broker_ready_capacity_projection import (
    BrokerReadyCapacityProjection,
)
from chattocodex.orchestrator_backend.worker_runtime.capacity.capacity_decision import (
    CapacityLimits,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.worker_assignment_lease_reconciler import (
    WorkerAssignmentLeaseReconciler,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_coordinator import (
    WorkerRuntimeCoordinator,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
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


class _Runtime:
    def __init__(self, *, tmp_path: Path) -> None:
        self.clock = _Clock()
        self.state_root = tmp_path / "state"
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

        self.workspace = WorkspaceProjectService(workspace_root=workspace_root)
        self.project_id = self.workspace.list_projects()[0].project_id
        self.locks = EntityLockRegistry()
        self.run_repository = RunStateRepository(state_root=self.state_root)
        self.worker_repository = WorkerStateRepository(state_root=self.state_root)
        self.task_repository = TaskStateRepository(state_root=self.state_root)
        self.broker_task_repository = BrokerTaskRepository(state_root=self.state_root)
        self.slot_repository = WorkerSlotRepository(state_root=self.state_root)
        self.work_broker_gateway = FakeWorkBrokerGateway()
        self.wake_target_repository = WakeTargetRepository(state_root=self.state_root)
        self.wake_presence_repository = WakePresenceRepository(
            state_root=self.state_root
        )
        self.run_service = RunOrchestrationService(
            run_repository=self.run_repository,
            workspace_project_service=self.workspace,
            command_runner=GitCommandRunner(),
        )
        self.broker_service = BrokerService(
            work_broker_gateway=self.work_broker_gateway,
            run_repository=self.run_repository,
            worker_repository=self.worker_repository,
            task_repository=self.task_repository,
            broker_task_repository=self.broker_task_repository,
            slot_repository=self.slot_repository,
            workspace_project_service=self.workspace,
            lock_registry=self.locks,
            now_provider=self.clock,
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
            assignment_lease_renewer=lambda slot_id: (
                self.broker_service.renew_assignment_lease(slot_id=slot_id)
            ),
            wake_target_service=self.wake_service,
            lock_registry=self.locks,
            now_provider=self.clock,
        )
        self.group = WorkerRuntimeCoordinator(
            state_store=WorkerRuntimeGroupStateStore(state_root=self.state_root),
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
            now_provider=self.clock,
        )
        self.facade = ChatFacadeService(
            run_service=self.run_service,
            broker_service=self.broker_service,
            wake_target_service=self.wake_service,
            workspace_project_service=self.workspace,
            worker_work_service=WorkerWorkService(
                broker_service=self.broker_service,
                wake_target_service=self.wake_service,
                workspace_project_service=self.workspace,
                worker_runtime_coordinator=self.group,
            ),
            worker_runtime_coordinator=self.group,
        )

    def register_target(self, number: int, *, runtime_heartbeat: bool = False) -> str:
        target_id = f"wtgt_{number:032x}"
        self.wake_service.register_target(
            wake_target_id=target_id,
            project_id=self.project_id,
        )
        if runtime_heartbeat:
            self.runtime_presence.heartbeat(wake_target_id=target_id)
        return target_id

    def publish_tasks(
        self,
        *,
        count: int,
        project_id: str | None = None,
        requested_parallelism: int | None = None,
    ):
        resolved_project_id = project_id or self.project_id
        run = self.run_service.create_run(
            goal="Consumer Group test",
            project_id=resolved_project_id,
            requested_parallelism=requested_parallelism or count,
        )
        tasks = tuple(
            self.broker_service.create_broker_task(
                run_id=run.state.run_id,
                run_handle=run.run_handle,
                objective=f"Task {index}",
                role=f"lane-{index}",
                depends_on=(),
            )
            for index in range(1, count + 1)
        )
        return run, tasks

    def configure_capacity_group(
        self,
        *,
        capacity_limits: CapacityLimits,
        pending_provisioning_count=None,
        on_capacity_decision=None,
    ) -> None:
        self.group = WorkerRuntimeCoordinator(
            state_store=WorkerRuntimeGroupStateStore(state_root=self.state_root),
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
            capacity_limits=capacity_limits,
            pending_provisioning_count=pending_provisioning_count,
            on_capacity_decision=on_capacity_decision,
            now_provider=self.clock,
        )
        self.facade = ChatFacadeService(
            run_service=self.run_service,
            broker_service=self.broker_service,
            wake_target_service=self.wake_service,
            workspace_project_service=self.workspace,
            worker_work_service=WorkerWorkService(
                broker_service=self.broker_service,
                wake_target_service=self.wake_service,
                workspace_project_service=self.workspace,
                worker_runtime_coordinator=self.group,
            ),
            worker_runtime_coordinator=self.group,
        )


def test_topology_generation_changes_only_when_recoverable_runtime_targets_change(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    target_a = runtime.register_target(1, runtime_heartbeat=True)

    generation_one = runtime.group.reconcile_runtime_topology(
        project_id=runtime.project_id
    )
    assert generation_one.topology_generation == 1
    assert generation_one.wake_target_ids == (target_a,)

    runtime.clock.advance(seconds=31)
    dormant_same_generation = runtime.group.reconcile_runtime_topology(
        project_id=runtime.project_id
    )
    assert dormant_same_generation.topology_generation == 1
    assert dormant_same_generation.wake_target_ids == (target_a,)

    target_b = runtime.register_target(2, runtime_heartbeat=True)
    generation_two = runtime.group.reconcile_runtime_topology(
        project_id=runtime.project_id
    )
    assert generation_two.topology_generation == 2
    assert generation_two.wake_target_ids == (target_a, target_b)

    runtime.clock.advance(seconds=45)
    generation_three = runtime.group.reconcile_runtime_topology(
        project_id=runtime.project_id
    )
    assert generation_three.topology_generation == 3
    assert generation_three.wake_target_ids == (target_b,)

    runtime.runtime_presence.heartbeat(wake_target_id=target_a)
    generation_four = runtime.group.reconcile_runtime_topology(
        project_id=runtime.project_id
    )
    assert generation_four.topology_generation == 4
    assert generation_four.wake_target_ids == (target_a, target_b)


def test_runtime_topology_change_keeps_busy_assignment_sticky_when_target_joins(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    target_a = runtime.register_target(10, runtime_heartbeat=True)
    _run, published = runtime.publish_tasks(count=2)

    initial = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert initial.group.topology_generation == 1
    waking_a = runtime.wake_service.get_target(wake_target_id=target_a)
    assert waking_a.status is WakeTargetStatus.WAKING

    first = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=None,
        slot_handle=None,
        label="consumer-a",
        wake_target_id=target_a,
        wake_revision=waking_a.wake_revision,
    )
    assert first.assignment is not None
    first_broker_task_id = first.assignment.broker_task.broker_task_id
    assert first_broker_task_id in {task.broker_task_id for task in published}

    target_b = runtime.register_target(11, runtime_heartbeat=True)
    rebalanced = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert rebalanced.group.topology_generation == 2
    assert rebalanced.group.wake_target_ids == (target_a, target_b)
    still_claimed = runtime.broker_task_repository.read(
        broker_task_id=first_broker_task_id
    )
    sticky_slot = runtime.slot_repository.read(slot_id=first.slot.slot_id)
    target_a_state = runtime.wake_service.get_target(wake_target_id=target_a)
    target_b_state = runtime.wake_service.get_target(wake_target_id=target_b)
    assert still_claimed.status is BrokerTaskStatus.CLAIMED
    assert still_claimed.assigned_slot_id == first.slot.slot_id
    assert sticky_slot.status is WorkerSlotStatus.BUSY
    assert target_a_state.status is WakeTargetStatus.BUSY
    assert target_b_state.status is WakeTargetStatus.WAKING


def test_dead_busy_target_is_recovered_then_remaining_target_gets_requeued_work(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    target_a = runtime.register_target(20)
    target_b = runtime.register_target(21)
    run, published = runtime.publish_tasks(count=1)
    runtime.group.reconcile_project(project_id=runtime.project_id)
    waking_a = runtime.wake_service.get_target(wake_target_id=target_a)
    assert waking_a.status is WakeTargetStatus.WAKING

    first = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=None,
        slot_handle=None,
        label="consumer-a",
        wake_target_id=target_a,
        wake_revision=waking_a.wake_revision,
    )
    assert first.assignment is not None

    runtime.clock.advance(seconds=60)
    runtime.runtime_presence.heartbeat(wake_target_id=target_b)
    runtime.clock.advance(seconds=61)
    runtime.work_broker_gateway.expire_task(
        run_id=run.state.run_id,
        task_id=published[0].broker_task_id,
    )
    recovered = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert recovered.group.wake_target_ids == (target_b,)
    assert recovered.group.topology_generation == 2
    assert recovered.recovered_slot_ids == (first.slot.slot_id,)
    broker_task = runtime.broker_task_repository.read(
        broker_task_id=published[0].broker_task_id
    )
    old_slot = runtime.slot_repository.read(slot_id=first.slot.slot_id)
    target_b_state = runtime.wake_service.get_target(wake_target_id=target_b)
    assert broker_task.status is BrokerTaskStatus.QUEUED
    assert old_slot.status is WorkerSlotStatus.IDLE
    assert target_b_state.status is WakeTargetStatus.WAKING

    replacement = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=None,
        slot_handle=None,
        label="consumer-b",
        wake_target_id=target_b,
        wake_revision=target_b_state.wake_revision,
    )
    assert replacement.assignment is not None
    assert (
        replacement.assignment.broker_task.broker_task_id == published[0].broker_task_id
    )
    assert replacement.assignment.task.task_id != first.assignment.task.task_id


def test_offline_target_cannot_claim_current_runtime_topology(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    target = runtime.register_target(30)
    first = runtime.group.reconcile_runtime_topology(project_id=runtime.project_id)
    assert first.wake_target_ids == (target,)

    runtime.clock.advance(seconds=31)
    offline = runtime.group.reconcile_runtime_topology(project_id=runtime.project_id)
    assert offline.wake_target_ids == ()
    assert offline.topology_generation == 2

    with pytest.raises(InvalidTransitionError, match="recoverable runtime topology"):
        runtime.group.require_wake_target(
            project_id=runtime.project_id,
            wake_target_id=target,
        )


def test_capacity_decision_bounds_n2_demand_and_reuses_existing_consumers(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=2, global_max_workers=2)
    )
    runtime.register_target(40, runtime_heartbeat=True)
    runtime.register_target(41, runtime_heartbeat=True)
    runtime.publish_tasks(count=2, requested_parallelism=2)

    first = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert first.capacity_decision is not None
    assert first.capacity_decision.requested_parallelism == 2
    assert first.capacity_decision.ready_task_count == 2
    assert first.capacity_decision.desired_capacity == 2
    assert first.capacity_decision.recoverable_consumer_count == 2
    assert first.capacity_decision.spawn_needed == 0

    # A new logical Run changes assignment roles/tasks, not physical Consumer
    # ownership. Existing project-local Consumers remain the reusable capacity.
    runtime.publish_tasks(count=2, requested_parallelism=2)
    second = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert second.capacity_decision is not None
    assert second.capacity_decision.desired_capacity == 2
    assert second.capacity_decision.recoverable_consumer_count == 2
    assert second.capacity_decision.spawn_needed == 0


def test_capacity_decision_exposes_exact_bounded_deficit_and_caps(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=1, global_max_workers=3)
    )
    runtime.publish_tasks(count=2, requested_parallelism=2)

    snapshot = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert snapshot.capacity_decision is not None
    assert snapshot.capacity_decision.ready_task_count == 2
    assert snapshot.capacity_decision.requested_parallelism == 2
    assert snapshot.capacity_decision.desired_capacity == 1
    assert snapshot.capacity_decision.capacity_deficit == 1
    assert snapshot.capacity_decision.spawn_needed == 1


def test_busy_consumers_consume_total_engaged_capacity_before_new_wakes(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=2, global_max_workers=2)
    )
    targets = tuple(
        runtime.register_target(number, runtime_heartbeat=True)
        for number in range(90, 94)
    )
    runtime.publish_tasks(count=2, requested_parallelism=2)
    runtime.group.reconcile_project(project_id=runtime.project_id)

    for target_id in targets[:2]:
        target = runtime.wake_service.get_target(wake_target_id=target_id)
        assert target.status is WakeTargetStatus.WAKING
        claimed = runtime.facade.work(
            project_id=runtime.project_id,
            slot_id=None,
            slot_handle=None,
            label=f"consumer-{target_id[-2:]}",
            wake_target_id=target_id,
            wake_revision=target.wake_revision,
        )
        assert claimed.assignment is not None

    runtime.publish_tasks(count=2, requested_parallelism=2)
    next_run = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert next_run.capacity_decision is not None
    assert next_run.capacity_decision.desired_capacity == 2
    assert next_run.capacity_decision.busy_consumer_count == 2
    assert next_run.capacity_decision.wake_target_count == 0
    assert tuple(
        runtime.wake_service.get_target(wake_target_id=target_id).status
        for target_id in targets
    ) == (
        WakeTargetStatus.BUSY,
        WakeTargetStatus.BUSY,
        WakeTargetStatus.IDLE,
        WakeTargetStatus.IDLE,
    )


def test_busy_consumer_reduces_wakes_to_remaining_desired_capacity(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=8, global_max_workers=8)
    )
    targets = tuple(
        runtime.register_target(number, runtime_heartbeat=True)
        for number in range(100, 103)
    )
    runtime.publish_tasks(count=1, requested_parallelism=1)
    runtime.group.reconcile_project(project_id=runtime.project_id)
    first_target = runtime.wake_service.get_target(wake_target_id=targets[0])
    assert first_target.status is WakeTargetStatus.WAKING
    claimed = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=None,
        slot_handle=None,
        label="busy-consumer",
        wake_target_id=targets[0],
        wake_revision=first_target.wake_revision,
    )
    assert claimed.assignment is not None

    runtime.publish_tasks(count=2, requested_parallelism=2)

    reconciled = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert reconciled.capacity_decision is not None
    assert reconciled.capacity_decision.desired_capacity == 3
    assert reconciled.capacity_decision.busy_consumer_count == 1
    assert reconciled.capacity_decision.wake_target_count == 2
    assert tuple(
        runtime.wake_service.get_target(wake_target_id=target_id).status
        for target_id in targets
    ) == (
        WakeTargetStatus.BUSY,
        WakeTargetStatus.WAKING,
        WakeTargetStatus.WAKING,
    )


def test_claimed_task_keeps_parallel_run_capacity_for_remaining_ready_task(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=2, global_max_workers=2)
    )
    target_id = runtime.register_target(110, runtime_heartbeat=True)
    runtime.publish_tasks(count=2, requested_parallelism=2)

    initial = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert initial.capacity_decision is not None
    assert initial.capacity_decision.desired_capacity == 2
    assert initial.capacity_decision.spawn_needed == 1

    target = runtime.wake_service.get_target(wake_target_id=target_id)
    claimed = runtime.facade.work(
        project_id=runtime.project_id,
        slot_id=None,
        slot_handle=None,
        label="first-consumer",
        wake_target_id=target_id,
        wake_revision=target.wake_revision,
    )
    assert claimed.assignment is not None

    while_first_is_busy = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert while_first_is_busy.capacity_decision is not None
    assert while_first_is_busy.capacity_decision.ready_task_count == 1
    assert while_first_is_busy.capacity_decision.busy_consumer_count == 1
    assert while_first_is_busy.capacity_decision.desired_capacity == 2
    assert while_first_is_busy.capacity_decision.spawn_needed == 1


def test_overlapping_runs_aggregate_per_run_engaged_capacity(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=4, global_max_workers=4)
    )
    target_ids = tuple(
        runtime.register_target(number, runtime_heartbeat=True)
        for number in range(120, 124)
    )
    runtime.publish_tasks(count=2, requested_parallelism=2)
    runtime.group.reconcile_project(project_id=runtime.project_id)

    for target_id in target_ids[:2]:
        target = runtime.wake_service.get_target(wake_target_id=target_id)
        claimed = runtime.facade.work(
            project_id=runtime.project_id,
            slot_id=None,
            slot_handle=None,
            label=f"claimed-{target_id[-2:]}",
            wake_target_id=target_id,
            wake_revision=target.wake_revision,
        )
        assert claimed.assignment is not None

    runtime.publish_tasks(count=2, requested_parallelism=2)
    overlapping = runtime.group.reconcile_project(project_id=runtime.project_id)

    assert overlapping.capacity_decision is not None
    assert overlapping.capacity_decision.requested_parallelism == 4
    assert overlapping.capacity_decision.ready_task_count == 2
    assert overlapping.capacity_decision.busy_consumer_count == 2
    assert overlapping.capacity_decision.desired_capacity == 4
    assert overlapping.capacity_decision.wake_target_count == 2


def test_global_waking_reservation_blocks_second_project_wake(tmp_path: Path) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    other_root = tmp_path / "workspace" / "waking-project"
    other_root.mkdir()
    (other_root / "pyproject.toml").write_text(
        "[project]\nname='waking-project'\nversion='0.1.0'\n",
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
            ("git", "-C", str(other_root), *args),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    other_project_id = next(
        project.project_id
        for project in runtime.workspace.list_projects()
        if project.root == other_root
    )
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=1, global_max_workers=1)
    )
    first_target_id = runtime.register_target(130, runtime_heartbeat=True)
    second_target_id = f"wtgt_{131:032x}"
    runtime.wake_service.register_target(
        wake_target_id=second_target_id,
        project_id=other_project_id,
    )
    runtime.runtime_presence.heartbeat(wake_target_id=second_target_id)
    runtime.publish_tasks(count=1, requested_parallelism=1)
    runtime.publish_tasks(
        count=1,
        project_id=other_project_id,
        requested_parallelism=1,
    )

    first = runtime.group.reconcile_project(project_id=runtime.project_id)
    second = runtime.group.reconcile_project(project_id=other_project_id)

    assert first.capacity_decision is not None
    assert first.capacity_decision.wake_target_count == 1
    assert second.capacity_decision is not None
    assert second.capacity_decision.global_busy_consumer_count == 0
    assert second.capacity_decision.wake_target_count == 0
    assert (
        runtime.wake_service.get_target(wake_target_id=first_target_id).status
        is WakeTargetStatus.WAKING
    )
    assert (
        runtime.wake_service.get_target(wake_target_id=second_target_id).status
        is WakeTargetStatus.IDLE
    )


def test_capacity_decision_excludes_blocked_and_cancelled_run_work(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    run = runtime.run_service.create_run(
        goal="Blocked DAG",
        project_id=runtime.project_id,
        requested_parallelism=2,
    )
    prerequisite = runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Complete prerequisite",
        role="first",
        depends_on=(),
    )
    runtime.broker_service.create_broker_task(
        run_id=run.state.run_id,
        run_handle=run.run_handle,
        objective="Blocked successor",
        role="second",
        depends_on=(prerequisite.broker_task_id,),
    )

    active = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert active.capacity_decision is not None
    assert active.capacity_decision.ready_task_count == 1
    assert active.capacity_decision.desired_capacity == 1
    assert active.capacity_decision.spawn_needed == 1

    runtime.run_repository.write(
        state=replace(
            run.state,
            status=RunStatus.CANCELLED,
            revision=run.state.revision + 1,
            updated_at=runtime.clock(),
        )
    )
    cancelled = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert cancelled.capacity_decision is not None
    assert cancelled.capacity_decision.ready_task_count == 0
    assert cancelled.capacity_decision.requested_parallelism == 0
    assert cancelled.capacity_decision.desired_capacity == 0
    assert cancelled.capacity_decision.spawn_needed == 0


def test_capacity_decision_never_uses_other_project_consumers_as_local_capacity(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    other_root = tmp_path / "workspace" / "other-project"
    other_root.mkdir()
    (other_root / "pyproject.toml").write_text(
        "[project]\nname='other-project'\nversion='0.1.0'\n",
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
            ("git", "-C", str(other_root), *args),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    other_project_id = next(
        project.project_id
        for project in runtime.workspace.list_projects()
        if project.root == other_root
    )
    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=2, global_max_workers=1)
    )
    other_target_id = f"wtgt_{50:032x}"
    runtime.wake_service.register_target(
        wake_target_id=other_target_id,
        project_id=other_project_id,
    )
    runtime.runtime_presence.heartbeat(wake_target_id=other_target_id)
    runtime.publish_tasks(count=1, requested_parallelism=1)

    own = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert own.capacity_decision is not None
    assert own.group.wake_target_ids == ()
    assert own.capacity_decision.recoverable_consumer_count == 0
    assert own.capacity_decision.global_headroom == 0
    assert own.capacity_decision.spawn_needed == 0
    assert (
        runtime.wake_service.get_target(wake_target_id=other_target_id).status
        is WakeTargetStatus.IDLE
    )


def test_concurrent_reconcile_reserves_one_deficit_and_never_steals_busy(
    tmp_path: Path,
) -> None:
    runtime = _Runtime(tmp_path=tmp_path)
    pending_by_project: dict[str | None, int] = {None: 0, runtime.project_id: 0}
    decisions = []

    def pending_count(project_id: str | None) -> int:
        return pending_by_project.get(project_id, 0)

    def reserve(decision) -> None:
        decisions.append(decision)
        if decision.spawn_needed:
            pending_by_project[decision.project_id] += decision.spawn_needed
            pending_by_project[None] += decision.spawn_needed

    runtime.configure_capacity_group(
        capacity_limits=CapacityLimits(project_max_workers=2, global_max_workers=2),
        pending_provisioning_count=pending_count,
        on_capacity_decision=reserve,
    )
    runtime.publish_tasks(count=1, requested_parallelism=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        snapshots = list(
            executor.map(
                lambda _index: runtime.group.reconcile_project(
                    project_id=runtime.project_id
                ),
                range(2),
            )
        )

    assert (
        sum(
            snapshot.capacity_decision.spawn_needed
            for snapshot in snapshots
            if snapshot.capacity_decision is not None
        )
        == 1
    )
    assert pending_by_project[runtime.project_id] == 1
    assert len(decisions) == 2

    # Existing cooperative rebalance assertions above establish assignment
    # stickiness; capacity reconciliation must retain that BUSY state as well.
    target = runtime.register_target(60, runtime_heartbeat=True)
    waking = runtime.group.reconcile_project(project_id=runtime.project_id)
    assert waking.capacity_decision is not None
    target_state = runtime.wake_service.get_target(wake_target_id=target)
    if target_state.status is WakeTargetStatus.WAKING:
        work = runtime.facade.work(
            project_id=runtime.project_id,
            slot_id=None,
            slot_handle=None,
            label="busy-consumer",
            wake_target_id=target,
            wake_revision=target_state.wake_revision,
        )
        assert work.assignment is not None
        busy = runtime.group.reconcile_project(project_id=runtime.project_id)
        assert busy.capacity_decision is not None
        assert (
            runtime.slot_repository.read(slot_id=work.slot.slot_id).status
            is WorkerSlotStatus.BUSY
        )
