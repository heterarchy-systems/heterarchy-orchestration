from collections.abc import Callable
from datetime import UTC, datetime
from typing import TypeVar

from chattocodex.consumer_group.capacity_decision import (
    CapacityDecision,
    CapacityLimits,
)
from chattocodex.consumer_group.consumer_group_state import (
    ConsumerGroupMember,
    ConsumerGroupSnapshot,
    ConsumerGroupState,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_exceptions import InvalidTransitionError
from chattocodex.persistence.consumer_group_state_store import ConsumerGroupStateStore
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.wake.wake_dto import WakeTargetStatus
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.worker_runtime.broker_ready_capacity_projection import (
    BrokerReadyCapacityProjection,
)
from chattocodex.worker_runtime.worker_assignment_lease_reconciler import (
    WorkerAssignmentLeaseReconciler,
)
from chattocodex.worker_runtime.worker_runtime_presence_state import (
    WorkerRuntimeAvailability,
)
from chattocodex.worker_runtime.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)

_MutationResult = TypeVar("_MutationResult")


class ConsumerGroupCoordinator:
    """Owns recoverable Worker membership, generations, and cooperative rebalance ticks."""

    def __init__(
        self,
        *,
        state_store: ConsumerGroupStateStore,
        wake_target_repository: WakeTargetRepository,
        wake_presence_service: WakePresenceService,
        runtime_presence_tracker: WorkerRuntimePresenceTracker,
        wake_target_service: WakeTargetService,
        ready_capacity_projection: BrokerReadyCapacityProjection,
        assignment_lease_reconciler: WorkerAssignmentLeaseReconciler,
        lock_registry: EntityLockRegistry,
        capacity_limits: CapacityLimits | None = None,
        pending_provisioning_count: Callable[[str | None], int] | None = None,
        on_capacity_decision: Callable[[CapacityDecision], object] | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Create a coordinator and its capacity-decision publication seam.

        Args:
            state_store: Durable Consumer Group membership/generation store.
            wake_target_repository: Project-bound physical Worker target store.
            wake_presence_service: Active Chat presence reader.
            runtime_presence_tracker: Recoverable dormant-runtime presence reader.
            wake_target_service: Reuse/wake executor; it never owns capacity policy.
            ready_capacity_projection: Broker-owned READY-demand reader.
            assignment_lease_reconciler: Cooperative dead-BUSY recovery service.
            lock_registry: Shared in-process synchronization registry.
            capacity_limits: Project/global caps. Defaults preserve the existing
                32-task boundary until an operator configures tighter limits.
            pending_provisioning_count: Optional durable reservation reader. It
                receives a project id, or ``None`` for global reservations.
            on_capacity_decision: Optional synchronous callback for an admission
                service to reserve the immutable remaining provisioning deficit.
                The callback must durably revalidate the decision under its own
                cross-process lock before creating any provisioning request.
            now_provider: UTC clock override for deterministic tests.
        """
        self._state_store = state_store
        self._wake_target_repository = wake_target_repository
        self._wake_presence_service = wake_presence_service
        self._runtime_presence_tracker = runtime_presence_tracker
        self._wake_target_service = wake_target_service
        self._ready_capacity_projection = ready_capacity_projection
        self._assignment_lease_reconciler = assignment_lease_reconciler
        self._lock_registry = lock_registry
        self._capacity_limits = capacity_limits or CapacityLimits()
        self._pending_provisioning_count = (
            pending_provisioning_count or _no_pending_provisioning
        )
        self._on_capacity_decision = on_capacity_decision
        self._now_provider = now_provider or _utc_now

    def reconcile_membership(self, *, project_id: str) -> ConsumerGroupState:
        """Refresh the durable recoverable-member generation for one project.

        Args:
            project_id: Canonical project identity.

        Returns:
            Persisted Consumer Group membership state.
        """
        with self._lock_registry.hold(entity_key=f"consumer-group:{project_id}"):
            state, _changed = self._reconcile_membership_locked(project_id=project_id)
            return state

    def require_member(
        self,
        *,
        project_id: str,
        wake_target_id: str,
    ) -> ConsumerGroupState:
        """Require a Worker target to belong to the current recoverable group.

        Args:
            project_id: Canonical project identity.
            wake_target_id: Opaque physical Worker routing target.

        Returns:
            Current durable Consumer Group state.

        Raises:
            InvalidTransitionError: If the target is no longer recoverable.
        """
        with self._lock_registry.hold(entity_key=f"consumer-group:{project_id}"):
            state, _changed = self._reconcile_membership_locked(project_id=project_id)
            if wake_target_id not in state.member_ids:
                raise InvalidTransitionError(
                    operation="consumer_group_claim",
                    resource_id=wake_target_id,
                    message=(
                        "Worker is not a recoverable member of the current Consumer Group "
                        "generation. Wait for runtime membership to recover before claiming work."
                    ),
                )
            return state

    def reconcile_project(self, *, project_id: str) -> ConsumerGroupSnapshot:
        """Reconcile membership, leases, wake reuse, and provisioning demand.

        Reconciliation is the scheduler choke point: it calculates the immutable
        capacity decision, wakes reusable ACTIVE consumers before DORMANT
        consumers, and publishes only the remaining provisionable deficit.

        Args:
            project_id: Canonical project identity.

        Returns:
            Membership, recovery, wake, and immutable capacity-decision state.
        """
        # The global capacity lock makes global headroom and the callback's durable
        # reservation write serializable across projects. The project lock retains
        # cooperative rebalance/assignment behavior within the group.
        with (
            self._lock_registry.hold(entity_key="consumer-group:capacity"),
            self._lock_registry.hold(entity_key=f"consumer-group:{project_id}"),
        ):
            return self._reconcile_project_locked(project_id=project_id)

    def mutate_project_and_reconcile(
        self,
        *,
        project_id: str,
        mutation: Callable[[], tuple[_MutationResult, bool]],
    ) -> _MutationResult:
        """Serialize one project mutation with exactly one scheduler reconcile.

        The callback must follow the existing downstream lock order and must not
        re-enter Consumer Group reconciliation.

        Args:
            project_id: Canonical project whose capacity is being mutated.
            mutation: Synchronous mutation returning its result and whether it
                changed lifecycle state. Reconciliation still runs for exact
                replays so the POST has one deterministic scheduler boundary.

        Returns:
            The callback result after the single locked reconcile completes.
        """
        with (
            self._lock_registry.hold(entity_key="consumer-group:capacity"),
            self._lock_registry.hold(entity_key=f"consumer-group:{project_id}"),
        ):
            result, _ = mutation()
            self._reconcile_project_locked(project_id=project_id)
            return result

    def _reconcile_project_locked(self, *, project_id: str) -> ConsumerGroupSnapshot:
        group, membership_changed = self._reconcile_membership_locked(
            project_id=project_id
        )
        recovered_slot_ids = self._assignment_lease_reconciler.reconcile_project(
            project_id=project_id
        )
        broker_capacity = self._ready_capacity_projection.project_capacity(
            project_id=project_id
        )
        decision = self._build_capacity_decision(
            project_id=project_id,
            group=group,
            ready_task_count=broker_capacity.ready_task_count,
            requested_parallelism=broker_capacity.requested_parallelism,
            engaged_task_count=broker_capacity.engaged_task_count,
        )
        wake_updates = self._wake_target_service.sync_ready_capacity(
            project_id=project_id,
            ready_count=decision.wake_target_count,
        )
        if self._on_capacity_decision is not None:
            self._on_capacity_decision(decision)
        members = self._member_views(project_id=project_id, member_ids=group.member_ids)
        return ConsumerGroupSnapshot(
            group=group,
            members=members,
            ready_count=decision.ready_task_count,
            recovered_slot_ids=recovered_slot_ids,
            wake_transition_count=len(wake_updates),
            membership_changed=membership_changed,
            capacity_decision=decision,
        )

    def _build_capacity_decision(
        self,
        *,
        project_id: str,
        group: ConsumerGroupState,
        ready_task_count: int,
        requested_parallelism: int,
        engaged_task_count: int,
    ) -> CapacityDecision:
        all_targets = self._wake_target_repository.list_all()
        project_targets = tuple(
            sorted(
                (target for target in all_targets if target.project_id == project_id),
                key=lambda target: (target.created_at, target.wake_target_id),
            )
        )
        availability_by_target = {
            target.wake_target_id: self._availability(
                wake_target_id=target.wake_target_id
            )
            for target in all_targets
        }
        recoverable_consumer_count = len(group.member_ids)
        busy_consumer_count = sum(
            target.status is WakeTargetStatus.BUSY
            and self._wake_target_service.is_connector_compatible(target=target)
            for target in project_targets
        )
        waking_consumer_count = sum(
            target.status in {WakeTargetStatus.WAKING, WakeTargetStatus.CLAIMING}
            and self._wake_target_service.is_connector_compatible(target=target)
            and availability_by_target[target.wake_target_id]
            is not WorkerRuntimeAvailability.OFFLINE
            for target in project_targets
        )
        project_pending = self._require_non_negative_pending(project_id=project_id)
        global_pending = self._require_non_negative_pending(project_id=None)
        global_recoverable_count = sum(
            availability_by_target[target.wake_target_id]
            is not WorkerRuntimeAvailability.OFFLINE
            and self._wake_target_service.is_connector_compatible(target=target)
            for target in all_targets
        )
        global_busy_consumer_count = sum(
            target.status is WakeTargetStatus.BUSY
            and self._wake_target_service.is_connector_compatible(target=target)
            for target in all_targets
        )
        global_waking_consumer_count = sum(
            target.status in {WakeTargetStatus.WAKING, WakeTargetStatus.CLAIMING}
            and self._wake_target_service.is_connector_compatible(target=target)
            and availability_by_target[target.wake_target_id]
            is not WorkerRuntimeAvailability.OFFLINE
            for target in all_targets
        )
        other_project_waking_count = (
            global_waking_consumer_count - waking_consumer_count
        )
        desired_capacity = min(
            engaged_task_count,
            self._capacity_limits.project_max_workers,
            self._capacity_limits.global_max_workers,
        )
        capacity_deficit = max(0, desired_capacity - recoverable_consumer_count)
        wake_target_count = min(
            max(0, desired_capacity - busy_consumer_count),
            max(0, self._capacity_limits.project_max_workers - busy_consumer_count),
            max(
                0,
                self._capacity_limits.global_max_workers
                - global_busy_consumer_count
                - other_project_waking_count,
            ),
        )
        global_headroom = max(
            0,
            self._capacity_limits.global_max_workers
            - global_recoverable_count
            - global_pending,
        )
        spawn_needed = min(
            max(0, capacity_deficit - project_pending),
            global_headroom,
        )
        return CapacityDecision(
            project_id=project_id,
            requested_parallelism=requested_parallelism,
            ready_task_count=ready_task_count,
            project_max_workers=self._capacity_limits.project_max_workers,
            global_max_workers=self._capacity_limits.global_max_workers,
            global_headroom=global_headroom,
            recoverable_consumer_count=recoverable_consumer_count,
            busy_consumer_count=busy_consumer_count,
            global_busy_consumer_count=global_busy_consumer_count,
            waking_consumer_count=waking_consumer_count,
            wake_target_count=wake_target_count,
            pending_provisioning_count=project_pending,
            global_pending_provisioning_count=global_pending,
            desired_capacity=desired_capacity,
            capacity_deficit=capacity_deficit,
            spawn_needed=spawn_needed,
        )

    def _require_non_negative_pending(self, *, project_id: str | None) -> int:
        pending = self._pending_provisioning_count(project_id)
        if pending < 0:
            scope = project_id or "global"
            raise ValueError(
                f"Pending provisioning count for {scope!r} cannot be negative"
            )
        return pending

    def _reconcile_membership_locked(
        self,
        *,
        project_id: str,
    ) -> tuple[ConsumerGroupState, bool]:
        member_ids = tuple(
            target.wake_target_id
            for target in self._project_targets(project_id=project_id)
            if self._availability(wake_target_id=target.wake_target_id)
            is not WorkerRuntimeAvailability.OFFLINE
            and self._wake_target_service.is_connector_compatible(target=target)
        )
        now = self._now_provider()
        if not self._state_store.exists(project_id=project_id):
            state = self._state_store.write(
                state=ConsumerGroupState(
                    schema_version=1,
                    project_id=project_id,
                    generation=1 if member_ids else 0,
                    member_ids=member_ids,
                    revision=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            return state, bool(member_ids)

        current = self._state_store.read(project_id=project_id)
        if current.member_ids == member_ids:
            return current, False
        state = self._state_store.write(
            state=ConsumerGroupState(
                schema_version=current.schema_version,
                project_id=current.project_id,
                generation=current.generation + 1,
                member_ids=member_ids,
                revision=current.revision + 1,
                created_at=current.created_at,
                updated_at=now,
            )
        )
        return state, True

    def _member_views(
        self,
        *,
        project_id: str,
        member_ids: tuple[str, ...],
    ) -> tuple[ConsumerGroupMember, ...]:
        member_id_set = set(member_ids)
        return tuple(
            ConsumerGroupMember(
                wake_target_id=target.wake_target_id,
                availability=self._availability(wake_target_id=target.wake_target_id),
                wake_status=target.status,
                bound_slot_id=target.bound_slot_id,
            )
            for target in self._project_targets(project_id=project_id)
            if target.wake_target_id in member_id_set
        )

    def _project_targets(self, *, project_id: str):
        return tuple(
            sorted(
                (
                    target
                    for target in self._wake_target_repository.list_all()
                    if target.project_id == project_id
                ),
                key=lambda target: (target.created_at, target.wake_target_id),
            )
        )

    def _availability(self, *, wake_target_id: str) -> WorkerRuntimeAvailability:
        if self._wake_presence_service.is_active(wake_target_id=wake_target_id):
            return WorkerRuntimeAvailability.ACTIVE
        if self._runtime_presence_tracker.is_active(wake_target_id=wake_target_id):
            return WorkerRuntimeAvailability.DORMANT
        return WorkerRuntimeAvailability.OFFLINE


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _no_pending_provisioning(_project_id: str | None) -> int:
    """Return zero when no provisioning reservation store is wired.

    Args:
        _project_id: Project scope, or ``None`` for the global scope.

    Returns:
        Zero, because this compatibility default has no reservation store.
    """
    return 0
