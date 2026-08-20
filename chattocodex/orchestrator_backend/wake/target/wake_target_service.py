from collections.abc import Callable
from datetime import UTC, datetime

from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_capacity_service import (
    WakeCapacityService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_connector_policy import (
    WakeTargetConnectorPolicy,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_retirement_service import (
    WakeTargetRetirementService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_state_policy import (
    transition_wake_target,
)
from chattocodex.orchestrator_backend.wake.wake_dto import (
    WakeTargetRetirement,
    WakeTargetState,
    WakeTargetStatus,
)
from chattocodex.orchestrator_backend.wake.wake_exceptions import (
    WakeTargetConflictError,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.wake_delivery_lease_tracker import (
    WakeDeliveryLeaseTracker,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class WakeTargetService:
    def __init__(
        self,
        repository: WakeTargetRepository,
        presence_service: WakePresenceService,
        lock_registry: EntityLockRegistry,
        runtime_presence_tracker: WorkerRuntimePresenceTracker | None = None,
        delivery_lease_tracker: WakeDeliveryLeaseTracker | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: repository, presence_service, lock_registry, runtime_presence_tracker, delivery_lease_tracker, now_provider. Returns: None."""
        self._repository = repository
        self._presence_service = presence_service
        self._delivery_lease_tracker = delivery_lease_tracker
        self._lock_registry = lock_registry
        self._now_provider = now_provider or _utc_now
        self._connector_policy = WakeTargetConnectorPolicy()
        self._retirement_service = WakeTargetRetirementService(
            repository=repository,
            presence_service=presence_service,
            lock_registry=lock_registry,
            runtime_presence_tracker=runtime_presence_tracker,
            delivery_lease_tracker=delivery_lease_tracker,
        )
        self._capacity_service = WakeCapacityService(
            repository=repository,
            presence_service=presence_service,
            is_connector_compatible=self.is_connector_compatible,
            lock_registry=lock_registry,
            now_provider=self._now_provider,
            runtime_presence_tracker=runtime_presence_tracker,
            delivery_lease_tracker=delivery_lease_tracker,
        )

    def attach_connector_epoch_resolver(self, resolver: Callable[[str], int]) -> None:
        """Attach connector epoch resolver. Args: resolver. Returns: None."""
        self._connector_policy.attach_epoch_resolver(resolver=resolver)

    def is_connector_compatible(self, target: WakeTargetState) -> bool:
        """Is connector compatible. Args: target. Returns: bool."""
        return self._connector_policy.is_compatible(target=target)

    def register_target(
        self,
        wake_target_id: str,
        project_id: str,
        registered_connector_epoch: int | None = None,
    ) -> WakeTargetState:
        """Register target. Args: wake_target_id, project_id, registered_connector_epoch. Returns: WakeTargetState."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            if self._repository.exists(wake_target_id=wake_target_id):
                existing = self._repository.read(wake_target_id=wake_target_id)
                if existing.project_id != project_id:
                    raise WakeTargetConflictError(
                        message=(
                            f"Wake target {wake_target_id!r} is already registered to a "
                            "different project. Register this Chat with its original project or "
                            "create a new wake target."
                        )
                    )
                if self.is_connector_compatible(target=existing):
                    self._presence_service.heartbeat(wake_target_id=wake_target_id)
                return existing

            now = self._now_provider()
            created = self._repository.create(
                state=WakeTargetState(
                    schema_version=1,
                    wake_target_id=wake_target_id,
                    project_id=project_id,
                    registered_connector_epoch=(
                        self._connector_policy.current_epoch(project_id=project_id)
                        if registered_connector_epoch is None
                        else registered_connector_epoch
                    ),
                    status=WakeTargetStatus.IDLE,
                    bound_slot_id=None,
                    wake_revision=0,
                    revision=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            self._presence_service.heartbeat(wake_target_id=wake_target_id)
            return created

    def unregister_provisional_target_if_matches(self, target: WakeTargetState) -> bool:
        """Delete only the exact pristine state created by a failed registration saga. Args: target. Returns: bool."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            if not self._repository.exists(wake_target_id=target.wake_target_id):
                return False
            current = self._repository.read(wake_target_id=target.wake_target_id)
            if current != target:
                return False
            self._repository.delete(wake_target_id=target.wake_target_id)
            return True

    def get_target(self, wake_target_id: str) -> WakeTargetState:
        """Get target. Args: wake_target_id. Returns: WakeTargetState."""
        return self._repository.read(wake_target_id=wake_target_id)

    def retire_unclaimed_target(
        self,
        wake_target_id: str,
        expected_revision: int,
        before_delete: Callable[[WakeTargetState], None] | None = None,
    ) -> WakeTargetState:
        """Retire unclaimed target. Args: wake_target_id, expected_revision, before_delete. Returns: WakeTargetState."""
        return self._retirement_service.retire_unclaimed_target(
            wake_target_id=wake_target_id,
            expected_revision=expected_revision,
            before_delete=before_delete,
        )

    def retire_unclaimed_targets_batch(
        self,
        project_id: str,
        targets: tuple[WakeTargetRetirement, ...],
        before_delete: Callable[[tuple[str, ...], tuple[str, ...]], bool] | None = None,
    ) -> tuple[tuple[str, ...], bool]:
        """Retire unclaimed targets batch. Args: project_id, targets, before_delete. Returns: tuple[tuple[str, ...], bool]."""
        return self._retirement_service.retire_unclaimed_targets_batch(
            project_id=project_id,
            targets=targets,
            before_delete=before_delete,
        )

    def reserve_wake_claim(
        self,
        wake_target_id: str,
        wake_revision: int,
    ) -> WakeTargetState:
        """Atomically consume one wake revision before allocating Broker state. Args: wake_target_id, wake_revision. Returns: WakeTargetState."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            self._connector_policy.require_compatible(target=target)
            if target.status is not WakeTargetStatus.WAKING:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message=(
                        "This wake reservation is no longer active. Wait for a fresh automated "
                        "wake instead of claiming work from a stale Chat turn."
                    ),
                )
            if target.wake_revision != wake_revision:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message=(
                        "This wake revision is stale. Wait for the current automated wake before "
                        "claiming delegated work."
                    ),
                )
            if (
                self._delivery_lease_tracker is not None
                and not self._delivery_lease_tracker.is_claimable(
                    wake_target_id=wake_target_id,
                    wake_revision=wake_revision,
                )
            ):
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message=(
                        "This wake delivery lease expired or is cooling down. Wait for a fresh "
                        "automated wake before claiming work."
                    ),
                )
            return self._repository.write(
                state=transition_wake_target(
                    target=target,
                    status=WakeTargetStatus.CLAIMING,
                    bound_slot_id=target.bound_slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )

    def validate_wake_claim(
        self,
        wake_target_id: str,
        wake_revision: int,
    ) -> WakeTargetState:
        """Validate a wake delivery without consuming it.

        Runtime work claims must use :meth:`reserve_wake_claim`. This read-only
        compatibility seam is kept for diagnostics and focused lease tests. Args: wake_target_id, wake_revision. Returns: WakeTargetState."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            self._connector_policy.require_compatible(target=target)
            if target.status is not WakeTargetStatus.WAKING:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message="This wake reservation is no longer active.",
                )
            if target.wake_revision != wake_revision:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message="This wake revision is stale.",
                )
            if (
                self._delivery_lease_tracker is not None
                and not self._delivery_lease_tracker.is_claimable(
                    wake_target_id=wake_target_id,
                    wake_revision=wake_revision,
                )
            ):
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message="This wake delivery lease is not claimable.",
                )
            return target

    def abort_wake_claim(
        self,
        wake_target_id: str,
        wake_revision: int,
    ) -> WakeTargetState:
        """Return an uncommitted durable claim to WAKING for a safe retry. Args: wake_target_id, wake_revision. Returns: WakeTargetState."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            if (
                target.status is not WakeTargetStatus.CLAIMING
                or target.wake_revision != wake_revision
            ):
                return target
            return self._repository.write(
                state=transition_wake_target(
                    target=target,
                    status=WakeTargetStatus.WAKING,
                    bound_slot_id=target.bound_slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )

    def sync_ready_capacity(
        self,
        project_id: str,
        ready_count: int,
    ) -> tuple[WakeTargetState, ...]:
        """Sync ready capacity. Args: project_id, ready_count. Returns: tuple[WakeTargetState, ...]."""
        return self._capacity_service.sync_ready_capacity(
            project_id=project_id,
            ready_count=ready_count,
        )

    def bind_target(
        self,
        wake_target_id: str,
        project_id: str,
        slot_id: str,
        has_assignment: bool,
        wake_revision: int | None = None,
    ) -> WakeTargetState:
        """Bind target. Args: wake_target_id, project_id, slot_id, has_assignment, wake_revision. Returns: WakeTargetState."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            self._connector_policy.require_compatible(target=target)
            if wake_revision is not None and (
                target.status is not WakeTargetStatus.CLAIMING
                or target.wake_revision != wake_revision
            ):
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message="Wake reservation changed before the Worker could bind its Slot.",
                )
            if target.project_id != project_id:
                raise WakeTargetConflictError(
                    message=(
                        f"Wake target {wake_target_id!r} belongs to another project and cannot "
                        "bind to this Worker Slot."
                    )
                )
            if (
                target.status is WakeTargetStatus.BUSY
                and target.bound_slot_id != slot_id
            ):
                raise WakeTargetConflictError(
                    message=(
                        f"Wake target {wake_target_id!r} is already busy with another Worker "
                        "Slot. Finish that assignment before rebinding it."
                    )
                )
            duplicate = next(
                (
                    existing
                    for existing in self._repository.list_all()
                    if existing.bound_slot_id == slot_id
                    and existing.wake_target_id != wake_target_id
                ),
                None,
            )
            if duplicate is not None:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message=(
                        f"Worker Slot {slot_id!r} is already bound to wake target "
                        f"{duplicate.wake_target_id!r}. A reusable Slot can have only one "
                        "physical wake target."
                    ),
                )
            status = WakeTargetStatus.BUSY if has_assignment else WakeTargetStatus.IDLE
            bound = self._repository.write(
                state=transition_wake_target(
                    target=target,
                    status=status,
                    bound_slot_id=slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )
            if wake_revision is not None and self._delivery_lease_tracker is not None:
                self._delivery_lease_tracker.mark_success(
                    wake_target_id=wake_target_id,
                    wake_revision=wake_revision,
                )
            return bound

    def refresh_bound_slot(
        self,
        slot_id: str,
        project_id: str,
        has_assignment: bool,
    ) -> WakeTargetState | None:
        """Refresh bound slot. Args: slot_id, project_id, has_assignment. Returns: WakeTargetState | None."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            matches = tuple(
                target
                for target in self._repository.list_all()
                if target.bound_slot_id == slot_id
            )
            if not matches:
                return None
            if len(matches) > 1:
                raise WakeTargetConflictError(
                    message=(
                        f"Worker Slot {slot_id!r} is bound to multiple wake targets. Resolve "
                        "the duplicate physical mapping before continuing."
                    )
                )
            target = matches[0]
            self._connector_policy.require_compatible(target=target)
            if target.project_id != project_id:
                raise WakeTargetConflictError(
                    wake_target_id=target.wake_target_id,
                    message="Bound wake target belongs to a different project.",
                )
            status = WakeTargetStatus.BUSY if has_assignment else WakeTargetStatus.IDLE
            return self._repository.write(
                state=transition_wake_target(
                    target=target,
                    status=status,
                    bound_slot_id=slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )

    def release_slot(self, slot_id: str) -> WakeTargetState | None:
        """Release slot. Args: slot_id. Returns: WakeTargetState | None."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            targets = tuple(
                target
                for target in self._repository.list_all()
                if target.bound_slot_id == slot_id
            )
            if not targets:
                return None
            if len(targets) > 1:
                raise WakeTargetConflictError(
                    message=(
                        f"Worker Slot {slot_id!r} is bound to multiple wake targets. Resolve the "
                        "duplicate target state before releasing this Slot."
                    )
                )
            target = targets[0]
            return self._repository.write(
                state=transition_wake_target(
                    target=target,
                    status=WakeTargetStatus.IDLE,
                    bound_slot_id=slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
