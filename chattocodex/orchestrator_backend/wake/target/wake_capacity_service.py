from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_state_policy import (
    transition_wake_target,
)
from chattocodex.orchestrator_backend.wake.wake_dto import (
    WakeTargetState,
    WakeTargetStatus,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.wake_delivery_lease_tracker import (
    WakeDeliveryLeaseTracker,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_enums import (
    WorkerRuntimeAvailability,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class ConnectorCompatibility(Protocol):
    """Determine whether a wake target is compatible with the active connector."""

    def __call__(self, target: WakeTargetState) -> bool:
        """Invoke this callable. Args: target. Returns: bool."""
        ...


class WakeCapacityService:
    """Reconcile ready Broker demand against reusable Worker wake-target capacity."""

    def __init__(
        self,
        repository: WakeTargetRepository,
        presence_service: WakePresenceService,
        is_connector_compatible: ConnectorCompatibility,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime],
        runtime_presence_tracker: WorkerRuntimePresenceTracker | None = None,
        delivery_lease_tracker: WakeDeliveryLeaseTracker | None = None,
    ) -> None:
        """Initialize this object. Args: repository, presence_service, is_connector_compatible, lock_registry, now_provider, runtime_presence_tracker, delivery_lease_tracker. Returns: None."""
        self._repository = repository
        self._presence_service = presence_service
        self._is_connector_compatible = is_connector_compatible
        self._lock_registry = lock_registry
        self._now_provider = now_provider
        self._runtime_presence_tracker = runtime_presence_tracker
        self._delivery_lease_tracker = delivery_lease_tracker

    def sync_ready_capacity(
        self,
        project_id: str,
        ready_count: int,
    ) -> tuple[WakeTargetState, ...]:
        """Recover stale deliveries, trim excess wakes, and wake available targets. Args: project_id, ready_count. Returns: tuple[WakeTargetState, ...]."""

        if ready_count < 0:
            raise ValueError("ready_count cannot be negative")
        with self._lock_registry.hold(entity_key="wake:coordination"):
            targets = self._project_targets(project_id=project_id)
            availability_by_target = self._availability_by_target(targets=targets)
            failed_target_ids: set[str] = set()
            updated: list[WakeTargetState] = []
            for target in targets:
                if target.status not in {
                    WakeTargetStatus.WAKING,
                    WakeTargetStatus.CLAIMING,
                }:
                    continue
                availability = availability_by_target[target.wake_target_id]
                lease_expired = (
                    self._delivery_lease_tracker is not None
                    and self._delivery_lease_tracker.is_expired(
                        wake_target_id=target.wake_target_id,
                        wake_revision=target.wake_revision,
                    )
                )
                if (
                    availability is not WorkerRuntimeAvailability.OFFLINE
                    and not lease_expired
                ):
                    continue
                current = self._repository.read(wake_target_id=target.wake_target_id)
                if current.status not in {
                    WakeTargetStatus.WAKING,
                    WakeTargetStatus.CLAIMING,
                }:
                    continue
                updated.append(self._write_idle(target=current))
                failed_target_ids.add(current.wake_target_id)
                if self._delivery_lease_tracker is not None:
                    self._delivery_lease_tracker.mark_failure(
                        wake_target_id=current.wake_target_id,
                        wake_revision=current.wake_revision,
                    )

            targets = self._project_targets(project_id=project_id)
            availability_by_target = self._availability_by_target(targets=targets)
            recoverable_target_ids = {
                target_id
                for target_id, availability in availability_by_target.items()
                if availability is not WorkerRuntimeAvailability.OFFLINE
            }
            claiming = [
                target
                for target in targets
                if target.status is WakeTargetStatus.CLAIMING
                and target.wake_target_id in recoverable_target_ids
            ]
            waking = [
                target
                for target in targets
                if target.status is WakeTargetStatus.WAKING
                and target.wake_target_id in recoverable_target_ids
            ]

            permitted_waking_count = max(0, ready_count - len(claiming))
            if len(waking) > permitted_waking_count:
                for target in reversed(waking[permitted_waking_count:]):
                    current = self._repository.read(
                        wake_target_id=target.wake_target_id
                    )
                    if current.status is not WakeTargetStatus.WAKING:
                        continue
                    updated.append(self._write_idle(target=current))
                    if self._delivery_lease_tracker is not None:
                        self._delivery_lease_tracker.cancel(
                            wake_target_id=current.wake_target_id,
                            wake_revision=current.wake_revision,
                        )
                return tuple(updated)

            needed = max(0, ready_count - len(claiming) - len(waking))
            if needed == 0:
                return tuple(updated)
            active_idle = [
                target
                for target in targets
                if target.status is WakeTargetStatus.IDLE
                and availability_by_target[target.wake_target_id]
                is WorkerRuntimeAvailability.ACTIVE
                and target.wake_target_id not in failed_target_ids
                and not self._in_delivery_cooldown(wake_target_id=target.wake_target_id)
            ]
            dormant_idle = [
                target
                for target in targets
                if target.status is WakeTargetStatus.IDLE
                and availability_by_target[target.wake_target_id]
                is WorkerRuntimeAvailability.DORMANT
                and target.wake_target_id not in failed_target_ids
                and not self._in_delivery_cooldown(wake_target_id=target.wake_target_id)
            ]
            for target in (*active_idle, *dormant_idle)[:needed]:
                current = self._repository.read(wake_target_id=target.wake_target_id)
                if current.status is not WakeTargetStatus.IDLE:
                    continue
                waking_target = self._repository.write(
                    state=transition_wake_target(
                        target=current,
                        status=WakeTargetStatus.WAKING,
                        bound_slot_id=current.bound_slot_id,
                        wake_revision=current.wake_revision + 1,
                        now=self._now_provider(),
                    )
                )
                updated.append(waking_target)
                if self._delivery_lease_tracker is not None:
                    self._delivery_lease_tracker.begin(
                        wake_target_id=waking_target.wake_target_id,
                        wake_revision=waking_target.wake_revision,
                    )
            return tuple(updated)

    def _write_idle(self, target: WakeTargetState) -> WakeTargetState:
        """Write idle. Args: target. Returns: WakeTargetState."""
        return self._repository.write(
            state=transition_wake_target(
                target=target,
                status=WakeTargetStatus.IDLE,
                bound_slot_id=target.bound_slot_id,
                wake_revision=target.wake_revision,
                now=self._now_provider(),
            )
        )

    def _project_targets(self, project_id: str) -> list[WakeTargetState]:
        """Project targets. Args: project_id. Returns: list[WakeTargetState]."""
        return sorted(
            (
                target
                for target in self._repository.list_all()
                if target.project_id == project_id
                and self._is_connector_compatible(target=target)
            ),
            key=lambda target: (target.created_at, target.wake_target_id),
        )

    def _availability_by_target(
        self,
        targets: list[WakeTargetState],
    ) -> dict[str, WorkerRuntimeAvailability]:
        """Availability by target. Args: targets. Returns: dict[str, WorkerRuntimeAvailability]."""
        return {
            target.wake_target_id: self._runtime_availability(
                wake_target_id=target.wake_target_id
            )
            for target in targets
        }

    def _runtime_availability(
        self,
        wake_target_id: str,
    ) -> WorkerRuntimeAvailability:
        """Runtime availability. Args: wake_target_id. Returns: WorkerRuntimeAvailability."""
        if self._presence_service.is_active(wake_target_id=wake_target_id):
            return WorkerRuntimeAvailability.ACTIVE
        if (
            self._runtime_presence_tracker is not None
            and self._runtime_presence_tracker.is_active(wake_target_id=wake_target_id)
        ):
            return WorkerRuntimeAvailability.DORMANT
        return WorkerRuntimeAvailability.OFFLINE

    def _in_delivery_cooldown(self, wake_target_id: str) -> bool:
        """In delivery cooldown. Args: wake_target_id. Returns: bool."""
        return (
            self._delivery_lease_tracker is not None
            and self._delivery_lease_tracker.is_in_cooldown(
                wake_target_id=wake_target_id
            )
        )
