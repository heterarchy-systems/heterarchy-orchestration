from collections.abc import Callable

from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
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


class WakeTargetRetirementService:
    """Own compare-and-swap retirement sagas for unclaimed Worker wake targets."""

    def __init__(
        self,
        repository: WakeTargetRepository,
        presence_service: WakePresenceService,
        lock_registry: EntityLockRegistry,
        runtime_presence_tracker: WorkerRuntimePresenceTracker | None = None,
        delivery_lease_tracker: WakeDeliveryLeaseTracker | None = None,
    ) -> None:
        """Initialize this object. Args: repository, presence_service, lock_registry, runtime_presence_tracker, delivery_lease_tracker. Returns: None."""
        self._repository = repository
        self._presence_service = presence_service
        self._lock_registry = lock_registry
        self._runtime_presence_tracker = runtime_presence_tracker
        self._delivery_lease_tracker = delivery_lease_tracker

    def retire_unclaimed_target(
        self,
        wake_target_id: str,
        expected_revision: int,
        before_delete: Callable[[WakeTargetState], None] | None = None,
    ) -> WakeTargetState:
        """Retire one exact non-executing Worker target after runtime ownership loss. Args: wake_target_id, expected_revision, before_delete. Returns: WakeTargetState."""

        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            if target.revision != expected_revision:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message=(
                        "Wake target changed before retirement. Refresh its current "
                        "revision and retry only if it is still unclaimed."
                    ),
                )
            if target.status not in {WakeTargetStatus.IDLE, WakeTargetStatus.WAKING}:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message=(
                        "A claiming or busy Worker target cannot be retired. Wait for "
                        "assignment recovery or completion before retrying."
                    ),
                )
            if target.bound_slot_id is not None:
                raise WakeTargetConflictError(
                    wake_target_id=wake_target_id,
                    message=(
                        "A Worker target bound to a reusable Slot cannot be retired. "
                        "Release or recover the Slot ownership before retrying."
                    ),
                )
            if before_delete is not None:
                before_delete(target)
            if (
                target.status is WakeTargetStatus.WAKING
                and self._delivery_lease_tracker is not None
            ):
                self._delivery_lease_tracker.cancel(
                    wake_target_id=wake_target_id,
                    wake_revision=target.wake_revision,
                )
            self._repository.delete(wake_target_id=wake_target_id)
            self._presence_service.remove(wake_target_id=wake_target_id)
            if self._runtime_presence_tracker is not None:
                self._runtime_presence_tracker.remove(wake_target_id=wake_target_id)
            return target

    def retire_unclaimed_targets_batch(
        self,
        project_id: str,
        targets: tuple[WakeTargetRetirement, ...],
        before_delete: Callable[[tuple[str, ...], tuple[str, ...]], bool] | None = None,
    ) -> tuple[tuple[str, ...], bool]:
        """Retire one exact project batch after validating every target. Args: project_id, targets, before_delete. Returns: tuple[tuple[str, ...], bool]."""

        if not targets:
            raise ValueError("targets cannot be empty")
        target_ids = tuple(item.wake_target_id for item in targets)
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("wake_target_id values must be unique")

        with self._lock_registry.hold(entity_key="wake:coordination"):
            live_targets: list[WakeTargetState] = []
            missing_target_ids: list[str] = []
            for item in targets:
                if not self._repository.exists(wake_target_id=item.wake_target_id):
                    missing_target_ids.append(item.wake_target_id)
                    continue
                target = self._repository.read(wake_target_id=item.wake_target_id)
                if target.project_id != project_id:
                    raise WakeTargetConflictError(
                        wake_target_id=item.wake_target_id,
                        message="Every retirement target must belong to the requested project.",
                    )
                if target.revision != item.expected_revision:
                    raise WakeTargetConflictError(
                        wake_target_id=item.wake_target_id,
                        message=(
                            "Wake target changed before batch retirement. Refresh the "
                            "project target list and retry the complete batch."
                        ),
                    )
                if (
                    target.status is not WakeTargetStatus.IDLE
                    or target.bound_slot_id is not None
                ):
                    raise WakeTargetConflictError(
                        wake_target_id=item.wake_target_id,
                        message=(
                            "Batch retirement accepts only IDLE, unbound Worker targets. "
                            "Wait for assignment recovery or completion before retrying."
                        ),
                    )
                live_targets.append(target)

            lifecycle_changed = (
                before_delete(target_ids, tuple(missing_target_ids))
                if before_delete is not None
                else False
            )
            for target in live_targets:
                self._repository.delete(wake_target_id=target.wake_target_id)
            for wake_target_id in target_ids:
                self._presence_service.remove(wake_target_id=wake_target_id)
                if self._runtime_presence_tracker is not None:
                    self._runtime_presence_tracker.remove(wake_target_id=wake_target_id)
            return target_ids, bool(live_targets) or lifecycle_changed
