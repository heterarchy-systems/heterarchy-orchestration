from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from chattocodex.orchestrator_backend.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.orchestrator_backend.broker.broker_dto import WorkerSlotState
from chattocodex.orchestrator_backend.persistence.wake.wake_presence_repository import (
    WakePresenceRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetStatus
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

_ASSIGNMENT_RECOVERY_WINDOW = timedelta(seconds=120)


class WorkerAssignmentLeaseReconciler:
    """Recovers BUSY assignments whose physical Worker Chat did not return after wake/runtime recovery."""

    def __init__(
        self,
        wake_target_repository: WakeTargetRepository,
        wake_presence_repository: WakePresenceRepository,
        abandoned_assignment_recovery: AbandonedAssignmentRecovery,
        wake_target_service: WakeTargetService,
        assignment_lease_renewer: Callable[[str], WorkerSlotState] | None = None,
        lock_registry: EntityLockRegistry | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: wake_target_repository, wake_presence_repository, abandoned_assignment_recovery, wake_target_service, assignment_lease_renewer, lock_registry, now_provider. Returns: None."""
        self._wake_target_repository = wake_target_repository
        self._wake_presence_repository = wake_presence_repository
        self._abandoned_assignment_recovery = abandoned_assignment_recovery
        self._assignment_lease_renewer = assignment_lease_renewer
        self._wake_target_service = wake_target_service
        self._lock_registry = lock_registry or EntityLockRegistry()
        self._now_provider = now_provider or _utc_now

    def reconcile_project(self, project_id: str) -> tuple[str, ...]:
        """Reconcile project. Args: project_id. Returns: tuple[str, ...]."""
        recovered: list[str] = []
        now = self._now_provider()
        for target in self._wake_target_repository.list_all():
            if (
                target.project_id != project_id
                or target.status is not WakeTargetStatus.BUSY
                or target.bound_slot_id is None
            ):
                continue
            presence = self._wake_presence_repository.read_optional(
                wake_target_id=target.wake_target_id
            )
            last_seen_at = (
                target.updated_at if presence is None else presence.last_seen_at
            )
            incompatible = not self._wake_target_service.is_connector_compatible(
                target=target
            )
            if not incompatible and now - last_seen_at <= _ASSIGNMENT_RECOVERY_WINDOW:
                if self._assignment_lease_renewer is not None:
                    self._assignment_lease_renewer(target.bound_slot_id)
                continue
            slot_id = target.bound_slot_id
            did_recover = False

            def should_recover(
                _slot: object,
                incompatible_snapshot: bool = incompatible,
                wake_target_id: str = target.wake_target_id,
                last_seen_snapshot: datetime = last_seen_at,
            ) -> bool:
                """Should recover. Args: _slot, incompatible_snapshot, wake_target_id, last_seen_snapshot. Returns: bool."""
                if incompatible_snapshot:
                    return True
                return (
                    self._wake_presence_repository.read(
                        wake_target_id=wake_target_id
                    ).last_seen_at
                    == last_seen_snapshot
                )

            def after_recovery(
                _recovered_slot: object, recovered_slot_id: str = slot_id
            ) -> None:
                """After recovery. Args: _recovered_slot, recovered_slot_id. Returns: None."""
                nonlocal did_recover
                did_recover = True
                self._wake_target_service.release_slot(slot_id=recovered_slot_id)

            self._abandoned_assignment_recovery.recover(
                slot_id=slot_id,
                reason=(
                    "Physical Worker Chat did not renew its assignment presence within the "
                    "recovery window."
                ),
                should_recover=should_recover,
                after_recovery=after_recovery,
            )
            if did_recover:
                recovered.append(slot_id)
        return tuple(recovered)


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
