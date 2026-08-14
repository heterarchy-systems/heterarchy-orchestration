from collections.abc import Callable
from datetime import UTC, datetime

from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.wake.wake_dto import (
    WakeTargetRetirement,
    WakeTargetState,
    WakeTargetStatus,
)
from chattocodex.wake.wake_exceptions import WakeTargetConflictError
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.worker_runtime.wake_delivery_lease_tracker import (
    WakeDeliveryLeaseTracker,
)
from chattocodex.worker_runtime.worker_runtime_presence_state import (
    WorkerRuntimeAvailability,
)
from chattocodex.worker_runtime.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)


class WakeTargetService:
    def __init__(
        self,
        *,
        repository: WakeTargetRepository,
        presence_service: WakePresenceService,
        lock_registry: EntityLockRegistry,
        runtime_presence_tracker: WorkerRuntimePresenceTracker | None = None,
        delivery_lease_tracker: WakeDeliveryLeaseTracker | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._presence_service = presence_service
        self._runtime_presence_tracker = runtime_presence_tracker
        self._delivery_lease_tracker = delivery_lease_tracker
        self._lock_registry = lock_registry
        self._now_provider = now_provider or _utc_now
        self._connector_epoch_resolver: Callable[[str], int] = lambda _project_id: 0

    def attach_connector_epoch_resolver(self, resolver: Callable[[str], int]) -> None:
        self._connector_epoch_resolver = resolver

    def is_connector_compatible(self, *, target: WakeTargetState) -> bool:
        return target.registered_connector_epoch == self._connector_epoch_resolver(
            target.project_id
        )

    def register_target(
        self,
        *,
        wake_target_id: str,
        project_id: str,
        registered_connector_epoch: int | None = None,
    ) -> WakeTargetState:
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
                        self._connector_epoch_resolver(project_id)
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

    def unregister_provisional_target_if_matches(
        self, *, target: WakeTargetState
    ) -> bool:
        """Delete only the exact pristine state created by a failed registration saga."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            if not self._repository.exists(wake_target_id=target.wake_target_id):
                return False
            current = self._repository.read(wake_target_id=target.wake_target_id)
            if current != target:
                return False
            self._repository.delete(wake_target_id=target.wake_target_id)
            return True

    def get_target(self, *, wake_target_id: str) -> WakeTargetState:
        return self._repository.read(wake_target_id=wake_target_id)

    def retire_unclaimed_target(
        self,
        *,
        wake_target_id: str,
        expected_revision: int,
        before_delete: Callable[[WakeTargetState], None] | None = None,
    ) -> WakeTargetState:
        """Retire one exact non-executing Worker target after runtime ownership loss.

        Args:
            wake_target_id: Exact server-issued target identity.
            expected_revision: Latest target revision observed by the runtime.
            before_delete: Optional synchronous durable lifecycle transition. It
                runs after target validation while the wake lock is held and must
                follow the canonical ``wake -> provisioning`` lock order. A
                failure leaves the target and both presence records unchanged so
                the retirement saga can be retried safely.

        Returns:
            The exact target state removed from durable wake capacity.
        """
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
        *,
        project_id: str,
        targets: tuple[WakeTargetRetirement, ...],
        before_delete: Callable[[tuple[str, ...], tuple[str, ...]], bool] | None = None,
    ) -> tuple[tuple[str, ...], bool]:
        """Retire one exact project batch after validating every target.

        Missing targets are accepted only when ``before_delete`` validates their
        durable provisioning cancellation tombstones. Live targets must all be
        IDLE, unbound, same-project, and at their caller-observed revisions before
        any lifecycle state is mutated.

        Args:
            project_id: Exact project shared by every target.
            targets: Unique compare-and-swap retirement inputs in request order.
            before_delete: Atomic lifecycle callback receiving all IDs and the
                subset already missing from the wake repository.

        Returns:
            Requested IDs in order and whether wake or lifecycle state changed.
        """
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

    def reserve_wake_claim(
        self,
        *,
        wake_target_id: str,
        wake_revision: int,
    ) -> WakeTargetState:
        """Atomically consume one wake revision before allocating Broker state."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            self._require_connector_compatible(target=target)
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
                state=_transition_target(
                    target=target,
                    status=WakeTargetStatus.CLAIMING,
                    bound_slot_id=target.bound_slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )

    def validate_wake_claim(
        self,
        *,
        wake_target_id: str,
        wake_revision: int,
    ) -> WakeTargetState:
        """Validate a wake delivery without consuming it.

        Runtime work claims must use :meth:`reserve_wake_claim`. This read-only
        compatibility seam is kept for diagnostics and focused lease tests.
        """
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            self._require_connector_compatible(target=target)
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
        *,
        wake_target_id: str,
        wake_revision: int,
    ) -> WakeTargetState:
        """Return an uncommitted durable claim to WAKING for a safe retry."""
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            if (
                target.status is not WakeTargetStatus.CLAIMING
                or target.wake_revision != wake_revision
            ):
                return target
            return self._repository.write(
                state=_transition_target(
                    target=target,
                    status=WakeTargetStatus.WAKING,
                    bound_slot_id=target.bound_slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )

    def sync_ready_capacity(
        self,
        *,
        project_id: str,
        ready_count: int,
    ) -> tuple[WakeTargetState, ...]:
        if ready_count < 0:
            raise ValueError("ready_count cannot be negative")
        with self._lock_registry.hold(entity_key="wake:coordination"):
            targets = self._project_targets(project_id=project_id)
            availability_by_target = {
                target.wake_target_id: self._runtime_availability(
                    wake_target_id=target.wake_target_id
                )
                for target in targets
            }
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
                updated.append(
                    self._repository.write(
                        state=_transition_target(
                            target=current,
                            status=WakeTargetStatus.IDLE,
                            bound_slot_id=current.bound_slot_id,
                            wake_revision=current.wake_revision,
                            now=self._now_provider(),
                        )
                    )
                )
                failed_target_ids.add(current.wake_target_id)
                if self._delivery_lease_tracker is not None:
                    self._delivery_lease_tracker.mark_failure(
                        wake_target_id=current.wake_target_id,
                        wake_revision=current.wake_revision,
                    )

            targets = self._project_targets(project_id=project_id)
            availability_by_target = {
                target.wake_target_id: self._runtime_availability(
                    wake_target_id=target.wake_target_id
                )
                for target in targets
            }
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
                    updated.append(
                        self._repository.write(
                            state=_transition_target(
                                target=current,
                                status=WakeTargetStatus.IDLE,
                                bound_slot_id=current.bound_slot_id,
                                wake_revision=current.wake_revision,
                                now=self._now_provider(),
                            )
                        )
                    )
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
                    state=_transition_target(
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

    def bind_target(
        self,
        *,
        wake_target_id: str,
        project_id: str,
        slot_id: str,
        has_assignment: bool,
        wake_revision: int | None = None,
    ) -> WakeTargetState:
        with self._lock_registry.hold(entity_key="wake:coordination"):
            target = self._repository.read(wake_target_id=wake_target_id)
            self._require_connector_compatible(target=target)
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
                state=_transition_target(
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
        *,
        slot_id: str,
        project_id: str,
        has_assignment: bool,
    ) -> WakeTargetState | None:
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
            self._require_connector_compatible(target=target)
            if target.project_id != project_id:
                raise WakeTargetConflictError(
                    wake_target_id=target.wake_target_id,
                    message="Bound wake target belongs to a different project.",
                )
            status = WakeTargetStatus.BUSY if has_assignment else WakeTargetStatus.IDLE
            return self._repository.write(
                state=_transition_target(
                    target=target,
                    status=status,
                    bound_slot_id=slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )

    def release_slot(self, *, slot_id: str) -> WakeTargetState | None:
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
                state=_transition_target(
                    target=target,
                    status=WakeTargetStatus.IDLE,
                    bound_slot_id=slot_id,
                    wake_revision=target.wake_revision,
                    now=self._now_provider(),
                )
            )

    def _project_targets(self, *, project_id: str) -> list[WakeTargetState]:
        return sorted(
            (
                target
                for target in self._repository.list_all()
                if target.project_id == project_id
                and self.is_connector_compatible(target=target)
            ),
            key=lambda target: (target.created_at, target.wake_target_id),
        )

    def _require_connector_compatible(self, *, target: WakeTargetState) -> None:
        if not self.is_connector_compatible(target=target):
            raise WakeTargetConflictError(
                wake_target_id=target.wake_target_id,
                message=(
                    "Wake target was registered under a different connector epoch. "
                    "Register a replacement Worker Chat for the current connector."
                ),
            )

    def _runtime_availability(
        self, *, wake_target_id: str
    ) -> WorkerRuntimeAvailability:
        if self._presence_service.is_active(wake_target_id=wake_target_id):
            return WorkerRuntimeAvailability.ACTIVE
        if (
            self._runtime_presence_tracker is not None
            and self._runtime_presence_tracker.is_active(wake_target_id=wake_target_id)
        ):
            return WorkerRuntimeAvailability.DORMANT
        return WorkerRuntimeAvailability.OFFLINE

    def _in_delivery_cooldown(self, *, wake_target_id: str) -> bool:
        return (
            self._delivery_lease_tracker is not None
            and self._delivery_lease_tracker.is_in_cooldown(
                wake_target_id=wake_target_id
            )
        )


def _transition_target(
    *,
    target: WakeTargetState,
    status: WakeTargetStatus,
    bound_slot_id: str | None,
    wake_revision: int,
    now: datetime,
) -> WakeTargetState:
    return WakeTargetState(
        schema_version=target.schema_version,
        wake_target_id=target.wake_target_id,
        project_id=target.project_id,
        registered_connector_epoch=target.registered_connector_epoch,
        status=status,
        bound_slot_id=bound_slot_id,
        wake_revision=wake_revision,
        revision=target.revision + 1,
        created_at=target.created_at,
        updated_at=now,
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)
