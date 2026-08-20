from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_wake_capability_repository import (
    WorkerWakeCapabilityRepository,
)
from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_dto import (
    WorkerWakeCapabilityState,
    WorkerWakeCapabilityStatus,
    WorkerWakeTransport,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

_DEFAULT_CAPABILITY_TTL = timedelta(minutes=30)


class WorkerWakeCapabilityService:
    """Issue and consume one-shot Worker wake capabilities independent of delivery UI."""

    def __init__(
        self,
        repository: WorkerWakeCapabilityRepository,
        lock_registry: EntityLockRegistry,
        physical_target_exists: Callable[[str], bool] | None = None,
        now_provider: Callable[[], datetime] | None = None,
        capability_ttl: timedelta = _DEFAULT_CAPABILITY_TTL,
    ) -> None:
        """Initialize this object. Args: repository, lock_registry, physical_target_exists, now_provider, capability_ttl. Returns: None."""
        self._repository = repository
        self._lock_registry = lock_registry
        self._physical_target_exists = physical_target_exists or (
            lambda _target_id: False
        )
        self._now_provider = now_provider or _utc_now
        self._capability_ttl = capability_ttl
        self._connector_epoch_resolver: Callable[[str], int] = lambda _project_id: 0

    def attach_connector_epoch_resolver(self, resolver: Callable[[str], int]) -> None:
        """Attach connector epoch resolver. Args: resolver. Returns: None."""
        self._connector_epoch_resolver = resolver

    def owns(self, wake_target_id: str) -> bool:
        """Owns. Args: wake_target_id. Returns: bool."""
        return self._repository.exists(wake_target_id=wake_target_id)

    def issue(
        self,
        project_id: str,
        run_id: str,
        transport: WorkerWakeTransport = WorkerWakeTransport.SCHEDULER,
    ) -> WorkerWakeCapabilityState:
        """Issue. Args: project_id, run_id, transport. Returns: WorkerWakeCapabilityState."""
        with self._lock_registry.hold(entity_key="wake:worker-capability"):
            wake_target_id = self._new_unique_target_id()
            now = self._now_provider()
            return self._repository.create(
                state=WorkerWakeCapabilityState(
                    schema_version=1,
                    wake_target_id=wake_target_id,
                    project_id=project_id,
                    run_id=run_id,
                    registered_connector_epoch=self._connector_epoch_resolver(
                        project_id
                    ),
                    wake_revision=1,
                    transport=transport,
                    status=WorkerWakeCapabilityStatus.ISSUED,
                    bound_slot_id=None,
                    revision=0,
                    issued_at=now,
                    expires_at=now + self._capability_ttl,
                    updated_at=now,
                )
            )

    def reserve(
        self,
        wake_target_id: str,
        wake_revision: int,
    ) -> WorkerWakeCapabilityState:
        """Reserve. Args: wake_target_id, wake_revision. Returns: WorkerWakeCapabilityState."""
        with self._lock_registry.hold(entity_key="wake:worker-capability"):
            current = self._repository.read(wake_target_id=wake_target_id)
            self._require_current(current=current, wake_revision=wake_revision)
            if current.status is not WorkerWakeCapabilityStatus.ISSUED:
                raise CapabilityDeniedError(
                    operation="worker_wake_capability_reserve",
                    resource_id=wake_target_id,
                )
            now = self._now_provider()
            if now >= current.expires_at:
                raise CapabilityDeniedError(
                    operation="worker_wake_capability_reserve",
                    resource_id=wake_target_id,
                )
            return self._repository.write(
                state=_transition(
                    current=current,
                    status=WorkerWakeCapabilityStatus.CLAIMING,
                    now=now,
                )
            )

    def commit(
        self,
        wake_target_id: str,
        wake_revision: int,
        slot_id: str,
    ) -> WorkerWakeCapabilityState:
        """Commit. Args: wake_target_id, wake_revision, slot_id. Returns: WorkerWakeCapabilityState."""
        with self._lock_registry.hold(entity_key="wake:worker-capability"):
            current = self._repository.read(wake_target_id=wake_target_id)
            self._require_current(current=current, wake_revision=wake_revision)
            if current.status is not WorkerWakeCapabilityStatus.CLAIMING:
                raise InvalidTransitionError(
                    operation="worker_wake_capability_commit",
                    resource_id=wake_target_id,
                    message="Worker wake capability is not reserved for a claim.",
                )
            return self._repository.write(
                state=_transition(
                    current=current,
                    status=WorkerWakeCapabilityStatus.CONSUMED,
                    now=self._now_provider(),
                    bound_slot_id=slot_id,
                )
            )

    def owns_slot(self, slot_id: str) -> bool:
        """Owns slot. Args: slot_id. Returns: bool."""
        return any(
            capability.bound_slot_id == slot_id
            for capability in self._repository.list_all()
        )

    def abort(
        self,
        wake_target_id: str,
        wake_revision: int,
    ) -> WorkerWakeCapabilityState:
        """Abort. Args: wake_target_id, wake_revision. Returns: WorkerWakeCapabilityState."""
        with self._lock_registry.hold(entity_key="wake:worker-capability"):
            current = self._repository.read(wake_target_id=wake_target_id)
            self._require_current(current=current, wake_revision=wake_revision)
            if current.status is not WorkerWakeCapabilityStatus.CLAIMING:
                return current
            return self._repository.write(
                state=_transition(
                    current=current,
                    status=WorkerWakeCapabilityStatus.ISSUED,
                    now=self._now_provider(),
                )
            )

    def _require_current(
        self,
        current: WorkerWakeCapabilityState,
        wake_revision: int,
    ) -> None:
        """Require current. Args: current, wake_revision. Returns: None."""
        if current.wake_revision != wake_revision:
            raise CapabilityDeniedError(
                operation="worker_wake_capability_validate",
                resource_id=current.wake_target_id,
            )
        if current.registered_connector_epoch != self._connector_epoch_resolver(
            current.project_id
        ):
            raise CapabilityDeniedError(
                operation="worker_wake_capability_validate",
                resource_id=current.wake_target_id,
            )

    def _new_unique_target_id(self) -> str:
        """New unique target id. Args: None. Returns: str."""
        for _ in range(8):
            candidate = f"wtgt_{uuid4().hex}"
            if self._repository.exists(wake_target_id=candidate):
                continue
            if self._physical_target_exists(candidate):
                continue
            return candidate
        raise RuntimeError("Unable to allocate a unique Worker wake capability id.")


def _transition(
    current: WorkerWakeCapabilityState,
    status: WorkerWakeCapabilityStatus,
    now: datetime,
    bound_slot_id: str | None = None,
) -> WorkerWakeCapabilityState:
    """Transition. Args: current, status, now, bound_slot_id. Returns: WorkerWakeCapabilityState."""
    return WorkerWakeCapabilityState(
        schema_version=current.schema_version,
        wake_target_id=current.wake_target_id,
        project_id=current.project_id,
        run_id=current.run_id,
        registered_connector_epoch=current.registered_connector_epoch,
        wake_revision=current.wake_revision,
        transport=current.transport,
        status=status,
        bound_slot_id=(
            current.bound_slot_id if bound_slot_id is None else bound_slot_id
        ),
        revision=current.revision + 1,
        issued_at=current.issued_at,
        expires_at=current.expires_at,
        updated_at=now,
    )


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
