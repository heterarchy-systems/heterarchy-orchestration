"""Runtime-ownership retirement for Worker provisioning requests."""

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime

from chattocodex.orchestrator_backend.persistence.worker.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningRequestState,
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_exceptions import (
    ProvisioningConflictError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_policy import (
    request_by_id,
    require_request,
    retain_terminal_history,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_sync import (
    TRANSIENT_REQUEST_STATUSES,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

_RUNTIME_OWNERSHIP_LOST = "runtime_ownership_lost"


class WorkerProvisioningRetirementService:
    """Own runtime-ownership retirement for registered Worker runtimes."""

    def __init__(
        self,
        repository: WorkerProvisioningRepository,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime],
        terminal_request_retention: int,
    ) -> None:
        """Initialize this object. Args: repository, lock_registry, now_provider, terminal_request_retention. Returns: None."""
        self._repository = repository
        self._lock_registry = lock_registry
        self._now_provider = now_provider
        self._terminal_request_retention = terminal_request_retention

    def retire_runtime_ownership(
        self, wake_target_id: str
    ) -> ProvisioningRequestState | None:
        """Retire the exact active request whose runtime ownership was lost. Args: wake_target_id. Returns: ProvisioningRequestState | None."""
        if not wake_target_id.strip():
            raise ValueError("wake_target_id cannot be empty")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            retired_request_id: str | None = None

            def retire(state: ProvisioningStoreState) -> ProvisioningStoreState:
                """Retire. Args: state. Returns: ProvisioningStoreState."""
                nonlocal retired_request_id
                matching = tuple(
                    request
                    for request in state.requests
                    if request.wake_target_id == wake_target_id
                )
                retirable = _select_retirable_request(requests=matching)
                if retirable is None:
                    already_retired = _select_retirement_tombstone(requests=matching)
                    if already_retired is not None:
                        retired_request_id = already_retired.request_id
                    return state
                retired_request_id = retirable.request_id
                admission = state.admission
                if admission.in_flight_request_id == retirable.request_id:
                    admission = replace(
                        admission,
                        in_flight_request_id=None,
                        revision=admission.revision + 1,
                    )
                return retain_terminal_history(
                    state=replace(
                        state,
                        requests=tuple(
                            replace(
                                retirable,
                                status=ProvisioningRequestStatus.CANCELLED,
                                failure_reason=_RUNTIME_OWNERSHIP_LOST,
                                revision=retirable.revision + 1,
                                updated_at=now,
                            )
                            if request.request_id == retirable.request_id
                            else request
                            for request in state.requests
                        ),
                        admission=admission,
                        revision=state.revision + 1,
                        updated_at=now,
                    ),
                    terminal_request_retention=self._terminal_request_retention,
                    now=now,
                    protected_request_ids=frozenset({retirable.request_id}),
                )

            persisted = self._repository.update(updater=retire)
            return request_by_id(
                requests=persisted.requests,
                request_id=retired_request_id,
            )

    def retire_runtime_ownership_batch(
        self,
        project_id: str,
        wake_target_ids: tuple[str, ...],
    ) -> tuple[tuple[ProvisioningRequestState, ...], bool]:
        """Atomically retire or validate tombstones for one exact project batch. Args: project_id, wake_target_ids. Returns: tuple[tuple[ProvisioningRequestState, ...], bool]."""
        if not wake_target_ids:
            raise ValueError("wake_target_ids cannot be empty")
        if len(wake_target_ids) != len(set(wake_target_ids)):
            raise ValueError("wake_target_ids must be unique")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            retired_request_ids: tuple[str, ...] = ()
            lifecycle_changed = False

            def retire(state: ProvisioningStoreState) -> ProvisioningStoreState:
                """Retire. Args: state. Returns: ProvisioningStoreState."""
                nonlocal lifecycle_changed, retired_request_ids
                requests_by_target: dict[str, list[ProvisioningRequestState]] = {}
                requested_target_ids = frozenset(wake_target_ids)
                for request in state.requests:
                    if (
                        request.project_id == project_id
                        and request.wake_target_id in requested_target_ids
                        and request.wake_target_id is not None
                    ):
                        requests_by_target.setdefault(
                            request.wake_target_id, []
                        ).append(request)

                matched: list[ProvisioningRequestState] = []
                for wake_target_id in wake_target_ids:
                    candidates = tuple(requests_by_target.get(wake_target_id, ()))
                    request = _select_retirable_request(requests=candidates)
                    if request is None:
                        request = _select_retirement_tombstone(requests=candidates)
                    if request is None:
                        raise ProvisioningConflictError(
                            request_id=wake_target_id,
                            message=(
                                "Batch retirement requires an ACTIVE or transient "
                                "provisioning owner, or its runtime-ownership-loss "
                                "tombstone in the requested project."
                            ),
                        )
                    matched.append(request)

                retired_request_ids = tuple(item.request_id for item in matched)
                retirable_request_ids = frozenset(
                    item.request_id
                    for item in matched
                    if _is_retirable_request(request=item)
                )
                if not retirable_request_ids:
                    return state
                lifecycle_changed = True
                admission = state.admission
                if admission.in_flight_request_id in retirable_request_ids:
                    admission = replace(
                        admission,
                        in_flight_request_id=None,
                        revision=admission.revision + 1,
                    )
                return retain_terminal_history(
                    state=replace(
                        state,
                        requests=tuple(
                            replace(
                                request,
                                status=ProvisioningRequestStatus.CANCELLED,
                                failure_reason=_RUNTIME_OWNERSHIP_LOST,
                                revision=request.revision + 1,
                                updated_at=now,
                            )
                            if request.request_id in retirable_request_ids
                            else request
                            for request in state.requests
                        ),
                        admission=admission,
                        revision=state.revision + 1,
                        updated_at=now,
                    ),
                    terminal_request_retention=self._terminal_request_retention,
                    now=now,
                    protected_request_ids=frozenset(retired_request_ids),
                )

            persisted = self._repository.update(updater=retire)
            return (
                tuple(
                    require_request(state=persisted, request_id=request_id)
                    for request_id in retired_request_ids
                ),
                lifecycle_changed,
            )


def _is_retirable_request(request: ProvisioningRequestState) -> bool:
    """Is retirable request. Args: request. Returns: bool."""
    return (
        request.status is ProvisioningRequestStatus.ACTIVE
        or request.status in TRANSIENT_REQUEST_STATUSES
    )


def _select_retirable_request(
    requests: tuple[ProvisioningRequestState, ...],
) -> ProvisioningRequestState | None:
    """Select retirable request. Args: requests. Returns: ProvisioningRequestState | None."""
    return next(
        (request for request in requests if _is_retirable_request(request=request)),
        None,
    )


def _select_retirement_tombstone(
    requests: tuple[ProvisioningRequestState, ...],
) -> ProvisioningRequestState | None:
    """Select retirement tombstone. Args: requests. Returns: ProvisioningRequestState | None."""
    return next(
        (
            request
            for request in requests
            if request.status is ProvisioningRequestStatus.CANCELLED
            and request.failure_reason == _RUNTIME_OWNERSHIP_LOST
        ),
        None,
    )
