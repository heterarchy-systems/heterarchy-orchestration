"""Browser callback lifecycle for Worker provisioning requests."""

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta

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
    ProvisioningStaleCallbackError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_policy import (
    failure_status,
    require_admission,
    require_demand,
    require_request,
    retain_terminal_history,
    retry_after_ms,
    state_with_request,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_sync import (
    TRANSIENT_REQUEST_STATUSES,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

_TRANSIENT_STATUSES = TRANSIENT_REQUEST_STATUSES
_PROGRESS_ORDER = {
    ProvisioningRequestStatus.OPENING_PROJECT: 1,
    ProvisioningRequestStatus.CREATING_CHAT: 2,
    ProvisioningRequestStatus.BOOTSTRAPPING: 3,
    ProvisioningRequestStatus.WAITING_REGISTRATION: 4,
}


class WorkerProvisioningCallbackService:
    """Own progress, failure, and cancellation callback state transitions."""

    def __init__(
        self,
        repository_provider: Callable[[], WorkerProvisioningRepository],
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime],
        minimum_admission_interval: timedelta,
        rate_limit_backoff: tuple[timedelta, ...],
        terminal_request_retention: int,
    ) -> None:
        """Initialize this object. Args: repository_provider, lock_registry, now_provider, minimum_admission_interval, rate_limit_backoff, terminal_request_retention. Returns: None."""
        self._repository_provider = repository_provider
        self._lock_registry = lock_registry
        self._now_provider = now_provider
        self._minimum_admission_interval = minimum_admission_interval
        self._rate_limit_backoff = rate_limit_backoff
        self._terminal_request_retention = terminal_request_retention

    @property
    def _repository(self) -> WorkerProvisioningRepository:
        """Repository. Args: None. Returns: WorkerProvisioningRepository."""
        return self._repository_provider()

    def progress_request(
        self,
        request_id: str,
        status: ProvisioningRequestStatus,
        admission_revision: int,
    ) -> ProvisioningRequestState:
        """Advance an admitted request through browser provisioning phases. Args: request_id, status, admission_revision. Returns: ProvisioningRequestState."""
        if status not in _TRANSIENT_STATUSES:
            raise ValueError("progress status must be a transient provisioning state")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()

            def progress(state: ProvisioningStoreState) -> ProvisioningStoreState:
                """Progress. Args: state. Returns: ProvisioningStoreState."""
                request = require_request(state=state, request_id=request_id)
                require_admission(
                    request=request,
                    admission=state.admission,
                    admission_revision=admission_revision,
                )
                if request.status is status:
                    return state
                if (
                    request.status not in _TRANSIENT_STATUSES
                    or _PROGRESS_ORDER[status] < _PROGRESS_ORDER[request.status]
                ):
                    raise ProvisioningConflictError(
                        request_id=request_id,
                        message=(
                            "Provisioning callback attempted an invalid lifecycle transition."
                        ),
                    )
                return state_with_request(
                    state=state,
                    request=replace(
                        request,
                        status=status,
                        revision=request.revision + 1,
                        updated_at=now,
                    ),
                    now=now,
                )

            persisted = self._repository.update(updater=progress)
            return require_request(state=persisted, request_id=request_id)

    def fail_callback(
        self,
        request_id: str,
        admission_revision: int,
        reason: str,
        rate_limited: bool = False,
    ) -> tuple[ProvisioningRequestState, int | None]:
        """Record a durable failure with conservative automatic retry policy. Args: request_id, admission_revision, reason, rate_limited. Returns: tuple[ProvisioningRequestState, int | None]."""
        if not reason.strip():
            raise ValueError("reason cannot be empty")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            initial = self._repository.read_state()
            initial_request = require_request(state=initial, request_id=request_id)
            expected_status = failure_status(
                reason=reason,
                rate_limited=rate_limited,
            )
            if initial_request.status is expected_status:
                if (
                    initial_request.admission_revision == admission_revision
                    and initial_request.failure_reason == reason
                ):
                    return initial_request, retry_after_ms(initial.admission, now=now)
                raise ProvisioningConflictError(
                    request_id=request_id,
                    message="Provisioning failure callback is stale or mismatched.",
                )

            def fail(state: ProvisioningStoreState) -> ProvisioningStoreState:
                """Fail. Args: state. Returns: ProvisioningStoreState."""
                request = require_request(state=state, request_id=request_id)
                if request.status not in _TRANSIENT_STATUSES:
                    raise ProvisioningConflictError(
                        request_id=request_id,
                        message="Provisioning failure callback is duplicate or terminal.",
                    )
                require_admission(
                    request=request,
                    admission=state.admission,
                    admission_revision=admission_revision,
                )
                attempt = (
                    min(
                        state.admission.backoff_attempt + 1,
                        len(self._rate_limit_backoff),
                    )
                    if rate_limited
                    else 0
                )
                backoff_until = (
                    now + self._rate_limit_backoff[attempt - 1]
                    if rate_limited
                    else None
                )
                admission = replace(
                    state.admission,
                    in_flight_request_id=None,
                    backoff_attempt=attempt,
                    backoff_until=backoff_until,
                    next_admission_at=backoff_until
                    or (now + self._minimum_admission_interval),
                    revision=state.admission.revision + 1,
                )
                return retain_terminal_history(
                    state=replace(
                        state,
                        requests=tuple(
                            replace(
                                request,
                                status=failure_status(
                                    reason=reason,
                                    rate_limited=rate_limited,
                                ),
                                failure_reason=reason,
                                revision=request.revision + 1,
                                updated_at=now,
                            )
                            if item.request_id == request_id
                            else item
                            for item in state.requests
                        ),
                        admission=admission,
                        revision=state.revision + 1,
                        updated_at=now,
                    ),
                    terminal_request_retention=self._terminal_request_retention,
                    now=now,
                    protected_request_ids=frozenset({request_id}),
                )

            persisted = self._repository.update(updater=fail)
            return (
                require_request(state=persisted, request_id=request_id),
                retry_after_ms(persisted.admission, now=now),
            )

    def cancel_request(
        self, request_id: str, admission_revision: int | None = None
    ) -> ProvisioningRequestState:
        """Cancel a queued or admitted request without demand resurrection. Args: request_id, admission_revision. Returns: ProvisioningRequestState."""
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()

            def cancel(state: ProvisioningStoreState) -> ProvisioningStoreState:
                """Cancel. Args: state. Returns: ProvisioningStoreState."""
                request = require_request(state=state, request_id=request_id)
                if request.status is ProvisioningRequestStatus.CANCELLED:
                    return state
                if request.status in {
                    ProvisioningRequestStatus.ACTIVE,
                    ProvisioningRequestStatus.OPERATOR_BLOCKED,
                    ProvisioningRequestStatus.FAILED,
                }:
                    raise ProvisioningConflictError(
                        request_id=request_id,
                        message="Terminal provisioning request cannot be cancelled.",
                    )
                if state.admission.in_flight_request_id == request_id:
                    if admission_revision is None:
                        raise ProvisioningStaleCallbackError(
                            request_id=request_id,
                            current_revision=state.admission.revision,
                        )
                    require_admission(
                        request=request,
                        admission=state.admission,
                        admission_revision=admission_revision,
                    )
                demand = require_demand(state=state, project_id=request.project_id)
                updated_demand = replace(
                    demand,
                    spawn_needed=max(0, demand.spawn_needed - 1),
                    revision=demand.revision + 1,
                    updated_at=now,
                )
                admission = state.admission
                if state.admission.in_flight_request_id == request_id:
                    admission = replace(
                        admission,
                        in_flight_request_id=None,
                        revision=admission.revision + 1,
                    )
                return retain_terminal_history(
                    state=replace(
                        state,
                        demands=tuple(
                            updated_demand
                            if item.project_id == demand.project_id
                            else item
                            for item in state.demands
                        ),
                        requests=tuple(
                            replace(
                                request,
                                status=ProvisioningRequestStatus.CANCELLED,
                                failure_reason="cancelled",
                                revision=request.revision + 1,
                                updated_at=now,
                            )
                            if item.request_id == request_id
                            else item
                            for item in state.requests
                        ),
                        admission=admission,
                        revision=state.revision + 1,
                        updated_at=now,
                    ),
                    terminal_request_retention=self._terminal_request_retention,
                    now=now,
                    protected_request_ids=frozenset({request_id}),
                )

            persisted = self._repository.update(updater=cancel)
            return require_request(state=persisted, request_id=request_id)
