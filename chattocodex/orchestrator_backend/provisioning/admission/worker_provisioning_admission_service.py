"""Serialized browser admission lifecycle for Worker provisioning."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timedelta

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    RevisionConflictError,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_dto import (
    ProjectBindingState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningRequestState,
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_exceptions import (
    ProvisioningConfigurationError,
    ProvisioningConflictError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_policy import (
    admission_blocked,
    admission_candidate_project_ids,
    clear_stale_in_flight,
    is_supported_binding,
    new_request,
    operator_blocked_project_ids,
    request_by_id,
    require_demand,
    require_request,
    retain_terminal_history,
    state_with_request,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_sync import (
    TRANSIENT_REQUEST_STATUSES,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

_TRANSIENT_STATUSES = TRANSIENT_REQUEST_STATUSES
_ADMISSION_CANDIDATE_STATUSES = frozenset(
    {
        ProvisioningRequestStatus.WAITING_ADMISSION,
        ProvisioningRequestStatus.RATE_LIMITED,
        ProvisioningRequestStatus.DEFERRED,
    }
)


class WorkerProvisioningAdmissionService:
    """Own serialized admission and deliberate operator retry transitions."""

    def __init__(
        self,
        repository_provider: Callable[[], WorkerProvisioningRepository],
        binding_provider: Callable[[str], ProjectBindingState | None],
        binding_required_provider: Callable[[], bool],
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime],
        minimum_admission_interval: timedelta,
        admission_lease: timedelta,
        terminal_request_retention: int,
    ) -> None:
        """Initialize this object. Args: repository_provider, binding_provider, binding_required_provider, lock_registry, now_provider, minimum_admission_interval, admission_lease, terminal_request_retention. Returns: None."""
        self._repository_provider = repository_provider
        self._binding_provider = binding_provider
        self._binding_required_provider = binding_required_provider
        self._lock_registry = lock_registry
        self._now_provider = now_provider
        self._minimum_admission_interval = minimum_admission_interval
        self._admission_lease = admission_lease
        self._terminal_request_retention = terminal_request_retention

    @property
    def _repository(self) -> WorkerProvisioningRepository:
        """Repository. Args: None. Returns: WorkerProvisioningRepository."""
        return self._repository_provider()

    def admit_next(self) -> ProvisioningRequestState | None:
        """Admit the next queued request under the global serialized gate. Args: None. Returns: ProvisioningRequestState | None."""
        snapshot = self._repository.read_state()
        candidate_project_ids = admission_candidate_project_ids(state=snapshot)
        binding_lock_project = (
            candidate_project_ids[0] if candidate_project_ids else None
        )
        binding_hold = (
            self._lock_registry.hold(
                entity_key=f"project-binding:{binding_lock_project}"
            )
            if binding_lock_project is not None
            else nullcontext()
        )
        with (
            binding_hold,
            self._lock_registry.hold(entity_key="worker-provisioning:capacity"),
        ):
            now = self._now_provider()
            snapshot = self._repository.read_state()
            candidate_project_ids = admission_candidate_project_ids(state=snapshot)
            bindings = {
                project_id: self._binding_provider(project_id)
                for project_id in candidate_project_ids
            }
            admitted_request_id: str | None = None

            def admit(state: ProvisioningStoreState) -> ProvisioningStoreState:
                """Admit. Args: state. Returns: ProvisioningStoreState."""
                nonlocal admitted_request_id
                in_flight = request_by_id(
                    requests=state.requests,
                    request_id=state.admission.in_flight_request_id,
                )
                if in_flight is not None and in_flight.status in _TRANSIENT_STATUSES:
                    if now - in_flight.updated_at < self._admission_lease:
                        return state
                    expired = replace(
                        in_flight,
                        status=ProvisioningRequestStatus.WAITING_ADMISSION,
                        admission_revision=None,
                        failure_reason="admission_lease_expired",
                        revision=in_flight.revision + 1,
                        updated_at=now,
                    )
                    state = replace(
                        state,
                        requests=tuple(
                            expired if item.request_id == expired.request_id else item
                            for item in state.requests
                        ),
                        admission=replace(
                            state.admission,
                            in_flight_request_id=None,
                            revision=state.admission.revision + 1,
                        ),
                    )
                state = clear_stale_in_flight(state=state)
                if admission_blocked(state.admission, now=now):
                    return state
                binding_required = self._binding_required_provider()
                blocked_project_ids = operator_blocked_project_ids(
                    state=state,
                    bindings=bindings,
                    binding_required=binding_required,
                )
                demand_by_project = {
                    demand.project_id: demand for demand in state.demands
                }
                candidates: list[ProvisioningRequestState] = []
                for request in state.requests:
                    if request.status not in _ADMISSION_CANDIDATE_STATUSES:
                        continue
                    demand = demand_by_project.get(request.project_id)
                    if demand is None:
                        demand = require_demand(
                            state=state,
                            project_id=request.project_id,
                        )
                    if (
                        demand.spawn_needed > 0
                        and request.project_id not in blocked_project_ids
                    ):
                        candidates.append(request)

                candidate_is_new = False
                if not candidates:
                    demand = next(
                        (
                            item
                            for item in state.demands
                            if item.spawn_needed > 0
                            and item.project_id not in blocked_project_ids
                        ),
                        None,
                    )
                    if demand is None:
                        return state
                    candidates = [new_request(demand=demand, now=now)]
                    candidate_is_new = True

                candidates.sort(key=lambda item: (item.created_at, item.request_id))
                for candidate in candidates:
                    binding = bindings.get(candidate.project_id)
                    if binding_required and candidate.project_id not in bindings:
                        continue
                    if binding_required and binding is None:
                        continue
                    if binding is not None and not is_supported_binding(binding):
                        continue
                    admitted = replace(
                        candidate,
                        status=ProvisioningRequestStatus.OPENING_PROJECT,
                        binding_revision=(
                            binding.revision
                            if binding is not None
                            else candidate.binding_revision
                        ),
                        adapter_metadata=(
                            binding.adapter_metadata
                            if binding is not None
                            else candidate.adapter_metadata
                        ),
                        admission_revision=state.admission.revision + 1,
                        revision=candidate.revision + 1,
                        updated_at=now,
                    )
                    requests = (
                        (*state.requests, admitted)
                        if candidate_is_new
                        else tuple(
                            admitted if item.request_id == admitted.request_id else item
                            for item in state.requests
                        )
                    )
                    admitted_request_id = admitted.request_id
                    return retain_terminal_history(
                        state=replace(
                            state,
                            requests=requests,
                            admission=replace(
                                state.admission,
                                in_flight_request_id=admitted.request_id,
                                last_admitted_at=now,
                                next_admission_at=(
                                    now + self._minimum_admission_interval
                                ),
                                revision=state.admission.revision + 1,
                            ),
                            revision=state.revision + 1,
                            updated_at=now,
                        ),
                        terminal_request_retention=self._terminal_request_retention,
                        now=now,
                    )
                return state

            persisted = self._repository.update(updater=admit)
            request = request_by_id(
                requests=persisted.requests,
                request_id=admitted_request_id,
            )
            if request is None or request.status not in _TRANSIENT_STATUSES:
                return None
            return request

    def retry_operator_blocked_request(
        self,
        request_id: str,
        expected_revision: int,
    ) -> ProvisioningRequestState:
        """Queue one operator-blocked request for a deliberate retry. Args: request_id, expected_revision. Returns: ProvisioningRequestState."""
        if expected_revision < 0:
            raise ValueError("expected_revision cannot be negative")
        initial = self._repository.read_state()
        initial_request = require_request(state=initial, request_id=request_id)
        with self._lock_registry.hold(
            entity_key=f"project-binding:{initial_request.project_id}"
        ):
            binding = self._binding_provider(initial_request.project_id)
            if binding is None or not is_supported_binding(binding):
                raise ProvisioningConfigurationError(
                    project_id=initial_request.project_id,
                    message=(
                        "Worker provisioning retry requires a current supported "
                        "ChatGPT project binding."
                    ),
                )
            with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
                now = self._now_provider()

                def retry(state: ProvisioningStoreState) -> ProvisioningStoreState:
                    """Retry. Args: state. Returns: ProvisioningStoreState."""
                    request = require_request(state=state, request_id=request_id)
                    if request.revision != expected_revision:
                        raise RevisionConflictError(
                            operation="worker_provisioning_retry",
                            resource_id=request_id,
                            current_revision=request.revision,
                        )
                    if request.status is not ProvisioningRequestStatus.OPERATOR_BLOCKED:
                        raise ProvisioningConflictError(
                            request_id=request_id,
                            message=(
                                "Only an operator-blocked provisioning request can be "
                                "retried explicitly."
                            ),
                        )
                    if (
                        request.binding_revision is None
                        or request.binding_revision != binding.revision
                        or request.adapter_metadata != binding.adapter_metadata
                    ):
                        raise RevisionConflictError(
                            operation="worker_provisioning_retry_binding",
                            resource_id=request.project_id,
                            current_revision=binding.revision,
                        )
                    if state.admission.in_flight_request_id == request_id:
                        raise ProvisioningConflictError(
                            request_id=request_id,
                            message=(
                                "An operator-blocked request cannot still own global "
                                "browser admission."
                            ),
                        )
                    return state_with_request(
                        state=state,
                        request=replace(
                            request,
                            status=ProvisioningRequestStatus.WAITING_ADMISSION,
                            admission_revision=None,
                            failure_reason=None,
                            revision=request.revision + 1,
                            updated_at=now,
                        ),
                        now=now,
                    )

                persisted = self._repository.update(updater=retry)
                return require_request(state=persisted, request_id=request_id)
