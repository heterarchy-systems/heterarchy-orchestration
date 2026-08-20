from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime
from uuid import uuid4

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_dto import (
    ProjectBindingState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningAdmissionState,
    ProvisioningDemandState,
    ProvisioningRequestState,
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_exceptions import (
    ProvisioningStaleCallbackError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_sync import (
    TRANSIENT_REQUEST_STATUSES,
    find_demand,
    prune_terminal_request_history,
)


def require_demand(
    state: ProvisioningStoreState, project_id: str
) -> ProvisioningDemandState:
    """Require demand. Args: state, project_id. Returns: ProvisioningDemandState."""
    demand = find_demand(state=state, project_id=project_id)
    if demand is None:
        raise ResourceNotFoundError(
            operation="worker_provisioning_demand_read", resource_id=project_id
        )
    return demand


def request_by_id(
    requests: tuple[ProvisioningRequestState, ...], request_id: str | None
) -> ProvisioningRequestState | None:
    """Request by id. Args: requests, request_id. Returns: ProvisioningRequestState | None."""
    if request_id is None:
        return None
    return next((item for item in requests if item.request_id == request_id), None)


def require_request(
    state: ProvisioningStoreState, request_id: str
) -> ProvisioningRequestState:
    """Require request. Args: state, request_id. Returns: ProvisioningRequestState."""
    request = request_by_id(requests=state.requests, request_id=request_id)
    if request is None:
        raise ResourceNotFoundError(
            operation="worker_provisioning_request_read", resource_id=request_id
        )
    return request


def require_admission(
    request: ProvisioningRequestState,
    admission: ProvisioningAdmissionState,
    admission_revision: int,
) -> None:
    """Require one callback to still own the current global admission lease. Args: request, admission, admission_revision. Returns: None."""
    if (
        admission.in_flight_request_id != request.request_id
        or request.admission_revision != admission_revision
        or admission.revision < admission_revision
    ):
        raise ProvisioningStaleCallbackError(
            request_id=request.request_id,
            current_revision=admission.revision,
        )


def admission_candidate_project_ids(state: ProvisioningStoreState) -> tuple[str, ...]:
    """Admission candidate project ids. Args: state. Returns: tuple[str, ...]."""
    in_flight = request_by_id(
        requests=state.requests,
        request_id=state.admission.in_flight_request_id,
    )
    if in_flight is not None and in_flight.status in TRANSIENT_REQUEST_STATUSES:
        return (in_flight.project_id,)
    demands_by_project = {item.project_id: item for item in state.demands}

    def demand_for(project_id: str) -> ProvisioningDemandState:
        """Demand for. Args: project_id. Returns: ProvisioningDemandState."""
        demand = demands_by_project.get(project_id)
        if demand is not None:
            return demand
        return require_demand(state=state, project_id=project_id)

    candidates = [
        item.project_id
        for item in state.requests
        if item.status
        in {
            ProvisioningRequestStatus.WAITING_ADMISSION,
            ProvisioningRequestStatus.RATE_LIMITED,
        }
        and demand_for(item.project_id).spawn_needed > 0
    ]
    if not candidates:
        demand = next((item for item in state.demands if item.spawn_needed > 0), None)
        if demand is not None:
            candidates.append(demand.project_id)
    return tuple(sorted(set(candidates)))


def state_with_request(
    state: ProvisioningStoreState,
    request: ProvisioningRequestState,
    now: datetime,
) -> ProvisioningStoreState:
    """Return a root-revisioned snapshot containing one updated request. Args: state, request, now. Returns: ProvisioningStoreState."""

    return replace(
        state,
        requests=tuple(
            request if item.request_id == request.request_id else item
            for item in state.requests
        ),
        revision=state.revision + 1,
        updated_at=now,
    )


def new_request(
    demand: ProvisioningDemandState, now: datetime
) -> ProvisioningRequestState:
    """New request. Args: demand, now. Returns: ProvisioningRequestState."""
    return ProvisioningRequestState(
        schema_version=1,
        request_id=f"prq_{uuid4().hex}",
        project_id=demand.project_id,
        binding_revision=demand.binding_revision,
        adapter_metadata=None,
        status=ProvisioningRequestStatus.WAITING_ADMISSION,
        admission_revision=None,
        revision=0,
        conversation_id=None,
        wake_target_id=f"wtgt_{uuid4().hex}",
        failure_reason=None,
        created_at=now,
        updated_at=now,
    )


def clear_stale_in_flight(state: ProvisioningStoreState) -> ProvisioningStoreState:
    """Clear stale in flight. Args: state. Returns: ProvisioningStoreState."""
    if state.admission.in_flight_request_id is None:
        return state
    request = request_by_id(
        requests=state.requests,
        request_id=state.admission.in_flight_request_id,
    )
    if request is not None and request.status in TRANSIENT_REQUEST_STATUSES:
        return state
    return replace(
        state,
        admission=replace(
            state.admission,
            in_flight_request_id=None,
            revision=state.admission.revision + 1,
        ),
        revision=state.revision + 1,
    )


def admission_blocked(admission: ProvisioningAdmissionState, now: datetime) -> bool:
    """Admission blocked. Args: admission, now. Returns: bool."""
    return any(
        value is not None and now < value
        for value in (admission.next_admission_at, admission.backoff_until)
    )


def operator_blocked_project_ids(
    state: ProvisioningStoreState,
    bindings: Mapping[str, ProjectBindingState | None],
    binding_required: bool,
) -> frozenset[str]:
    """Return projects with a durable non-rate-limit terminal browser failure. Args: state, bindings, binding_required. Returns: frozenset[str]."""

    blocked_project_ids: set[str] = set()
    for request in state.requests:
        if request.status is not ProvisioningRequestStatus.OPERATOR_BLOCKED:
            continue
        if not binding_required:
            blocked_project_ids.add(request.project_id)
            continue
        binding = bindings.get(request.project_id)
        if binding is None:
            blocked_project_ids.add(request.project_id)
            continue
        if (
            request.binding_revision == binding.revision
            and request.adapter_metadata == binding.adapter_metadata
        ):
            blocked_project_ids.add(request.project_id)
    return frozenset(blocked_project_ids)


def retry_after_ms(admission: ProvisioningAdmissionState, now: datetime) -> int | None:
    """Retry after ms. Args: admission, now. Returns: int | None."""
    values = [
        value
        for value in (admission.next_admission_at, admission.backoff_until)
        if value is not None and value > now
    ]
    if not values:
        return None
    return max(0, int((min(values) - now).total_seconds() * 1000))


def validate_demand_values(
    desired_capacity: int, spawn_needed: int, binding_revision: int | None
) -> None:
    """Validate demand values. Args: desired_capacity, spawn_needed, binding_revision. Returns: None."""
    if desired_capacity < 0 or spawn_needed < 0:
        raise ValueError("capacity values cannot be negative")
    if spawn_needed > desired_capacity:
        raise ValueError("spawn_needed cannot exceed desired_capacity")
    if binding_revision is not None and binding_revision < 0:
        raise ValueError("binding_revision cannot be negative")


def failure_status(reason: str, rate_limited: bool) -> ProvisioningRequestStatus:
    """Map an adapter failure to its durable retry disposition. Args: reason, rate_limited. Returns: ProvisioningRequestStatus."""

    if rate_limited:
        return ProvisioningRequestStatus.RATE_LIMITED
    if reason.strip().lower() in {
        "browser_open_cooldown",
        "worker_retirement_pending",
    }:
        return ProvisioningRequestStatus.DEFERRED
    return ProvisioningRequestStatus.OPERATOR_BLOCKED


def is_supported_binding(binding: ProjectBindingState) -> bool:
    """Return whether a binding can be opened by the ChatGPT browser adapter. Args: binding. Returns: bool."""

    metadata = binding.adapter_metadata
    if (
        metadata is None
        or metadata.provider != "chatgpt"
        or not metadata.project_ref
        or not metadata.project_path
    ):
        return False
    return metadata.project_path.rstrip("/") == f"/g/{metadata.project_ref}"


def retain_terminal_history(
    state: ProvisioningStoreState,
    terminal_request_retention: int,
    now: datetime,
    protected_request_ids: frozenset[str] | None = None,
) -> ProvisioningStoreState:
    """Prune terminal history while preserving records required by the caller. Args: state, terminal_request_retention, now, protected_request_ids. Returns: ProvisioningStoreState."""
    return prune_terminal_request_history(
        state=state,
        terminal_request_retention=terminal_request_retention,
        protected_request_ids=protected_request_ids or frozenset(),
        now=now,
    )
