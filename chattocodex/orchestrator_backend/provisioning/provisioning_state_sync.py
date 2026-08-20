"""Pure state synchronization helpers for Worker provisioning demand records."""

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime

from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningDemandState,
    ProvisioningRequestState,
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)

TRANSIENT_REQUEST_STATUSES = frozenset(
    {
        ProvisioningRequestStatus.OPENING_PROJECT,
        ProvisioningRequestStatus.CREATING_CHAT,
        ProvisioningRequestStatus.BOOTSTRAPPING,
        ProvisioningRequestStatus.WAITING_REGISTRATION,
    }
)
PENDING_REQUEST_STATUSES = frozenset(
    {
        ProvisioningRequestStatus.WAITING_ADMISSION,
        *TRANSIENT_REQUEST_STATUSES,
        ProvisioningRequestStatus.RATE_LIMITED,
        ProvisioningRequestStatus.DEFERRED,
    }
)
QUEUED_REQUEST_STATUSES = frozenset(
    {
        ProvisioningRequestStatus.WAITING_ADMISSION,
        ProvisioningRequestStatus.RATE_LIMITED,
        ProvisioningRequestStatus.DEFERRED,
    }
)
PRUNABLE_TERMINAL_REQUEST_STATUSES = frozenset(
    {
        ProvisioningRequestStatus.FAILED,
        ProvisioningRequestStatus.CANCELLED,
    }
)

RequestFactory = Callable[[ProvisioningDemandState, datetime], ProvisioningRequestState]


def apply_capacity_decision_state(
    state: ProvisioningStoreState,
    project_id: str,
    desired_capacity: int,
    capacity_deficit: int,
    spawn_needed: int,
    project_pending_snapshot: int,
    global_headroom: int,
    global_pending_snapshot: int,
    now: datetime,
    request_factory: RequestFactory,
) -> ProvisioningStoreState:
    """Reserve a capacity decision after revalidating durable pending state.

    Args:
        state: Latest durable provisioning snapshot.
        project_id: Project receiving the immutable Consumer Group decision.
        desired_capacity: Decision's bounded capacity target.
        capacity_deficit: Deficit before durable provisioning reservations.
        spawn_needed: Decision's requested new reservation count.
        project_pending_snapshot: Project pending count observed by the scheduler.
        global_headroom: Global capacity headroom observed by the scheduler.
        global_pending_snapshot: Global pending count observed by the scheduler.
        now: Timestamp for changed records.
        request_factory: Pure request constructor supplied by the service boundary.

    Returns:
        Updated provisioning snapshot.
    """
    current = find_demand(state=state, project_id=project_id)
    current_pending = current.spawn_needed if current is not None else 0
    current_global_pending = sum(item.spawn_needed for item in state.demands)
    project_advance = max(0, current_pending - project_pending_snapshot)
    additional_since_snapshot = max(0, current_global_pending - global_pending_snapshot)
    remaining_decision = max(0, spawn_needed - project_advance)
    fresh_global_allowance = max(0, global_headroom - additional_since_snapshot)
    target_pending = min(
        capacity_deficit,
        current_pending + min(remaining_decision, fresh_global_allowance),
    )
    demand = _demand_state(
        current=current,
        project_id=project_id,
        desired_capacity=desired_capacity,
        spawn_needed=target_pending,
        binding_revision=None,
        preserve_current_binding_revision=True,
        now=now,
    )
    return _with_synchronized_demand(
        state=state,
        demand=demand,
        target_spawn_needed=target_pending,
        now=now,
        request_factory=request_factory,
        cancel_statuses_when_zero=QUEUED_REQUEST_STATUSES,
    )


def upsert_demand_state(
    state: ProvisioningStoreState,
    project_id: str,
    desired_capacity: int,
    spawn_needed: int,
    binding_revision: int | None,
    now: datetime,
    request_factory: RequestFactory,
) -> ProvisioningStoreState:
    """Upsert one explicit demand and synchronize its request reservations.

    Args:
        state: Latest durable provisioning snapshot.
        project_id: Project receiving the demand.
        desired_capacity: Backend-computed target capacity.
        spawn_needed: Durable reservation target.
        binding_revision: Optional binding revision to persist on the demand.
        now: Timestamp for changed records.
        request_factory: Pure request constructor supplied by the service boundary.

    Returns:
        Updated provisioning snapshot.
    """
    current = find_demand(state=state, project_id=project_id)
    demand = _demand_state(
        current=current,
        project_id=project_id,
        desired_capacity=desired_capacity,
        spawn_needed=spawn_needed,
        binding_revision=binding_revision,
        preserve_current_binding_revision=binding_revision is None,
        now=now,
    )
    return _with_synchronized_demand(
        state=state,
        demand=demand,
        target_spawn_needed=spawn_needed,
        now=now,
        request_factory=request_factory,
        cancel_statuses_when_zero=PENDING_REQUEST_STATUSES,
    )


def prune_terminal_request_history(
    state: ProvisioningStoreState,
    terminal_request_retention: int,
    protected_request_ids: frozenset[str],
    now: datetime,
) -> ProvisioningStoreState:
    """Bound prunable terminal request history without touching live records.

    Args:
        state: Latest candidate provisioning snapshot.
        terminal_request_retention: Maximum retained failed/cancelled records.
        protected_request_ids: Terminal request IDs that the current call must return.
        now: Timestamp for the root snapshot mutation when pruning occurs.

    Returns:
        ``state`` unchanged when below the retention cap, otherwise a pruned snapshot.
    """
    prunable = [
        request
        for request in state.requests
        if request.status in PRUNABLE_TERMINAL_REQUEST_STATUSES
        and request.request_id not in protected_request_ids
    ]
    if len(prunable) <= terminal_request_retention:
        return state

    retained_terminal_ids = {
        request.request_id
        for request in sorted(
            prunable,
            key=lambda item: (item.updated_at, item.request_id),
        )[-terminal_request_retention:]
    }
    retained_requests = tuple(
        request
        for request in state.requests
        if request.status not in PRUNABLE_TERMINAL_REQUEST_STATUSES
        or request.request_id in protected_request_ids
        or request.request_id in retained_terminal_ids
    )
    if retained_requests == state.requests:
        return state
    return replace(
        state,
        requests=retained_requests,
        revision=state.revision + 1,
        updated_at=now,
    )


def find_demand(
    state: ProvisioningStoreState, project_id: str
) -> ProvisioningDemandState | None:
    """Return one project's demand, if present. Args: state, project_id. Returns: ProvisioningDemandState | None."""
    return next((item for item in state.demands if item.project_id == project_id), None)


def _demand_state(
    current: ProvisioningDemandState | None,
    project_id: str,
    desired_capacity: int,
    spawn_needed: int,
    binding_revision: int | None,
    preserve_current_binding_revision: bool,
    now: datetime,
) -> ProvisioningDemandState:
    """Demand state. Args: current, project_id, desired_capacity, spawn_needed, binding_revision, preserve_current_binding_revision, now. Returns: ProvisioningDemandState."""
    if current is None:
        return ProvisioningDemandState(
            schema_version=1,
            project_id=project_id,
            desired_capacity=desired_capacity,
            spawn_needed=spawn_needed,
            binding_revision=binding_revision,
            revision=0,
            created_at=now,
            updated_at=now,
        )
    next_binding_revision = (
        current.binding_revision
        if preserve_current_binding_revision
        else binding_revision
    )
    if (
        current.desired_capacity == desired_capacity
        and current.spawn_needed == spawn_needed
        and current.binding_revision == next_binding_revision
    ):
        return current
    return replace(
        current,
        desired_capacity=desired_capacity,
        spawn_needed=spawn_needed,
        binding_revision=next_binding_revision,
        revision=current.revision + 1,
        updated_at=now,
    )


def _with_synchronized_demand(
    state: ProvisioningStoreState,
    demand: ProvisioningDemandState,
    target_spawn_needed: int,
    now: datetime,
    request_factory: RequestFactory,
    cancel_statuses_when_zero: frozenset[ProvisioningRequestStatus],
) -> ProvisioningStoreState:
    """With synchronized demand. Args: state, demand, target_spawn_needed, now, request_factory, cancel_statuses_when_zero. Returns: ProvisioningStoreState."""
    demands = _replace_or_append_demand(state=state, demand=demand)
    requests = list(state.requests)
    outstanding = sum(
        item.project_id == demand.project_id
        and item.status
        in {
            *PENDING_REQUEST_STATUSES,
            ProvisioningRequestStatus.OPERATOR_BLOCKED,
        }
        for item in requests
    )
    while outstanding < target_spawn_needed:
        requests.append(request_factory(demand, now))
        outstanding += 1
    admission = state.admission
    if target_spawn_needed == 0:
        requests = [
            replace(
                item,
                status=ProvisioningRequestStatus.CANCELLED,
                failure_reason="capacity_no_longer_needed",
                revision=item.revision + 1,
                updated_at=now,
            )
            if item.project_id == demand.project_id
            and item.status in cancel_statuses_when_zero
            else item
            for item in requests
        ]
        admission_cancelled = any(
            item.request_id == state.admission.in_flight_request_id
            and item.project_id == demand.project_id
            and item.status in cancel_statuses_when_zero
            for item in state.requests
        )
        if admission_cancelled:
            admission = replace(
                state.admission,
                in_flight_request_id=None,
                revision=state.admission.revision + 1,
            )
    synchronized_requests = tuple(requests)
    if (
        demands == state.demands
        and synchronized_requests == state.requests
        and admission == state.admission
    ):
        return state
    return replace(
        state,
        demands=demands,
        requests=synchronized_requests,
        admission=admission,
        revision=state.revision + 1,
        updated_at=now,
    )


def _replace_or_append_demand(
    state: ProvisioningStoreState, demand: ProvisioningDemandState
) -> tuple[ProvisioningDemandState, ...]:
    """Replace or append demand. Args: state, demand. Returns: tuple[ProvisioningDemandState, ...]."""
    if find_demand(state=state, project_id=demand.project_id) is None:
        return (*state.demands, demand)
    return tuple(
        demand if item.project_id == demand.project_id else item
        for item in state.demands
    )
