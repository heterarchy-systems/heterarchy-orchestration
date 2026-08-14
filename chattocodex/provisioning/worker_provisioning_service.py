"""Application service for safe, project-bound Worker provisioning."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from chattocodex.consumer_group.consumer_group_state import ConsumerGroupState
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_exceptions import (
    OrchestrationError,
    ResourceNotFoundError,
    RevisionConflictError,
)
from chattocodex.persistence.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.project_runtime.project_binding_dto import ProjectBindingState
from chattocodex.project_runtime.project_binding_mapper import (
    metadata_state_from_schema,
)
from chattocodex.project_runtime.project_binding_service import ProjectBindingService
from chattocodex.provisioning.provisioning_dto import (
    ProvisioningAdmissionState,
    ProvisioningDemandState,
    ProvisioningRequestState,
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)
from chattocodex.provisioning.provisioning_exceptions import (
    ProvisioningConfigurationError,
    ProvisioningConflictError,
    ProvisioningStaleCallbackError,
)
from chattocodex.provisioning.provisioning_mapper import (
    demand_view_from_state,
    request_view_from_state,
)
from chattocodex.provisioning.provisioning_state_sync import (
    PENDING_REQUEST_STATUSES,
    TRANSIENT_REQUEST_STATUSES,
    apply_capacity_decision_state,
    find_demand,
    prune_terminal_request_history,
    upsert_demand_state,
)
from chattocodex.schemas.project_binding_schemas import ProjectBindingAdapterMetadata
from chattocodex.wake.wake_target_service import WakeTargetService

if TYPE_CHECKING:
    from chattocodex.consumer_group.capacity_decision import CapacityDecision


class ConsumerGroupMembershipCoordinator(Protocol):
    """Minimal membership seam used after automatic wake-target registration."""

    def reconcile_membership(self, *, project_id: str) -> ConsumerGroupState:
        """Reconcile and return the canonical project's Consumer Group state."""

    def reconcile_project(self, *, project_id: str) -> object:
        """Reconcile READY capacity after a new recoverable member appears."""


_TRANSIENT_STATUSES = TRANSIENT_REQUEST_STATUSES
_PENDING_STATUSES = PENDING_REQUEST_STATUSES
_PROGRESS_ORDER = {
    ProvisioningRequestStatus.OPENING_PROJECT: 1,
    ProvisioningRequestStatus.CREATING_CHAT: 2,
    ProvisioningRequestStatus.BOOTSTRAPPING: 3,
    ProvisioningRequestStatus.WAITING_REGISTRATION: 4,
}


class WorkerProvisioningService:
    """Own durable demand, global admission, and registration lifecycle."""

    def __init__(
        self,
        *,
        repository: WorkerProvisioningRepository,
        project_binding_service: ProjectBindingService | None = None,
        wake_target_service: WakeTargetService | None = None,
        consumer_group_coordinator: ConsumerGroupMembershipCoordinator | None = None,
        lock_registry: EntityLockRegistry | None = None,
        now_provider: Callable[[], datetime] | None = None,
        minimum_admission_interval: timedelta = timedelta(seconds=12),
        admission_lease: timedelta = timedelta(seconds=90),
        rate_limit_backoff: tuple[timedelta, ...] = (
            timedelta(seconds=30),
            timedelta(seconds=60),
            timedelta(seconds=120),
        ),
        terminal_request_retention: int = 128,
    ) -> None:
        """Create a project-bound Worker provisioning service.

        Args:
            repository: Atomic filesystem provisioning state repository.
            project_binding_service: Canonical local project binding authority.
            wake_target_service: Runtime target registration boundary.
            consumer_group_coordinator: Membership confirmation boundary.
            lock_registry: Optional in-process scheduler lock registry.
            now_provider: Time provider used by tests and rate-limit policy.
            minimum_admission_interval: Minimum interval between browser admissions.
            admission_lease: Maximum time one browser attempt owns the global gate.
            rate_limit_backoff: Bounded provider backoff durations.
            terminal_request_retention: Count of failed/cancelled records retained in
                the durable browser-facing snapshot.

        Raises:
            ValueError: If admission intervals or backoff settings are invalid.
        """
        if minimum_admission_interval < timedelta(0):
            raise ValueError("minimum_admission_interval cannot be negative")
        if admission_lease <= timedelta(0):
            raise ValueError("admission_lease must be positive")
        if not rate_limit_backoff or any(
            item <= timedelta(0) for item in rate_limit_backoff
        ):
            raise ValueError("rate_limit_backoff must contain positive durations")
        if terminal_request_retention < 1:
            raise ValueError("terminal_request_retention must be at least one")
        self._repository = repository
        self._project_binding_service = project_binding_service
        self._wake_target_service = wake_target_service
        self._consumer_group_coordinator = consumer_group_coordinator
        self._lock_registry = lock_registry or EntityLockRegistry()
        self._now_provider = now_provider or _utc_now
        self._minimum_admission_interval = minimum_admission_interval
        self._admission_lease = admission_lease
        self._rate_limit_backoff = rate_limit_backoff
        self._terminal_request_retention = terminal_request_retention
        if (
            self._wake_target_service is not None
            and self._project_binding_service is not None
        ):
            self._wake_target_service.attach_connector_epoch_resolver(
                lambda project_id: (
                    self._project_binding_service.resolve_connector_epoch(
                        project_id=project_id
                    )
                )
            )

    def attach_consumer_group_coordinator(
        self, coordinator: ConsumerGroupMembershipCoordinator
    ) -> None:
        """Attach the membership confirmer after coordinator construction.

        The Control Panel constructs provisioning before the Consumer Group to let
        the coordinator receive the non-reentrant capacity callbacks. Registration
        callbacks use this one-way attachment only after construction is complete.

        Args:
            coordinator: Canonical Consumer Group membership coordinator.
        """
        self._consumer_group_coordinator = coordinator

    def pending_provisioning_count(self, project_id: str | None = None) -> int:
        """Return outstanding reservations for the capacity reconciliation seam.

        Args:
            project_id: Canonical project identifier, or ``None`` for global count.

        Returns:
            Non-negative durable pending reservation count.
        """
        demands = self._repository.list_demands()
        if project_id is None:
            return sum(item.spawn_needed for item in demands)
        return sum(
            item.spawn_needed for item in demands if item.project_id == project_id
        )

    def upsert_demand(
        self,
        *,
        project_id: str,
        desired_capacity: int,
        spawn_needed: int,
        binding_revision: int | None = None,
    ) -> ProvisioningDemandState:
        """Persist one backend-computed project capacity deficit.

        Args:
            project_id: Canonical local project identifier.
            desired_capacity: Bounded capacity target from capacity_core.
            spawn_needed: Remaining deficit after reuse and existing reservations.
            binding_revision: Optional canonical binding revision observed by caller.

        Returns:
            Durable demand state after the atomic upsert.

        Raises:
            ValueError: If capacity values or revisions are invalid.
        """
        _validate_demand_values(
            desired_capacity=desired_capacity,
            spawn_needed=spawn_needed,
            binding_revision=binding_revision,
        )
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            updated = self._repository.update(
                updater=lambda state: self._with_retained_terminal_history(
                    state=upsert_demand_state(
                        state=state,
                        project_id=project_id,
                        desired_capacity=desired_capacity,
                        spawn_needed=spawn_needed,
                        binding_revision=binding_revision,
                        now=now,
                        request_factory=_new_request,
                    ),
                    now=now,
                )
            )
            return _require_demand(state=updated, project_id=project_id)

    def on_capacity_decision(self, decision: CapacityDecision) -> None:
        """Atomically reserve the remaining deficit from capacity_core.

        This callback is synchronous and never re-enters Consumer Group
        reconciliation. The caller owns scheduler locks while invoking it.

        Args:
            decision: Immutable capacity decision from the Consumer Group seam.

        Raises:
            ValueError: If the decision contains invalid capacity values.
        """
        _validate_demand_values(
            desired_capacity=decision.desired_capacity,
            spawn_needed=decision.spawn_needed,
            binding_revision=None,
        )
        if decision.capacity_deficit < 0:
            raise ValueError("capacity_deficit cannot be negative")
        binding = self._binding_for(project_id=decision.project_id)

        def request_factory(
            demand: ProvisioningDemandState, now: datetime
        ) -> ProvisioningRequestState:
            request = _new_request(demand=demand, now=now)
            if binding is None:
                return request
            return replace(
                request,
                binding_revision=binding.revision,
                adapter_metadata=binding.adapter_metadata,
            )

        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            updated = self._repository.update(
                updater=lambda state: self._with_retained_terminal_history(
                    state=apply_capacity_decision_state(
                        state=state,
                        project_id=decision.project_id,
                        desired_capacity=decision.desired_capacity,
                        capacity_deficit=decision.capacity_deficit,
                        spawn_needed=decision.spawn_needed,
                        project_pending_snapshot=decision.pending_provisioning_count,
                        global_headroom=decision.global_headroom,
                        global_pending_snapshot=(
                            decision.global_pending_provisioning_count
                        ),
                        now=now,
                        request_factory=request_factory,
                    ),
                    now=now,
                )
            )
            _require_demand(state=updated, project_id=decision.project_id)

    def admit_next(self) -> ProvisioningRequestState | None:
        """Admit the next queued request under the global serialized gate.

        Returns:
            The explicitly admitted opaque request, or ``None`` while cooling down,
            rate-limited, unbound, or already waiting for a browser callback.
        """
        snapshot = self._repository.read_state()
        candidate_project_ids = _admission_candidate_project_ids(state=snapshot)
        binding_lock_project = (
            candidate_project_ids[0] if candidate_project_ids else None
        )
        binding_hold = (
            self._lock_registry.hold(
                entity_key=f"project-binding:{binding_lock_project}"
            )
            if binding_lock_project is not None
            else _null_context()
        )
        with (
            binding_hold,
            self._lock_registry.hold(entity_key="worker-provisioning:capacity"),
        ):
            now = self._now_provider()
            snapshot = self._repository.read_state()
            candidate_project_ids = _admission_candidate_project_ids(state=snapshot)
            bindings = {
                project_id: self._binding_for(project_id=project_id)
                for project_id in candidate_project_ids
            }
            admitted_request_id: str | None = None

            def admit(state: ProvisioningStoreState) -> ProvisioningStoreState:
                nonlocal admitted_request_id
                in_flight = _request_by_id(
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
                state = _clear_stale_in_flight(state=state)
                if _admission_blocked(state.admission, now=now):
                    return state
                operator_blocked_project_ids = _operator_blocked_project_ids(
                    state=state,
                    bindings=bindings,
                    binding_required=self._project_binding_service is not None,
                )
                candidates = [
                    item
                    for item in state.requests
                    if item.status
                    in {
                        ProvisioningRequestStatus.WAITING_ADMISSION,
                        ProvisioningRequestStatus.RATE_LIMITED,
                        ProvisioningRequestStatus.DEFERRED,
                    }
                    and _require_demand(
                        state=state, project_id=item.project_id
                    ).spawn_needed
                    > 0
                    and item.project_id not in operator_blocked_project_ids
                ]
                if not candidates:
                    demand = next(
                        (
                            item
                            for item in state.demands
                            if item.spawn_needed > 0
                            and item.project_id not in operator_blocked_project_ids
                        ),
                        None,
                    )
                    if demand is None:
                        return state
                    candidate = _new_request(demand=demand, now=now)
                    candidates = [candidate]
                candidates.sort(key=lambda item: (item.created_at, item.request_id))
                for candidate in candidates:
                    binding = bindings.get(candidate.project_id)
                    if (
                        self._project_binding_service is not None
                        and candidate.project_id not in bindings
                    ):
                        continue
                    if self._project_binding_service is not None and binding is None:
                        continue
                    if binding is not None and not _is_supported_binding(binding):
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
                    existing_requests = tuple(
                        admitted if item.request_id == admitted.request_id else item
                        for item in state.requests
                    )
                    if not any(
                        item.request_id == admitted.request_id
                        for item in state.requests
                    ):
                        existing_requests = (*state.requests, admitted)
                    admitted_request_id = admitted.request_id
                    return self._with_retained_terminal_history(
                        state=replace(
                            state,
                            requests=existing_requests,
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
                        now=now,
                    )
                return state

            persisted = self._repository.update(updater=admit)
            request = _request_by_id(
                requests=persisted.requests,
                request_id=admitted_request_id,
            )
            if request is None or request.status not in _TRANSIENT_STATUSES:
                return None
            return request

    def progress_request(
        self,
        *,
        request_id: str,
        status: ProvisioningRequestStatus,
        admission_revision: int,
    ) -> ProvisioningRequestState:
        """Advance an admitted request through browser provisioning phases.

        Args:
            request_id: Opaque request identifier.
            status: Next non-terminal provisioning state.
            admission_revision: Server-issued admission revision.

        Returns:
            Updated durable request state.

        Raises:
            ProvisioningStaleCallbackError: If the admission revision is stale.
            ProvisioningConflictError: If the transition is invalid.
        """
        if status not in _TRANSIENT_STATUSES:
            raise ValueError("progress status must be a transient provisioning state")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()

            def progress(state: ProvisioningStoreState) -> ProvisioningStoreState:
                request = _require_request(state=state, request_id=request_id)
                self._require_admission(
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
                            "Provisioning callback attempted an invalid lifecycle "
                            "transition."
                        ),
                    )
                return _state_with_request(
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
            return _require_request(state=persisted, request_id=request_id)

    def register_callback(
        self,
        *,
        request_id: str,
        conversation_id: str,
        wake_target_id: str,
        admission_revision: int,
        adapter_metadata: ProjectBindingAdapterMetadata | None = None,
    ) -> ProvisioningRequestState:
        """Register a persistent Worker target and confirm group membership.

        Args:
            request_id: Opaque admitted request identifier.
            conversation_id: Persistent provider conversation identity.
            wake_target_id: Server-issued opaque wake target identity.
            admission_revision: Server-issued admission revision.
            adapter_metadata: Optional provider observation; never canonical authority.

        Returns:
            Active durable request after idempotent target registration.

        Raises:
            ProvisioningStaleCallbackError: If admission or binding revision is stale.
            ProvisioningConflictError: If identities or membership conflict.
            OrchestrationError: If the wake boundary rejects registration.
        """
        if not conversation_id.strip():
            raise ValueError("conversation_id cannot be empty")
        initial = self._repository.read_state()
        initial_request = _require_request(state=initial, request_id=request_id)
        if initial_request.status is ProvisioningRequestStatus.ACTIVE:
            if (
                initial_request.conversation_id == conversation_id
                and initial_request.wake_target_id == wake_target_id
                and initial_request.admission_revision == admission_revision
            ):
                self._require_active_registration_recoverable(request=initial_request)
                return initial_request
            raise ProvisioningConflictError(
                request_id=request_id,
                message="Duplicate registration uses stale or different identities.",
            )
        self._require_admission(
            request=initial_request,
            admission=initial.admission,
            admission_revision=admission_revision,
        )
        if initial_request.wake_target_id != wake_target_id:
            raise ProvisioningConflictError(
                request_id=request_id,
                message="Callback wake target does not match the server-issued target.",
            )
        binding = self._binding_for(project_id=initial_request.project_id)
        if (
            binding is not None
            and initial_request.binding_revision is not None
            and binding.revision != initial_request.binding_revision
        ):
            raise ProvisioningStaleCallbackError(
                request_id=request_id, current_revision=binding.revision
            )
        if self._wake_target_service is not None:
            provisional_target = self._wake_target_service.register_target(
                wake_target_id=wake_target_id,
                project_id=initial_request.project_id,
                registered_connector_epoch=(
                    binding.connector_epoch if binding is not None else None
                ),
            )
        else:
            provisional_target = None
        try:
            if self._consumer_group_coordinator is not None:
                membership = self._consumer_group_coordinator.reconcile_membership(
                    project_id=initial_request.project_id
                )
                if wake_target_id not in membership.member_ids:
                    raise ProvisioningConflictError(
                        request_id=request_id,
                        message=(
                            "Registered Worker is not a recoverable Consumer Group member yet."
                        ),
                    )
            current_binding = self._binding_for(project_id=initial_request.project_id)
            if binding is not None and (
                current_binding is None
                or current_binding.revision != binding.revision
                or current_binding.connector_epoch != binding.connector_epoch
            ):
                raise ProvisioningConflictError(
                    request_id=request_id,
                    message="Project binding changed during Worker registration.",
                )
        except OrchestrationError as error:
            if provisional_target is not None and self._wake_target_service is not None:
                self._wake_target_service.unregister_provisional_target_if_matches(
                    target=provisional_target
                )
            reason = (
                "registration_membership_confirmation_failed"
                if self._consumer_group_coordinator is not None
                and "recoverable Consumer Group member" in error.message
                else "project_binding_changed_during_registration"
            )
            self._cancel_registration_failure(
                request_id=request_id,
                admission_revision=admission_revision,
                reason=reason,
            )
            raise
        now = self._now_provider()

        def register(state: ProvisioningStoreState) -> ProvisioningStoreState:
            request = _require_request(state=state, request_id=request_id)
            if request.status is ProvisioningRequestStatus.ACTIVE:
                if (
                    request.conversation_id == conversation_id
                    and request.wake_target_id == wake_target_id
                    and request.admission_revision == admission_revision
                ):
                    return state
                raise ProvisioningConflictError(
                    request_id=request_id,
                    message="Duplicate registration uses stale or different identities.",
                )
            self._require_admission(
                request=request,
                admission=state.admission,
                admission_revision=admission_revision,
            )
            if request.wake_target_id != wake_target_id:
                raise ProvisioningConflictError(
                    request_id=request_id,
                    message=(
                        "Callback wake target does not match the server-issued target."
                    ),
                )
            demand = _require_demand(state=state, project_id=request.project_id)
            updated_request = replace(
                request,
                status=ProvisioningRequestStatus.ACTIVE,
                conversation_id=conversation_id,
                adapter_metadata=(
                    metadata_state_from_schema(adapter_metadata)
                    if adapter_metadata is not None
                    else request.adapter_metadata
                ),
                revision=request.revision + 1,
                updated_at=now,
            )
            updated_demand = replace(
                demand,
                spawn_needed=max(0, demand.spawn_needed - 1),
                revision=demand.revision + 1,
                updated_at=now,
            )
            return replace(
                state,
                demands=tuple(
                    updated_demand if item.project_id == demand.project_id else item
                    for item in state.demands
                ),
                requests=tuple(
                    updated_request if item.request_id == request_id else item
                    for item in state.requests
                ),
                admission=replace(
                    state.admission,
                    in_flight_request_id=None,
                    backoff_attempt=0,
                    backoff_until=None,
                    revision=state.admission.revision + 1,
                ),
                revision=state.revision + 1,
                updated_at=now,
            )

        persisted = self._repository.update(updater=register)
        result = _require_request(state=persisted, request_id=request_id)
        if self._consumer_group_coordinator is not None:
            self._consumer_group_coordinator.reconcile_project(
                project_id=initial_request.project_id
            )
        return result

    def fail_callback(
        self,
        *,
        request_id: str,
        admission_revision: int,
        reason: str,
        rate_limited: bool = False,
    ) -> tuple[ProvisioningRequestState, int | None]:
        """Record a durable failure with conservative automatic retry policy.

        Provider rate limits retain the bounded 30/60/120-second retry sequence.
        Runtime admission cooldown and a browser-side pending-retirement barrier are
        durable deferred attempts that may re-enter after the normal admission interval.
        Every other browser/provider UI failure becomes an operator-blocking terminal
        record. Its unmet durable demand remains visible, but no later scheduler replay
        or process restart may admit another browser-open attempt automatically.

        Args:
            request_id: Opaque admitted request identifier.
            admission_revision: Server-issued admission revision.
            reason: Short adapter-safe failure reason.
            rate_limited: Whether provider rate limiting was observed.

        Returns:
            Updated request and optional retry delay in milliseconds.

        Raises:
            ProvisioningStaleCallbackError: If callback revision is stale.
        """
        if not reason.strip():
            raise ValueError("reason cannot be empty")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            initial = self._repository.read_state()
            initial_request = _require_request(state=initial, request_id=request_id)
            expected_status = _failure_status(
                reason=reason,
                rate_limited=rate_limited,
            )
            if initial_request.status is expected_status:
                if (
                    initial_request.admission_revision == admission_revision
                    and initial_request.failure_reason == reason
                ):
                    return initial_request, _retry_after_ms(initial.admission, now=now)
                raise ProvisioningConflictError(
                    request_id=request_id,
                    message="Provisioning failure callback is stale or mismatched.",
                )

            def fail(state: ProvisioningStoreState) -> ProvisioningStoreState:
                request = _require_request(state=state, request_id=request_id)
                if request.status not in _TRANSIENT_STATUSES:
                    raise ProvisioningConflictError(
                        request_id=request_id,
                        message="Provisioning failure callback is duplicate or terminal.",
                    )
                self._require_admission(
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
                return self._with_retained_terminal_history(
                    state=replace(
                        state,
                        requests=tuple(
                            replace(
                                request,
                                status=(
                                    _failure_status(
                                        reason=reason,
                                        rate_limited=rate_limited,
                                    )
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
                    now=now,
                    protected_request_ids=frozenset({request_id}),
                )

            persisted = self._repository.update(updater=fail)
            return _require_request(
                state=persisted, request_id=request_id
            ), _retry_after_ms(persisted.admission, now=now)

    def cancel_request(
        self, *, request_id: str, admission_revision: int | None = None
    ) -> ProvisioningRequestState:
        """Cancel a queued or admitted request without demand resurrection.

        Args:
            request_id: Opaque request identifier.
            admission_revision: Optional revision required for an in-flight request.

        Returns:
            Cancelled durable request, idempotently.

        Raises:
            ProvisioningStaleCallbackError: If an in-flight revision is stale.
        """
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()

            def cancel(state: ProvisioningStoreState) -> ProvisioningStoreState:
                request = _require_request(state=state, request_id=request_id)
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
                    self._require_admission(
                        request=request,
                        admission=state.admission,
                        admission_revision=admission_revision,
                    )
                demand = _require_demand(state=state, project_id=request.project_id)
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
                return self._with_retained_terminal_history(
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
                    now=now,
                    protected_request_ids=frozenset({request_id}),
                )

            persisted = self._repository.update(updater=cancel)
            return _require_request(state=persisted, request_id=request_id)

    def retire_runtime_ownership(
        self, *, wake_target_id: str
    ) -> ProvisioningRequestState | None:
        """Retire the exact active request whose runtime ownership was lost.

        This lifecycle repair is demand-neutral. When retirement wins during
        registration, it also releases that exact request's global admission gate.
        Replays return the same retained terminal record without advancing any
        revision.

        Args:
            wake_target_id: Server-issued identity of the retired runtime target.

        Returns:
            The cancelled provisioning request when an active, registering, or
            already-retired exact match exists, otherwise ``None``.

        Raises:
            ValueError: If ``wake_target_id`` is empty.
        """
        if not wake_target_id.strip():
            raise ValueError("wake_target_id cannot be empty")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            retired_request_id: str | None = None

            def retire(state: ProvisioningStoreState) -> ProvisioningStoreState:
                nonlocal retired_request_id
                matching = tuple(
                    request
                    for request in state.requests
                    if request.wake_target_id == wake_target_id
                )
                retirable = next(
                    (
                        request
                        for request in matching
                        if request.status is ProvisioningRequestStatus.ACTIVE
                        or request.status in _TRANSIENT_STATUSES
                    ),
                    None,
                )
                if retirable is None:
                    already_retired = next(
                        (
                            request
                            for request in matching
                            if request.status is ProvisioningRequestStatus.CANCELLED
                            and request.failure_reason == "runtime_ownership_lost"
                        ),
                        None,
                    )
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
                return self._with_retained_terminal_history(
                    state=replace(
                        state,
                        requests=tuple(
                            replace(
                                retirable,
                                status=ProvisioningRequestStatus.CANCELLED,
                                failure_reason="runtime_ownership_lost",
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
                    now=now,
                    protected_request_ids=frozenset({retirable.request_id}),
                )

            persisted = self._repository.update(updater=retire)
            return _request_by_id(
                requests=persisted.requests,
                request_id=retired_request_id,
            )

    def retire_runtime_ownership_batch(
        self,
        *,
        project_id: str,
        wake_target_ids: tuple[str, ...],
    ) -> tuple[tuple[ProvisioningRequestState, ...], bool]:
        """Atomically retire or validate tombstones for one exact project batch.

        Every target must map to an ACTIVE or transient request, or an
        already-cancelled ``runtime_ownership_lost`` tombstone in the requested
        project. The entire provisioning snapshot is validated before any request
        changes state.

        Args:
            project_id: Exact canonical project owning every request.
            wake_target_ids: Unique target identities in caller order.

        Returns:
            Matched requests in caller order and whether any live request was
            changed to its cancellation tombstone.
        """
        if not wake_target_ids:
            raise ValueError("wake_target_ids cannot be empty")
        if len(wake_target_ids) != len(set(wake_target_ids)):
            raise ValueError("wake_target_ids must be unique")
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            retired_request_ids: tuple[str, ...] = ()
            lifecycle_changed = False

            def retire(state: ProvisioningStoreState) -> ProvisioningStoreState:
                nonlocal lifecycle_changed, retired_request_ids
                matched: list[ProvisioningRequestState] = []
                for wake_target_id in wake_target_ids:
                    candidates = tuple(
                        request
                        for request in state.requests
                        if request.wake_target_id == wake_target_id
                        and request.project_id == project_id
                    )
                    request = next(
                        (
                            item
                            for item in candidates
                            if item.status is ProvisioningRequestStatus.ACTIVE
                            or item.status in _TRANSIENT_STATUSES
                        ),
                        None,
                    )
                    if request is None:
                        request = next(
                            (
                                item
                                for item in candidates
                                if item.status is ProvisioningRequestStatus.CANCELLED
                                and item.failure_reason == "runtime_ownership_lost"
                            ),
                            None,
                        )
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
                    if item.status is ProvisioningRequestStatus.ACTIVE
                    or item.status in _TRANSIENT_STATUSES
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
                return self._with_retained_terminal_history(
                    state=replace(
                        state,
                        requests=tuple(
                            replace(
                                request,
                                status=ProvisioningRequestStatus.CANCELLED,
                                failure_reason="runtime_ownership_lost",
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
                    now=now,
                    protected_request_ids=frozenset(retired_request_ids),
                )

            persisted = self._repository.update(updater=retire)
            return (
                tuple(
                    _require_request(state=persisted, request_id=request_id)
                    for request_id in retired_request_ids
                ),
                lifecycle_changed,
            )

    def _require_active_registration_recoverable(
        self, *, request: ProvisioningRequestState
    ) -> None:
        """Reject and retire an ACTIVE replay whose runtime identity disappeared."""
        try:
            if self._wake_target_service is not None:
                target = self._wake_target_service.get_target(
                    wake_target_id=request.wake_target_id or ""
                )
                if target.project_id != request.project_id:
                    raise ProvisioningConflictError(
                        request_id=request.request_id,
                        message=(
                            "Active registration wake target belongs to a different "
                            "project."
                        ),
                    )
            if self._consumer_group_coordinator is not None:
                membership = self._consumer_group_coordinator.reconcile_membership(
                    project_id=request.project_id
                )
                if request.wake_target_id not in membership.member_ids:
                    raise ProvisioningConflictError(
                        request_id=request.request_id,
                        message=(
                            "Active registration is no longer a recoverable Consumer "
                            "Group member."
                        ),
                    )
        except OrchestrationError as error:
            self.retire_runtime_ownership(wake_target_id=request.wake_target_id or "")
            raise ProvisioningConflictError(
                request_id=request.request_id,
                message=(
                    "Active registration no longer has a recoverable runtime target."
                ),
            ) from error

    def retry_operator_blocked_request(
        self, *, request_id: str, expected_revision: int
    ) -> ProvisioningRequestState:
        """Queue one operator-blocked request for a deliberate retry.

        The retry is permitted only while the request still belongs to the exact
        canonical project binding revision captured at admission. The global
        admission record is intentionally left unchanged, so an existing in-flight
        request, cooldown, or rate-limit backoff continues to serialize browser work.

        Args:
            request_id: Opaque provisioning request identifier.
            expected_revision: Latest request revision observed by the operator.

        Returns:
            The request reset to ``WAITING_ADMISSION``.

        Raises:
            ValueError: If ``expected_revision`` is negative.
            ProvisioningConfigurationError: If no supported current binding exists.
            ProvisioningConflictError: If the request is not operator-blocked.
            RevisionConflictError: If the request or binding revision is stale.
        """
        if expected_revision < 0:
            raise ValueError("expected_revision cannot be negative")
        initial = self._repository.read_state()
        initial_request = _require_request(state=initial, request_id=request_id)
        with self._lock_registry.hold(
            entity_key=f"project-binding:{initial_request.project_id}"
        ):
            binding = self._binding_for(project_id=initial_request.project_id)
            if binding is None or not _is_supported_binding(binding):
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
                    request = _require_request(state=state, request_id=request_id)
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
                    return _state_with_request(
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
                return _require_request(state=persisted, request_id=request_id)

    def list_status(self):
        """Return opaque demand and request views for local inspection.

        Returns:
            Tuple containing demand views and request views.
        """
        state = self._repository.read_state()
        return (
            tuple(demand_view_from_state(item) for item in state.demands),
            tuple(request_view_from_state(item) for item in state.requests),
        )

    def _with_retained_terminal_history(
        self,
        *,
        state: ProvisioningStoreState,
        now: datetime,
        protected_request_ids: frozenset[str] | None = None,
    ) -> ProvisioningStoreState:
        return prune_terminal_request_history(
            state=state,
            terminal_request_retention=self._terminal_request_retention,
            protected_request_ids=protected_request_ids or frozenset(),
            now=now,
        )

    def _cancel_registration_failure(
        self, *, request_id: str, admission_revision: int, reason: str
    ) -> None:
        now = self._now_provider()

        def cancel(state: ProvisioningStoreState) -> ProvisioningStoreState:
            request = _require_request(state=state, request_id=request_id)
            self._require_admission(
                request=request,
                admission=state.admission,
                admission_revision=admission_revision,
            )
            return _state_with_request(
                state=replace(
                    state,
                    admission=replace(
                        state.admission,
                        in_flight_request_id=None,
                        revision=state.admission.revision + 1,
                    ),
                ),
                request=replace(
                    request,
                    status=ProvisioningRequestStatus.CANCELLED,
                    failure_reason=reason,
                    revision=request.revision + 1,
                    updated_at=now,
                ),
                now=now,
            )

        self._repository.update(updater=cancel)

    def _require_admission(
        self,
        *,
        request: ProvisioningRequestState,
        admission: ProvisioningAdmissionState,
        admission_revision: int,
    ) -> None:
        if (
            admission.in_flight_request_id != request.request_id
            or request.admission_revision != admission_revision
            or admission.revision < admission_revision
        ):
            raise ProvisioningStaleCallbackError(
                request_id=request.request_id,
                current_revision=admission.revision,
            )

    def _binding_for(self, *, project_id: str) -> ProjectBindingState | None:
        if self._project_binding_service is None:
            return None
        try:
            return self._project_binding_service.get_binding(project_id=project_id)
        except ResourceNotFoundError:
            return None
        except OrchestrationError as error:
            raise ProvisioningConfigurationError(
                project_id=project_id, message=error.message
            ) from error


def _require_demand(
    *, state: ProvisioningStoreState, project_id: str
) -> ProvisioningDemandState:
    demand = find_demand(state=state, project_id=project_id)
    if demand is None:
        raise ResourceNotFoundError(
            operation="worker_provisioning_demand_read", resource_id=project_id
        )
    return demand


def _request_by_id(
    *, requests: tuple[ProvisioningRequestState, ...], request_id: str | None
) -> ProvisioningRequestState | None:
    if request_id is None:
        return None
    return next((item for item in requests if item.request_id == request_id), None)


def _require_request(
    *, state: ProvisioningStoreState, request_id: str
) -> ProvisioningRequestState:
    request = _request_by_id(requests=state.requests, request_id=request_id)
    if request is None:
        raise ResourceNotFoundError(
            operation="worker_provisioning_request_read", resource_id=request_id
        )
    return request


def _admission_candidate_project_ids(
    *, state: ProvisioningStoreState
) -> tuple[str, ...]:
    in_flight = _request_by_id(
        requests=state.requests,
        request_id=state.admission.in_flight_request_id,
    )
    if in_flight is not None and in_flight.status in _TRANSIENT_STATUSES:
        return (in_flight.project_id,)
    candidates = [
        item.project_id
        for item in state.requests
        if item.status
        in {
            ProvisioningRequestStatus.WAITING_ADMISSION,
            ProvisioningRequestStatus.RATE_LIMITED,
        }
        and _require_demand(state=state, project_id=item.project_id).spawn_needed > 0
    ]
    if not candidates:
        demand = next((item for item in state.demands if item.spawn_needed > 0), None)
        if demand is not None:
            candidates.append(demand.project_id)
    return tuple(sorted(set(candidates)))


def _state_with_request(
    *,
    state: ProvisioningStoreState,
    request: ProvisioningRequestState,
    now: datetime,
) -> ProvisioningStoreState:
    """Return a root-revisioned snapshot containing one updated request.

    Args:
        state: Latest durable provisioning snapshot.
        request: Updated request belonging to ``state``.
        now: Timestamp for the root snapshot mutation.

    Returns:
        Immutable updated root snapshot.
    """
    return replace(
        state,
        requests=tuple(
            request if item.request_id == request.request_id else item
            for item in state.requests
        ),
        revision=state.revision + 1,
        updated_at=now,
    )


def _new_request(
    demand: ProvisioningDemandState, now: datetime
) -> ProvisioningRequestState:
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


def _clear_stale_in_flight(*, state: ProvisioningStoreState) -> ProvisioningStoreState:
    if state.admission.in_flight_request_id is None:
        return state
    request = _request_by_id(
        requests=state.requests,
        request_id=state.admission.in_flight_request_id,
    )
    if request is not None and request.status in _TRANSIENT_STATUSES:
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


def _admission_blocked(admission: ProvisioningAdmissionState, *, now: datetime) -> bool:
    return any(
        value is not None and now < value
        for value in (admission.next_admission_at, admission.backoff_until)
    )


def _operator_blocked_project_ids(
    *,
    state: ProvisioningStoreState,
    bindings: dict[str, ProjectBindingState | None],
    binding_required: bool,
) -> frozenset[str]:
    """Return projects with a durable non-rate-limit terminal browser failure.

    A failed provider/UI attempt is intentionally fail-closed. Capacity reconciliation
    may keep publishing the unmet deficit, but it cannot convert that durable operator
    blocker into repeated background tab creation.
    """
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


def _retry_after_ms(
    admission: ProvisioningAdmissionState, *, now: datetime
) -> int | None:
    values = [
        value
        for value in (admission.next_admission_at, admission.backoff_until)
        if value is not None and value > now
    ]
    if not values:
        return None
    return max(0, int((min(values) - now).total_seconds() * 1000))


def _validate_demand_values(
    *, desired_capacity: int, spawn_needed: int, binding_revision: int | None
) -> None:
    if desired_capacity < 0 or spawn_needed < 0:
        raise ValueError("capacity values cannot be negative")
    if spawn_needed > desired_capacity:
        raise ValueError("spawn_needed cannot exceed desired_capacity")
    if binding_revision is not None and binding_revision < 0:
        raise ValueError("binding_revision cannot be negative")


def _failure_status(*, reason: str, rate_limited: bool) -> ProvisioningRequestStatus:
    """Map an adapter failure to its durable retry disposition."""
    if rate_limited:
        return ProvisioningRequestStatus.RATE_LIMITED
    if reason.strip().lower() in {
        "browser_open_cooldown",
        "worker_retirement_pending",
    }:
        return ProvisioningRequestStatus.DEFERRED
    return ProvisioningRequestStatus.OPERATOR_BLOCKED


def _is_supported_binding(binding: ProjectBindingState) -> bool:
    """Return whether a binding can be opened by the ChatGPT browser adapter.

    Args:
        binding: Canonical project binding selected for admission.

    Returns:
        ``True`` only for complete, internally consistent ChatGPT project metadata.
    """
    metadata = binding.adapter_metadata
    if (
        metadata is None
        or metadata.provider != "chatgpt"
        or not metadata.project_ref
        or not metadata.project_path
    ):
        return False
    return metadata.project_path.rstrip("/") == f"/g/{metadata.project_ref}"


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp."""
    return datetime.now(UTC)


def _null_context():
    return nullcontext()
