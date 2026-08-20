"""Registration lifecycle for provisioned Worker runtimes."""

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from typing import Protocol

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_dto import (
    ProjectBindingState,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_mapper import (
    metadata_state_from_schema,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningRequestState,
    ProvisioningRequestStatus,
    ProvisioningStoreState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_exceptions import (
    ProvisioningConfigurationError,
    ProvisioningConflictError,
    ProvisioningStaleCallbackError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_policy import (
    require_admission,
    require_demand,
    require_request,
    state_with_request,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_group_state import (
    WorkerRuntimeGroupState,
)
from chattocodex.shared.contracts.project.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)


class WorkerRuntimeTopologyCoordinator(Protocol):
    """Runtime-topology seam required after automatic target registration."""

    def reconcile_runtime_topology(self, project_id: str) -> WorkerRuntimeGroupState:
        """Reconcile and return the project's recoverable runtime topology. Args: project_id. Returns: WorkerRuntimeGroupState."""

    def reconcile_project(self, project_id: str) -> object:
        """Reconcile READY capacity after a recoverable member appears. Args: project_id. Returns: object."""


class RuntimeOwnershipRetirer(Protocol):
    """Retirement seam used when an ACTIVE registration replay is unrecoverable."""

    def __call__(self, wake_target_id: str) -> ProvisioningRequestState | None:
        """Retire runtime ownership for one exact wake target. Args: wake_target_id. Returns: ProvisioningRequestState | None."""


class WorkerProvisioningRegistrationService:
    """Own registration confirmation and ACTIVE replay recoverability."""

    def __init__(
        self,
        repository_provider: Callable[[], WorkerProvisioningRepository],
        project_binding_service_provider: Callable[[], ProjectBindingService | None],
        wake_target_service_provider: Callable[[], WakeTargetService | None],
        worker_runtime_coordinator_provider: Callable[
            [], WorkerRuntimeTopologyCoordinator | None
        ],
        retire_runtime_ownership: RuntimeOwnershipRetirer,
        now_provider: Callable[[], datetime],
    ) -> None:
        """Initialize this object. Args: repository_provider, project_binding_service_provider, wake_target_service_provider, worker_runtime_coordinator_provider, retire_runtime_ownership, now_provider. Returns: None."""
        self._repository_provider = repository_provider
        self._project_binding_service_provider = project_binding_service_provider
        self._wake_target_service_provider = wake_target_service_provider
        self._worker_runtime_coordinator_provider = worker_runtime_coordinator_provider
        self._retire_runtime_ownership = retire_runtime_ownership
        self._now_provider = now_provider

    @property
    def _repository(self) -> WorkerProvisioningRepository:
        """Repository. Args: None. Returns: WorkerProvisioningRepository."""
        return self._repository_provider()

    @property
    def _project_binding_service(self) -> ProjectBindingService | None:
        """Project binding service. Args: None. Returns: ProjectBindingService | None."""
        return self._project_binding_service_provider()

    @property
    def _wake_target_service(self) -> WakeTargetService | None:
        """Wake target service. Args: None. Returns: WakeTargetService | None."""
        return self._wake_target_service_provider()

    @property
    def _worker_runtime_coordinator(self) -> WorkerRuntimeTopologyCoordinator | None:
        """Worker runtime coordinator. Args: None. Returns: WorkerRuntimeTopologyCoordinator | None."""
        return self._worker_runtime_coordinator_provider()

    def register_callback(
        self,
        request_id: str,
        conversation_id: str,
        wake_target_id: str,
        admission_revision: int,
        adapter_metadata: ProjectBindingAdapterMetadata | None = None,
    ) -> ProvisioningRequestState:
        """Register one persistent Worker target and confirm runtime topology. Args: request_id, conversation_id, wake_target_id, admission_revision, adapter_metadata. Returns: ProvisioningRequestState."""
        if not conversation_id.strip():
            raise ValueError("conversation_id cannot be empty")
        initial = self._repository.read_state()
        initial_request = require_request(state=initial, request_id=request_id)
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
        require_admission(
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
                request_id=request_id,
                current_revision=binding.revision,
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
            if self._worker_runtime_coordinator is not None:
                runtime_topology = (
                    self._worker_runtime_coordinator.reconcile_runtime_topology(
                        project_id=initial_request.project_id
                    )
                )
                if wake_target_id not in runtime_topology.wake_target_ids:
                    raise ProvisioningConflictError(
                        request_id=request_id,
                        message=(
                            "Registered Worker is not part of the recoverable runtime topology yet."
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
                "registration_runtime_topology_confirmation_failed"
                if self._worker_runtime_coordinator is not None
                and "recoverable runtime topology" in error.message
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
            """Register. Args: state. Returns: ProvisioningStoreState."""
            request = require_request(state=state, request_id=request_id)
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
            require_admission(
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
            demand = require_demand(state=state, project_id=request.project_id)
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
        result = require_request(state=persisted, request_id=request_id)
        if self._worker_runtime_coordinator is not None:
            self._worker_runtime_coordinator.reconcile_project(
                project_id=initial_request.project_id
            )
        return result

    def _require_active_registration_recoverable(
        self, request: ProvisioningRequestState
    ) -> None:
        """Require active registration recoverable. Args: request. Returns: None."""
        try:
            if self._wake_target_service is not None:
                target = self._wake_target_service.get_target(
                    wake_target_id=request.wake_target_id or ""
                )
                if target.project_id != request.project_id:
                    raise ProvisioningConflictError(
                        request_id=request.request_id,
                        message=(
                            "Active registration wake target belongs to a different project."
                        ),
                    )
            if self._worker_runtime_coordinator is not None:
                runtime_topology = (
                    self._worker_runtime_coordinator.reconcile_runtime_topology(
                        project_id=request.project_id
                    )
                )
                if request.wake_target_id not in runtime_topology.wake_target_ids:
                    raise ProvisioningConflictError(
                        request_id=request.request_id,
                        message=(
                            "Active registration is no longer a recoverable Consumer runtime topology."
                        ),
                    )
        except OrchestrationError as error:
            self._retire_runtime_ownership(wake_target_id=request.wake_target_id or "")
            raise ProvisioningConflictError(
                request_id=request.request_id,
                message=(
                    "Active registration no longer has a recoverable runtime target."
                ),
            ) from error

    def _cancel_registration_failure(
        self, request_id: str, admission_revision: int, reason: str
    ) -> None:
        """Cancel registration failure. Args: request_id, admission_revision, reason. Returns: None."""
        now = self._now_provider()

        def cancel(state: ProvisioningStoreState) -> ProvisioningStoreState:
            """Cancel. Args: state. Returns: ProvisioningStoreState."""
            request = require_request(state=state, request_id=request_id)
            require_admission(
                request=request,
                admission=state.admission,
                admission_revision=admission_revision,
            )
            return state_with_request(
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

    def _binding_for(self, project_id: str) -> ProjectBindingState | None:
        """Binding for. Args: project_id. Returns: ProjectBindingState | None."""
        if self._project_binding_service is None:
            return None
        try:
            return self._project_binding_service.get_binding(project_id=project_id)
        except ResourceNotFoundError:
            return None
        except OrchestrationError as error:
            raise ProvisioningConfigurationError(
                project_id=project_id,
                message=error.message,
            ) from error
