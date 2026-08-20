"""Application service for safe, project-bound Worker provisioning."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

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
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.orchestrator_backend.provisioning.admission.worker_provisioning_admission_service import (
    WorkerProvisioningAdmissionService,
)
from chattocodex.orchestrator_backend.provisioning.capacity.worker_provisioning_capacity_service import (
    WorkerProvisioningCapacityService,
)
from chattocodex.orchestrator_backend.provisioning.lifecycle.worker_provisioning_callback_service import (
    WorkerProvisioningCallbackService,
)
from chattocodex.orchestrator_backend.provisioning.lifecycle.worker_provisioning_registration_service import (
    WorkerProvisioningRegistrationService,
)
from chattocodex.orchestrator_backend.provisioning.lifecycle.worker_provisioning_retirement_service import (
    WorkerProvisioningRetirementService,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningDemandState,
    ProvisioningRequestState,
    ProvisioningRequestStatus,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_exceptions import (
    ProvisioningConfigurationError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_mapper import (
    demand_view_from_state,
    request_view_from_state,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_sync import (
    PENDING_REQUEST_STATUSES,
    TRANSIENT_REQUEST_STATUSES,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_group_state import (
    WorkerRuntimeGroupState,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.shared.contracts.project.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)
from chattocodex.shared.contracts.worker.worker_provisioning_schemas import (
    WorkerProvisioningDemandView,
    WorkerProvisioningRequestView,
)

if TYPE_CHECKING:
    from chattocodex.orchestrator_backend.worker_runtime.capacity.capacity_decision import (
        CapacityDecision,
    )


class ConsumerGroupMembershipCoordinator(Protocol):
    """Minimal runtime-topology seam used after automatic wake-target registration."""

    def reconcile_runtime_topology(self, project_id: str) -> WorkerRuntimeGroupState:
        """Reconcile and return the project's recoverable Worker runtime topology. Args: project_id. Returns: WorkerRuntimeGroupState."""

    def reconcile_project(self, project_id: str) -> object:
        """Reconcile READY capacity after a new recoverable member appears. Args: project_id. Returns: object."""


_TRANSIENT_STATUSES = TRANSIENT_REQUEST_STATUSES
_PENDING_STATUSES = PENDING_REQUEST_STATUSES


class WorkerProvisioningService:
    """Own durable demand, global admission, and registration lifecycle."""

    def __init__(
        self,
        repository: WorkerProvisioningRepository,
        project_binding_service: ProjectBindingService | None = None,
        wake_target_service: WakeTargetService | None = None,
        worker_runtime_coordinator: ConsumerGroupMembershipCoordinator | None = None,
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
            worker_runtime_coordinator: Runtime-topology confirmation boundary.
            lock_registry: Optional in-process scheduler lock registry.
            now_provider: Time provider used by tests and rate-limit policy.
            minimum_admission_interval: Minimum interval between browser admissions.
            admission_lease: Maximum time one browser attempt owns the global gate.
            rate_limit_backoff: Bounded provider backoff durations.
            terminal_request_retention: Count of failed/cancelled records retained in
                the durable browser-facing snapshot.

        Raises:
            ValueError: If admission intervals or backoff settings are invalid. Returns: None."""
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
        self._worker_runtime_coordinator = worker_runtime_coordinator
        self._lock_registry = lock_registry or EntityLockRegistry()
        self._now_provider = now_provider or _utc_now
        self._minimum_admission_interval = minimum_admission_interval
        self._admission_lease = admission_lease
        self._rate_limit_backoff = rate_limit_backoff
        self._terminal_request_retention = terminal_request_retention
        self._retirement_service = WorkerProvisioningRetirementService(
            repository=repository,
            lock_registry=self._lock_registry,
            now_provider=self._now_provider,
            terminal_request_retention=terminal_request_retention,
        )
        self._registration_service = WorkerProvisioningRegistrationService(
            repository_provider=lambda: self._repository,
            project_binding_service_provider=lambda: self._project_binding_service,
            wake_target_service_provider=lambda: self._wake_target_service,
            worker_runtime_coordinator_provider=lambda: (
                self._worker_runtime_coordinator
            ),
            retire_runtime_ownership=self._retirement_service.retire_runtime_ownership,
            now_provider=self._now_provider,
        )
        self._callback_service = WorkerProvisioningCallbackService(
            repository_provider=lambda: self._repository,
            lock_registry=self._lock_registry,
            now_provider=self._now_provider,
            minimum_admission_interval=self._minimum_admission_interval,
            rate_limit_backoff=self._rate_limit_backoff,
            terminal_request_retention=self._terminal_request_retention,
        )
        self._capacity_service = WorkerProvisioningCapacityService(
            repository_provider=lambda: self._repository,
            binding_provider=lambda project_id: self._binding_for(
                project_id=project_id
            ),
            lock_registry=self._lock_registry,
            now_provider=self._now_provider,
            terminal_request_retention=self._terminal_request_retention,
        )
        self._admission_service = WorkerProvisioningAdmissionService(
            repository_provider=lambda: self._repository,
            binding_provider=lambda project_id: self._binding_for(
                project_id=project_id
            ),
            binding_required_provider=lambda: self._project_binding_service is not None,
            lock_registry=self._lock_registry,
            now_provider=self._now_provider,
            minimum_admission_interval=self._minimum_admission_interval,
            admission_lease=self._admission_lease,
            terminal_request_retention=self._terminal_request_retention,
        )
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

    def attach_worker_runtime_coordinator(
        self, coordinator: ConsumerGroupMembershipCoordinator
    ) -> None:
        """Attach the runtime-topology coordinator after construction.

        The Control Panel constructs provisioning before the Worker runtime coordinator so
        the coordinator can receive non-reentrant capacity callbacks. Registration
        callbacks use this one-way attachment only after construction is complete.

        Args:
            coordinator: Canonical Worker runtime coordinator. Returns: None."""
        self._worker_runtime_coordinator = coordinator

    def pending_provisioning_count(self, project_id: str | None = None) -> int:
        """Return outstanding reservations for one project or globally. Args: project_id. Returns: int."""
        return self._capacity_service.pending_provisioning_count(project_id)

    def upsert_demand(
        self,
        project_id: str,
        desired_capacity: int,
        spawn_needed: int,
        binding_revision: int | None = None,
    ) -> ProvisioningDemandState:
        """Persist one backend-computed project capacity deficit. Args: project_id, desired_capacity, spawn_needed, binding_revision. Returns: ProvisioningDemandState."""
        return self._capacity_service.upsert_demand(
            project_id=project_id,
            desired_capacity=desired_capacity,
            spawn_needed=spawn_needed,
            binding_revision=binding_revision,
        )

    def on_capacity_decision(self, decision: CapacityDecision) -> None:
        """Atomically reserve the remaining deficit from capacity_core. Args: decision. Returns: None."""
        self._capacity_service.on_capacity_decision(decision)

    def admit_next(self) -> ProvisioningRequestState | None:
        """Admit the next queued request under the global serialized gate. Args: None. Returns: ProvisioningRequestState | None."""
        return self._admission_service.admit_next()

    def progress_request(
        self,
        request_id: str,
        status: ProvisioningRequestStatus,
        admission_revision: int,
    ) -> ProvisioningRequestState:
        """Progress request. Args: request_id, status, admission_revision. Returns: ProvisioningRequestState."""
        return self._callback_service.progress_request(
            request_id=request_id,
            status=status,
            admission_revision=admission_revision,
        )

    def register_callback(
        self,
        request_id: str,
        conversation_id: str,
        wake_target_id: str,
        admission_revision: int,
        adapter_metadata: ProjectBindingAdapterMetadata | None = None,
    ) -> ProvisioningRequestState:
        """Register callback. Args: request_id, conversation_id, wake_target_id, admission_revision, adapter_metadata. Returns: ProvisioningRequestState."""
        return self._registration_service.register_callback(
            request_id=request_id,
            conversation_id=conversation_id,
            wake_target_id=wake_target_id,
            admission_revision=admission_revision,
            adapter_metadata=adapter_metadata,
        )

    def fail_callback(
        self,
        request_id: str,
        admission_revision: int,
        reason: str,
        rate_limited: bool = False,
    ) -> tuple[ProvisioningRequestState, int | None]:
        """Fail callback. Args: request_id, admission_revision, reason, rate_limited. Returns: tuple[ProvisioningRequestState, int | None]."""
        return self._callback_service.fail_callback(
            request_id=request_id,
            admission_revision=admission_revision,
            reason=reason,
            rate_limited=rate_limited,
        )

    def cancel_request(
        self, request_id: str, admission_revision: int | None = None
    ) -> ProvisioningRequestState:
        """Cancel request. Args: request_id, admission_revision. Returns: ProvisioningRequestState."""
        return self._callback_service.cancel_request(
            request_id=request_id,
            admission_revision=admission_revision,
        )

    def retire_runtime_ownership(
        self,
        wake_target_id: str,
    ) -> ProvisioningRequestState | None:
        """Retire runtime ownership. Args: wake_target_id. Returns: ProvisioningRequestState | None."""
        return self._retirement_service.retire_runtime_ownership(
            wake_target_id=wake_target_id,
        )

    def retire_runtime_ownership_batch(
        self,
        project_id: str,
        wake_target_ids: tuple[str, ...],
    ) -> tuple[tuple[ProvisioningRequestState, ...], bool]:
        """Retire runtime ownership batch. Args: project_id, wake_target_ids. Returns: tuple[tuple[ProvisioningRequestState, ...], bool]."""
        return self._retirement_service.retire_runtime_ownership_batch(
            project_id=project_id,
            wake_target_ids=wake_target_ids,
        )

    def retry_operator_blocked_request(
        self, request_id: str, expected_revision: int
    ) -> ProvisioningRequestState:
        """Queue one operator-blocked request for a deliberate retry. Args: request_id, expected_revision. Returns: ProvisioningRequestState."""
        return self._admission_service.retry_operator_blocked_request(
            request_id=request_id,
            expected_revision=expected_revision,
        )

    def list_status(
        self,
    ) -> tuple[
        tuple[WorkerProvisioningDemandView, ...],
        tuple[WorkerProvisioningRequestView, ...],
    ]:
        """Return opaque demand and request views for local inspection. Args: None. Returns: tuple[tuple[WorkerProvisioningDemandView, ...], tuple[WorkerProvisioningRequestView, ...]]."""
        state = self._repository.read_state()
        return (
            tuple(demand_view_from_state(item) for item in state.demands),
            tuple(request_view_from_state(item) for item in state.requests),
        )

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
                project_id=project_id, message=error.message
            ) from error


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp. Args: None. Returns: datetime."""
    return datetime.now(UTC)
