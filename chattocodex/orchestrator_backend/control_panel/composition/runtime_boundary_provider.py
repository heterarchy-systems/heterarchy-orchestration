"""Workspace-scoped runtime boundary composition for the loopback Control Panel."""

from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Protocol

from chattocodex.orchestrator_backend.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.orchestrator_backend.main_runtime.application.main_review_project_catalog import (
    MainReviewProjectCatalog,
)
from chattocodex.orchestrator_backend.main_runtime.delivery.main_review_delivery_lease_tracker import (
    MainReviewDeliveryLeaseTracker,
)
from chattocodex.orchestrator_backend.main_runtime.interface.main_review_route_adapter import (
    MainReviewRouteAdapter,
)
from chattocodex.orchestrator_backend.main_runtime.presence.main_review_chat_presence_tracker import (
    MainReviewChatPresenceTracker,
)
from chattocodex.orchestrator_backend.main_runtime.target.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.orchestrator_backend.persistence.main_runtime.main_review_chat_presence_store import (
    MainReviewChatPresenceStore,
)
from chattocodex.orchestrator_backend.persistence.main_runtime.main_review_delivery_lease_store import (
    MainReviewDeliveryLeaseStore,
)
from chattocodex.orchestrator_backend.persistence.main_runtime.main_review_target_store import (
    MainReviewTargetStore,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.run_control_state_store import (
    RunControlStateStore,
)
from chattocodex.orchestrator_backend.persistence.project.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_delivery_lease_store import (
    WakeDeliveryLeaseStore,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_presence_repository import (
    WakePresenceRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_group_state_store import (
    WorkerRuntimeGroupStateStore,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_exceptions import (
    ProjectBindingConfigurationError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.orchestrator_backend.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.wake.wake_control_service import (
    WakeControlService,
)
from chattocodex.orchestrator_backend.worker_runtime.capacity.broker_ready_capacity_projection import (
    BrokerReadyCapacityProjection,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.wake_delivery_lease_tracker import (
    WakeDeliveryLeaseTracker,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.worker_assignment_lease_reconciler import (
    WorkerAssignmentLeaseReconciler,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_coordinator import (
    WorkerRuntimeCoordinator,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.shared.pathing.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)
from chattocodex.shared.storage.atomic_json_file import AtomicJsonFileError
from chattocodex.shared.storage.runtime_lock_layout import RuntimeLockLayout


@dataclass(frozen=True)
class RuntimeBoundaries:
    """Services that must share one workspace-local execution state root."""

    execution_root: Path
    wake_service: WakeControlService
    main_review_adapter: MainReviewRouteAdapter
    project_binding_service: ProjectBindingService
    worker_provisioning_service: WorkerProvisioningService


class RuntimeBoundaryProvider(Protocol):
    """Resolve runtime services for the currently configured workspace."""

    def current(self) -> RuntimeBoundaries:
        """Current. Args: None. Returns: RuntimeBoundaries."""
        ...


class WorkspaceRuntimeBoundaryProvider:
    """Compose and cache boundaries for the current durable runtime config."""

    def __init__(
        self,
        runtime_config_repository: RuntimeConfigRepository,
    ) -> None:
        """Initialize this object. Args: runtime_config_repository. Returns: None."""
        self._runtime_config_repository = runtime_config_repository
        self._lock = RLock()
        self._cached: RuntimeBoundaries | None = None

    def current(self) -> RuntimeBoundaries:
        """Current. Args: None. Returns: RuntimeBoundaries."""
        execution_root = self._current_execution_root()
        with self._lock:
            if self._cached is None or self._cached.execution_root != execution_root:
                self._cached = _build_runtime_boundaries(
                    execution_root=execution_root,
                    runtime_config_repository=self._runtime_config_repository,
                )
            return self._cached

    def _current_execution_root(self) -> Path:
        """Current execution root. Args: None. Returns: Path."""
        if not self._runtime_config_repository.exists():
            raise ProjectBindingConfigurationError(
                message=(
                    "ChatToCodex runtime is not configured. Save and start the Control "
                    "Panel before using runtime routes."
                )
            )
        try:
            config = self._runtime_config_repository.read()
        except AtomicJsonFileError as error:
            raise ProjectBindingConfigurationError(
                message=(
                    "ChatToCodex runtime configuration cannot be read. Save the Control "
                    "Panel settings again before using runtime routes."
                )
            ) from error
        workspace_root = Path(config.workspace_root).expanduser().resolve()
        if not workspace_root.is_dir():
            raise ProjectBindingConfigurationError(
                message=f"Configured workspace does not exist: {workspace_root}."
            )
        return WorkspaceExecutionLayout.state_root(workspace_root=workspace_root)


def _build_runtime_boundaries(
    execution_root: Path,
    runtime_config_repository: RuntimeConfigRepository,
) -> RuntimeBoundaries:
    """Build runtime boundaries. Args: execution_root, runtime_config_repository. Returns: RuntimeBoundaries."""
    lock_registry = EntityLockRegistry(
        lock_directory=RuntimeLockLayout.lock_directory(
            state_root=execution_root,
            namespace="execution",
        )
    )
    wake_presence_service = WakePresenceService(
        repository=WakePresenceRepository(state_root=execution_root)
    )
    runtime_presence_tracker = WorkerRuntimePresenceTracker(
        store=WorkerRuntimePresenceStore(state_root=execution_root)
    )
    delivery_lease_tracker = WakeDeliveryLeaseTracker(
        store=WakeDeliveryLeaseStore(state_root=execution_root)
    )
    wake_target_repository = WakeTargetRepository(state_root=execution_root)
    wake_target_service = WakeTargetService(
        repository=wake_target_repository,
        presence_service=wake_presence_service,
        runtime_presence_tracker=runtime_presence_tracker,
        delivery_lease_tracker=delivery_lease_tracker,
        lock_registry=lock_registry,
    )
    assignment_lease_reconciler = WorkerAssignmentLeaseReconciler(
        wake_target_repository=wake_target_repository,
        wake_presence_repository=WakePresenceRepository(state_root=execution_root),
        abandoned_assignment_recovery=AbandonedAssignmentRecovery(
            broker_task_repository=BrokerTaskRepository(state_root=execution_root),
            slot_repository=WorkerSlotRepository(state_root=execution_root),
            task_repository=TaskStateRepository(state_root=execution_root),
            lock_registry=lock_registry,
        ),
        wake_target_service=wake_target_service,
        lock_registry=lock_registry,
    )
    ready_capacity_projection = BrokerReadyCapacityProjection(
        broker_task_repository=BrokerTaskRepository(state_root=execution_root),
        run_repository=RunStateRepository(state_root=execution_root),
    )
    project_binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=execution_root),
        runtime_config_repository=runtime_config_repository,
        lock_registry=lock_registry,
    )
    wake_target_service.attach_connector_epoch_resolver(
        lambda project_id: project_binding_service.resolve_connector_epoch(
            project_id=project_id
        )
    )
    worker_provisioning_service = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=execution_root),
        project_binding_service=project_binding_service,
        wake_target_service=wake_target_service,
        lock_registry=lock_registry,
    )
    worker_runtime_coordinator = WorkerRuntimeCoordinator(
        state_store=WorkerRuntimeGroupStateStore(state_root=execution_root),
        wake_target_repository=wake_target_repository,
        wake_presence_service=wake_presence_service,
        runtime_presence_tracker=runtime_presence_tracker,
        wake_target_service=wake_target_service,
        ready_capacity_projection=ready_capacity_projection,
        assignment_lease_reconciler=assignment_lease_reconciler,
        lock_registry=lock_registry,
        pending_provisioning_count=worker_provisioning_service.pending_provisioning_count,
        on_capacity_decision=worker_provisioning_service.on_capacity_decision,
    )
    worker_provisioning_service.attach_worker_runtime_coordinator(
        worker_runtime_coordinator
    )
    wake_service = WakeControlService(
        runtime_config_repository=runtime_config_repository,
        wake_target_service=wake_target_service,
        wake_presence_service=wake_presence_service,
        runtime_presence_tracker=runtime_presence_tracker,
        ready_capacity_projection=ready_capacity_projection,
        assignment_lease_reconciler=assignment_lease_reconciler,
        worker_runtime_coordinator=worker_runtime_coordinator,
        worker_provisioning_service=worker_provisioning_service,
        lock_registry=lock_registry,
    )
    main_review_adapter = MainReviewRouteAdapter(
        target_coordinator=MainReviewTargetCoordinator(
            target_store=MainReviewTargetStore(state_root=execution_root),
            run_control_state_store=RunControlStateStore(state_root=execution_root),
            delivery_lease_tracker=MainReviewDeliveryLeaseTracker(
                store=MainReviewDeliveryLeaseStore(state_root=execution_root)
            ),
            chat_presence_tracker=MainReviewChatPresenceTracker(
                store=MainReviewChatPresenceStore(state_root=execution_root)
            ),
            lock_registry=lock_registry,
        ),
        project_catalog=MainReviewProjectCatalog(
            runtime_config_repository=runtime_config_repository
        ),
        project_binding_service=project_binding_service,
    )
    return RuntimeBoundaries(
        execution_root=execution_root,
        wake_service=wake_service,
        main_review_adapter=main_review_adapter,
        project_binding_service=project_binding_service,
        worker_provisioning_service=worker_provisioning_service,
    )
