from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from chattocodex.orchestrator_backend.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.composition.workspace_service_composition import (
    build_workspace_application_services,
)
from chattocodex.orchestrator_backend.facade.facade_service import ChatFacadeService
from chattocodex.orchestrator_backend.facade.worker_work_service import (
    WorkerWorkService,
)
from chattocodex.orchestrator_backend.integration.integration_apply_service import (
    IntegrationApplyService,
)
from chattocodex.orchestrator_backend.integration.integration_preview_service import (
    IntegrationPreviewService,
)
from chattocodex.orchestrator_backend.main_runtime.application.main_review_resume_coordinator import (
    MainReviewResumeCoordinator,
)
from chattocodex.orchestrator_backend.main_runtime.delivery.main_review_delivery_lease_tracker import (
    MainReviewDeliveryLeaseTracker,
)
from chattocodex.orchestrator_backend.main_runtime.presence.main_review_chat_presence_tracker import (
    MainReviewChatPresenceTracker,
)
from chattocodex.orchestrator_backend.main_runtime.target.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.orchestration.worker_execution_service import (
    WorkerExecutionService,
)
from chattocodex.orchestrator_backend.organization.group_folder_service import (
    GroupFolderService,
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
    WorkerStateRepository,
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
from chattocodex.orchestrator_backend.persistence.worker.worker_wake_capability_repository import (
    WorkerWakeCapabilityRepository,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.orchestrator_backend.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.orchestrator_backend.report.worker_report_service import (
    WorkerReportService,
)
from chattocodex.orchestrator_backend.repository.project_read_service import (
    ProjectReadService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_mutation_service import (
    WorkerDirectoryMutationService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_query_service import (
    WorkerDirectoryQueryService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_file_mutation_service import (
    WorkerFileMutationService,
)
from chattocodex.orchestrator_backend.run_control.run_control_coordinator import (
    RunControlCoordinator,
)
from chattocodex.orchestrator_backend.run_control.workflow_refresh_service import (
    WorkflowRefreshService,
)
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.orchestrator_backend.version_control.worker_git_worktree_service import (
    WorkerGitWorktreeService,
)
from chattocodex.orchestrator_backend.version_control.worker_result_commit_service import (
    WorkerResultCommitService,
)
from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_service import (
    WorkerWakeCapabilityService,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
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
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.shared.contracts.broker.broker_gateway_contract import (
    WorkBrokerGateway,
)
from chattocodex.shared.storage.runtime_lock_layout import RuntimeLockLayout


@dataclass(frozen=True, slots=True)
class OrchestratorBackendServices:
    """Expose application-facing services to transport adapters without repositories."""

    project_read_service: ProjectReadService
    worktree_service: WorkerGitWorktreeService
    file_mutation_service: WorkerFileMutationService
    directory_query_service: WorkerDirectoryQueryService
    directory_mutation_service: WorkerDirectoryMutationService
    result_commit_service: WorkerResultCommitService
    worker_report_service: WorkerReportService
    integration_preview_service: IntegrationPreviewService
    integration_apply_service: IntegrationApplyService
    workspace_project_service: WorkspaceProjectService
    worker_execution_service: WorkerExecutionService
    facade_service: ChatFacadeService
    run_control_coordinator: RunControlCoordinator
    main_review_resume_coordinator: MainReviewResumeCoordinator
    group_folder_service: GroupFolderService | None


def build_orchestrator_backend_services(
    state_root: Path,
    workspace_root: Path,
    work_broker_gateway: WorkBrokerGateway,
    execution_state_root: Path | None = None,
    group_folder_service: GroupFolderService | None = None,
) -> OrchestratorBackendServices:
    """Compose the backend application graph behind one transport-neutral boundary.

    Args:
        state_root: Process state root used when no separate execution root is supplied.
        workspace_root: Root that contains discoverable orchestration projects.
        work_broker_gateway: Broker gateway used for durable assignment coordination.
        execution_state_root: Optional isolated root for execution state.
        group_folder_service: Optional PostgreSQL-backed group-folder application service.

    Returns:
        Fully composed application services that MCP or HTTP adapters may invoke.
    """
    execution_root = (execution_state_root or state_root).expanduser().resolve()
    run_repository = RunStateRepository(state_root=execution_root)
    worker_repository = WorkerStateRepository(state_root=execution_root)
    task_repository = TaskStateRepository(state_root=execution_root)
    broker_task_repository = BrokerTaskRepository(state_root=execution_root)
    worker_slot_repository = WorkerSlotRepository(state_root=execution_root)
    wake_presence_repository = WakePresenceRepository(state_root=execution_root)
    wake_target_repository = WakeTargetRepository(state_root=execution_root)
    run_control_state_store = RunControlStateStore(state_root=execution_root)
    lock_registry = EntityLockRegistry(
        lock_directory=RuntimeLockLayout.lock_directory(
            state_root=execution_root,
            namespace="execution",
        )
    )
    workspace_project_service = WorkspaceProjectService(workspace_root=workspace_root)
    git_command_runner = GitCommandRunner()

    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=workspace_project_service,
        command_runner=git_command_runner,
    )
    worker_execution_service = WorkerExecutionService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        lock_registry=lock_registry,
    )
    broker_service = BrokerService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        broker_task_repository=broker_task_repository,
        slot_repository=worker_slot_repository,
        work_broker_gateway=work_broker_gateway,
        workspace_project_service=workspace_project_service,
        lock_registry=lock_registry,
    )
    wake_presence_service = WakePresenceService(repository=wake_presence_repository)
    runtime_presence_tracker = WorkerRuntimePresenceTracker(
        store=WorkerRuntimePresenceStore(state_root=execution_root)
    )
    delivery_lease_tracker = WakeDeliveryLeaseTracker(
        store=WakeDeliveryLeaseStore(state_root=execution_root)
    )
    wake_target_service = WakeTargetService(
        repository=wake_target_repository,
        presence_service=wake_presence_service,
        runtime_presence_tracker=runtime_presence_tracker,
        delivery_lease_tracker=delivery_lease_tracker,
        lock_registry=lock_registry,
    )
    project_binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=execution_root),
        workspace_project_service=workspace_project_service,
        lock_registry=lock_registry,
    )
    wake_target_service.attach_connector_epoch_resolver(
        lambda project_id: project_binding_service.resolve_connector_epoch(
            project_id=project_id
        )
    )
    worker_wake_capability_service = WorkerWakeCapabilityService(
        repository=WorkerWakeCapabilityRepository(state_root=execution_root),
        lock_registry=lock_registry,
        physical_target_exists=lambda wake_target_id: wake_target_repository.exists(
            wake_target_id=wake_target_id
        ),
    )
    worker_wake_capability_service.attach_connector_epoch_resolver(
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
    ready_capacity_projection = BrokerReadyCapacityProjection(
        broker_task_repository=broker_task_repository,
        run_repository=run_repository,
    )
    assignment_lease_reconciler = WorkerAssignmentLeaseReconciler(
        wake_target_repository=wake_target_repository,
        wake_presence_repository=wake_presence_repository,
        abandoned_assignment_recovery=AbandonedAssignmentRecovery(
            broker_task_repository=broker_task_repository,
            slot_repository=worker_slot_repository,
            task_repository=task_repository,
            lock_registry=lock_registry,
        ),
        assignment_lease_renewer=lambda slot_id: broker_service.renew_assignment_lease(
            slot_id=slot_id
        ),
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
    run_control_coordinator = RunControlCoordinator(
        repository=run_control_state_store,
        run_service=run_service,
        broker_service=broker_service,
        wake_target_service=wake_target_service,
        workspace_project_service=workspace_project_service,
        lock_registry=lock_registry,
        workflow_refresh_service=WorkflowRefreshService(
            repository=run_control_state_store,
            broker_service=broker_service,
            lock_registry=lock_registry,
        ),
        worker_runtime_coordinator=worker_runtime_coordinator,
    )
    main_review_target_coordinator = MainReviewTargetCoordinator(
        target_store=MainReviewTargetStore(state_root=execution_root),
        run_control_state_store=run_control_state_store,
        delivery_lease_tracker=MainReviewDeliveryLeaseTracker(
            store=MainReviewDeliveryLeaseStore(state_root=execution_root)
        ),
        chat_presence_tracker=MainReviewChatPresenceTracker(
            store=MainReviewChatPresenceStore(state_root=execution_root)
        ),
        lock_registry=lock_registry,
    )
    main_review_resume_coordinator = MainReviewResumeCoordinator(
        target_coordinator=main_review_target_coordinator,
        run_control_coordinator=run_control_coordinator,
    )
    facade_service = ChatFacadeService(
        run_service=run_service,
        broker_service=broker_service,
        wake_target_service=wake_target_service,
        workspace_project_service=workspace_project_service,
        worker_work_service=WorkerWorkService(
            broker_service=broker_service,
            wake_target_service=wake_target_service,
            workspace_project_service=workspace_project_service,
            run_control_coordinator=run_control_coordinator,
            worker_runtime_coordinator=worker_runtime_coordinator,
            worker_wake_capability_service=worker_wake_capability_service,
        ),
        worker_runtime_coordinator=worker_runtime_coordinator,
        worker_wake_capability_service=worker_wake_capability_service,
    )
    workspace_services = build_workspace_application_services(
        execution_root=execution_root,
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        broker_task_repository=broker_task_repository,
        run_control_state_store=run_control_state_store,
        lock_registry=lock_registry,
        git_command_runner=git_command_runner,
    )
    return OrchestratorBackendServices(
        project_read_service=workspace_services.project_read_service,
        worktree_service=workspace_services.worktree_service,
        file_mutation_service=workspace_services.file_mutation_service,
        directory_query_service=workspace_services.directory_query_service,
        directory_mutation_service=workspace_services.directory_mutation_service,
        result_commit_service=workspace_services.result_commit_service,
        worker_report_service=workspace_services.worker_report_service,
        integration_preview_service=workspace_services.integration_preview_service,
        integration_apply_service=workspace_services.integration_apply_service,
        workspace_project_service=workspace_project_service,
        worker_execution_service=worker_execution_service,
        facade_service=facade_service,
        run_control_coordinator=run_control_coordinator,
        main_review_resume_coordinator=main_review_resume_coordinator,
        group_folder_service=group_folder_service,
    )
