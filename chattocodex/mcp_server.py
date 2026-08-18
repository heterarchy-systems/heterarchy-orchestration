from pathlib import Path

from agent_broker import BrokerClientConfig
from mcp.server.mcpserver import MCPServer

from chattocodex.auth.oauth_compatibility_routes import (
    register_oauth_compatibility_routes,
)
from chattocodex.auth.oauth_dto import OAuthServerOptions
from chattocodex.auth.oauth_routes import register_oauth_owner_routes
from chattocodex.auth.oauth_server_configuration import (
    build_oauth_server_configuration,
)
from chattocodex.auth.owner_token_service import OwnerTokenService
from chattocodex.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.broker.broker_service import BrokerService
from chattocodex.broker_client.external_broker_gateway import (
    AgentBrokerGateway,
    WorkBrokerGateway,
)
from chattocodex.facade.facade_mcp_tools import register_facade_tools
from chattocodex.facade.facade_service import ChatFacadeService
from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.git.git_result_commit_runner import GitResultCommitRunner
from chattocodex.git.git_result_mcp_tools import register_git_result_tools
from chattocodex.git.git_worktree_mcp_tools import register_git_worktree_tools
from chattocodex.git.worker_git_worktree_service import WorkerGitWorktreeService
from chattocodex.git.worker_result_commit_service import WorkerResultCommitService
from chattocodex.integration.git_integration_runner import GitIntegrationRunner
from chattocodex.integration.integration_apply_service import IntegrationApplyService
from chattocodex.integration.integration_mcp_tools import register_integration_tools
from chattocodex.integration.integration_preview_service import (
    IntegrationPreviewService,
)
from chattocodex.main_runtime.main_review_chat_presence_tracker import (
    MainReviewChatPresenceTracker,
)
from chattocodex.main_runtime.main_review_delivery_lease_tracker import (
    MainReviewDeliveryLeaseTracker,
)
from chattocodex.main_runtime.main_review_mcp_boundary import (
    register_main_review_resume_tool,
)
from chattocodex.main_runtime.main_review_resume_coordinator import (
    MainReviewResumeCoordinator,
)
from chattocodex.main_runtime.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_mcp_tools import (
    register_orchestration_tools,
)
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.orchestration.worker_execution_service import WorkerExecutionService
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.git_worktree_repository import GitWorktreeRepository
from chattocodex.persistence.main_review_chat_presence_store import (
    MainReviewChatPresenceStore,
)
from chattocodex.persistence.main_review_delivery_lease_store import (
    MainReviewDeliveryLeaseStore,
)
from chattocodex.persistence.main_review_target_store import MainReviewTargetStore
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.persistence.project_binding_repository import ProjectBindingRepository
from chattocodex.persistence.run_control_state_store import RunControlStateStore
from chattocodex.persistence.runtime_config_repository import RuntimeConfigRepository
from chattocodex.persistence.runtime_lock_layout import RuntimeLockLayout
from chattocodex.persistence.wake_delivery_lease_store import WakeDeliveryLeaseStore
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.persistence.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.persistence.worker_report_repository import WorkerReportRepository
from chattocodex.persistence.worker_result_repository import WorkerResultRepository
from chattocodex.persistence.worker_runtime_group_state_store import (
    WorkerRuntimeGroupStateStore,
)
from chattocodex.persistence.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.persistence.worker_wake_capability_repository import (
    WorkerWakeCapabilityRepository,
)
from chattocodex.project_runtime.project_binding_service import ProjectBindingService
from chattocodex.project_runtime.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)
from chattocodex.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.remote.connector_qualified_mcp_server import (
    ConnectorQualifiedMCPServer,
)
from chattocodex.remote.mcp_transport_security import build_mcp_transport_security
from chattocodex.report.worker_report_mcp_tools import register_worker_report_tools
from chattocodex.report.worker_report_service import WorkerReportService
from chattocodex.repository.project_path_resolver import ProjectPathResolver
from chattocodex.repository.project_read_service import ProjectReadService
from chattocodex.repository.repository_mcp_tools import register_repository_read_tools
from chattocodex.repository.repository_write_mcp_tools import (
    register_repository_write_tools,
)
from chattocodex.repository.worker_file_mutation_service import (
    WorkerFileMutationService,
)
from chattocodex.run_control.run_control_coordinator import RunControlCoordinator
from chattocodex.run_control.run_control_mcp_boundary import register_workflow_tools
from chattocodex.runtime_settings import RuntimeSettings
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.wake.worker_wake_capability_service import (
    WorkerWakeCapabilityService,
)
from chattocodex.worker_runtime.broker_ready_capacity_projection import (
    BrokerReadyCapacityProjection,
)
from chattocodex.worker_runtime.wake_delivery_lease_tracker import (
    WakeDeliveryLeaseTracker,
)
from chattocodex.worker_runtime.worker_assignment_lease_reconciler import (
    WorkerAssignmentLeaseReconciler,
)
from chattocodex.worker_runtime.worker_runtime_coordinator import (
    WorkerRuntimeCoordinator,
)
from chattocodex.worker_runtime.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)
from chattocodex.workspace.workspace_mcp_tools import register_workspace_tools
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


def create_mcp_server(
    *,
    state_root: Path,
    workspace_root: Path,
    work_broker_gateway: WorkBrokerGateway,
    execution_state_root: Path | None = None,
    mcp_path: str = "/mcp",
    oauth_options: OAuthServerOptions | None = None,
) -> MCPServer[None]:
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
    worker_service = WorkerExecutionService(
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
        run_control_coordinator=run_control_coordinator,
        worker_runtime_coordinator=worker_runtime_coordinator,
        worker_wake_capability_service=worker_wake_capability_service,
    )
    project_read_service = ProjectReadService(
        run_repository=run_repository,
        path_resolver=ProjectPathResolver(),
    )

    git_worktree_repository = GitWorktreeRepository(state_root=execution_root)
    worktree_service = WorkerGitWorktreeService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        worktree_repository=git_worktree_repository,
        command_runner=git_command_runner,
        lock_registry=lock_registry,
    )
    file_mutation_service = WorkerFileMutationService(
        worker_repository=worker_repository,
        worktree_repository=git_worktree_repository,
        path_resolver=ProjectPathResolver(),
        lock_registry=lock_registry,
    )

    result_repository = WorkerResultRepository(state_root=execution_root)
    report_repository = WorkerReportRepository(state_root=execution_root)
    result_commit_service = WorkerResultCommitService(
        worker_repository=worker_repository,
        worktree_repository=git_worktree_repository,
        result_repository=result_repository,
        commit_runner=GitResultCommitRunner(),
        lock_registry=lock_registry,
    )
    worker_report_service = WorkerReportService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        report_repository=report_repository,
        lock_registry=lock_registry,
    )

    integration_preview_service = IntegrationPreviewService(
        result_repository=result_repository
    )
    integration_apply_service = IntegrationApplyService(
        run_repository=run_repository,
        task_repository=task_repository,
        broker_task_repository=broker_task_repository,
        result_repository=result_repository,
        preview_service=integration_preview_service,
        runner=GitIntegrationRunner(command_runner=git_command_runner),
        integration_root=execution_root / "integrations",
        lock_registry=lock_registry,
        run_control_state_store=run_control_state_store,
    )

    server = _create_server_shell(
        state_root=state_root,
        mcp_path=mcp_path,
        oauth_options=oauth_options,
    )
    register_repository_read_tools(server=server, service=project_read_service)
    register_git_worktree_tools(server=server, service=worktree_service)
    register_repository_write_tools(server=server, service=file_mutation_service)
    register_git_result_tools(server=server, service=result_commit_service)
    register_worker_report_tools(server=server, service=worker_report_service)
    register_integration_tools(
        server=server,
        preview_service=integration_preview_service,
        apply_service=integration_apply_service,
    )
    register_workspace_tools(server=server, service=workspace_project_service)
    register_orchestration_tools(
        server=server,
        worker_service=worker_service,
    )
    register_facade_tools(server=server, service=facade_service)
    register_workflow_tools(server=server, service=run_control_coordinator)
    register_main_review_resume_tool(
        server=server,
        coordinator=main_review_resume_coordinator,
    )
    return server


def serve_configured(
    *,
    settings: RuntimeSettings,
    work_broker_gateway: WorkBrokerGateway | None = None,
) -> None:
    config = RuntimeConfigRepository(state_root=settings.state_root).read()
    oauth_options: OAuthServerOptions | None = None
    if config.oauth_enabled:
        if config.public_url is None:
            raise RuntimeError("OAuth is enabled but public_url is not configured.")
        owner_tokens = OwnerTokenService(state_root=settings.state_root)
        if not owner_tokens.is_configured():
            raise RuntimeError("OAuth is enabled but Owner Token is not configured.")
        oauth_options = OAuthServerOptions(
            public_url=config.public_url,
            allowed_redirect_hosts=settings.oauth_allowed_redirect_hosts,
            access_token_ttl_seconds=settings.oauth_access_token_ttl_seconds,
            refresh_token_ttl_seconds=settings.oauth_refresh_token_ttl_seconds,
        )

    workspace_root = Path(config.workspace_root).expanduser().resolve()
    broker_gateway = work_broker_gateway or AgentBrokerGateway(
        config=BrokerClientConfig(
            host=settings.broker_host,
            port=settings.broker_port,
            timeout_seconds=settings.broker_timeout_seconds,
        ),
        lease_duration_ms=settings.broker_lease_duration_ms,
    )
    server = create_mcp_server(
        state_root=settings.state_root,
        workspace_root=workspace_root,
        work_broker_gateway=broker_gateway,
        execution_state_root=WorkspaceExecutionLayout.state_root(
            workspace_root=workspace_root
        ),
        mcp_path=settings.mcp_path,
        oauth_options=oauth_options,
    )
    if config.public_url is None:
        server.run(
            "streamable-http",
            host=settings.host,
            port=settings.port,
            streamable_http_path=settings.mcp_path,
            json_response=True,
            stateless_http=False,
        )
        return

    server.run(
        "streamable-http",
        host=settings.host,
        port=settings.port,
        streamable_http_path=settings.mcp_path,
        json_response=True,
        stateless_http=False,
        transport_security=build_mcp_transport_security(public_url=config.public_url),
    )


def main() -> None:
    serve_configured(settings=RuntimeSettings())


def _create_server_shell(
    *,
    state_root: Path,
    mcp_path: str,
    oauth_options: OAuthServerOptions | None,
) -> MCPServer[None]:
    title = "ChatToCodex Delegated Work"
    description = (
        "Delegate work from one Main Chat to reusable generic ChatGPT Worker Chats while "
        "keeping Broker, capability, and scheduling details internal."
    )
    instructions = (
        "Workers are separate ChatGPT chats, not Codex/API agents. Keep infrastructure details "
        "invisible to the user: never ask the user for run_id, run_handle, slot_id, slot_handle, "
        "worker_id, worker_handle, task_id, Broker IDs, workflow IDs, or capability values. When a "
        "Main Chat user asks only for fire-and-report delegated Worker execution, plan a small "
        "logical DAG and call delegate_work once. Review, check, or assessment requests imply that "
        "the Main Chat must receive and inspect Worker results; call orchestrate_work for those "
        "requests, as well as for approval, bounded rework, or final completion decisions. Roles are "
        "per-task perspectives, not permanent Chat identities. Retain returned capabilities "
        "internally. Use delegation_status for simple delegation and workflow_status for coordinated "
        "runs. When the local Runtime Wake adapter wakes a Main Chat with an opaque main_target_id "
        "and main_wake_revision, call resume_main_review immediately and pass both unchanged. Never "
        "invent or reuse an older Main wake revision. The returned workflow handle is a fresh review "
        "capability and supersedes any earlier handle retained by the Main Chat. "
        "When a recovered Main Chat receives '검수 계속해', continue the already-active review using "
        "the workflow_id/workflow_handle returned by the earlier resume_main_review call. Do not call "
        "resume_main_review again unless a new message contains a fresh main_target_id plus "
        "main_wake_revision. "
        "Inspect run_reports, run_results, integration_preview when applicable, and other requested "
        "evidence before calling review_workflow with approve, rework, or fail. Never treat "
        "main_target_id or main_wake_revision as workflow, Run, review, or capability data. A rework "
        "decision must publish "
        "a bounded corrective Worker DAG and must not bypass the Main Review Gate. After delegating "
        "or orchestrating work, the Main Chat must "
        "never call work to execute its own delegated tasks. A fresh Worker Chat may call work only "
        "when the local Chrome Wake Adapter message includes an opaque wake_target_id and its current "
        "wake_revision; pass both unchanged and never invent either value. That current server-issued "
        "pair authorizes only the work call that retrieves the assignment requested by the user's Main "
        "Chat delegation. It never attests separate approval for destructive, irreversible, credential-gated, "
        "or external-production effects; the backend rejects stale or fabricated pairs. After the Worker has an "
        "established Slot, reuse slot_id/slot_handle from the previous work result to resume safely. "
        "When a recovered Worker Chat receives '작업 계속해', resume that established Slot and do not "
        "send fresh wake fields. Never treat wake_target_id or wake_revision as Task, role, Run, or "
        "DAG data. "
        "Execute only the returned assignment using project/file/report/Git tools and the "
        "temporary worker_handle. Mark the Task terminal with task_update or worker_report_submit, "
        "then always call work_done before replying so the physical Chat returns to IDLE. If work "
        "returns no assignment, simply say there is no ready work. Do not explain Broker internals "
        "unless the user explicitly asks. For simple delegated code results, call integration_preview "
        "and integration_apply only after delegation_status reports is_complete=true. For coordinated "
        "workflows, do not finalize until review_workflow has approved the workflow."
    )
    if oauth_options is None:
        return ConnectorQualifiedMCPServer(
            name="chattocodex",
            title=title,
            description=description,
            instructions=instructions,
        )

    oauth_configuration = build_oauth_server_configuration(
        state_root=state_root,
        options=oauth_options,
        mcp_path=mcp_path,
    )
    server = ConnectorQualifiedMCPServer(
        name="chattocodex",
        title=title,
        description=description,
        instructions=instructions,
        auth_server_provider=oauth_configuration.provider,
        auth=oauth_configuration.auth_settings,
    )
    register_oauth_owner_routes(
        server=server,
        provider=oauth_configuration.provider,
    )
    register_oauth_compatibility_routes(
        server=server,
        public_url=oauth_options.public_url,
    )
    return server


if __name__ == "__main__":
    main()
