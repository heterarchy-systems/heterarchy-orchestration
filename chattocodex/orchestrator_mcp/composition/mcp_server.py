from __future__ import annotations

from pathlib import Path

from mcp.server.mcpserver import MCPServer

from chattocodex.orchestrator_backend.composition.runtime_services import (
    OrchestratorBackendServices,
    build_orchestrator_backend_services,
)
from chattocodex.orchestrator_backend.organization.group_folder_service import (
    GroupFolderService,
)
from chattocodex.orchestrator_mcp.auth.oauth_dto import OAuthServerOptions
from chattocodex.orchestrator_mcp.composition.mcp_lifespan import McpLifespan
from chattocodex.orchestrator_mcp.composition.mcp_runtime_launcher import (
    run_configured_mcp,
)
from chattocodex.orchestrator_mcp.tools.coordination.facade_tools import (
    register_facade_tools,
)
from chattocodex.orchestrator_mcp.tools.coordination.main_review_tools import (
    register_main_review_resume_tool,
)
from chattocodex.orchestrator_mcp.tools.coordination.orchestration_tools import (
    register_orchestration_tools,
)
from chattocodex.orchestrator_mcp.tools.coordination.run_control_tools import (
    register_workflow_tools,
)
from chattocodex.orchestrator_mcp.tools.coordination.worker_report_tools import (
    register_worker_report_tools,
)
from chattocodex.orchestrator_mcp.tools.organization.group_folder_tools import (
    register_group_folder_tools,
)
from chattocodex.orchestrator_mcp.tools.revision.integration_tools import (
    register_integration_tools,
)
from chattocodex.orchestrator_mcp.tools.revision.revision_result_tools import (
    register_git_result_tools,
)
from chattocodex.orchestrator_mcp.tools.revision.revision_workspace_tools import (
    register_git_worktree_tools,
)
from chattocodex.orchestrator_mcp.tools.workspace.repository_read_tools import (
    register_repository_read_tools,
)
from chattocodex.orchestrator_mcp.tools.workspace.repository_write_tools import (
    register_repository_write_tools,
)
from chattocodex.orchestrator_mcp.tools.workspace.worker_directory_tools import (
    register_worker_directory_tools,
)
from chattocodex.orchestrator_mcp.tools.workspace.workspace_tools import (
    register_workspace_tools,
)
from chattocodex.orchestrator_mcp.transport.mcp_server_shell import (
    create_mcp_server_shell,
)
from chattocodex.shared.contracts.broker.broker_gateway_contract import (
    WorkBrokerGateway,
)
from chattocodex.shared.settings.runtime_settings import RuntimeSettings


def create_mcp_server(
    state_root: Path,
    workspace_root: Path,
    work_broker_gateway: WorkBrokerGateway,
    execution_state_root: Path | None = None,
    mcp_path: str = "/mcp",
    oauth_options: OAuthServerOptions | None = None,
    group_folder_service: GroupFolderService | None = None,
    lifespan: McpLifespan | None = None,
) -> MCPServer[None]:
    """Bind transport-facing MCP tools to the backend application service graph.

    Args:
        state_root: Process-local state root.
        workspace_root: Configured project workspace root.
        work_broker_gateway: Broker gateway supplied to backend application services.
        execution_state_root: Optional isolated execution-state root.
        mcp_path: Streamable HTTP MCP route path.
        oauth_options: Optional owner OAuth configuration.
        group_folder_service: Optional PostgreSQL-backed folder application service.
        lifespan: Optional MCP lifespan callback for backend resources.

    Returns:
        Configured MCP server whose tools call backend application services only.
    """
    services = build_orchestrator_backend_services(
        state_root=state_root,
        workspace_root=workspace_root,
        work_broker_gateway=work_broker_gateway,
        execution_state_root=execution_state_root,
        group_folder_service=group_folder_service,
    )
    return _create_mcp_server_from_services(
        state_root=state_root,
        backend_services=services,
        mcp_path=mcp_path,
        oauth_options=oauth_options,
        lifespan=lifespan,
    )


def _create_mcp_server_from_services(
    state_root: Path,
    backend_services: OrchestratorBackendServices,
    mcp_path: str,
    oauth_options: OAuthServerOptions | None,
    lifespan: McpLifespan | None,
) -> MCPServer[None]:
    """Register MCP tools against an already composed backend service graph.

    Args:
        state_root: Process-local state root used by MCP-owned transport state.
        backend_services: Transport-neutral backend application service graph.
        mcp_path: Streamable HTTP MCP route path.
        oauth_options: Optional owner OAuth configuration.
        lifespan: Optional MCP lifespan callback owning backend resources.

    Returns:
        Configured MCP server whose tools call backend application services only.
    """
    server = create_mcp_server_shell(
        state_root=state_root,
        mcp_path=mcp_path,
        oauth_options=oauth_options,
        lifespan=lifespan,
    )
    register_repository_read_tools(
        server=server,
        service=backend_services.project_read_service,
    )
    register_git_worktree_tools(
        server=server,
        service=backend_services.worktree_service,
    )
    register_repository_write_tools(
        server=server,
        service=backend_services.file_mutation_service,
    )
    register_worker_directory_tools(
        server=server,
        query_service=backend_services.directory_query_service,
        mutation_service=backend_services.directory_mutation_service,
    )
    register_git_result_tools(
        server=server,
        service=backend_services.result_commit_service,
    )
    register_worker_report_tools(
        server=server,
        service=backend_services.worker_report_service,
    )
    register_integration_tools(
        server=server,
        preview_service=backend_services.integration_preview_service,
        apply_service=backend_services.integration_apply_service,
    )
    register_workspace_tools(
        server=server,
        service=backend_services.workspace_project_service,
    )
    register_orchestration_tools(
        server=server,
        worker_service=backend_services.worker_execution_service,
    )
    register_facade_tools(
        server=server,
        service=backend_services.facade_service,
    )
    register_workflow_tools(
        server=server,
        service=backend_services.run_control_coordinator,
    )
    if backend_services.group_folder_service is not None:
        register_group_folder_tools(
            server=server,
            service=backend_services.group_folder_service,
        )
    register_main_review_resume_tool(
        server=server,
        coordinator=backend_services.main_review_resume_coordinator,
    )
    return server


def serve_configured(
    settings: RuntimeSettings,
    work_broker_gateway: WorkBrokerGateway | None = None,
) -> None:
    """Run the MCP server from validated runtime settings.

    Args:
        settings: Process settings resolved from environment variables.
        work_broker_gateway: Optional broker gateway override for tests or embedding.

    Returns:
        None.
    """
    run_configured_mcp(
        settings=settings,
        server_factory=_create_mcp_server_from_services,
        work_broker_gateway=work_broker_gateway,
    )


def main() -> None:
    """Start the configured orchestration MCP process.

    Args:
        None.

    Returns:
        None.
    """
    serve_configured(settings=RuntimeSettings())


if __name__ == "__main__":
    main()
