from asyncer import asyncify
from mcp.server.mcpserver import MCPServer

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_tool_error,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_mapper import (
    build_project_view,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError
from chattocodex.shared.contracts.project.project_schemas import ProjectListSuccess


def register_workspace_tools(
    server: MCPServer[None],
    service: WorkspaceProjectService,
) -> None:
    """Register workspace tools. Args: server, service. Returns: None."""

    @server.tool(
        name="project_list",
        description="Main Chat: list projects safely discoverable under the configured workspace root.",
        structured_output=True,
    )
    async def project_list() -> ProjectListSuccess | ToolError:
        """Project list. Args: None. Returns: ProjectListSuccess | ToolError."""
        try:
            projects = await asyncify(service.list_projects)()
            return ProjectListSuccess(
                projects=tuple(
                    build_project_view(project=project) for project in projects
                )
            )
        except OrchestrationError as error:
            return build_tool_error(error=error)
