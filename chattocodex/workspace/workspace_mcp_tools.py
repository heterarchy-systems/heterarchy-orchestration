import asyncio

from mcp.server.mcpserver import MCPServer

from chattocodex.orchestration.orchestration_exceptions import OrchestrationError
from chattocodex.orchestration.orchestration_view_mapper import build_tool_error
from chattocodex.schemas.orchestration_schemas import ToolError
from chattocodex.schemas.project_schemas import ProjectListSuccess
from chattocodex.workspace.workspace_project_mapper import build_project_view
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


def register_workspace_tools(
    *,
    server: MCPServer[None],
    service: WorkspaceProjectService,
) -> None:
    @server.tool(
        name="project_list",
        description="Main Chat: list projects safely discoverable under the configured workspace root.",
        structured_output=True,
    )
    async def project_list() -> ProjectListSuccess | ToolError:
        try:
            projects = await asyncio.to_thread(service.list_projects)
            return ProjectListSuccess(
                projects=tuple(
                    build_project_view(project=project) for project in projects
                )
            )
        except OrchestrationError as error:
            return build_tool_error(error=error)
