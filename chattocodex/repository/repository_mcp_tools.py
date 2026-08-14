import asyncio

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestration.orchestration_exceptions import OrchestrationError
from chattocodex.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.repository.project_read_service import ProjectReadService
from chattocodex.repository.repository_view_mapper import (
    build_project_file_read_success,
    build_project_text_search_success,
    build_project_tree_success,
)
from chattocodex.schemas.orchestration_schemas import ToolError
from chattocodex.schemas.repository_schemas import (
    ProjectFileReadRequest,
    ProjectFileReadSuccess,
    ProjectTextSearchRequest,
    ProjectTextSearchSuccess,
    ProjectTreeRequest,
    ProjectTreeSuccess,
)


def register_repository_read_tools(
    *,
    server: MCPServer[None],
    service: ProjectReadService,
) -> None:
    @server.tool(
        name="project_tree",
        description=(
            "Read-only: list a bounded tree inside the project permanently bound to a Run."
        ),
        structured_output=True,
    )
    async def project_tree(
        run_id: str,
        relative_path: str = "",
        max_depth: int = 2,
        max_entries: int = 200,
    ) -> ProjectTreeSuccess | ToolError:
        operation = "project_tree"
        try:
            request = ProjectTreeRequest(
                run_id=run_id,
                relative_path=relative_path,
                max_depth=max_depth,
                max_entries=max_entries,
            )
            snapshot = await asyncio.to_thread(
                service.tree,
                run_id=request.run_id,
                relative_path=request.relative_path,
                max_depth=request.max_depth,
                max_entries=request.max_entries,
            )
            return build_project_tree_success(snapshot=snapshot)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="project_file_read",
        description=(
            "Read-only: read at most 500 UTF-8 text lines from a file in the Run project."
        ),
        structured_output=True,
    )
    async def project_file_read(
        run_id: str,
        relative_path: str,
        start_line: int = 1,
        end_line: int = 200,
    ) -> ProjectFileReadSuccess | ToolError:
        operation = "project_file_read"
        try:
            request = ProjectFileReadRequest(
                run_id=run_id,
                relative_path=relative_path,
                start_line=start_line,
                end_line=end_line,
            )
            file_slice = await asyncio.to_thread(
                service.read_file,
                run_id=request.run_id,
                relative_path=request.relative_path,
                start_line=request.start_line,
                end_line=request.end_line,
            )
            return build_project_file_read_success(file_slice=file_slice)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="project_text_search",
        description=(
            "Read-only: case-insensitive literal text search across bounded UTF-8 files "
            "inside the Run project."
        ),
        structured_output=True,
    )
    async def project_text_search(
        run_id: str,
        query: str,
        max_results: int = 50,
    ) -> ProjectTextSearchSuccess | ToolError:
        operation = "project_text_search"
        try:
            request = ProjectTextSearchRequest(
                run_id=run_id,
                query=query,
                max_results=max_results,
            )
            result = await asyncio.to_thread(
                service.search_text,
                run_id=request.run_id,
                query=request.query,
                max_results=request.max_results,
            )
            return build_project_text_search_success(result=result)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
