from asyncer import asyncify
from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_file_mutation_service import (
    WorkerFileMutationService,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError
from chattocodex.shared.contracts.repository.repository_write_schemas import (
    WorkerFileCreateRequest,
    WorkerFileMutationSuccess,
    WorkerFileReplaceRequest,
)


def register_repository_write_tools(
    server: MCPServer[None],
    service: WorkerFileMutationService,
) -> None:
    """Register repository write tools. Args: server, service. Returns: None."""

    @server.tool(
        name="worker_file_create",
        description=(
            "Worker Chat: create a UTF-8 text file only inside its prepared isolated "
            "Git worktree. Fails if the target already exists."
        ),
        structured_output=True,
    )
    async def worker_file_create(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        content: str,
    ) -> WorkerFileMutationSuccess | ToolError:
        """Worker file create. Args: run_id, worker_id, worker_handle, relative_path, content. Returns: WorkerFileMutationSuccess | ToolError."""
        operation = "worker_file_create"
        try:
            request = WorkerFileCreateRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                relative_path=relative_path,
                content=content,
            )
            result = await asyncify(service.create_file)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                relative_path=request.relative_path,
                content=request.content,
            )
            return WorkerFileMutationSuccess(
                run_id=result.run_id,
                worker_id=result.worker_id,
                relative_path=result.relative_path,
                sha256=result.sha256,
                size_bytes=result.size_bytes,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="worker_file_replace",
        description=(
            "Worker Chat: atomically replace a UTF-8 text file inside its prepared "
            "isolated Git worktree only when expected_sha256 matches current content."
        ),
        structured_output=True,
    )
    async def worker_file_replace(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        expected_sha256: str,
        content: str,
    ) -> WorkerFileMutationSuccess | ToolError:
        """Worker file replace. Args: run_id, worker_id, worker_handle, relative_path, expected_sha256, content. Returns: WorkerFileMutationSuccess | ToolError."""
        operation = "worker_file_replace"
        try:
            request = WorkerFileReplaceRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                relative_path=relative_path,
                expected_sha256=expected_sha256,
                content=content,
            )
            result = await asyncify(service.replace_file)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                relative_path=request.relative_path,
                expected_sha256=request.expected_sha256,
                content=request.content,
            )
            return WorkerFileMutationSuccess(
                run_id=result.run_id,
                worker_id=result.worker_id,
                relative_path=result.relative_path,
                sha256=result.sha256,
                size_bytes=result.size_bytes,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
