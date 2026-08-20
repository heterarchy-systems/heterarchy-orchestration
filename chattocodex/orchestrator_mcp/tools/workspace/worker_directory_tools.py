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
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_mutation_service import (
    WorkerDirectoryMutationService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_query_service import (
    WorkerDirectoryQueryService,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError
from chattocodex.shared.contracts.repository.worker_workspace_schemas import (
    WorkerDirectoryCreateRequest,
    WorkerDirectoryDeleteRequest,
    WorkerDirectoryEntryView,
    WorkerDirectoryListRequest,
    WorkerDirectoryListSuccess,
    WorkerDirectoryMoveRequest,
    WorkerDirectoryMutationSuccess,
    WorkerPathStatRequest,
    WorkerPathStatSuccess,
)


def register_worker_directory_tools(
    server: MCPServer[None],
    query_service: WorkerDirectoryQueryService,
    mutation_service: WorkerDirectoryMutationService,
) -> None:
    """Register filesystem directory tools for isolated Worker worktrees.

    Args:
        server: MCP server receiving the Worker filesystem tools.
        query_service: Read-only Worker directory query service.
        mutation_service: Mutable Worker directory application service.

    Returns:
        None.
    """

    @server.tool(
        name="worker_directory_list",
        description=(
            "Worker Chat: list one directory inside the prepared isolated worktree. "
            "Git metadata is never exposed and symlink/path traversal is rejected."
        ),
        structured_output=True,
    )
    async def worker_directory_list(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str = ".",
    ) -> WorkerDirectoryListSuccess | ToolError:
        """List one authorized Worker worktree directory.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is inspected.
            worker_handle: Worker capability returned by the assignment flow.
            relative_path: Worktree-relative directory path.

        Returns:
            Structured directory listing or structured tool error.
        """
        operation = "worker_directory_list"
        try:
            request = WorkerDirectoryListRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                relative_path=relative_path,
            )
            listing = await asyncify(query_service.list_directory)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                relative_path=request.relative_path,
            )
            return WorkerDirectoryListSuccess(
                run_id=listing.run_id,
                worker_id=listing.worker_id,
                relative_path=listing.relative_path,
                entries=tuple(
                    WorkerDirectoryEntryView(
                        name=entry.name,
                        relative_path=entry.relative_path,
                        kind=entry.kind,
                        size_bytes=entry.size_bytes,
                    )
                    for entry in listing.entries
                ),
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="worker_path_stat",
        description=(
            "Worker Chat: inspect one file or directory path inside the isolated worktree "
            "without following symlink escapes."
        ),
        structured_output=True,
    )
    async def worker_path_stat(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
    ) -> WorkerPathStatSuccess | ToolError:
        """Inspect one authorized Worker worktree path.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is inspected.
            worker_handle: Worker capability returned by the assignment flow.
            relative_path: Worktree-relative path to inspect.

        Returns:
            Structured path metadata or structured tool error.
        """
        operation = "worker_path_stat"
        try:
            request = WorkerPathStatRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                relative_path=relative_path,
            )
            info = await asyncify(query_service.stat_path)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                relative_path=request.relative_path,
            )
            return WorkerPathStatSuccess(
                run_id=info.run_id,
                worker_id=info.worker_id,
                relative_path=info.relative_path,
                kind=info.kind,
                size_bytes=info.size_bytes,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="worker_directory_create",
        description=(
            "Worker Chat: create a directory inside the prepared isolated worktree. "
            "Set parents=true only when creating missing parent directories is intended."
        ),
        structured_output=True,
    )
    async def worker_directory_create(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        parents: bool = False,
    ) -> WorkerDirectoryMutationSuccess | ToolError:
        """Create one authorized Worker worktree directory.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Worker capability returned by the assignment flow.
            relative_path: Worktree-relative directory path to create.
            parents: Whether missing parent directories may be created.

        Returns:
            Structured mutation result or structured tool error.
        """
        operation = "worker_directory_create"
        try:
            request = WorkerDirectoryCreateRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                relative_path=relative_path,
                parents=parents,
            )
            result = await asyncify(mutation_service.create_directory)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                relative_path=request.relative_path,
                parents=request.parents,
            )
            return WorkerDirectoryMutationSuccess(
                run_id=result.run_id,
                worker_id=result.worker_id,
                relative_path=result.relative_path,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="worker_directory_move",
        description=(
            "Worker Chat: move or rename one directory inside the isolated worktree. "
            "The destination must not exist and cannot be inside the source directory."
        ),
        structured_output=True,
    )
    async def worker_directory_move(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        source_relative_path: str,
        destination_relative_path: str,
    ) -> WorkerDirectoryMutationSuccess | ToolError:
        """Move or rename one authorized Worker worktree directory.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Worker capability returned by the assignment flow.
            source_relative_path: Existing source directory path.
            destination_relative_path: New destination directory path.

        Returns:
            Structured mutation result or structured tool error.
        """
        operation = "worker_directory_move"
        try:
            request = WorkerDirectoryMoveRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                source_relative_path=source_relative_path,
                destination_relative_path=destination_relative_path,
            )
            result = await asyncify(mutation_service.move_directory)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                source_relative_path=request.source_relative_path,
                destination_relative_path=request.destination_relative_path,
            )
            return WorkerDirectoryMutationSuccess(
                run_id=result.run_id,
                worker_id=result.worker_id,
                relative_path=result.relative_path,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="worker_directory_delete",
        description=(
            "Worker Chat: delete a directory inside the isolated worktree. Non-empty "
            "directories require explicit recursive=true and the worktree root cannot be deleted."
        ),
        structured_output=True,
    )
    async def worker_directory_delete(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        relative_path: str,
        recursive: bool = False,
    ) -> WorkerDirectoryMutationSuccess | ToolError:
        """Delete one authorized Worker worktree directory.

        Args:
            run_id: Run that owns the Worker.
            worker_id: Worker whose isolated worktree is mutated.
            worker_handle: Worker capability returned by the assignment flow.
            relative_path: Worktree-relative directory path to delete.
            recursive: Whether a non-empty subtree may be removed.

        Returns:
            Structured mutation result or structured tool error.
        """
        operation = "worker_directory_delete"
        try:
            request = WorkerDirectoryDeleteRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                relative_path=relative_path,
                recursive=recursive,
            )
            result = await asyncify(mutation_service.delete_directory)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                relative_path=request.relative_path,
                recursive=request.recursive,
            )
            return WorkerDirectoryMutationSuccess(
                run_id=result.run_id,
                worker_id=result.worker_id,
                relative_path=result.relative_path,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
