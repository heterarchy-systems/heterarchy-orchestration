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
from chattocodex.orchestrator_backend.version_control.worker_git_worktree_service import (
    WorkerGitWorktreeService,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError
from chattocodex.shared.contracts.version_control.git_worktree_schemas import (
    WorkerWorkspacePrepareRequest,
    WorkerWorkspacePrepareSuccess,
)


def register_git_worktree_tools(
    server: MCPServer[None],
    service: WorkerGitWorktreeService,
) -> None:
    """Register git worktree tools. Args: server, service. Returns: None."""

    @server.tool(
        name="worker_workspace_prepare",
        description=(
            "Worker Chat: lazily prepare an isolated detached Git worktree from the "
            "Run base commit before any write operation."
        ),
        structured_output=True,
    )
    async def worker_workspace_prepare(
        run_id: str,
        worker_id: str,
        worker_handle: str,
    ) -> WorkerWorkspacePrepareSuccess | ToolError:
        """Worker workspace prepare. Args: run_id, worker_id, worker_handle. Returns: WorkerWorkspacePrepareSuccess | ToolError."""
        operation = "worker_workspace_prepare"
        try:
            request = WorkerWorkspacePrepareRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
            )
            workspace = await asyncify(service.prepare)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
            )
            return WorkerWorkspacePrepareSuccess(
                run_id=workspace.run_id,
                worker_id=workspace.worker_id,
                worktree_root=workspace.worktree_root,
                base_commit=workspace.base_commit,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
