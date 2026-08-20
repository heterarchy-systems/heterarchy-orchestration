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
from chattocodex.orchestrator_backend.version_control.git_result_dto import (
    WorkerResultCommit,
)
from chattocodex.orchestrator_backend.version_control.worker_result_commit_service import (
    WorkerResultCommitService,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError
from chattocodex.shared.contracts.version_control.git_result_schemas import (
    RunResultsRequest,
    RunResultsSuccess,
    WorkerResultCommitRequest,
    WorkerResultCommitSuccess,
)


def register_git_result_tools(
    server: MCPServer[None],
    service: WorkerResultCommitService,
) -> None:
    """Register git result tools. Args: server, service. Returns: None."""

    @server.tool(
        name="worker_result_commit",
        description=(
            "Worker Chat: finalize all changes in its isolated worktree into one bounded "
            "Git result commit without running repository commit hooks."
        ),
        structured_output=True,
    )
    async def worker_result_commit(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        summary: str,
    ) -> WorkerResultCommitSuccess | ToolError:
        """Worker result commit. Args: run_id, worker_id, worker_handle, summary. Returns: WorkerResultCommitSuccess | ToolError."""
        operation = "worker_result_commit"
        try:
            request = WorkerResultCommitRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                summary=summary,
            )
            result = await asyncify(service.commit)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                summary=request.summary,
            )
            return _build_result_view(result=result)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="run_results",
        description="Main Chat: list immutable Worker result commits produced for one Run.",
        structured_output=True,
    )
    async def run_results(run_id: str) -> RunResultsSuccess | ToolError:
        """Run results. Args: run_id. Returns: RunResultsSuccess | ToolError."""
        operation = "run_results"
        try:
            request = RunResultsRequest(run_id=run_id)
            results = await asyncify(service.list_for_run)(run_id=request.run_id)
            return RunResultsSuccess(
                run_id=request.run_id,
                results=tuple(_build_result_view(result=result) for result in results),
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)


def _build_result_view(result: WorkerResultCommit) -> WorkerResultCommitSuccess:
    """Build result view. Args: result. Returns: WorkerResultCommitSuccess."""
    return WorkerResultCommitSuccess(
        run_id=result.run_id,
        worker_id=result.worker_id,
        base_commit=result.base_commit,
        commit_sha=result.commit_sha,
        changed_paths=result.changed_paths,
        summary=result.summary,
        created_at=result.created_at,
    )
