import asyncio

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.git.git_result_dto import WorkerResultCommit
from chattocodex.git.worker_result_commit_service import WorkerResultCommitService
from chattocodex.orchestration.orchestration_exceptions import OrchestrationError
from chattocodex.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.schemas.git_result_schemas import (
    RunResultsRequest,
    RunResultsSuccess,
    WorkerResultCommitRequest,
    WorkerResultCommitSuccess,
)
from chattocodex.schemas.orchestration_schemas import ToolError


def register_git_result_tools(
    *,
    server: MCPServer[None],
    service: WorkerResultCommitService,
) -> None:
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
        operation = "worker_result_commit"
        try:
            request = WorkerResultCommitRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                summary=summary,
            )
            result = await asyncio.to_thread(
                service.commit,
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
        operation = "run_results"
        try:
            request = RunResultsRequest(run_id=run_id)
            results = await asyncio.to_thread(
                service.list_for_run, run_id=request.run_id
            )
            return RunResultsSuccess(
                run_id=request.run_id,
                results=tuple(_build_result_view(result=result) for result in results),
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)


def _build_result_view(*, result: WorkerResultCommit) -> WorkerResultCommitSuccess:
    return WorkerResultCommitSuccess(
        run_id=result.run_id,
        worker_id=result.worker_id,
        base_commit=result.base_commit,
        commit_sha=result.commit_sha,
        changed_paths=result.changed_paths,
        summary=result.summary,
        created_at=result.created_at,
    )
