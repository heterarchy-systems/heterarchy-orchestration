from asyncer import asyncify
from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestrator_backend.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_task_view,
    build_tool_error,
    build_validation_error,
    build_worker_context_success,
)
from chattocodex.orchestrator_backend.orchestration.worker_execution_service import (
    WorkerExecutionService,
)
from chattocodex.shared.contracts.orchestration_schemas import (
    TaskUpdateRequest,
    TaskUpdateSuccess,
    ToolError,
    WorkerContextRequest,
    WorkerContextSuccess,
)


def register_orchestration_tools(
    server: MCPServer[None],
    worker_service: WorkerExecutionService,
) -> None:
    """Register orchestration tools. Args: server, worker_service. Returns: None."""

    @server.tool(
        name="worker_context",
        description=(
            "Generic Worker Chat: read the current temporary Worker identity and Task "
            "returned by work. Keep all identity values internal to the Worker Chat."
        ),
        structured_output=True,
    )
    async def worker_context(
        run_id: str,
        worker_id: str,
        worker_handle: str,
    ) -> WorkerContextSuccess | ToolError:
        """Worker context. Args: run_id, worker_id, worker_handle. Returns: WorkerContextSuccess | ToolError."""
        operation = "worker_context"
        try:
            request = WorkerContextRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
            )
            snapshot = await asyncify(worker_service.get_worker_context)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
            )
            return build_worker_context_success(snapshot=snapshot)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="task_update",
        description=(
            "Generic Worker Chat: update progress/status/result for the temporary Task "
            "returned by work using CAS revision."
        ),
        structured_output=True,
    )
    async def task_update(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        task_id: str,
        expected_revision: int,
        status: TaskStatus,
        progress_summary: str,
        result: str | None = None,
    ) -> TaskUpdateSuccess | ToolError:
        """Task update. Args: run_id, worker_id, worker_handle, task_id, expected_revision, status, progress_summary, result. Returns: TaskUpdateSuccess | ToolError."""
        operation = "task_update"
        try:
            request = TaskUpdateRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                task_id=task_id,
                expected_revision=expected_revision,
                status=status,
                progress_summary=progress_summary,
                result=result,
            )
            task = await asyncify(worker_service.update_task)(
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                task_id=request.task_id,
                expected_revision=request.expected_revision,
                status=TaskStatus(request.status),
                progress_summary=request.progress_summary,
                result=request.result,
            )
            return TaskUpdateSuccess(task=build_task_view(state=task))
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
