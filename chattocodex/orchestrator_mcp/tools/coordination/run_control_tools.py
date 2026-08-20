from asyncer import asyncify
from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestrator_backend.broker.broker_view_mapper import (
    build_broker_run_status_success,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.orchestrator_backend.run_control.run_control_coordinator import (
    RunControlCoordinator,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowDecision,
    WorkflowTaskSpec,
)
from chattocodex.orchestrator_backend.run_control.run_control_view_mapper import (
    build_review_success,
    build_workflow_status,
    build_workflow_view,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError
from chattocodex.shared.contracts.run_control_schemas import (
    OrchestrateWorkRequest,
    OrchestrateWorkSuccess,
    ReviewWorkflowRequest,
    ReviewWorkflowSuccess,
    WorkflowStatusRequest,
    WorkflowStatusSuccess,
    WorkflowTaskInput,
)


def _task_specs(tasks: tuple[WorkflowTaskInput, ...]) -> tuple[WorkflowTaskSpec, ...]:
    """Task specs. Args: tasks. Returns: tuple[WorkflowTaskSpec, ...]."""
    return tuple(
        WorkflowTaskSpec(
            key=task.key,
            objective=task.objective,
            role=task.role,
            depends_on=task.depends_on,
        )
        for task in tasks
    )


def register_workflow_tools(
    server: MCPServer[None], service: RunControlCoordinator
) -> None:
    """Register workflow tools. Args: server, service. Returns: None."""

    @server.tool(
        name="orchestrate_work",
        description=(
            "Main Chat workflow entry point. Use when the user wants Worker tasks followed by Main "
            "review, approval, bounded rework, or failure handling. Build the Worker task DAG once; "
            "the Workflow Coordinator owns stage completion and Main Review Gate state. Keep all "
            "workflow IDs and capabilities internal. This includes review requests such as "
            "'review this', 'check this', '검토해줘', '확인해줘', or '두 명이 검토해줘' when the "
            "Main Chat is expected to receive and assess the Worker results."
        ),
        structured_output=True,
    )
    async def orchestrate_work(
        goal: str,
        tasks: list[WorkflowTaskInput],
        review_instructions: str = (
            "Inspect all worker results, reports, diffs, and tests. Approve only when the goal is "
            "met; otherwise request bounded rework or fail the workflow."
        ),
        max_rework_rounds: int = 2,
        requested_parallelism: int | None = None,
        project_id: str | None = None,
    ) -> OrchestrateWorkSuccess | ToolError:
        """Orchestrate work. Args: goal, tasks, review_instructions, max_rework_rounds, requested_parallelism, project_id. Returns: OrchestrateWorkSuccess | ToolError."""
        operation = "orchestrate_work"
        try:
            request = OrchestrateWorkRequest(
                goal=goal,
                tasks=tuple(tasks),
                review_instructions=review_instructions,
                max_rework_rounds=max_rework_rounds,
                requested_parallelism=requested_parallelism,
                project_id=project_id,
            )
            created = await asyncify(service.start_workflow)(
                goal=request.goal,
                tasks=_task_specs(request.tasks),
                review_instructions=request.review_instructions,
                max_rework_rounds=request.max_rework_rounds,
                requested_parallelism=request.requested_parallelism,
                project_id=request.project_id,
            )
            snapshot = await asyncify(service.get_status)(
                workflow_id=created.state.workflow_id,
                workflow_handle=created.workflow_handle,
            )
            return OrchestrateWorkSuccess(
                workflow=build_workflow_view(state=snapshot.workflow),
                workflow_handle=created.workflow_handle,
                current_run=build_broker_run_status_success(
                    snapshot=snapshot.current_run
                ),
                needs_main_review=snapshot.needs_main_review,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="workflow_status",
        description=(
            "Main Chat workflow status. Refreshes durable stage state and reports whether the "
            "workflow is waiting at the Main Review Gate. Keep workflow_id/workflow_handle internal."
        ),
        structured_output=True,
    )
    async def workflow_status(
        workflow_id: str,
        workflow_handle: str,
    ) -> WorkflowStatusSuccess | ToolError:
        """Workflow status. Args: workflow_id, workflow_handle. Returns: WorkflowStatusSuccess | ToolError."""
        operation = "workflow_status"
        try:
            request = WorkflowStatusRequest(
                workflow_id=workflow_id,
                workflow_handle=workflow_handle,
            )
            snapshot = await asyncify(service.get_status)(
                workflow_id=request.workflow_id,
                workflow_handle=request.workflow_handle,
            )
            return build_workflow_status(snapshot=snapshot)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="review_workflow",
        description=(
            "Main Chat Review Gate action. Call only when workflow_status says needs_main_review=true. "
            "Inspect results/reports/diffs/tests first, then choose approve, rework, or fail. Rework "
            "must include a new bounded corrective Worker task DAG; it starts a new execution round."
        ),
        structured_output=True,
    )
    async def review_workflow(
        workflow_id: str,
        workflow_handle: str,
        decision: WorkflowDecision,
        review_summary: str,
        rework_tasks: list[WorkflowTaskInput] | None = None,
    ) -> ReviewWorkflowSuccess | ToolError:
        """Review workflow. Args: workflow_id, workflow_handle, decision, review_summary, rework_tasks. Returns: ReviewWorkflowSuccess | ToolError."""
        operation = "review_workflow"
        try:
            request = ReviewWorkflowRequest(
                workflow_id=workflow_id,
                workflow_handle=workflow_handle,
                decision=decision,
                review_summary=review_summary,
                rework_tasks=tuple(rework_tasks or ()),
            )
            result = await asyncify(service.review_workflow)(
                workflow_id=request.workflow_id,
                workflow_handle=request.workflow_handle,
                decision=WorkflowDecision(request.decision),
                review_summary=request.review_summary,
                rework_tasks=_task_specs(request.rework_tasks),
            )
            return build_review_success(result=result)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
