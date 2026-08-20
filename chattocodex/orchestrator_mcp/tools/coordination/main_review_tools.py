from asyncer import asyncify
from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestrator_backend.main_runtime.application.main_review_resume_coordinator import (
    MainReviewResumeCoordinator,
)
from chattocodex.orchestrator_backend.main_runtime.application.main_review_view_mapper import (
    build_main_review_target_view,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.orchestrator_backend.run_control.run_control_view_mapper import (
    build_workflow_status,
)
from chattocodex.shared.contracts.main_runtime.main_runtime_schemas import (
    ResumeMainReviewRequest,
    ResumeMainReviewSuccess,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError


def register_main_review_resume_tool(
    server: MCPServer[None],
    coordinator: MainReviewResumeCoordinator,
) -> None:
    """Register main review resume tool. Args: server, coordinator. Returns: None."""

    @server.tool(
        name="resume_main_review",
        description=(
            "Main Chat resume entry point. Use only when the local Main Runtime Adapter wakes this "
            "Chat with an opaque main_target_id plus main_wake_revision. Pass both unchanged. "
            "Resolves the pending review server-side and returns a fresh workflow review capability. "
            "Keep all IDs and capability values internal."
        ),
        structured_output=True,
    )
    async def resume_main_review(
        main_target_id: str,
        main_wake_revision: int,
    ) -> ResumeMainReviewSuccess | ToolError:
        """Resume main review. Args: main_target_id, main_wake_revision. Returns: ResumeMainReviewSuccess | ToolError."""
        operation = "resume_main_review"
        try:
            request = ResumeMainReviewRequest(
                main_target_id=main_target_id,
                main_wake_revision=main_wake_revision,
            )
            resumed = await asyncify(coordinator.resume_review)(
                main_target_id=request.main_target_id,
                main_wake_revision=request.main_wake_revision,
            )
            workflow_status = build_workflow_status(snapshot=resumed.workflow)
            return ResumeMainReviewSuccess(
                workflow=workflow_status.workflow,
                current_run=workflow_status.current_run,
                needs_main_review=workflow_status.needs_main_review,
                main_target=build_main_review_target_view(state=resumed.target),
                workflow_handle=resumed.workflow_handle,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
