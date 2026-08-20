from asyncer import asyncify
from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestrator_backend.integration.integration_apply_service import (
    IntegrationApplyService,
)
from chattocodex.orchestrator_backend.integration.integration_preview_service import (
    IntegrationPreviewService,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.shared.contracts.integration_schemas import (
    IntegrationApplyRequest,
    IntegrationApplySuccess,
    IntegrationConflictView,
    IntegrationPreviewRequest,
    IntegrationPreviewSuccess,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError


def register_integration_tools(
    server: MCPServer[None],
    preview_service: IntegrationPreviewService,
    apply_service: IntegrationApplyService,
) -> None:
    """Register integration tools. Args: server, preview_service, apply_service. Returns: None."""

    @server.tool(
        name="integration_preview",
        description=(
            "Main Chat: preview overlapping changed paths across Worker result commits "
            "before any merge or apply operation."
        ),
        structured_output=True,
    )
    async def integration_preview(
        run_id: str,
    ) -> IntegrationPreviewSuccess | ToolError:
        """Integration preview. Args: run_id. Returns: IntegrationPreviewSuccess | ToolError."""
        operation = "integration_preview"
        try:
            request = IntegrationPreviewRequest(run_id=run_id)
            preview = await asyncify(preview_service.preview)(
                run_id=request.run_id,
            )
            return IntegrationPreviewSuccess(
                run_id=preview.run_id,
                base_commit=preview.base_commit,
                result_count=preview.result_count,
                ready_to_integrate=preview.ready_to_integrate,
                conflicts=tuple(
                    IntegrationConflictView(
                        relative_path=conflict.relative_path,
                        worker_ids=conflict.worker_ids,
                        commit_shas=conflict.commit_shas,
                    )
                    for conflict in preview.conflicts
                ),
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="integration_apply",
        description=(
            "Main Chat: integrate completed non-conflicting Worker result commits into "
            "the Run-bound project only when its clean HEAD still matches the Run base commit."
        ),
        structured_output=True,
    )
    async def integration_apply(
        run_id: str,
        run_handle: str,
    ) -> IntegrationApplySuccess | ToolError:
        """Integration apply. Args: run_id, run_handle. Returns: IntegrationApplySuccess | ToolError."""
        operation = "integration_apply"
        try:
            request = IntegrationApplyRequest(run_id=run_id, run_handle=run_handle)
            applied = await asyncify(apply_service.apply)(
                run_id=request.run_id,
                run_handle=request.run_handle,
            )
            return IntegrationApplySuccess(
                run_id=applied.run_id,
                base_commit=applied.base_commit,
                integration_commit=applied.integration_commit,
                result_count=applied.result_count,
                changed_paths=applied.changed_paths,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)
