import asyncio

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestration.orchestration_exceptions import OrchestrationError
from chattocodex.orchestration.orchestration_view_mapper import (
    build_tool_error,
    build_validation_error,
)
from chattocodex.report.worker_report_dto import WorkerReport
from chattocodex.report.worker_report_service import WorkerReportService
from chattocodex.schemas.orchestration_schemas import ToolError
from chattocodex.schemas.worker_report_schemas import (
    RunReportsRequest,
    RunReportsSuccess,
    WorkerReportSubmitRequest,
    WorkerReportSubmitSuccess,
    WorkerReportView,
)


def register_worker_report_tools(
    *,
    server: MCPServer[None],
    service: WorkerReportService,
) -> None:
    @server.tool(
        name="worker_report_submit",
        description=(
            "Worker Chat: submit one immutable non-code report for an assigned Task and "
            "complete that Task without preparing a Git worktree. Use for research, review, "
            "analysis, or other evidence/results that should be handed back to Main Chat."
        ),
        structured_output=True,
    )
    async def worker_report_submit(
        run_id: str,
        worker_id: str,
        worker_handle: str,
        task_id: str,
        expected_revision: int,
        title: str,
        summary: str,
        content: str,
        references: tuple[str, ...] = (),
    ) -> WorkerReportSubmitSuccess | ToolError:
        operation = "worker_report_submit"
        try:
            request = WorkerReportSubmitRequest(
                run_id=run_id,
                worker_id=worker_id,
                worker_handle=worker_handle,
                task_id=task_id,
                expected_revision=expected_revision,
                title=title,
                summary=summary,
                content=content,
                references=references,
            )
            report = await asyncio.to_thread(
                service.submit,
                run_id=request.run_id,
                worker_id=request.worker_id,
                worker_handle=request.worker_handle,
                task_id=request.task_id,
                expected_revision=request.expected_revision,
                title=request.title,
                summary=request.summary,
                content=request.content,
                references=request.references,
            )
            return WorkerReportSubmitSuccess(report=_build_report_view(report=report))
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="run_reports",
        description=(
            "Main Chat: read immutable non-code Worker reports submitted for one Run, "
            "including research/review content and references."
        ),
        structured_output=True,
    )
    async def run_reports(run_id: str) -> RunReportsSuccess | ToolError:
        operation = "run_reports"
        try:
            request = RunReportsRequest(run_id=run_id)
            reports = await asyncio.to_thread(
                service.list_for_run,
                run_id=request.run_id,
            )
            return RunReportsSuccess(
                run_id=request.run_id,
                reports=tuple(_build_report_view(report=report) for report in reports),
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)


def _build_report_view(*, report: WorkerReport) -> WorkerReportView:
    return WorkerReportView(
        run_id=report.run_id,
        worker_id=report.worker_id,
        task_id=report.task_id,
        title=report.title,
        summary=report.summary,
        content=report.content,
        references=report.references,
        created_at=report.created_at,
    )
