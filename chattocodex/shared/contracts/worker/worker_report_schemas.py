from datetime import datetime
from typing import Literal

from chattocodex.shared.contracts.orchestration_schemas import (
    Revision,
    RunId,
    TaskId,
    WorkerHandle,
    WorkerId,
)
from chattocodex.shared.contracts.schema_base import StrictSchemaModel
from chattocodex.shared.contracts.types.coordination.worker_report_types import (
    ReportContent,
    ReportReferences,
    ReportSummary,
    ReportTitle,
)


class WorkerReportBoundaryModel(StrictSchemaModel):
    pass


class WorkerReportRecord(WorkerReportBoundaryModel):
    schema_version: Literal[1] = 1
    run_id: RunId
    worker_id: WorkerId
    task_id: TaskId
    title: ReportTitle
    summary: ReportSummary
    content: ReportContent
    references: ReportReferences
    created_at: datetime


class WorkerReportSubmitRequest(WorkerReportBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    worker_handle: WorkerHandle
    task_id: TaskId
    expected_revision: Revision
    title: ReportTitle
    summary: ReportSummary
    content: ReportContent
    references: ReportReferences = ()


class WorkerReportView(WorkerReportBoundaryModel):
    run_id: RunId
    worker_id: WorkerId
    task_id: TaskId
    title: ReportTitle
    summary: ReportSummary
    content: ReportContent
    references: ReportReferences
    created_at: datetime


class WorkerReportSubmitSuccess(WorkerReportBoundaryModel):
    ok: Literal[True] = True
    report: WorkerReportView


class RunReportsRequest(WorkerReportBoundaryModel):
    run_id: RunId


class RunReportsSuccess(WorkerReportBoundaryModel):
    ok: Literal[True] = True
    run_id: RunId
    reports: tuple[WorkerReportView, ...]
