from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from chattocodex.schemas.orchestration_schemas import (
    Revision,
    RunId,
    TaskId,
    WorkerHandle,
    WorkerId,
)

ReportTitle = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=200),
]
ReportSummary = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=4_000),
]
ReportContent = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100_000),
]
ReportReference = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=4_096),
]
ReportReferences = Annotated[tuple[ReportReference, ...], Field(max_length=32)]


class WorkerReportBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


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
