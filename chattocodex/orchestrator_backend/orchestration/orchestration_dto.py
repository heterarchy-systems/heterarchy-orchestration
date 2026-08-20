from dataclasses import dataclass
from datetime import datetime

from chattocodex.shared.element_collect.orchestration_enums import (
    RunStatus,
    TaskStatus,
    WorkerStatus,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class RunState:
    schema_version: int
    run_id: str
    goal: str
    project_id: str
    project_name: str
    project_root: str
    run_handle_digest: str
    status: RunStatus
    revision: int
    created_at: datetime
    updated_at: datetime
    requested_parallelism: int = 1


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerState:
    schema_version: int
    run_id: str
    worker_id: str
    worker_handle_digest: str
    status: WorkerStatus
    role: str | None
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskState:
    schema_version: int
    run_id: str
    task_id: str
    assigned_worker_id: str
    objective: str
    status: TaskStatus
    revision: int
    progress_summary: str | None
    result: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class CreatedRun:
    state: RunState
    run_handle: str


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerContextSnapshot:
    run: RunState
    worker: WorkerState
    tasks: tuple[TaskState, ...]
