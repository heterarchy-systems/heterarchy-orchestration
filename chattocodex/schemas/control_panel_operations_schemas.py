from datetime import datetime
from typing import Literal

from pydantic import Field

from chattocodex.orchestration.orchestration_dto import RunStatus
from chattocodex.schemas.broker_management_schemas import BrokerClusterOperationsView
from chattocodex.schemas.control_panel_schemas import ControlPanelBoundaryModel
from chattocodex.wake.wake_dto import WakeTargetStatus
from chattocodex.wake.worker_browser_runtime_dto import WorkerBrowserRuntimeMode


class WorkerBrowserOperationsView(ControlPanelBoundaryModel):
    running: bool
    mode: WorkerBrowserRuntimeMode | None
    profile_initialized: bool


class DatabaseOperationsView(ControlPanelBoundaryModel):
    enabled: bool
    state: Literal["disabled", "ready", "driver_missing", "unavailable"]
    projected_at: datetime | None
    message: str | None


class ProviderBindingOperationsView(ControlPanelBoundaryModel):
    provider: str | None
    project_ref: str | None
    project_label: str | None
    project_path: str | None
    connector_ready: bool
    revision: int = Field(ge=0)


class ConsumerWorkerOperationsView(ControlPanelBoundaryModel):
    wake_target_id: str
    status: WakeTargetStatus
    wake_revision: int = Field(ge=0)
    updated_at: datetime


class ConsumerGroupOperationsView(ControlPanelBoundaryModel):
    topology: Literal["project_default"] = "project_default"
    initialized: bool
    generation: int = Field(ge=0)
    revision: int = Field(ge=0)
    workers: tuple[ConsumerWorkerOperationsView, ...]
    idle: int = Field(ge=0)
    waking: int = Field(ge=0)
    claiming: int = Field(ge=0)
    busy: int = Field(ge=0)


class ProjectOperationsView(ControlPanelBoundaryModel):
    project_id: str
    name: str
    root: str
    binding: ProviderBindingOperationsView | None
    consumer_group: ConsumerGroupOperationsView
    desired_capacity: int = Field(ge=0)
    spawn_needed: int = Field(ge=0)
    provisioning_pending: int = Field(ge=0)


class RecentRunOperationsView(ControlPanelBoundaryModel):
    run_id: str
    project_id: str
    project_name: str
    goal: str
    status: RunStatus
    requested_parallelism: int = Field(ge=1)
    updated_at: datetime


class OperationsTotalsView(ControlPanelBoundaryModel):
    project_count: int = Field(ge=0)
    consumer_group_count: int = Field(ge=0)
    worker_count: int = Field(ge=0)
    idle: int = Field(ge=0)
    waking: int = Field(ge=0)
    claiming: int = Field(ge=0)
    busy: int = Field(ge=0)
    provisioning_pending: int = Field(ge=0)
    active_runs: int = Field(ge=0)


class ControlPanelOperationsResponse(ControlPanelBoundaryModel):
    ok: Literal[True] = True
    browser: WorkerBrowserOperationsView
    database: DatabaseOperationsView
    broker: BrokerClusterOperationsView | None = None
    totals: OperationsTotalsView
    projects: tuple[ProjectOperationsView, ...]
    recent_runs: tuple[RecentRunOperationsView, ...]
