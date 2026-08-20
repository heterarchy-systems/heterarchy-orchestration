from datetime import datetime
from typing import Annotated, Literal

from chattocodex.shared.contracts.broker.broker_management_schemas import (
    BrokerOperationsView,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelBoundaryModel,
)
from chattocodex.shared.contracts.schema_base import described_field
from chattocodex.shared.element_collect.control_panel_enums import (
    DatabaseOperationsState,
    ExecutionTaskPhase,
    RunContinuationPhase,
    WorkerRuntimeTopology,
)
from chattocodex.shared.element_collect.orchestration_enums import RunStatus
from chattocodex.shared.element_collect.run_control_enums import WorkflowStatus
from chattocodex.shared.element_collect.wake_enums import (
    WakeTargetStatus,
    WorkerBrowserRuntimeMode,
)


class WorkerBrowserOperationsView(ControlPanelBoundaryModel):
    running: bool
    mode: WorkerBrowserRuntimeMode | None
    profile_initialized: bool


class DatabaseOperationsView(ControlPanelBoundaryModel):
    enabled: bool
    state: DatabaseOperationsState
    projected_at: datetime | None
    message: str | None


class ProviderBindingOperationsView(ControlPanelBoundaryModel):
    provider: str | None
    project_ref: str | None
    project_label: str | None
    project_path: str | None
    connector_ready: bool
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this provider binding operations view.",
            ge=0,
        ),
    ]


class ConsumerWorkerOperationsView(ControlPanelBoundaryModel):
    wake_target_id: str
    status: WakeTargetStatus
    wake_revision: Annotated[
        int,
        described_field(
            description="Wake revision for this consumer worker operations view.", ge=0
        ),
    ]
    updated_at: datetime


class WorkerRuntimeGroupOperationsView(ControlPanelBoundaryModel):
    topology: WorkerRuntimeTopology = WorkerRuntimeTopology.PROJECT_DEFAULT
    initialized: bool
    topology_generation: Annotated[
        int,
        described_field(
            description="Topology generation for this worker runtime group operations view.",
            ge=0,
        ),
    ]
    revision: Annotated[
        int,
        described_field(
            description="Monotonic revision for this worker runtime group operations view.",
            ge=0,
        ),
    ]
    workers: tuple[ConsumerWorkerOperationsView, ...]
    idle: Annotated[
        int,
        described_field(
            description="Idle for this worker runtime group operations view.", ge=0
        ),
    ]
    waking: Annotated[
        int,
        described_field(
            description="Waking for this worker runtime group operations view.", ge=0
        ),
    ]
    claiming: Annotated[
        int,
        described_field(
            description="Claiming for this worker runtime group operations view.", ge=0
        ),
    ]
    busy: Annotated[
        int,
        described_field(
            description="Busy for this worker runtime group operations view.", ge=0
        ),
    ]


class ProjectOperationsView(ControlPanelBoundaryModel):
    project_id: str
    name: str
    root: str
    binding: ProviderBindingOperationsView | None
    worker_runtime: WorkerRuntimeGroupOperationsView
    desired_capacity: Annotated[
        int,
        described_field(
            description="Desired capacity for this project operations view.", ge=0
        ),
    ]
    spawn_needed: Annotated[
        int,
        described_field(
            description="Spawn needed for this project operations view.", ge=0
        ),
    ]
    provisioning_pending: Annotated[
        int,
        described_field(
            description="Provisioning pending for this project operations view.", ge=0
        ),
    ]


class RecentRunOperationsView(ControlPanelBoundaryModel):
    run_id: str
    project_id: str
    project_name: str
    goal: str
    status: RunStatus
    requested_parallelism: Annotated[
        int,
        described_field(
            description="Requested parallelism for this recent run operations view.",
            ge=1,
        ),
    ]
    updated_at: datetime


class ExecutionLeaseOperationsView(ControlPanelBoundaryModel):
    broker_consumer_id: str
    term: Annotated[
        int,
        described_field(
            description="Term for this execution lease operations view.", ge=1
        ),
    ]
    generation: Annotated[
        int,
        described_field(
            description="Generation for this execution lease operations view.", ge=0
        ),
    ]
    lease_id: str
    lease_epoch: Annotated[
        int,
        described_field(
            description="Lease epoch for this execution lease operations view.", ge=1
        ),
    ]
    lease_expires_at_ms: Annotated[
        int,
        described_field(
            description="Lease expires at duration in milliseconds for this execution lease operations view.",
            ge=0,
        ),
    ]


class ExecutionTaskOperationsView(ControlPanelBoundaryModel):
    broker_task_id: str
    objective: str
    role: str | None
    depends_on: tuple[str, ...]
    phase: ExecutionTaskPhase
    external_published: bool
    worker_id: str | None
    task_id: str | None
    progress_summary: str | None
    result: str | None
    lease: ExecutionLeaseOperationsView | None
    updated_at: datetime


class RunExecutionOperationsView(ControlPanelBoundaryModel):
    run_id: str
    project_id: str
    project_name: str
    goal: str
    status: RunStatus
    continuation_phase: RunContinuationPhase
    next_step: str
    workflow_id: str | None
    workflow_status: WorkflowStatus | None
    requested_parallelism: Annotated[
        int,
        described_field(
            description="Requested parallelism for this run execution operations view.",
            ge=1,
        ),
    ]
    total_tasks: Annotated[
        int,
        described_field(
            description="Total tasks for this run execution operations view.", ge=0
        ),
    ]
    blocked: Annotated[
        int,
        described_field(
            description="Blocked for this run execution operations view.", ge=0
        ),
    ]
    ready: Annotated[
        int,
        described_field(
            description="Ready for this run execution operations view.", ge=0
        ),
    ]
    running: Annotated[
        int,
        described_field(
            description="Running for this run execution operations view.", ge=0
        ),
    ]
    completed: Annotated[
        int,
        described_field(
            description="Completed for this run execution operations view.", ge=0
        ),
    ]
    failed: Annotated[
        int,
        described_field(
            description="Failed for this run execution operations view.", ge=0
        ),
    ]
    tasks: tuple[ExecutionTaskOperationsView, ...]
    updated_at: datetime


class OperationsTotalsView(ControlPanelBoundaryModel):
    project_count: Annotated[
        int,
        described_field(
            description="Number of project entries represented by this operations totals view.",
            ge=0,
        ),
    ]
    runtime_group_count: Annotated[
        int,
        described_field(
            description="Number of runtime group entries represented by this operations totals view.",
            ge=0,
        ),
    ]
    worker_count: Annotated[
        int,
        described_field(
            description="Number of worker entries represented by this operations totals view.",
            ge=0,
        ),
    ]
    idle: Annotated[
        int, described_field(description="Idle for this operations totals view.", ge=0)
    ]
    waking: Annotated[
        int,
        described_field(description="Waking for this operations totals view.", ge=0),
    ]
    claiming: Annotated[
        int,
        described_field(description="Claiming for this operations totals view.", ge=0),
    ]
    busy: Annotated[
        int, described_field(description="Busy for this operations totals view.", ge=0)
    ]
    provisioning_pending: Annotated[
        int,
        described_field(
            description="Provisioning pending for this operations totals view.", ge=0
        ),
    ]
    active_runs: Annotated[
        int,
        described_field(
            description="Active runs for this operations totals view.", ge=0
        ),
    ]


class ControlPanelOperationsResponse(ControlPanelBoundaryModel):
    ok: Literal[True] = True
    browser: WorkerBrowserOperationsView
    database: DatabaseOperationsView
    broker: BrokerOperationsView | None = None
    totals: OperationsTotalsView
    projects: tuple[ProjectOperationsView, ...]
    executions: tuple[RunExecutionOperationsView, ...]
    recent_runs: tuple[RecentRunOperationsView, ...]
