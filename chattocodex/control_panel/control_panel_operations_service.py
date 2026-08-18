from collections import Counter
from pathlib import Path

from agent_broker import AgentBrokerError

from chattocodex.broker.broker_management_dto import BrokerManagementSnapshot
from chattocodex.broker.broker_management_service import BrokerManagementService
from chattocodex.broker_client.broker_operations_gateway import BrokerOperationsGateway
from chattocodex.orchestration.orchestration_dto import RunStatus
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.orchestration_state_repository import RunStateRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.persistence.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.persistence.worker_runtime_group_state_store import (
    WorkerRuntimeGroupStateStore,
)
from chattocodex.project_runtime.project_binding_dto import ProjectBindingState
from chattocodex.provisioning.provisioning_state_sync import PENDING_REQUEST_STATUSES
from chattocodex.schemas.broker_management_schemas import (
    BrokerConsumerGroupOperationsView,
    BrokerNamespaceOperationsView,
    BrokerOperationsView,
)
from chattocodex.schemas.control_panel_operations_schemas import (
    ConsumerWorkerOperationsView,
    ControlPanelOperationsResponse,
    DatabaseOperationsView,
    OperationsTotalsView,
    ProjectOperationsView,
    ProviderBindingOperationsView,
    RecentRunOperationsView,
    WorkerBrowserOperationsView,
    WorkerRuntimeGroupOperationsView,
)
from chattocodex.wake.wake_dto import WakeTargetStatus
from chattocodex.wake.worker_browser_runtime_dto import WorkerBrowserRuntimeMode
from chattocodex.worker_runtime.worker_runtime_group_state import (
    WorkerRuntimeGroupState,
)
from chattocodex.workspace.workspace_project_dto import WorkspaceProject

_RECENT_RUN_LIMIT = 8


class ControlPanelOperationsService:
    """Build one read-only operations snapshot from durable runtime state."""

    def __init__(
        self,
        *,
        execution_root: Path,
        broker_operations: BrokerOperationsGateway | None = None,
    ) -> None:
        self._group_store = WorkerRuntimeGroupStateStore(state_root=execution_root)
        self._wake_targets = WakeTargetRepository(state_root=execution_root)
        self._provisioning = WorkerProvisioningRepository(state_root=execution_root)
        self._runs = RunStateRepository(state_root=execution_root)
        self._broker_management = (
            BrokerManagementService(
                broker_task_repository=BrokerTaskRepository(state_root=execution_root),
                slot_repository=WorkerSlotRepository(state_root=execution_root),
                run_repository=self._runs,
                broker_operations=broker_operations,
            )
            if broker_operations is not None
            else None
        )

    def snapshot(
        self,
        *,
        projects: tuple[WorkspaceProject, ...],
        bindings: tuple[ProjectBindingState, ...],
        browser_running: bool,
        browser_mode: WorkerBrowserRuntimeMode | None,
        browser_profile_initialized: bool,
    ) -> ControlPanelOperationsResponse:
        binding_by_project = {item.project_id: item for item in bindings}
        targets_by_id = {
            target.wake_target_id: target for target in self._wake_targets.list_all()
        }
        demands = {
            demand.project_id: demand for demand in self._provisioning.list_demands()
        }
        requests = self._provisioning.list_requests()
        pending_by_project = Counter(
            request.project_id
            for request in requests
            if request.status in PENDING_REQUEST_STATUSES
        )

        project_views: list[ProjectOperationsView] = []
        status_totals: Counter[WakeTargetStatus] = Counter()
        runtime_group_count = 0
        worker_count = 0
        for project in sorted(
            projects, key=lambda item: (item.name.lower(), item.project_id)
        ):
            group = self._read_group(project_id=project.project_id)
            member_ids = group.wake_target_ids if group is not None else ()
            workers = tuple(
                ConsumerWorkerOperationsView(
                    wake_target_id=target.wake_target_id,
                    status=target.status,
                    wake_revision=target.wake_revision,
                    updated_at=target.updated_at,
                )
                for member_id in member_ids
                if (target := targets_by_id.get(member_id)) is not None
            )
            status_counts = Counter(worker.status for worker in workers)
            if group is not None:
                runtime_group_count += 1
                worker_count += len(workers)
                status_totals.update(worker.status for worker in workers)
            demand = demands.get(project.project_id)
            project_views.append(
                ProjectOperationsView(
                    project_id=project.project_id,
                    name=project.name,
                    root=str(project.root),
                    binding=self._binding_view(
                        state=binding_by_project.get(project.project_id)
                    ),
                    worker_runtime=WorkerRuntimeGroupOperationsView(
                        initialized=group is not None,
                        topology_generation=group.topology_generation
                        if group is not None
                        else 0,
                        revision=group.revision if group is not None else 0,
                        workers=workers,
                        idle=status_counts[WakeTargetStatus.IDLE],
                        waking=status_counts[WakeTargetStatus.WAKING],
                        claiming=status_counts[WakeTargetStatus.CLAIMING],
                        busy=status_counts[WakeTargetStatus.BUSY],
                    ),
                    desired_capacity=demand.desired_capacity
                    if demand is not None
                    else 0,
                    spawn_needed=demand.spawn_needed if demand is not None else 0,
                    provisioning_pending=pending_by_project[project.project_id],
                )
            )

        runs = tuple(
            sorted(
                self._runs.list_all(), key=lambda item: item.updated_at, reverse=True
            )[:_RECENT_RUN_LIMIT]
        )
        recent_runs = tuple(
            RecentRunOperationsView(
                run_id=run.run_id,
                project_id=run.project_id,
                project_name=run.project_name,
                goal=run.goal,
                status=run.status,
                requested_parallelism=run.requested_parallelism,
                updated_at=run.updated_at,
            )
            for run in runs
        )
        broker_snapshot = self._broker_snapshot(
            project_ids=tuple(project.project_id for project in projects)
        )
        return ControlPanelOperationsResponse(
            browser=WorkerBrowserOperationsView(
                running=browser_running,
                mode=browser_mode,
                profile_initialized=browser_profile_initialized,
            ),
            database=DatabaseOperationsView(
                enabled=False,
                state="disabled",
                projected_at=None,
                message="PostgreSQL read model is not configured.",
            ),
            broker=self._broker_view(snapshot=broker_snapshot)
            if broker_snapshot is not None
            else None,
            totals=OperationsTotalsView(
                project_count=len(project_views),
                runtime_group_count=runtime_group_count,
                worker_count=worker_count,
                idle=status_totals[WakeTargetStatus.IDLE],
                waking=status_totals[WakeTargetStatus.WAKING],
                claiming=status_totals[WakeTargetStatus.CLAIMING],
                busy=status_totals[WakeTargetStatus.BUSY],
                provisioning_pending=sum(pending_by_project.values()),
                active_runs=sum(run.status is RunStatus.ACTIVE for run in runs),
            ),
            projects=tuple(project_views),
            recent_runs=recent_runs,
        )

    def _read_group(self, *, project_id: str) -> WorkerRuntimeGroupState | None:
        if not self._group_store.exists(project_id=project_id):
            return None
        return self._group_store.read(project_id=project_id)

    def _broker_snapshot(
        self, *, project_ids: tuple[str, ...]
    ) -> BrokerManagementSnapshot | None:
        if self._broker_management is None:
            return None
        try:
            return self._broker_management.snapshot(project_ids=project_ids)
        except AgentBrokerError, RuntimeError:
            return None

    @staticmethod
    def _broker_view(*, snapshot: BrokerManagementSnapshot) -> BrokerOperationsView:
        return BrokerOperationsView(
            term=snapshot.term,
            revision=snapshot.revision,
            namespaces=tuple(
                BrokerNamespaceOperationsView(
                    project_id=namespace.project_id,
                    queued_count=namespace.queued_count,
                    ready_count=namespace.ready_count,
                    claimed_count=namespace.claimed_count,
                    completed_count=namespace.completed_count,
                    failed_count=namespace.failed_count,
                    idle_slot_count=namespace.idle_slot_count,
                    busy_slot_count=namespace.busy_slot_count,
                )
                for namespace in snapshot.namespaces
            ),
            consumer_groups=tuple(
                BrokerConsumerGroupOperationsView(
                    group_id=group.group_id,
                    namespace_id=group.namespace_id,
                    generation=group.generation,
                    group_revision=group.group_revision,
                    consumer_count=group.consumer_count,
                )
                for group in snapshot.consumer_groups
            ),
            queued_count=snapshot.queued_count,
            ready_count=snapshot.ready_count,
            claimed_count=snapshot.claimed_count,
            completed_count=snapshot.completed_count,
            failed_count=snapshot.failed_count,
            idle_slot_count=snapshot.idle_slot_count,
            busy_slot_count=snapshot.busy_slot_count,
        )

    @staticmethod
    def _binding_view(
        *, state: ProjectBindingState | None
    ) -> ProviderBindingOperationsView | None:
        if state is None:
            return None
        metadata = state.adapter_metadata
        return ProviderBindingOperationsView(
            provider=metadata.provider if metadata is not None else None,
            project_ref=metadata.project_ref if metadata is not None else None,
            project_label=metadata.project_label if metadata is not None else None,
            project_path=metadata.project_path if metadata is not None else None,
            connector_ready=bool(
                metadata is not None and metadata.app_ref and metadata.app_label
            ),
            revision=state.revision,
        )
