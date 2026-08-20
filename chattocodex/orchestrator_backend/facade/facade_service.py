from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.facade.facade_dto import (
    DelegatedTask,
    DelegationStarted,
    DelegationStatus,
    DelegationTaskSpec,
    ScheduledDelegationStarted,
    WorkerWork,
    WorkerWorkDone,
)
from chattocodex.orchestrator_backend.facade.worker_work_service import (
    WorkerWorkService,
)
from chattocodex.orchestrator_backend.orchestration.logical_task_dag_policy import (
    resolve_requested_parallelism,
    validate_and_order_logical_tasks,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_service import (
    WorkerWakeCapabilityService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_coordinator import (
    WorkerRuntimeCoordinator,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_selection_policy import (
    resolve_workspace_project_id,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


class ChatFacadeService:
    def __init__(
        self,
        run_service: RunOrchestrationService,
        broker_service: BrokerService,
        wake_target_service: WakeTargetService,
        workspace_project_service: WorkspaceProjectService,
        worker_work_service: WorkerWorkService,
        worker_runtime_coordinator: WorkerRuntimeCoordinator | None = None,
        worker_wake_capability_service: WorkerWakeCapabilityService | None = None,
    ) -> None:
        """Initialize this object. Args: run_service, broker_service, wake_target_service, workspace_project_service, worker_work_service, worker_runtime_coordinator, worker_wake_capability_service. Returns: None."""
        self._run_service = run_service
        self._broker_service = broker_service
        self._wake_target_service = wake_target_service
        self._workspace_project_service = workspace_project_service
        self._worker_work_service = worker_work_service
        self._worker_runtime_coordinator = worker_runtime_coordinator
        self._worker_wake_capability_service = worker_wake_capability_service

    def delegate_work(
        self,
        goal: str,
        tasks: tuple[DelegationTaskSpec, ...],
        requested_parallelism: int | None = None,
        project_id: str | None = None,
    ) -> DelegationStarted:
        """Publish a logical DAG and reconcile its authoritative capacity demand.

        Args:
            goal: Human-readable delegation goal.
            tasks: Logical task DAG owned by the Broker.
            requested_parallelism: Main/backend concurrency request. When omitted,
                the number of independent root tasks is persisted as the safe
                compatibility default.
            project_id: Optional canonical project identity.

        Returns:
            Persisted Run plus published Broker tasks.

        Raises:
            OrchestrationError: If the DAG or project cannot be created safely.
        """
        ordered_specs = validate_and_order_logical_tasks(
            tasks=tasks,
            operation="delegate_work",
            empty_message="Delegate at least one logical task.",
        )
        resolved_project_id = self._resolve_project_id(
            project_id=project_id,
            prefer_ready=False,
        )
        created_run = self._run_service.create_run(
            goal=goal,
            project_id=resolved_project_id,
            requested_parallelism=resolve_requested_parallelism(
                requested_parallelism=requested_parallelism,
                tasks=ordered_specs,
            ),
        )

        created_by_key: dict[str, DelegatedTask] = {}
        for spec in ordered_specs:
            broker_task = self._broker_service.create_broker_task(
                run_id=created_run.state.run_id,
                run_handle=created_run.run_handle,
                objective=spec.objective,
                role=spec.role,
                depends_on=tuple(
                    created_by_key[dependency].broker_task.broker_task_id
                    for dependency in spec.depends_on
                ),
            )
            created_by_key[spec.key] = DelegatedTask(
                key=spec.key,
                broker_task=broker_task,
            )

        snapshot = self._broker_service.get_run_status(run_id=created_run.state.run_id)
        if self._worker_runtime_coordinator is not None:
            self._worker_runtime_coordinator.reconcile_project(
                project_id=created_run.state.project_id
            )
        else:
            self._wake_target_service.sync_ready_capacity(
                project_id=created_run.state.project_id,
                ready_count=snapshot.ready_count,
            )

        return DelegationStarted(
            run=created_run,
            tasks=tuple(created_by_key[spec.key] for spec in tasks),
        )

    def prepare_scheduled_work(
        self,
        goal: str,
        tasks: tuple[DelegationTaskSpec, ...],
        requested_parallelism: int | None = None,
        project_id: str | None = None,
    ) -> ScheduledDelegationStarted:
        """Publish Broker work and issue one-shot wake capabilities without browser provisioning. Args: goal, tasks, requested_parallelism, project_id. Returns: ScheduledDelegationStarted."""
        if self._worker_wake_capability_service is None:
            raise InvalidTransitionError(
                operation="prepare_scheduled_work",
                resource_id="worker-wake-capability",
                message="Scheduled Worker wake capabilities are not configured.",
            )
        ordered_specs = validate_and_order_logical_tasks(
            tasks=tasks,
            operation="delegate_work",
            empty_message="Delegate at least one logical task.",
        )
        resolved_project_id = self._resolve_project_id(
            project_id=project_id,
            prefer_ready=False,
        )
        parallelism = resolve_requested_parallelism(
            requested_parallelism=requested_parallelism,
            tasks=ordered_specs,
        )
        created_run = self._run_service.create_run(
            goal=goal,
            project_id=resolved_project_id,
            requested_parallelism=parallelism,
        )

        created_by_key: dict[str, DelegatedTask] = {}
        for spec in ordered_specs:
            broker_task = self._broker_service.create_broker_task(
                run_id=created_run.state.run_id,
                run_handle=created_run.run_handle,
                objective=spec.objective,
                role=spec.role,
                depends_on=tuple(
                    created_by_key[dependency].broker_task.broker_task_id
                    for dependency in spec.depends_on
                ),
            )
            created_by_key[spec.key] = DelegatedTask(
                key=spec.key,
                broker_task=broker_task,
            )

        root_count = sum(not spec.depends_on for spec in ordered_specs)
        wake_count = min(parallelism, max(1, root_count))
        wake_capabilities = tuple(
            self._worker_wake_capability_service.issue(
                project_id=resolved_project_id,
                run_id=created_run.state.run_id,
            )
            for _ in range(wake_count)
        )
        return ScheduledDelegationStarted(
            run=created_run,
            tasks=tuple(created_by_key[spec.key] for spec in tasks),
            wake_capabilities=wake_capabilities,
        )

    def delegation_status(self, run_id: str) -> DelegationStatus:
        """Return the Broker-owned status for one logical delegation Run.

        Args:
            run_id: Internal durable Run identity.

        Returns:
            Current Broker snapshot.
        """
        return DelegationStatus(
            snapshot=self._broker_service.get_run_status(run_id=run_id)
        )

    def work(
        self,
        project_id: str | None,
        slot_id: str | None,
        slot_handle: str | None,
        label: str | None,
        wake_target_id: str | None = None,
        wake_revision: int | None = None,
    ) -> WorkerWork:
        """Delegate Worker wake/slot acquisition to the Worker lifecycle service. Args: project_id, slot_id, slot_handle, label, wake_target_id, wake_revision. Returns: WorkerWork."""

        return self._worker_work_service.work(
            project_id=project_id,
            slot_id=slot_id,
            slot_handle=slot_handle,
            label=label,
            wake_target_id=wake_target_id,
            wake_revision=wake_revision,
        )

    def work_done(self, slot_id: str, slot_handle: str) -> WorkerWorkDone:
        """Delegate terminal assignment release to the Worker lifecycle service. Args: slot_id, slot_handle. Returns: WorkerWorkDone."""

        return self._worker_work_service.work_done(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )

    def _resolve_project_id(
        self,
        project_id: str | None,
        prefer_ready: bool,
    ) -> str:
        """Resolve project id. Args: project_id, prefer_ready. Returns: str."""
        return resolve_workspace_project_id(
            workspace_project_service=self._workspace_project_service,
            project_id=project_id,
            preferred_project_ids=(
                self._broker_service.ready_project_ids() if prefer_ready else ()
            ),
            operation="resolve_project",
            multiple_projects_message=(
                "The workspace contains multiple projects. Select the intended project internally "
                "with project_list and retry; do not ask the user for Broker or Worker IDs."
            ),
        )
