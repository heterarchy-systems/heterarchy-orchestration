from collections.abc import Callable
from datetime import UTC, datetime

from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
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
from chattocodex.orchestrator_backend.persistence.orchestration.run_control_state_store import (
    RunControlStateStore,
)
from chattocodex.orchestrator_backend.run_control.main_review_capability import (
    MainReviewCapability,
)
from chattocodex.orchestrator_backend.run_control.run_control_capability_rotation import (
    rotate_workflow_capability,
)
from chattocodex.orchestrator_backend.run_control.run_control_identity import (
    digest_capability,
    new_workflow_handle,
    new_workflow_id,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    CreatedWorkflow,
    WorkflowDecision,
    WorkflowReviewResult,
    WorkflowRoundState,
    WorkflowSnapshot,
    WorkflowState,
    WorkflowStatus,
    WorkflowTaskSpec,
)
from chattocodex.orchestrator_backend.run_control.run_control_state_policy import (
    require_workflow_capability,
    transition_workflow_state,
)
from chattocodex.orchestrator_backend.run_control.workflow_refresh_service import (
    WorkflowRefreshService,
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
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class RunControlCoordinator:
    def __init__(
        self,
        repository: RunControlStateStore,
        run_service: RunOrchestrationService,
        broker_service: BrokerService,
        wake_target_service: WakeTargetService,
        workspace_project_service: WorkspaceProjectService,
        lock_registry: EntityLockRegistry,
        workflow_refresh_service: WorkflowRefreshService,
        worker_runtime_coordinator: WorkerRuntimeCoordinator | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Create the Main-only workflow coordinator.

        Args:
            repository: Durable workflow state store.
            run_service: Run creator that persists requested parallelism.
            broker_service: Broker-owned task/DAG service.
            wake_target_service: Legacy direct wake fallback for embeddings that
                do not yet provide a Consumer Group coordinator.
            workspace_project_service: Canonical project resolver.
            lock_registry: Shared workflow synchronization registry.
            worker_runtime_coordinator: Worker runtime-topology choke point for capacity/reuse.
            now_provider: UTC clock override for deterministic tests. Args: repository, run_service, broker_service, wake_target_service, workspace_project_service, lock_registry, workflow_refresh_service, worker_runtime_coordinator, now_provider. Returns: None."""
        self._repository = repository
        self._run_service = run_service
        self._broker_service = broker_service
        self._wake_target_service = wake_target_service
        self._workspace_project_service = workspace_project_service
        self._lock_registry = lock_registry
        self._workflow_refresh_service = workflow_refresh_service
        self._worker_runtime_coordinator = worker_runtime_coordinator
        self._now_provider = now_provider or _utc_now

    def start_workflow(
        self,
        goal: str,
        tasks: tuple[WorkflowTaskSpec, ...],
        review_instructions: str,
        max_rework_rounds: int,
        requested_parallelism: int | None = None,
        project_id: str | None = None,
    ) -> CreatedWorkflow:
        """Create a workflow and publish its first capacity-constrained Run.

        Args:
            goal: Human-readable workflow goal.
            tasks: Logical Worker task DAG.
            review_instructions: Main review criteria.
            max_rework_rounds: Maximum bounded corrective rounds.
            requested_parallelism: Explicit Main/backend concurrency request. An
                omitted value safely defaults to the independent root count.
            project_id: Optional canonical project identity.

        Returns:
            Durable workflow state and opaque workflow capability.

        Raises:
            OrchestrationError: If the project or DAG cannot be created safely.
        """
        ordered = validate_and_order_logical_tasks(
            tasks=tasks,
            operation="orchestrate_work",
            empty_message="Workflow stage requires at least one Worker task.",
        )
        resolved_project_id = self._resolve_project_id(project_id=project_id)
        workflow_id = new_workflow_id()
        workflow_handle = new_workflow_handle()
        now = self._now_provider()
        resolved_parallelism = resolve_requested_parallelism(
            requested_parallelism=requested_parallelism,
            tasks=ordered,
        )

        round_state = self._create_round(
            workflow_goal=goal,
            round_number=1,
            project_id=resolved_project_id,
            tasks=ordered,
            requested_parallelism=resolved_parallelism,
        )
        state = WorkflowState(
            schema_version=1,
            workflow_id=workflow_id,
            workflow_handle_digest=digest_capability(workflow_handle),
            goal=goal,
            project_id=resolved_project_id,
            review_instructions=review_instructions,
            max_rework_rounds=max_rework_rounds,
            rework_rounds=0,
            status=WorkflowStatus.RUNNING,
            rounds=(round_state,),
            last_review_summary=None,
            revision=0,
            created_at=now,
            updated_at=now,
            requested_parallelism=resolved_parallelism,
        )
        created = self._repository.create(state=state)
        return CreatedWorkflow(state=created, workflow_handle=workflow_handle)

    def get_status(
        self,
        workflow_id: str,
        workflow_handle: str,
    ) -> WorkflowSnapshot:
        """Refresh and return the Main-visible state of one workflow.

        Args:
            workflow_id: Durable workflow identity.
            workflow_handle: Current opaque Main capability.

        Returns:
            Current workflow and Broker snapshot.

        Raises:
            OrchestrationError: If the capability or workflow transition is invalid.
        """
        with self._lock_registry.hold(entity_key=f"workflow:{workflow_id}"):
            state = self._repository.read(workflow_id=workflow_id)
            require_workflow_capability(
                state=state,
                workflow_handle=workflow_handle,
            )
            state = self._workflow_refresh_service.refresh_state(state=state)
            return self._snapshot(state=state)

    def issue_main_review_capability(self, workflow_id: str) -> MainReviewCapability:
        """Rotate and issue a fresh Main-review capability for a waiting workflow.

        Args:
            workflow_id: Durable workflow identity.

        Returns:
            Fresh Main-review capability and workflow snapshot.

        Raises:
            InvalidTransitionError: If the workflow is not waiting for review.
        """
        with self._lock_registry.hold(entity_key=f"workflow:{workflow_id}"):
            state = self._repository.read(workflow_id=workflow_id)
            state = self._workflow_refresh_service.refresh_state(state=state)
            if state.status is not WorkflowStatus.WAITING_MAIN_REVIEW:
                raise InvalidTransitionError(
                    operation="resume_main_review",
                    resource_id=workflow_id,
                    message="Workflow is not waiting for Main review.",
                )
            workflow_handle = new_workflow_handle()
            rotated = rotate_workflow_capability(
                state=state,
                workflow_handle_digest=digest_capability(workflow_handle),
                now=self._now_provider(),
            )
            saved = self._repository.write(state=rotated)
            return MainReviewCapability(
                snapshot=self._snapshot(state=saved),
                workflow_handle=workflow_handle,
            )

    def review_workflow(
        self,
        workflow_id: str,
        workflow_handle: str,
        decision: WorkflowDecision,
        review_summary: str,
        rework_tasks: tuple[WorkflowTaskSpec, ...],
    ) -> WorkflowReviewResult:
        """Apply the Main review decision and optionally publish a rework round.

        Args:
            workflow_id: Durable workflow identity.
            workflow_handle: Current opaque Main capability.
            decision: Approve, fail, or request bounded rework.
            review_summary: Main's result summary.
            rework_tasks: New logical DAG when requesting rework.

        Returns:
            Persisted review decision and latest workflow snapshot.

        Raises:
            OrchestrationError: If the review/capability transition is invalid.
        """
        with self._lock_registry.hold(entity_key=f"workflow:{workflow_id}"):
            state = self._repository.read(workflow_id=workflow_id)
            require_workflow_capability(
                state=state,
                workflow_handle=workflow_handle,
            )
            state = self._workflow_refresh_service.refresh_state(state=state)
            if state.status is not WorkflowStatus.WAITING_MAIN_REVIEW:
                raise InvalidTransitionError(
                    operation="review_workflow",
                    resource_id=workflow_id,
                    message="Workflow is not waiting for Main review.",
                )

            current = self._broker_service.get_run_status(
                run_id=state.current_round.run_id
            )
            now = self._now_provider()
            if decision is WorkflowDecision.APPROVE:
                if current.failed_count:
                    raise InvalidTransitionError(
                        operation="review_workflow",
                        resource_id=workflow_id,
                        message=(
                            "Cannot approve a workflow round containing failed Worker tasks. "
                            "Choose rework or fail."
                        ),
                    )
                updated = transition_workflow_state(
                    state=state,
                    status=WorkflowStatus.COMPLETED,
                    last_review_summary=review_summary,
                    rounds=state.rounds,
                    rework_rounds=state.rework_rounds,
                    now=now,
                )
                saved = self._repository.write(state=updated)
                return WorkflowReviewResult(
                    snapshot=self._snapshot(state=saved),
                    decision=decision,
                    new_round_started=False,
                )

            if decision is WorkflowDecision.FAIL:
                updated = transition_workflow_state(
                    state=state,
                    status=WorkflowStatus.FAILED,
                    last_review_summary=review_summary,
                    rounds=state.rounds,
                    rework_rounds=state.rework_rounds,
                    now=now,
                )
                saved = self._repository.write(state=updated)
                return WorkflowReviewResult(
                    snapshot=self._snapshot(state=saved),
                    decision=decision,
                    new_round_started=False,
                )

            if not rework_tasks:
                raise InvalidTransitionError(
                    operation="review_workflow",
                    resource_id=workflow_id,
                    message="Rework requires at least one corrective Worker task.",
                )
            if state.rework_rounds >= state.max_rework_rounds:
                raise InvalidTransitionError(
                    operation="review_workflow",
                    resource_id=workflow_id,
                    message=(
                        "Workflow reached its maximum rework rounds. Approve only if valid or fail "
                        "the workflow."
                    ),
                )
            ordered = validate_and_order_logical_tasks(
                tasks=rework_tasks,
                operation="review_workflow",
                empty_message="Workflow stage requires at least one Worker task.",
            )
            next_round_number = len(state.rounds) + 1
            next_round = self._create_round(
                workflow_goal=f"{state.goal} — rework round {next_round_number - 1}",
                round_number=next_round_number,
                project_id=state.project_id,
                tasks=ordered,
                requested_parallelism=state.requested_parallelism,
            )
            updated = transition_workflow_state(
                state=state,
                status=WorkflowStatus.RUNNING,
                last_review_summary=review_summary,
                rounds=(*state.rounds, next_round),
                rework_rounds=state.rework_rounds + 1,
                now=now,
            )
            saved = self._repository.write(state=updated)
            return WorkflowReviewResult(
                snapshot=self._snapshot(state=saved),
                decision=decision,
                new_round_started=True,
            )

    def refresh_by_run(self, run_id: str) -> tuple[WorkflowState, ...]:
        """Refresh active workflows whose current round matches a completed Run.

        Args:
            run_id: Durable Run identity that may have reached a terminal round.

        Returns:
            Workflow states inspected/refreshed for the Run.
        """
        return self._workflow_refresh_service.refresh_by_run(run_id=run_id)

    def _create_round(
        self,
        workflow_goal: str,
        round_number: int,
        project_id: str,
        tasks: tuple[WorkflowTaskSpec, ...],
        requested_parallelism: int,
    ) -> WorkflowRoundState:
        """Create round. Args: workflow_goal, round_number, project_id, tasks, requested_parallelism. Returns: WorkflowRoundState."""
        created_run = self._run_service.create_run(
            goal=workflow_goal,
            project_id=project_id,
            requested_parallelism=requested_parallelism,
        )
        created_by_key: dict[str, str] = {}
        for spec in tasks:
            broker_task = self._broker_service.create_broker_task(
                run_id=created_run.state.run_id,
                run_handle=created_run.run_handle,
                objective=spec.objective,
                role=spec.role,
                depends_on=tuple(created_by_key[key] for key in spec.depends_on),
            )
            created_by_key[spec.key] = broker_task.broker_task_id
        snapshot = self._broker_service.get_run_status(run_id=created_run.state.run_id)
        if self._worker_runtime_coordinator is not None:
            self._worker_runtime_coordinator.reconcile_project(project_id=project_id)
        else:
            self._wake_target_service.sync_ready_capacity(
                project_id=project_id,
                ready_count=min(snapshot.ready_count, requested_parallelism),
            )
        return WorkflowRoundState(
            round_number=round_number,
            run_id=created_run.state.run_id,
            task_keys=tuple(spec.key for spec in tasks),
            broker_task_ids=tuple(created_by_key[spec.key] for spec in tasks),
            created_at=self._now_provider(),
        )

    def _snapshot(self, state: WorkflowState) -> WorkflowSnapshot:
        """Snapshot. Args: state. Returns: WorkflowSnapshot."""
        current = self._broker_service.get_run_status(run_id=state.current_round.run_id)
        return WorkflowSnapshot(
            workflow=state,
            current_run=current,
            needs_main_review=state.status is WorkflowStatus.WAITING_MAIN_REVIEW,
        )

    def _resolve_project_id(self, project_id: str | None) -> str:
        """Resolve project id. Args: project_id. Returns: str."""
        return resolve_workspace_project_id(
            workspace_project_service=self._workspace_project_service,
            project_id=project_id,
            operation="orchestrate_work",
            multiple_projects_message=(
                "The workspace contains multiple projects. Resolve the intended project internally "
                "with project_list and retry."
            ),
        )


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
