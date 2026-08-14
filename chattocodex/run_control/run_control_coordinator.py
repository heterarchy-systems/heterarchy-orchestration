from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime

from chattocodex.broker.broker_service import BrokerService
from chattocodex.consumer_group.consumer_group_coordinator import (
    ConsumerGroupCoordinator,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.persistence.run_control_state_store import RunControlStateStore
from chattocodex.run_control.main_review_capability import MainReviewCapability
from chattocodex.run_control.run_control_capability_rotation import (
    rotate_workflow_capability,
)
from chattocodex.run_control.run_control_identity import (
    capability_matches,
    digest_capability,
    new_workflow_handle,
    new_workflow_id,
)
from chattocodex.run_control.run_control_state import (
    CreatedWorkflow,
    WorkflowDecision,
    WorkflowReviewResult,
    WorkflowRoundState,
    WorkflowSnapshot,
    WorkflowState,
    WorkflowStatus,
    WorkflowTaskSpec,
)
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class RunControlCoordinator:
    def __init__(
        self,
        *,
        repository: RunControlStateStore,
        run_service: RunOrchestrationService,
        broker_service: BrokerService,
        wake_target_service: WakeTargetService,
        workspace_project_service: WorkspaceProjectService,
        lock_registry: EntityLockRegistry,
        consumer_group_coordinator: ConsumerGroupCoordinator | None = None,
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
            consumer_group_coordinator: Scheduler choke point for capacity/reuse.
            now_provider: UTC clock override for deterministic tests.
        """
        self._repository = repository
        self._run_service = run_service
        self._broker_service = broker_service
        self._wake_target_service = wake_target_service
        self._workspace_project_service = workspace_project_service
        self._lock_registry = lock_registry
        self._consumer_group_coordinator = consumer_group_coordinator
        self._now_provider = now_provider or _utc_now

    def start_workflow(
        self,
        *,
        goal: str,
        tasks: tuple[WorkflowTaskSpec, ...],
        review_instructions: str,
        max_rework_rounds: int,
        requested_parallelism: int | None = None,
        project_id: str | None,
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
        ordered = self._validate_and_order_tasks(
            tasks=tasks, operation="orchestrate_work"
        )
        resolved_project_id = self._resolve_project_id(project_id=project_id)
        workflow_id = new_workflow_id()
        workflow_handle = new_workflow_handle()
        now = self._now_provider()
        resolved_parallelism = self._resolve_requested_parallelism(
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
        *,
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
            self._require_capability(state=state, workflow_handle=workflow_handle)
            state = self._refresh_locked(state=state)
            return self._snapshot(state=state)

    def issue_main_review_capability(self, *, workflow_id: str) -> MainReviewCapability:
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
            state = self._refresh_locked(state=state)
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
        *,
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
            self._require_capability(state=state, workflow_handle=workflow_handle)
            state = self._refresh_locked(state=state)
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
                updated = self._transition(
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
                updated = self._transition(
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
            ordered = self._validate_and_order_tasks(
                tasks=rework_tasks,
                operation="review_workflow",
            )
            next_round_number = len(state.rounds) + 1
            next_round = self._create_round(
                workflow_goal=f"{state.goal} — rework round {next_round_number - 1}",
                round_number=next_round_number,
                project_id=state.project_id,
                tasks=ordered,
                requested_parallelism=state.requested_parallelism,
            )
            updated = self._transition(
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

    def refresh_by_run(self, *, run_id: str) -> tuple[WorkflowState, ...]:
        """Refresh active workflows whose current round matches a completed Run.

        Args:
            run_id: Durable Run identity that may have reached a terminal round.

        Returns:
            Workflow states inspected/refreshed for the Run.
        """
        refreshed: list[WorkflowState] = []
        for state in self._repository.list_for_run(run_id=run_id):
            if (
                state.current_round.run_id != run_id
                or state.status is not WorkflowStatus.RUNNING
            ):
                continue
            with self._lock_registry.hold(entity_key=f"workflow:{state.workflow_id}"):
                current = self._repository.read(workflow_id=state.workflow_id)
                if current.current_round.run_id != run_id:
                    continue
                refreshed.append(self._refresh_locked(state=current))
        return tuple(refreshed)

    def _create_round(
        self,
        *,
        workflow_goal: str,
        round_number: int,
        project_id: str,
        tasks: tuple[WorkflowTaskSpec, ...],
        requested_parallelism: int,
    ) -> WorkflowRoundState:
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
        if self._consumer_group_coordinator is not None:
            self._consumer_group_coordinator.reconcile_project(project_id=project_id)
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

    def _refresh_locked(self, *, state: WorkflowState) -> WorkflowState:
        if state.status is not WorkflowStatus.RUNNING:
            return state
        snapshot = self._broker_service.get_run_status(
            run_id=state.current_round.run_id
        )
        terminal_count = snapshot.completed_count + snapshot.failed_count
        if not snapshot.broker_tasks or terminal_count != len(snapshot.broker_tasks):
            return state
        updated = self._transition(
            state=state,
            status=WorkflowStatus.WAITING_MAIN_REVIEW,
            last_review_summary=state.last_review_summary,
            rounds=state.rounds,
            rework_rounds=state.rework_rounds,
            now=self._now_provider(),
        )
        return self._repository.write(state=updated)

    def _snapshot(self, *, state: WorkflowState) -> WorkflowSnapshot:
        current = self._broker_service.get_run_status(run_id=state.current_round.run_id)
        return WorkflowSnapshot(
            workflow=state,
            current_run=current,
            needs_main_review=state.status is WorkflowStatus.WAITING_MAIN_REVIEW,
        )

    @staticmethod
    def _transition(
        *,
        state: WorkflowState,
        status: WorkflowStatus,
        last_review_summary: str | None,
        rounds: tuple[WorkflowRoundState, ...],
        rework_rounds: int,
        now: datetime,
    ) -> WorkflowState:
        return WorkflowState(
            schema_version=state.schema_version,
            workflow_id=state.workflow_id,
            workflow_handle_digest=state.workflow_handle_digest,
            goal=state.goal,
            project_id=state.project_id,
            review_instructions=state.review_instructions,
            max_rework_rounds=state.max_rework_rounds,
            rework_rounds=rework_rounds,
            status=status,
            rounds=rounds,
            last_review_summary=last_review_summary,
            revision=state.revision + 1,
            created_at=state.created_at,
            updated_at=now,
            requested_parallelism=state.requested_parallelism,
        )

    @staticmethod
    def _require_capability(*, state: WorkflowState, workflow_handle: str) -> None:
        if not capability_matches(
            handle=workflow_handle,
            expected_digest=state.workflow_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="workflow_capability",
                resource_id=state.workflow_id,
            )

    def _resolve_project_id(self, *, project_id: str | None) -> str:
        if project_id is not None:
            return self._workspace_project_service.require_project(
                project_id=project_id
            ).project_id
        projects = self._workspace_project_service.list_projects()
        if len(projects) == 1:
            return projects[0].project_id
        if not projects:
            raise InvalidTransitionError(
                operation="orchestrate_work",
                resource_id="workspace",
                message="No project is available in the configured workspace.",
            )
        raise InvalidTransitionError(
            operation="orchestrate_work",
            resource_id="workspace",
            message=(
                "The workspace contains multiple projects. Resolve the intended project internally "
                "with project_list and retry."
            ),
        )

    @staticmethod
    def _resolve_requested_parallelism(
        *,
        requested_parallelism: int | None,
        tasks: tuple[WorkflowTaskSpec, ...],
    ) -> int:
        """Resolve a persisted workflow parallelism request from its logical DAG.

        Args:
            requested_parallelism: Explicit Main/backend request, if present.
            tasks: Validated logical Worker task DAG.

        Returns:
            Explicit request, or the number of dependency-free roots.

        Raises:
            ValueError: If an explicit request is less than one.
        """
        if requested_parallelism is not None:
            if requested_parallelism < 1:
                raise ValueError("requested_parallelism must be at least one")
            return requested_parallelism
        return max(1, sum(not task.depends_on for task in tasks))

    @staticmethod
    def _validate_and_order_tasks(
        *,
        tasks: tuple[WorkflowTaskSpec, ...],
        operation: str,
    ) -> tuple[WorkflowTaskSpec, ...]:
        if not tasks:
            raise InvalidTransitionError(
                operation=operation,
                resource_id="tasks",
                message="Workflow stage requires at least one Worker task.",
            )
        by_key: dict[str, WorkflowTaskSpec] = {}
        input_index: dict[str, int] = {}
        for index, task in enumerate(tasks):
            if task.key in by_key:
                raise InvalidTransitionError(
                    operation=operation,
                    resource_id=task.key,
                    message=f"Logical task key {task.key!r} is duplicated.",
                )
            by_key[task.key] = task
            input_index[task.key] = index
        indegree = {task.key: 0 for task in tasks}
        dependents: dict[str, list[str]] = {task.key: [] for task in tasks}
        for task in tasks:
            seen: set[str] = set()
            for dependency in task.depends_on:
                if dependency == task.key:
                    raise InvalidTransitionError(
                        operation=operation,
                        resource_id=task.key,
                        message="A logical task cannot depend on itself.",
                    )
                if dependency not in by_key:
                    raise InvalidTransitionError(
                        operation=operation,
                        resource_id=dependency,
                        message=f"Unknown logical task dependency {dependency!r}.",
                    )
                if dependency in seen:
                    continue
                seen.add(dependency)
                indegree[task.key] += 1
                dependents[dependency].append(task.key)
        ready = deque(
            sorted(
                (key for key, degree in indegree.items() if degree == 0),
                key=input_index.__getitem__,
            )
        )
        ordered_keys: list[str] = []
        while ready:
            key = ready.popleft()
            ordered_keys.append(key)
            newly_ready: list[str] = []
            for dependent in dependents[key]:
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    newly_ready.append(dependent)
            for dependent in sorted(newly_ready, key=input_index.__getitem__):
                ready.append(dependent)
        if len(ordered_keys) != len(tasks):
            raise InvalidTransitionError(
                operation=operation,
                resource_id="tasks",
                message="Logical task dependencies contain a cycle.",
            )
        return tuple(by_key[key] for key in ordered_keys)


def _utc_now() -> datetime:
    return datetime.now(UTC)
