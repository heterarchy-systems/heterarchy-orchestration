from collections import deque

from chattocodex.broker.broker_dto import WorkerSlotStatus
from chattocodex.broker.broker_service import BrokerService
from chattocodex.consumer_group.consumer_group_coordinator import (
    ConsumerGroupCoordinator,
)
from chattocodex.facade.facade_dto import (
    DelegatedTask,
    DelegationStarted,
    DelegationStatus,
    DelegationTaskSpec,
    ScheduledDelegationStarted,
    WorkerWork,
    WorkerWorkDone,
)
from chattocodex.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    OrchestrationError,
)
from chattocodex.orchestration.run_orchestration_service import RunOrchestrationService
from chattocodex.run_control.run_control_coordinator import RunControlCoordinator
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.wake.worker_wake_capability_service import WorkerWakeCapabilityService
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class ChatFacadeService:
    def __init__(
        self,
        *,
        run_service: RunOrchestrationService,
        broker_service: BrokerService,
        wake_target_service: WakeTargetService,
        workspace_project_service: WorkspaceProjectService,
        run_control_coordinator: RunControlCoordinator | None = None,
        consumer_group_coordinator: ConsumerGroupCoordinator | None = None,
        worker_wake_capability_service: WorkerWakeCapabilityService | None = None,
    ) -> None:
        self._run_service = run_service
        self._broker_service = broker_service
        self._wake_target_service = wake_target_service
        self._workspace_project_service = workspace_project_service
        self._run_control_coordinator = run_control_coordinator
        self._consumer_group_coordinator = consumer_group_coordinator
        self._worker_wake_capability_service = worker_wake_capability_service

    def delegate_work(
        self,
        *,
        goal: str,
        tasks: tuple[DelegationTaskSpec, ...],
        requested_parallelism: int | None = None,
        project_id: str | None,
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
        ordered_specs = self._validate_and_order_tasks(tasks=tasks)
        resolved_project_id = self._resolve_project_id(
            project_id=project_id,
            prefer_ready=False,
        )
        created_run = self._run_service.create_run(
            goal=goal,
            project_id=resolved_project_id,
            requested_parallelism=_resolve_requested_parallelism(
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
        if self._consumer_group_coordinator is not None:
            self._consumer_group_coordinator.reconcile_project(
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
        *,
        goal: str,
        tasks: tuple[DelegationTaskSpec, ...],
        requested_parallelism: int | None = None,
        project_id: str | None,
    ) -> ScheduledDelegationStarted:
        """Publish Broker work and issue one-shot wake capabilities without browser provisioning."""
        if self._worker_wake_capability_service is None:
            raise InvalidTransitionError(
                operation="prepare_scheduled_work",
                resource_id="worker-wake-capability",
                message="Scheduled Worker wake capabilities are not configured.",
            )
        ordered_specs = self._validate_and_order_tasks(tasks=tasks)
        resolved_project_id = self._resolve_project_id(
            project_id=project_id,
            prefer_ready=False,
        )
        parallelism = _resolve_requested_parallelism(
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

    def delegation_status(self, *, run_id: str) -> DelegationStatus:
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
        *,
        project_id: str | None,
        slot_id: str | None,
        slot_handle: str | None,
        label: str | None,
        wake_target_id: str | None = None,
        wake_revision: int | None = None,
    ) -> WorkerWork:
        """Acquire or resume an assignment for a project-bound Worker slot.

        Args:
            project_id: Optional canonical project identity.
            slot_id: Existing Worker Slot id, if this is a resume.
            slot_handle: Existing Worker Slot capability, if this is a resume.
            label: Optional display label for a newly bound Worker Slot.
            wake_target_id: Opaque routing target for a fresh wake claim.
            wake_revision: Current opaque wake-delivery revision.

        Returns:
            The reusable slot and its current assignment, if any.

        Raises:
            OrchestrationError: If the wake/slot/project boundary is invalid.
        """
        if (slot_id is None) != (slot_handle is None):
            raise InvalidTransitionError(
                operation="work",
                resource_id="worker-slot",
                message="Worker Slot identity must be supplied completely or omitted completely.",
            )
        if slot_id is None and (wake_target_id is None or wake_revision is None):
            raise InvalidTransitionError(
                operation="work",
                resource_id="wake-target",
                message=(
                    "A new Worker Slot can claim work only from a current server-issued Worker "
                    "Wake delivery. Wait for an authorized delivery transport to supply both "
                    "wake_target_id and wake_revision instead of claiming delegated work from "
                    "the Main Chat."
                ),
            )
        if slot_id is not None and wake_revision is not None:
            raise InvalidTransitionError(
                operation="work",
                resource_id=slot_id,
                message="wake_revision is only valid for a fresh Worker wake claim.",
            )

        fresh_wake_claim = slot_id is None
        if fresh_wake_claim:
            assert wake_target_id is not None
            assert wake_revision is not None
            return self._work_from_fresh_wake(
                project_id=project_id,
                label=label,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )

        assert slot_id is not None
        assert slot_handle is not None
        acquired = self._broker_service.acquire_task(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        try:
            self._wake_target_service.refresh_bound_slot(
                slot_id=acquired.slot.slot_id,
                project_id=acquired.slot.project_id,
                has_assignment=acquired.assignment is not None,
            )
        except OrchestrationError:
            if acquired.assignment is not None:
                self._broker_service.recover_abandoned_assignment(
                    slot_id=acquired.slot.slot_id,
                    reason="Connector epoch changed during Worker Slot resume.",
                )
            self._wake_target_service.release_slot(slot_id=acquired.slot.slot_id)
            raise
        return WorkerWork(
            slot=acquired.slot,
            slot_handle=slot_handle,
            assignment=acquired.assignment,
            registered=False,
            resumed=acquired.resumed,
            released_previous=acquired.released_previous,
        )

    def _work_from_fresh_wake(
        self,
        *,
        project_id: str | None,
        label: str | None,
        wake_target_id: str,
        wake_revision: int,
    ) -> WorkerWork:
        """Consume one wake revision while its cross-process claim lock is held."""
        if (
            self._worker_wake_capability_service is not None
            and self._worker_wake_capability_service.owns(wake_target_id=wake_target_id)
        ):
            return self._work_from_server_wake_capability(
                project_id=project_id,
                label=label,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )
        wake_target = self._wake_target_service.reserve_wake_claim(
            wake_target_id=wake_target_id,
            wake_revision=wake_revision,
        )
        acquired = None
        try:
            wake_project_id = wake_target.project_id
            if self._consumer_group_coordinator is not None:
                self._consumer_group_coordinator.require_member(
                    project_id=wake_project_id,
                    wake_target_id=wake_target_id,
                )
            if project_id is not None and project_id != wake_project_id:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=wake_target_id,
                    message="Automated wake target belongs to a different project.",
                )

            resolved_project_id = self._resolve_project_id(
                project_id=project_id or wake_project_id,
                prefer_ready=True,
            )
            registered = wake_target.bound_slot_id is None
            if registered:
                created = self._broker_service.register_slot(
                    project_id=resolved_project_id,
                    label=label,
                )
            else:
                assert wake_target.bound_slot_id is not None
                created = self._broker_service.reclaim_slot_for_wake(
                    slot_id=wake_target.bound_slot_id,
                    project_id=resolved_project_id,
                )
            acquired = self._broker_service.acquire_task(
                slot_id=created.state.slot_id,
                slot_handle=created.slot_handle,
            )
            self._wake_target_service.bind_target(
                wake_target_id=wake_target_id,
                project_id=acquired.slot.project_id,
                slot_id=acquired.slot.slot_id,
                has_assignment=acquired.assignment is not None,
                wake_revision=wake_revision,
            )
        except OrchestrationError:
            if acquired is not None and acquired.assignment is not None:
                self._broker_service.recover_abandoned_assignment(
                    slot_id=acquired.slot.slot_id,
                    reason="Wake reservation changed before Worker binding completed.",
                )
            self._wake_target_service.abort_wake_claim(
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )
            raise
        return WorkerWork(
            slot=acquired.slot,
            slot_handle=created.slot_handle,
            assignment=acquired.assignment,
            registered=registered,
            resumed=acquired.resumed,
            released_previous=acquired.released_previous,
        )

    def _work_from_server_wake_capability(
        self,
        *,
        project_id: str | None,
        label: str | None,
        wake_target_id: str,
        wake_revision: int,
    ) -> WorkerWork:
        assert self._worker_wake_capability_service is not None
        capability = self._worker_wake_capability_service.reserve(
            wake_target_id=wake_target_id,
            wake_revision=wake_revision,
        )
        acquired = None
        try:
            if project_id is not None and project_id != capability.project_id:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=wake_target_id,
                    message="Server-issued Worker wake capability belongs to a different project.",
                )
            resolved_project_id = self._resolve_project_id(
                project_id=project_id or capability.project_id,
                prefer_ready=False,
            )
            created = self._broker_service.register_slot(
                project_id=resolved_project_id,
                label=label,
            )
            assignment = self._broker_service.claim_task_for_run(
                slot_id=created.state.slot_id,
                slot_handle=created.slot_handle,
                run_id=capability.run_id,
            )
            if assignment is None:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=capability.run_id,
                    message="Scheduled Worker wake arrived before its Run had a ready Broker Task.",
                )
            acquired = assignment
            self._worker_wake_capability_service.commit(
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
                slot_id=assignment.slot.slot_id,
            )
        except OrchestrationError:
            if acquired is not None:
                self._broker_service.recover_abandoned_assignment(
                    slot_id=acquired.slot.slot_id,
                    reason="Server-issued Worker wake capability claim did not commit.",
                )
            self._worker_wake_capability_service.abort(
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )
            raise
        return WorkerWork(
            slot=assignment.slot,
            slot_handle=created.slot_handle,
            assignment=assignment,
            registered=True,
            resumed=False,
            released_previous=False,
        )

    def work_done(self, *, slot_id: str, slot_handle: str) -> WorkerWorkDone:
        """Release a terminal assignment and reconcile follow-up project capacity.

        Args:
            slot_id: Worker Slot identity.
            slot_handle: Worker Slot capability.

        Returns:
            Finalized assignment state and reusable slot state.

        Raises:
            OrchestrationError: If the Slot or assignment transition is invalid.
        """
        slot = self._broker_service.get_slot(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        scheduled_slot = (
            self._worker_wake_capability_service is not None
            and self._worker_wake_capability_service.owns_slot(slot_id=slot.slot_id)
        )
        if slot.status is WorkerSlotStatus.IDLE:
            if not scheduled_slot:
                self._wake_target_service.release_slot(slot_id=slot.slot_id)
            return WorkerWorkDone(slot=slot, finished=None, already_idle=True)
        active_run_id = slot.current_run_id
        finished = self._broker_service.finish_assignment(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        if not scheduled_slot:
            self._wake_target_service.release_slot(slot_id=slot_id)
        if active_run_id is not None:
            snapshot = self._broker_service.get_run_status(run_id=active_run_id)
            if not scheduled_slot:
                if self._consumer_group_coordinator is not None:
                    self._consumer_group_coordinator.reconcile_project(
                        project_id=finished.slot.project_id
                    )
                else:
                    self._wake_target_service.sync_ready_capacity(
                        project_id=finished.slot.project_id,
                        ready_count=snapshot.ready_count,
                    )
            if self._run_control_coordinator is not None:
                self._run_control_coordinator.refresh_by_run(run_id=active_run_id)
        return WorkerWorkDone(
            slot=finished.slot,
            finished=finished,
            already_idle=False,
        )

    def _resolve_project_id(
        self,
        *,
        project_id: str | None,
        prefer_ready: bool,
    ) -> str:
        if project_id is not None:
            return self._workspace_project_service.require_project(
                project_id=project_id
            ).project_id

        projects = self._workspace_project_service.list_projects()
        if prefer_ready:
            ready_project_ids = self._broker_service.ready_project_ids()
            if len(ready_project_ids) == 1:
                return ready_project_ids[0]
        if len(projects) == 1:
            return projects[0].project_id
        if not projects:
            raise InvalidTransitionError(
                operation="resolve_project",
                resource_id="workspace",
                message="No project is available in the configured workspace.",
            )
        raise InvalidTransitionError(
            operation="resolve_project",
            resource_id="workspace",
            message=(
                "The workspace contains multiple projects. Select the intended project internally "
                "with project_list and retry; do not ask the user for Broker or Worker IDs."
            ),
        )

    @staticmethod
    def _validate_and_order_tasks(
        *,
        tasks: tuple[DelegationTaskSpec, ...],
    ) -> tuple[DelegationTaskSpec, ...]:
        if not tasks:
            raise InvalidTransitionError(
                operation="delegate_work",
                resource_id="tasks",
                message="Delegate at least one logical task.",
            )

        by_key: dict[str, DelegationTaskSpec] = {}
        input_index: dict[str, int] = {}
        for index, task in enumerate(tasks):
            if task.key in by_key:
                raise InvalidTransitionError(
                    operation="delegate_work",
                    resource_id=task.key,
                    message=f"Logical task key {task.key!r} is duplicated.",
                )
            by_key[task.key] = task
            input_index[task.key] = index

        indegree = {task.key: 0 for task in tasks}
        dependents: dict[str, list[str]] = {task.key: [] for task in tasks}
        for task in tasks:
            seen_dependencies: set[str] = set()
            for dependency in task.depends_on:
                if dependency == task.key:
                    raise InvalidTransitionError(
                        operation="delegate_work",
                        resource_id=task.key,
                        message="A logical task cannot depend on itself.",
                    )
                if dependency not in by_key:
                    raise InvalidTransitionError(
                        operation="delegate_work",
                        resource_id=dependency,
                        message=f"Unknown logical task dependency {dependency!r}.",
                    )
                if dependency in seen_dependencies:
                    continue
                seen_dependencies.add(dependency)
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
                operation="delegate_work",
                resource_id="tasks",
                message="Logical task dependencies contain a cycle.",
            )
        return tuple(by_key[key] for key in ordered_keys)


def _resolve_requested_parallelism(
    *,
    requested_parallelism: int | None,
    tasks: tuple[DelegationTaskSpec, ...],
) -> int:
    """Resolve a persisted parallelism request without inferring browser capacity.

    Args:
        requested_parallelism: Explicit Main/backend request, if supplied.
        tasks: Validated logical DAG.

    Returns:
        Explicit request, or the number of independent DAG roots.

    Raises:
        ValueError: If an explicit request is less than one.
    """
    if requested_parallelism is not None:
        if requested_parallelism < 1:
            raise ValueError("requested_parallelism must be at least one")
        return requested_parallelism
    return max(1, sum(not task.depends_on for task in tasks))
