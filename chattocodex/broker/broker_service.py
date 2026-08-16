import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

from chattocodex.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.broker.broker_dto import (
    BrokerAcquireResult,
    BrokerAssignment,
    BrokerRunSnapshot,
    BrokerTaskState,
    BrokerTaskStatus,
    CreatedWorkerSlot,
    FinishedAssignment,
    WorkerSlotState,
    WorkerSlotStatus,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import (
    RunStatus,
    TaskState,
    TaskStatus,
    WorkerState,
    WorkerStatus,
)
from chattocodex.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestration.orchestration_identity import (
    capability_matches,
    digest_capability,
    new_task_id,
    new_worker_handle,
    new_worker_id,
)
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class BrokerService:
    def __init__(
        self,
        *,
        run_repository: RunStateRepository,
        worker_repository: WorkerStateRepository,
        task_repository: TaskStateRepository,
        broker_task_repository: BrokerTaskRepository,
        slot_repository: WorkerSlotRepository,
        workspace_project_service: WorkspaceProjectService,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._run_repository = run_repository
        self._worker_repository = worker_repository
        self._task_repository = task_repository
        self._broker_task_repository = broker_task_repository
        self._slot_repository = slot_repository
        self._workspace_project_service = workspace_project_service
        self._lock_registry = lock_registry
        self._now_provider = now_provider or _utc_now
        self._abandoned_assignment_recovery = AbandonedAssignmentRecovery(
            broker_task_repository=broker_task_repository,
            slot_repository=slot_repository,
            task_repository=task_repository,
            lock_registry=lock_registry,
            now_provider=self._now_provider,
        )

    def register_slot(self, *, project_id: str, label: str | None) -> CreatedWorkerSlot:
        project = self._workspace_project_service.require_project(project_id=project_id)
        slot_handle = _new_slot_handle()
        now = self._now_provider()
        slot = self._slot_repository.create(
            state=WorkerSlotState(
                schema_version=1,
                slot_id=_new_slot_id(),
                project_id=project.project_id,
                slot_handle_digest=digest_capability(slot_handle),
                label=label,
                status=WorkerSlotStatus.IDLE,
                current_broker_task_id=None,
                current_run_id=None,
                current_worker_id=None,
                current_task_id=None,
                revision=0,
                created_at=now,
                updated_at=now,
            )
        )
        return CreatedWorkerSlot(state=slot, slot_handle=slot_handle)

    def reclaim_slot_for_wake(
        self,
        *,
        slot_id: str,
        project_id: str,
    ) -> CreatedWorkerSlot:
        """Rotate an idle Slot capability after its bound wake target is validated.

        The caller must first validate the current wake reservation and its
        project membership. Rotating the capability lets a persistent Worker
        Chat reclaim its backend-owned Slot without storing Slot secrets in the
        browser or creating an unreachable duplicate Slot on every wake.
        """
        project = self._workspace_project_service.require_project(project_id=project_id)
        with (
            self._lock_registry.hold(entity_key="broker:claim-task"),
            self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"),
        ):
            slot = self._slot_repository.read(slot_id=slot_id)
            if slot.project_id != project.project_id:
                raise InvalidTransitionError(
                    operation="worker_slot_reclaim",
                    resource_id=slot.slot_id,
                    message=(
                        "Wake target cannot reclaim a Worker Slot from another project."
                    ),
                )
            if slot.status is not WorkerSlotStatus.IDLE:
                raise InvalidTransitionError(
                    operation="worker_slot_reclaim",
                    resource_id=slot.slot_id,
                    message="Only an idle Worker Slot can be reclaimed by a fresh wake.",
                )
            slot_handle = _new_slot_handle()
            now = self._now_provider()
            reclaimed = self._slot_repository.write(
                state=WorkerSlotState(
                    schema_version=slot.schema_version,
                    slot_id=slot.slot_id,
                    project_id=slot.project_id,
                    slot_handle_digest=digest_capability(slot_handle),
                    label=slot.label,
                    status=slot.status,
                    current_broker_task_id=None,
                    current_run_id=None,
                    current_worker_id=None,
                    current_task_id=None,
                    revision=slot.revision + 1,
                    created_at=slot.created_at,
                    updated_at=now,
                )
            )
            return CreatedWorkerSlot(state=reclaimed, slot_handle=slot_handle)

    def get_slot(self, *, slot_id: str, slot_handle: str) -> WorkerSlotState:
        return self._require_slot_capability(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )

    def ready_project_ids(self) -> tuple[str, ...]:
        project_ids: set[str] = set()
        for broker_task in self._broker_task_repository.list_all():
            if broker_task.status is not BrokerTaskStatus.QUEUED:
                continue
            run = self._run_repository.read(run_id=broker_task.run_id)
            if run.status is not RunStatus.ACTIVE:
                continue
            if self._dependencies_completed(broker_task=broker_task):
                project_ids.add(run.project_id)
        return tuple(sorted(project_ids))

    def create_broker_task(
        self,
        *,
        run_id: str,
        run_handle: str,
        objective: str,
        role: str | None,
        depends_on: tuple[str, ...],
    ) -> BrokerTaskState:
        run = self._run_repository.read(run_id=run_id)
        if not capability_matches(
            handle=run_handle,
            expected_digest=run.run_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="broker_task_create",
                resource_id=run_id,
            )
        if run.status is not RunStatus.ACTIVE:
            raise InvalidTransitionError(
                operation="broker_task_create",
                resource_id=run_id,
                message="Broker Tasks can only be published to an active Run.",
            )
        for dependency_id in depends_on:
            dependency = self._broker_task_repository.read(broker_task_id=dependency_id)
            if dependency.run_id != run.run_id:
                raise InvalidTransitionError(
                    operation="broker_task_create",
                    resource_id=dependency_id,
                    message="Broker Task dependencies must belong to the same Run.",
                )

        now = self._now_provider()
        return self._broker_task_repository.create(
            state=BrokerTaskState(
                schema_version=1,
                broker_task_id=_new_broker_task_id(),
                run_id=run.run_id,
                objective=objective,
                role=role,
                depends_on=depends_on,
                status=BrokerTaskStatus.QUEUED,
                assigned_slot_id=None,
                assigned_worker_id=None,
                assigned_task_id=None,
                revision=0,
                created_at=now,
                updated_at=now,
            )
        )

    def claim_task(
        self,
        *,
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAssignment | None:
        with (
            self._lock_registry.hold(entity_key="broker:claim-task"),
            self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"),
        ):
            return self._claim_task_locked(
                slot_id=slot_id,
                slot_handle=slot_handle,
            )

    def claim_task_for_run(
        self,
        *,
        slot_id: str,
        slot_handle: str,
        run_id: str,
    ) -> BrokerAssignment | None:
        with (
            self._lock_registry.hold(entity_key="broker:claim-task"),
            self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"),
        ):
            return self._claim_task_locked(
                slot_id=slot_id,
                slot_handle=slot_handle,
                run_id=run_id,
            )

    def _claim_task_locked(
        self,
        *,
        slot_id: str,
        slot_handle: str,
        run_id: str | None = None,
    ) -> BrokerAssignment | None:
        slot = self._require_slot_capability(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        if slot.status is not WorkerSlotStatus.IDLE:
            raise InvalidTransitionError(
                operation="worker_claim_task",
                resource_id=slot.slot_id,
                message="Worker Slot is already busy with an assignment.",
            )
        broker_task = (
            self._next_ready_task(project_id=slot.project_id)
            if run_id is None
            else self._next_ready_task(project_id=slot.project_id, run_id=run_id)
        )
        if broker_task is None:
            return None

        run = self._run_repository.read(run_id=broker_task.run_id)
        now = self._now_provider()
        worker_handle = new_worker_handle()
        worker = self._worker_repository.create(
            state=WorkerState(
                schema_version=1,
                run_id=run.run_id,
                worker_id=new_worker_id(),
                worker_handle_digest=digest_capability(worker_handle),
                status=WorkerStatus.ACTIVE,
                role=broker_task.role,
                revision=0,
                created_at=now,
                updated_at=now,
            )
        )
        task = self._task_repository.create(
            state=TaskState(
                schema_version=1,
                run_id=run.run_id,
                task_id=new_task_id(),
                assigned_worker_id=worker.worker_id,
                objective=broker_task.objective,
                status=TaskStatus.IN_PROGRESS,
                revision=0,
                progress_summary="Claimed from Broker by a generic Worker Slot.",
                result=None,
                created_at=now,
                updated_at=now,
            )
        )
        claimed_task = self._broker_task_repository.write(
            state=BrokerTaskState(
                schema_version=broker_task.schema_version,
                broker_task_id=broker_task.broker_task_id,
                run_id=broker_task.run_id,
                objective=broker_task.objective,
                role=broker_task.role,
                depends_on=broker_task.depends_on,
                status=BrokerTaskStatus.CLAIMED,
                assigned_slot_id=slot.slot_id,
                assigned_worker_id=worker.worker_id,
                assigned_task_id=task.task_id,
                revision=broker_task.revision + 1,
                created_at=broker_task.created_at,
                updated_at=now,
            )
        )
        busy_slot = self._slot_repository.write(
            state=WorkerSlotState(
                schema_version=slot.schema_version,
                slot_id=slot.slot_id,
                project_id=slot.project_id,
                slot_handle_digest=slot.slot_handle_digest,
                label=slot.label,
                status=WorkerSlotStatus.BUSY,
                current_broker_task_id=claimed_task.broker_task_id,
                current_run_id=run.run_id,
                current_worker_id=worker.worker_id,
                current_task_id=task.task_id,
                revision=slot.revision + 1,
                created_at=slot.created_at,
                updated_at=now,
            )
        )
        return BrokerAssignment(
            broker_task=claimed_task,
            slot=busy_slot,
            run=run,
            worker=worker,
            task=task,
            worker_handle=worker_handle,
        )

    def acquire_task(
        self,
        *,
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAcquireResult:
        slot = self._require_slot_capability(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        if slot.status is WorkerSlotStatus.IDLE:
            assignment = self.claim_task(slot_id=slot_id, slot_handle=slot_handle)
            return BrokerAcquireResult(
                assignment=assignment,
                slot=assignment.slot if assignment is not None else slot,
                resumed=False,
                released_previous=False,
            )

        if slot.current_run_id is None or slot.current_task_id is None:
            raise InvalidTransitionError(
                operation="work",
                resource_id=slot.slot_id,
                message="Busy Worker Slot is missing assignment metadata.",
            )
        task = self._task_repository.read(
            run_id=slot.current_run_id,
            task_id=slot.current_task_id,
        )
        if task.status.is_terminal:
            self.finish_assignment(slot_id=slot_id, slot_handle=slot_handle)
            assignment = self.claim_task(slot_id=slot_id, slot_handle=slot_handle)
            current_slot = (
                assignment.slot
                if assignment is not None
                else self._require_slot_capability(
                    slot_id=slot_id,
                    slot_handle=slot_handle,
                )
            )
            return BrokerAcquireResult(
                assignment=assignment,
                slot=current_slot,
                resumed=False,
                released_previous=True,
            )

        assignment = self._resume_assignment(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        return BrokerAcquireResult(
            assignment=assignment,
            slot=assignment.slot,
            resumed=True,
            released_previous=False,
        )

    def finish_assignment(
        self,
        *,
        slot_id: str,
        slot_handle: str,
    ) -> FinishedAssignment:
        with self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"):
            slot = self._require_slot_capability(
                slot_id=slot_id,
                slot_handle=slot_handle,
            )
            if slot.status is not WorkerSlotStatus.BUSY:
                raise InvalidTransitionError(
                    operation="worker_assignment_finish",
                    resource_id=slot.slot_id,
                    message="Worker Slot has no active assignment to finish.",
                )
            if (
                slot.current_broker_task_id is None
                or slot.current_run_id is None
                or slot.current_task_id is None
            ):
                raise InvalidTransitionError(
                    operation="worker_assignment_finish",
                    resource_id=slot.slot_id,
                    message="Busy Worker Slot is missing assignment metadata.",
                )
            broker_task = self._broker_task_repository.read(
                broker_task_id=slot.current_broker_task_id
            )
            task = self._task_repository.read(
                run_id=slot.current_run_id,
                task_id=slot.current_task_id,
            )
            if not task.status.is_terminal:
                raise InvalidTransitionError(
                    operation="worker_assignment_finish",
                    resource_id=task.task_id,
                    message="Underlying Task must be terminal before releasing the Worker Slot.",
                )
            if (
                broker_task.assigned_slot_id != slot.slot_id
                or broker_task.assigned_task_id != task.task_id
            ):
                raise InvalidTransitionError(
                    operation="worker_assignment_finish",
                    resource_id=broker_task.broker_task_id,
                    message="Broker assignment does not match the Worker Slot state.",
                )

            now = self._now_provider()
            broker_status = (
                BrokerTaskStatus.COMPLETED
                if task.status is TaskStatus.COMPLETED
                else BrokerTaskStatus.FAILED
            )
            finished_task = self._broker_task_repository.write(
                state=BrokerTaskState(
                    schema_version=broker_task.schema_version,
                    broker_task_id=broker_task.broker_task_id,
                    run_id=broker_task.run_id,
                    objective=broker_task.objective,
                    role=broker_task.role,
                    depends_on=broker_task.depends_on,
                    status=broker_status,
                    assigned_slot_id=broker_task.assigned_slot_id,
                    assigned_worker_id=broker_task.assigned_worker_id,
                    assigned_task_id=broker_task.assigned_task_id,
                    revision=broker_task.revision + 1,
                    created_at=broker_task.created_at,
                    updated_at=now,
                )
            )
            idle_slot = self._slot_repository.write(
                state=WorkerSlotState(
                    schema_version=slot.schema_version,
                    slot_id=slot.slot_id,
                    project_id=slot.project_id,
                    slot_handle_digest=slot.slot_handle_digest,
                    label=slot.label,
                    status=WorkerSlotStatus.IDLE,
                    current_broker_task_id=None,
                    current_run_id=None,
                    current_worker_id=None,
                    current_task_id=None,
                    revision=slot.revision + 1,
                    created_at=slot.created_at,
                    updated_at=now,
                )
            )
            return FinishedAssignment(
                broker_task=finished_task,
                slot=idle_slot,
                task=task,
            )

    def recover_abandoned_assignment(
        self,
        *,
        slot_id: str,
        reason: str,
    ) -> WorkerSlotState:
        recovered = self._abandoned_assignment_recovery.recover(
            slot_id=slot_id,
            reason=reason,
        )
        if recovered is None:
            raise RuntimeError(
                "Unconditional abandoned-assignment recovery returned no slot."
            )
        return recovered

    def get_run_status(self, *, run_id: str) -> BrokerRunSnapshot:
        run = self._run_repository.read(run_id=run_id)
        broker_tasks = tuple(
            sorted(
                (
                    task
                    for task in self._broker_task_repository.list_all()
                    if task.run_id == run.run_id
                ),
                key=lambda task: (task.created_at, task.broker_task_id),
            )
        )
        slots = tuple(
            sorted(
                (
                    slot
                    for slot in self._slot_repository.list_all()
                    if slot.project_id == run.project_id
                ),
                key=lambda slot: (slot.created_at, slot.slot_id),
            )
        )
        ready_count = sum(
            task.status is BrokerTaskStatus.QUEUED
            and run.status is RunStatus.ACTIVE
            and self._dependencies_completed(broker_task=task)
            for task in broker_tasks
        )
        queued_count = sum(
            task.status is BrokerTaskStatus.QUEUED for task in broker_tasks
        )
        claimed_count = sum(
            task.status is BrokerTaskStatus.CLAIMED for task in broker_tasks
        )
        completed_count = sum(
            task.status is BrokerTaskStatus.COMPLETED for task in broker_tasks
        )
        failed_count = sum(
            task.status is BrokerTaskStatus.FAILED for task in broker_tasks
        )
        idle_slot_count = sum(slot.status is WorkerSlotStatus.IDLE for slot in slots)
        busy_slot_count = sum(slot.status is WorkerSlotStatus.BUSY for slot in slots)
        return BrokerRunSnapshot(
            run=run,
            broker_tasks=broker_tasks,
            slots=slots,
            ready_count=ready_count,
            queued_count=queued_count,
            claimed_count=claimed_count,
            completed_count=completed_count,
            failed_count=failed_count,
            idle_slot_count=idle_slot_count,
            busy_slot_count=busy_slot_count,
            is_complete=bool(broker_tasks) and completed_count == len(broker_tasks),
        )

    def _next_ready_task(
        self,
        *,
        project_id: str,
        run_id: str | None = None,
    ) -> BrokerTaskState | None:
        queued = sorted(
            (
                task
                for task in self._broker_task_repository.list_all()
                if task.status is BrokerTaskStatus.QUEUED
            ),
            key=lambda task: (task.created_at, task.broker_task_id),
        )
        for broker_task in queued:
            if run_id is not None and broker_task.run_id != run_id:
                continue
            run = self._run_repository.read(run_id=broker_task.run_id)
            if run.status is not RunStatus.ACTIVE or run.project_id != project_id:
                continue
            if self._dependencies_completed(broker_task=broker_task):
                return broker_task
        return None

    def _resume_assignment(
        self,
        *,
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAssignment:
        with self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"):
            slot = self._require_slot_capability(
                slot_id=slot_id,
                slot_handle=slot_handle,
            )
            if slot.status is not WorkerSlotStatus.BUSY:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=slot.slot_id,
                    message="Worker Slot is no longer busy; retry work acquisition.",
                )
            if (
                slot.current_broker_task_id is None
                or slot.current_run_id is None
                or slot.current_worker_id is None
                or slot.current_task_id is None
            ):
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=slot.slot_id,
                    message="Busy Worker Slot is missing assignment metadata.",
                )

            broker_task = self._broker_task_repository.read(
                broker_task_id=slot.current_broker_task_id
            )
            run = self._run_repository.read(run_id=slot.current_run_id)
            task = self._task_repository.read(
                run_id=slot.current_run_id,
                task_id=slot.current_task_id,
            )
            if task.status.is_terminal:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=task.task_id,
                    message="Terminal assignment must be released before it can be resumed.",
                )
            worker = self._worker_repository.read(
                run_id=slot.current_run_id,
                worker_id=slot.current_worker_id,
            )
            worker_handle = new_worker_handle()
            now = self._now_provider()
            refreshed_worker = self._worker_repository.write(
                state=WorkerState(
                    schema_version=worker.schema_version,
                    run_id=worker.run_id,
                    worker_id=worker.worker_id,
                    worker_handle_digest=digest_capability(worker_handle),
                    status=worker.status,
                    role=worker.role,
                    revision=worker.revision + 1,
                    created_at=worker.created_at,
                    updated_at=now,
                )
            )
            return BrokerAssignment(
                broker_task=broker_task,
                slot=slot,
                run=run,
                worker=refreshed_worker,
                task=task,
                worker_handle=worker_handle,
            )

    def _dependencies_completed(self, *, broker_task: BrokerTaskState) -> bool:
        return all(
            self._broker_task_repository.read(broker_task_id=dependency_id).status
            is BrokerTaskStatus.COMPLETED
            for dependency_id in broker_task.depends_on
        )

    def _require_slot_capability(
        self,
        *,
        slot_id: str,
        slot_handle: str,
    ) -> WorkerSlotState:
        slot = self._slot_repository.read(slot_id=slot_id)
        if not capability_matches(
            handle=slot_handle,
            expected_digest=slot.slot_handle_digest,
        ):
            raise CapabilityDeniedError(
                operation="validate_worker_slot_capability",
                resource_id=slot_id,
            )
        return slot


def _new_broker_task_id() -> str:
    return f"btask_{uuid4().hex[:24]}"


def _new_slot_id() -> str:
    return f"slot_{uuid4().hex[:24]}"


def _new_slot_handle() -> str:
    return f"slh_{secrets.token_urlsafe(32)}"


def _utc_now() -> datetime:
    return datetime.now(UTC)
