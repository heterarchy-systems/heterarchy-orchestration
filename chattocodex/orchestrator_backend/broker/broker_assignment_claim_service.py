from collections.abc import Callable
from datetime import datetime

from chattocodex.orchestrator_backend.broker.broker_assignment_completion_service import (
    BrokerAssignmentCompletionService,
)
from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerAcquireResult,
    BrokerAssignment,
    BrokerTaskState,
    BrokerTaskStatus,
    WorkerSlotState,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.broker.broker_slot_capability_policy import (
    require_slot_capability,
)
from chattocodex.orchestrator_backend.broker.external_broker_assignment_service import (
    ExternalBrokerAssignmentService,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunStatus,
    TaskState,
    TaskStatus,
    WorkerState,
    WorkerStatus,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_identity import (
    digest_capability,
    new_task_id,
    new_worker_handle,
    new_worker_id,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class BrokerAssignmentClaimService:
    """Own Worker Slot claim, resume, and reusable acquisition flow."""

    def __init__(
        self,
        slot_repository: WorkerSlotRepository,
        broker_task_repository: BrokerTaskRepository,
        run_repository: RunStateRepository,
        worker_repository: WorkerStateRepository,
        task_repository: TaskStateRepository,
        external_broker_assignment_service: ExternalBrokerAssignmentService,
        completion_service: BrokerAssignmentCompletionService,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime],
    ) -> None:
        """Initialize this object. Args: slot_repository, broker_task_repository, run_repository, worker_repository, task_repository, external_broker_assignment_service, completion_service, lock_registry, now_provider. Returns: None."""
        self._slot_repository = slot_repository
        self._broker_task_repository = broker_task_repository
        self._run_repository = run_repository
        self._worker_repository = worker_repository
        self._task_repository = task_repository
        self._external_broker_assignment_service = external_broker_assignment_service
        self._completion_service = completion_service
        self._lock_registry = lock_registry
        self._now_provider = now_provider

    def claim_task(
        self,
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAssignment | None:
        """Claim the next dependency-ready task for one idle Worker Slot. Args: slot_id, slot_handle. Returns: BrokerAssignment | None."""

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
        slot_id: str,
        slot_handle: str,
        run_id: str,
    ) -> BrokerAssignment | None:
        """Claim the next dependency-ready task for one exact Run. Args: slot_id, slot_handle, run_id. Returns: BrokerAssignment | None."""

        with (
            self._lock_registry.hold(entity_key="broker:claim-task"),
            self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"),
        ):
            return self._claim_task_locked(
                slot_id=slot_id,
                slot_handle=slot_handle,
                run_id=run_id,
            )

    def acquire_task(
        self,
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAcquireResult:
        """Claim, resume, or recycle one Worker Slot assignment. Args: slot_id, slot_handle. Returns: BrokerAcquireResult."""

        slot = require_slot_capability(
            slot_repository=self._slot_repository,
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
            self._completion_service.finish_assignment(
                slot_id=slot_id,
                slot_handle=slot_handle,
            )
            assignment = self.claim_task(slot_id=slot_id, slot_handle=slot_handle)
            current_slot = (
                assignment.slot
                if assignment is not None
                else require_slot_capability(
                    slot_repository=self._slot_repository,
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

    def _claim_task_locked(
        self,
        slot_id: str,
        slot_handle: str,
        run_id: str | None = None,
    ) -> BrokerAssignment | None:
        """Claim task locked. Args: slot_id, slot_handle, run_id. Returns: BrokerAssignment | None."""
        slot = require_slot_capability(
            slot_repository=self._slot_repository,
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        if slot.status is not WorkerSlotStatus.IDLE:
            raise InvalidTransitionError(
                operation="worker_claim_task",
                resource_id=slot.slot_id,
                message="Worker Slot is already busy with an assignment.",
            )
        self._external_broker_assignment_service.publish_ready_tasks(
            project_id=slot.project_id,
            run_id=run_id,
        )
        candidate = self._next_ready_task(
            project_id=slot.project_id,
            run_id=run_id,
        )
        if candidate is None:
            return None
        target_run_id = candidate.run_id if run_id is None else run_id
        claim = self._external_broker_assignment_service.claim_task(
            project_id=slot.project_id,
            run_id=target_run_id,
            member_id=slot.broker_consumer_id,
        )
        if claim is None:
            return None
        broker_task = self._broker_task_repository.read(broker_task_id=claim.task_id)
        if (
            broker_task.run_id != target_run_id
            or broker_task.status is not BrokerTaskStatus.QUEUED
            or not broker_task.external_published
            or not self._dependencies_completed(broker_task=broker_task)
            or broker_task.objective != claim.objective
        ):
            raise InvalidTransitionError(
                operation="worker_claim_task",
                resource_id=claim.task_id,
                message="External Broker claim does not match the local orchestration DAG projection.",
            )

        run = self._run_repository.read(run_id=target_run_id)
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
                external_published=broker_task.external_published,
            )
        )
        busy_slot = self._slot_repository.write(
            state=WorkerSlotState(
                schema_version=slot.schema_version,
                slot_id=slot.slot_id,
                project_id=slot.project_id,
                broker_consumer_id=slot.broker_consumer_id,
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
                broker_term=claim.term,
                broker_group_generation=claim.generation,
                broker_lease_id=claim.lease_id,
                broker_lease_epoch=claim.lease_epoch,
                broker_lease_expires_at_ms=claim.lease_expires_at_ms,
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

    def _next_ready_task(
        self,
        project_id: str,
        run_id: str | None = None,
    ) -> BrokerTaskState | None:
        """Next ready task. Args: project_id, run_id. Returns: BrokerTaskState | None."""
        queued = sorted(
            (
                task
                for task in self._broker_task_repository.list_all()
                if task.status is BrokerTaskStatus.QUEUED and task.external_published
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
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAssignment:
        """Resume assignment. Args: slot_id, slot_handle. Returns: BrokerAssignment."""
        with self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"):
            slot = require_slot_capability(
                slot_repository=self._slot_repository,
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
            renewed_slot = self._external_broker_assignment_service.renew_assignment(
                slot=slot
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
                slot=renewed_slot,
                run=run,
                worker=refreshed_worker,
                task=task,
                worker_handle=worker_handle,
            )

    def _dependencies_completed(self, broker_task: BrokerTaskState) -> bool:
        """Dependencies completed. Args: broker_task. Returns: bool."""
        return all(
            self._broker_task_repository.read(broker_task_id=dependency_id).status
            is BrokerTaskStatus.COMPLETED
            for dependency_id in broker_task.depends_on
        )
