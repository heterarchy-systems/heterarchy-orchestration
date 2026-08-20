from collections.abc import Callable
from datetime import datetime

from chattocodex.orchestrator_backend.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskState,
    BrokerTaskStatus,
    FinishedAssignment,
    WorkerSlotState,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.broker.broker_slot_capability_policy import (
    require_slot_capability,
)
from chattocodex.orchestrator_backend.broker.external_broker_assignment_service import (
    ExternalBrokerAssignmentService,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import TaskStatus
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    TaskStateRepository,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class BrokerAssignmentCompletionService:
    """Own terminal assignment completion, Slot release, and abandoned recovery."""

    def __init__(
        self,
        slot_repository: WorkerSlotRepository,
        broker_task_repository: BrokerTaskRepository,
        task_repository: TaskStateRepository,
        abandoned_assignment_recovery: AbandonedAssignmentRecovery,
        external_broker_assignment_service: ExternalBrokerAssignmentService,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime],
    ) -> None:
        """Initialize this object. Args: slot_repository, broker_task_repository, task_repository, abandoned_assignment_recovery, external_broker_assignment_service, lock_registry, now_provider. Returns: None."""
        self._slot_repository = slot_repository
        self._broker_task_repository = broker_task_repository
        self._task_repository = task_repository
        self._abandoned_assignment_recovery = abandoned_assignment_recovery
        self._external_broker_assignment_service = external_broker_assignment_service
        self._lock_registry = lock_registry
        self._now_provider = now_provider

    def finish_assignment(
        self,
        slot_id: str,
        slot_handle: str,
    ) -> FinishedAssignment:
        """Complete a terminal local task and release its reusable Worker Slot. Args: slot_id, slot_handle. Returns: FinishedAssignment."""

        with self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"):
            slot = require_slot_capability(
                slot_repository=self._slot_repository,
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
            self._external_broker_assignment_service.complete_assignment(
                slot=slot,
                task=task,
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
                    external_published=broker_task.external_published,
                )
            )
            idle_slot = self._slot_repository.write(
                state=WorkerSlotState(
                    schema_version=slot.schema_version,
                    slot_id=slot.slot_id,
                    project_id=slot.project_id,
                    broker_consumer_id=slot.broker_consumer_id,
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
            if broker_status is BrokerTaskStatus.COMPLETED:
                self._external_broker_assignment_service.publish_ready_tasks(
                    project_id=slot.project_id,
                    run_id=broker_task.run_id,
                )
            return FinishedAssignment(
                broker_task=finished_task,
                slot=idle_slot,
                task=task,
            )

    def recover_abandoned_assignment(
        self,
        slot_id: str,
        reason: str,
    ) -> WorkerSlotState:
        """Recover an abandoned assignment and require a concrete Slot result. Args: slot_id, reason. Returns: WorkerSlotState."""

        recovered = self._abandoned_assignment_recovery.recover(
            slot_id=slot_id,
            reason=reason,
        )
        if recovered is None:
            raise RuntimeError(
                "Unconditional abandoned-assignment recovery returned no slot."
            )
        return recovered
