from collections.abc import Callable
from datetime import UTC, datetime

from chattocodex.broker.broker_dto import (
    BrokerTaskState,
    BrokerTaskStatus,
    WorkerSlotState,
    WorkerSlotStatus,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_dto import TaskState, TaskStatus
from chattocodex.orchestration.orchestration_exceptions import InvalidTransitionError
from chattocodex.persistence.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.persistence.orchestration_state_repository import TaskStateRepository
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository

_RECOVERY_PROGRESS_PREFIX = "Assignment lease revoked: "


class AbandonedAssignmentRecovery:
    """Coordinator-only transition for reclaiming work from a dead consumer."""

    def __init__(
        self,
        *,
        broker_task_repository: BrokerTaskRepository,
        slot_repository: WorkerSlotRepository,
        task_repository: TaskStateRepository,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._broker_task_repository = broker_task_repository
        self._slot_repository = slot_repository
        self._task_repository = task_repository
        self._lock_registry = lock_registry
        self._now_provider = now_provider or _utc_now

    def recover(
        self,
        *,
        slot_id: str,
        reason: str,
        should_recover: Callable[[WorkerSlotState], bool] | None = None,
        after_recovery: Callable[[WorkerSlotState], None] | None = None,
        expected_last_seen_at: datetime | None = None,
        wake_target_id: str | None = None,
        wake_presence_repository: WakePresenceRepository | None = None,
    ) -> WorkerSlotState | None:
        with (
            self._lock_registry.hold(entity_key="broker:claim-task"),
            self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"),
        ):
            slot = self._slot_repository.read(slot_id=slot_id)
            if slot.status is WorkerSlotStatus.IDLE:
                return slot
            if (
                expected_last_seen_at is not None
                and wake_target_id is not None
                and wake_presence_repository is not None
                and wake_presence_repository.exists(wake_target_id=wake_target_id)
                and wake_presence_repository.read(
                    wake_target_id=wake_target_id
                ).last_seen_at
                != expected_last_seen_at
            ):
                return None
            if should_recover is not None and not should_recover(slot):
                return slot
            run_id, task_id = self._require_slot_assignment(slot=slot)
            with self._lock_registry.hold(entity_key=f"task:{run_id}:{task_id}"):
                recovered = self._recover_assignment_locked(
                    slot_id=slot_id,
                    reason=reason,
                )
            if after_recovery is not None:
                after_recovery(recovered)
            return recovered

    def _recover_assignment_locked(
        self,
        *,
        slot_id: str,
        reason: str,
    ) -> WorkerSlotState:
        slot = self._slot_repository.read(slot_id=slot_id)
        run_id, task_id = self._require_slot_assignment(slot=slot)
        broker_task = self._broker_task_repository.read(
            broker_task_id=slot.current_broker_task_id or ""
        )
        task = self._task_repository.read(run_id=run_id, task_id=task_id)
        now = self._now_provider()

        if _is_recovery_failed_task(task=task):
            desired_broker_status = BrokerTaskStatus.QUEUED
            retain_assignment = False
        elif task.status.is_terminal:
            desired_broker_status = (
                BrokerTaskStatus.COMPLETED
                if task.status is TaskStatus.COMPLETED
                else BrokerTaskStatus.FAILED
            )
            retain_assignment = True
        else:
            self._require_claimed_assignment(
                slot=slot,
                broker_task=broker_task,
                task=task,
            )
            task = self._task_repository.write(
                state=TaskState(
                    schema_version=task.schema_version,
                    run_id=task.run_id,
                    task_id=task.task_id,
                    assigned_worker_id=task.assigned_worker_id,
                    objective=task.objective,
                    status=TaskStatus.FAILED,
                    revision=task.revision + 1,
                    progress_summary=f"{_RECOVERY_PROGRESS_PREFIX}{reason}",
                    result=None,
                    created_at=task.created_at,
                    updated_at=now,
                ),
                expected_revision=task.revision,
            )
            desired_broker_status = BrokerTaskStatus.QUEUED
            retain_assignment = False

        if not _broker_recovery_phase_matches(
            slot=slot,
            broker_task=broker_task,
            task=task,
            desired_status=desired_broker_status,
            retain_assignment=retain_assignment,
        ):
            raise InvalidTransitionError(
                operation="recover_abandoned_assignment",
                resource_id=broker_task.broker_task_id,
                message="Broker assignment does not match the abandoned Worker Slot.",
            )

        if broker_task.status is not desired_broker_status:
            broker_task = self._broker_task_repository.write(
                state=BrokerTaskState(
                    schema_version=broker_task.schema_version,
                    broker_task_id=broker_task.broker_task_id,
                    run_id=broker_task.run_id,
                    objective=broker_task.objective,
                    role=broker_task.role,
                    depends_on=broker_task.depends_on,
                    status=desired_broker_status,
                    assigned_slot_id=(
                        broker_task.assigned_slot_id if retain_assignment else None
                    ),
                    assigned_worker_id=(
                        broker_task.assigned_worker_id if retain_assignment else None
                    ),
                    assigned_task_id=(
                        broker_task.assigned_task_id if retain_assignment else None
                    ),
                    revision=broker_task.revision + 1,
                    created_at=broker_task.created_at,
                    updated_at=now,
                )
            )

        return self._slot_repository.write(
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

    def _require_slot_assignment(self, *, slot: WorkerSlotState) -> tuple[str, str]:
        if (
            slot.status is not WorkerSlotStatus.BUSY
            or slot.current_broker_task_id is None
            or slot.current_run_id is None
            or slot.current_task_id is None
        ):
            raise InvalidTransitionError(
                operation="recover_abandoned_assignment",
                resource_id=slot.slot_id,
                message="Busy Worker Slot is missing assignment metadata.",
            )
        return slot.current_run_id, slot.current_task_id

    def _require_claimed_assignment(
        self,
        *,
        slot: WorkerSlotState,
        broker_task: BrokerTaskState,
        task: TaskState,
    ) -> None:
        if not _claimed_assignment_matches(
            slot=slot,
            broker_task=broker_task,
            task=task,
        ):
            raise InvalidTransitionError(
                operation="recover_abandoned_assignment",
                resource_id=broker_task.broker_task_id,
                message="Broker assignment does not match the abandoned Worker Slot.",
            )


def _broker_recovery_phase_matches(
    *,
    slot: WorkerSlotState,
    broker_task: BrokerTaskState,
    task: TaskState,
    desired_status: BrokerTaskStatus,
    retain_assignment: bool,
) -> bool:
    if _claimed_assignment_matches(
        slot=slot,
        broker_task=broker_task,
        task=task,
    ):
        return True
    if broker_task.status is not desired_status:
        return False
    if retain_assignment:
        return _assignment_identity_matches(
            slot=slot,
            broker_task=broker_task,
            task=task,
        )
    return (
        broker_task.assigned_slot_id is None
        and broker_task.assigned_worker_id is None
        and broker_task.assigned_task_id is None
    )


def _claimed_assignment_matches(
    *,
    slot: WorkerSlotState,
    broker_task: BrokerTaskState,
    task: TaskState,
) -> bool:
    return (
        broker_task.status is BrokerTaskStatus.CLAIMED
        and _assignment_identity_matches(
            slot=slot,
            broker_task=broker_task,
            task=task,
        )
    )


def _assignment_identity_matches(
    *,
    slot: WorkerSlotState,
    broker_task: BrokerTaskState,
    task: TaskState,
) -> bool:
    return (
        broker_task.run_id == slot.current_run_id == task.run_id
        and broker_task.assigned_slot_id == slot.slot_id
        and broker_task.assigned_worker_id == slot.current_worker_id
        and broker_task.assigned_worker_id == task.assigned_worker_id
        and broker_task.assigned_task_id == slot.current_task_id == task.task_id
    )


def _is_recovery_failed_task(*, task: TaskState) -> bool:
    return (
        task.status is TaskStatus.FAILED
        and task.result is None
        and bool(task.progress_summary)
        and task.progress_summary.startswith(_RECOVERY_PROGRESS_PREFIX)
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)
