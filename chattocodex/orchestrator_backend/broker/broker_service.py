from collections.abc import Callable
from datetime import datetime

from chattocodex.orchestrator_backend.broker.abandoned_assignment_recovery import (
    AbandonedAssignmentRecovery,
)
from chattocodex.orchestrator_backend.broker.broker_assignment_claim_service import (
    BrokerAssignmentClaimService,
)
from chattocodex.orchestrator_backend.broker.broker_assignment_completion_service import (
    BrokerAssignmentCompletionService,
)
from chattocodex.orchestrator_backend.broker.broker_dto import (
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
from chattocodex.orchestrator_backend.broker.broker_slot_capability_policy import (
    require_slot_capability,
)
from chattocodex.orchestrator_backend.broker.external_broker_assignment_service import (
    ExternalBrokerAssignmentService,
)
from chattocodex.orchestrator_backend.broker.foundation.broker_identity import (
    new_broker_consumer_id as _new_broker_consumer_id,
    new_broker_task_id as _new_broker_task_id,
    new_slot_handle as _new_slot_handle,
    new_slot_id as _new_slot_id,
    utc_now as _utc_now,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunStatus,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    CapabilityDeniedError,
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_identity import (
    capability_matches,
    digest_capability,
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
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.shared.contracts.broker.broker_gateway_contract import (
    WorkBrokerGateway,
)


class BrokerService:
    """Coordinate orchestration projections with the external work Broker.

    The historical class name is retained for compatibility. This service no
    longer implements Broker authority: it owns Run/DAG readiness, Worker-slot
    projection, and translation through `WorkBrokerGateway` only.
    """

    def __init__(
        self,
        run_repository: RunStateRepository,
        worker_repository: WorkerStateRepository,
        task_repository: TaskStateRepository,
        broker_task_repository: BrokerTaskRepository,
        slot_repository: WorkerSlotRepository,
        work_broker_gateway: WorkBrokerGateway,
        workspace_project_service: WorkspaceProjectService,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: run_repository, worker_repository, task_repository, broker_task_repository, slot_repository, work_broker_gateway, workspace_project_service, lock_registry, now_provider. Returns: None."""
        self._run_repository = run_repository
        self._worker_repository = worker_repository
        self._task_repository = task_repository
        self._broker_task_repository = broker_task_repository
        self._slot_repository = slot_repository
        self._work_broker_gateway = work_broker_gateway
        self._workspace_project_service = workspace_project_service
        self._lock_registry = lock_registry
        self._now_provider = now_provider or _utc_now
        self._external_broker_assignment_service = ExternalBrokerAssignmentService(
            run_repository=run_repository,
            broker_task_repository=broker_task_repository,
            slot_repository=slot_repository,
            work_broker_gateway=work_broker_gateway,
            now_provider=self._now_provider,
        )
        self._abandoned_assignment_recovery = AbandonedAssignmentRecovery(
            broker_task_repository=broker_task_repository,
            slot_repository=slot_repository,
            task_repository=task_repository,
            lock_registry=lock_registry,
            now_provider=self._now_provider,
        )
        self._assignment_completion_service = BrokerAssignmentCompletionService(
            slot_repository=slot_repository,
            broker_task_repository=broker_task_repository,
            task_repository=task_repository,
            abandoned_assignment_recovery=self._abandoned_assignment_recovery,
            external_broker_assignment_service=self._external_broker_assignment_service,
            lock_registry=lock_registry,
            now_provider=self._now_provider,
        )
        self._assignment_claim_service = BrokerAssignmentClaimService(
            slot_repository=slot_repository,
            broker_task_repository=broker_task_repository,
            run_repository=run_repository,
            worker_repository=worker_repository,
            task_repository=task_repository,
            external_broker_assignment_service=self._external_broker_assignment_service,
            completion_service=self._assignment_completion_service,
            lock_registry=lock_registry,
            now_provider=self._now_provider,
        )

    def register_slot(self, project_id: str, label: str | None) -> CreatedWorkerSlot:
        """Register slot. Args: project_id, label. Returns: CreatedWorkerSlot."""
        project = self._workspace_project_service.require_project(project_id=project_id)
        slot_handle = _new_slot_handle()
        now = self._now_provider()
        slot = self._slot_repository.create(
            state=WorkerSlotState(
                schema_version=1,
                slot_id=_new_slot_id(),
                project_id=project.project_id,
                broker_consumer_id=_new_broker_consumer_id(),
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
        slot_id: str,
        project_id: str,
    ) -> CreatedWorkerSlot:
        """Rotate an idle Slot capability after its bound wake target is validated.

        The caller must first validate the current wake reservation and its
        project membership. Rotating the capability lets a persistent Worker
        Chat reclaim its backend-owned Slot without storing Slot secrets in the
        browser or creating an unreachable duplicate Slot on every wake. Args: slot_id, project_id. Returns: CreatedWorkerSlot."""
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
                    broker_consumer_id=slot.broker_consumer_id,
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

    def get_slot(self, slot_id: str, slot_handle: str) -> WorkerSlotState:
        """Get slot. Args: slot_id, slot_handle. Returns: WorkerSlotState."""
        return require_slot_capability(
            slot_repository=self._slot_repository,
            slot_id=slot_id,
            slot_handle=slot_handle,
        )

    def ready_project_ids(self) -> tuple[str, ...]:
        """Ready project ids. Args: None. Returns: tuple[str, ...]."""
        project_ids: set[str] = set()
        broker_tasks = self._broker_task_repository.list_all()
        completed_task_ids = frozenset(
            task.broker_task_id
            for task in broker_tasks
            if task.status is BrokerTaskStatus.COMPLETED
        )
        for broker_task in broker_tasks:
            if broker_task.status is not BrokerTaskStatus.QUEUED:
                continue
            run = self._run_repository.read(run_id=broker_task.run_id)
            if run.status is not RunStatus.ACTIVE:
                continue
            if all(
                dependency_id in completed_task_ids
                for dependency_id in broker_task.depends_on
            ):
                project_ids.add(run.project_id)
        return tuple(sorted(project_ids))

    def create_broker_task(
        self,
        run_id: str,
        run_handle: str,
        objective: str,
        role: str | None,
        depends_on: tuple[str, ...],
    ) -> BrokerTaskState:
        """Create broker task. Args: run_id, run_handle, objective, role, depends_on. Returns: BrokerTaskState."""
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
        broker_task = self._broker_task_repository.create(
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
        return self._external_broker_assignment_service.publish_if_ready(
            broker_task=broker_task
        )

    def claim_task(
        self,
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAssignment | None:
        """Claim task. Args: slot_id, slot_handle. Returns: BrokerAssignment | None."""
        return self._assignment_claim_service.claim_task(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )

    def claim_task_for_run(
        self,
        slot_id: str,
        slot_handle: str,
        run_id: str,
    ) -> BrokerAssignment | None:
        """Claim task for run. Args: slot_id, slot_handle, run_id. Returns: BrokerAssignment | None."""
        return self._assignment_claim_service.claim_task_for_run(
            slot_id=slot_id,
            slot_handle=slot_handle,
            run_id=run_id,
        )

    def acquire_task(
        self,
        slot_id: str,
        slot_handle: str,
    ) -> BrokerAcquireResult:
        """Acquire task. Args: slot_id, slot_handle. Returns: BrokerAcquireResult."""
        return self._assignment_claim_service.acquire_task(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )

    def finish_assignment(
        self,
        slot_id: str,
        slot_handle: str,
    ) -> FinishedAssignment:
        """Finish assignment. Args: slot_id, slot_handle. Returns: FinishedAssignment."""
        return self._assignment_completion_service.finish_assignment(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )

    def recover_abandoned_assignment(
        self,
        slot_id: str,
        reason: str,
    ) -> WorkerSlotState:
        """Recover abandoned assignment. Args: slot_id, reason. Returns: WorkerSlotState."""
        return self._assignment_completion_service.recover_abandoned_assignment(
            slot_id=slot_id,
            reason=reason,
        )

    def get_run_status(self, run_id: str) -> BrokerRunSnapshot:
        """Get run status. Args: run_id. Returns: BrokerRunSnapshot."""
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
        completed_task_ids = frozenset(
            task.broker_task_id
            for task in broker_tasks
            if task.status is BrokerTaskStatus.COMPLETED
        )
        ready_count = sum(
            task.status is BrokerTaskStatus.QUEUED
            and run.status is RunStatus.ACTIVE
            and all(
                dependency_id in completed_task_ids for dependency_id in task.depends_on
            )
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

    def renew_assignment_lease(self, slot_id: str) -> WorkerSlotState:
        """Renew assignment lease. Args: slot_id. Returns: WorkerSlotState."""
        with self._lock_registry.hold(entity_key=f"broker:slot:{slot_id}"):
            slot = self._slot_repository.read(slot_id=slot_id)
            if slot.status is not WorkerSlotStatus.BUSY:
                return slot
            return self._external_broker_assignment_service.renew_assignment(slot=slot)
