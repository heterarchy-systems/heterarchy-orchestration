from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskState,
    BrokerTaskStatus,
    WorkerSlotState,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunStatus,
    TaskState,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
    WorkerSlotRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
)
from chattocodex.shared.contracts.broker.broker_gateway_contract import (
    BrokerClaim,
    WorkBrokerGateway,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ExternalBrokerLease:
    """Typed external Broker lease metadata persisted on one busy Worker Slot."""

    task_id: str
    run_id: str
    term: int
    generation: int
    lease_id: str
    lease_epoch: int


class ExternalBrokerAssignmentService:
    """Translate orchestration readiness and assignments through WorkBrokerGateway."""

    def __init__(
        self,
        run_repository: RunStateRepository,
        broker_task_repository: BrokerTaskRepository,
        slot_repository: WorkerSlotRepository,
        work_broker_gateway: WorkBrokerGateway,
        now_provider: Callable[[], datetime],
    ) -> None:
        """Initialize this object. Args: run_repository, broker_task_repository, slot_repository, work_broker_gateway, now_provider. Returns: None."""
        self._run_repository = run_repository
        self._broker_task_repository = broker_task_repository
        self._slot_repository = slot_repository
        self._work_broker_gateway = work_broker_gateway
        self._now_provider = now_provider

    def publish_ready_tasks(
        self,
        project_id: str,
        run_id: str | None,
    ) -> None:
        """Publish queued dependency-ready tasks for one project/run scope. Args: project_id, run_id. Returns: None."""

        for broker_task in sorted(
            self._broker_task_repository.list_all(),
            key=lambda task: (task.created_at, task.broker_task_id),
        ):
            if broker_task.status is not BrokerTaskStatus.QUEUED:
                continue
            if run_id is not None and broker_task.run_id != run_id:
                continue
            run = self._run_repository.read(run_id=broker_task.run_id)
            if run.project_id != project_id or run.status is not RunStatus.ACTIVE:
                continue
            self.publish_if_ready(broker_task=broker_task)

    def claim_task(
        self,
        project_id: str,
        run_id: str,
        member_id: str,
    ) -> BrokerClaim | None:
        """Claim one ready task from the external Broker provider. Args: project_id, run_id, member_id. Returns: BrokerClaim | None."""

        return self._work_broker_gateway.claim_task(
            project_id=project_id,
            run_id=run_id,
            member_id=member_id,
        )

    def publish_if_ready(self, broker_task: BrokerTaskState) -> BrokerTaskState:
        """Publish one queued task only after its local DAG dependencies are complete. Args: broker_task. Returns: BrokerTaskState."""

        if (
            broker_task.status is not BrokerTaskStatus.QUEUED
            or broker_task.external_published
            or not self._dependencies_completed(broker_task=broker_task)
        ):
            return broker_task
        run = self._run_repository.read(run_id=broker_task.run_id)
        if run.status is not RunStatus.ACTIVE:
            return broker_task
        self._work_broker_gateway.publish_task(
            project_id=run.project_id,
            run_id=run.run_id,
            task_id=broker_task.broker_task_id,
            objective=broker_task.objective,
        )
        return self._broker_task_repository.write(
            state=BrokerTaskState(
                schema_version=broker_task.schema_version,
                broker_task_id=broker_task.broker_task_id,
                run_id=broker_task.run_id,
                objective=broker_task.objective,
                role=broker_task.role,
                depends_on=broker_task.depends_on,
                status=broker_task.status,
                assigned_slot_id=broker_task.assigned_slot_id,
                assigned_worker_id=broker_task.assigned_worker_id,
                assigned_task_id=broker_task.assigned_task_id,
                revision=broker_task.revision + 1,
                created_at=broker_task.created_at,
                updated_at=self._now_provider(),
                external_published=True,
            )
        )

    def renew_assignment(self, slot: WorkerSlotState) -> WorkerSlotState:
        """Renew one busy Slot's external Broker lease and persist the new fence. Args: slot. Returns: WorkerSlotState."""

        lease = self._require_external_lease(slot=slot)
        renewed = self._work_broker_gateway.renew_task(
            project_id=slot.project_id,
            run_id=lease.run_id,
            member_id=slot.broker_consumer_id,
            task_id=lease.task_id,
            term=lease.term,
            generation=lease.generation,
            lease_id=lease.lease_id,
            lease_epoch=lease.lease_epoch,
        )
        return self._slot_repository.write(
            state=WorkerSlotState(
                schema_version=slot.schema_version,
                slot_id=slot.slot_id,
                project_id=slot.project_id,
                broker_consumer_id=slot.broker_consumer_id,
                slot_handle_digest=slot.slot_handle_digest,
                label=slot.label,
                status=slot.status,
                current_broker_task_id=slot.current_broker_task_id,
                current_run_id=slot.current_run_id,
                current_worker_id=slot.current_worker_id,
                current_task_id=slot.current_task_id,
                revision=slot.revision + 1,
                created_at=slot.created_at,
                updated_at=self._now_provider(),
                broker_term=renewed.term,
                broker_group_generation=renewed.generation,
                broker_lease_id=renewed.lease_id,
                broker_lease_epoch=renewed.lease_epoch,
                broker_lease_expires_at_ms=renewed.lease_expires_at_ms,
            )
        )

    def complete_assignment(
        self,
        slot: WorkerSlotState,
        task: TaskState,
    ) -> None:
        """Complete one external Broker lease with the local terminal task result. Args: slot, task. Returns: None."""

        lease = self._require_external_lease(slot=slot)
        result = task.result or task.progress_summary or task.status.value
        self._work_broker_gateway.complete_task(
            project_id=slot.project_id,
            run_id=lease.run_id,
            member_id=slot.broker_consumer_id,
            task_id=lease.task_id,
            term=lease.term,
            generation=lease.generation,
            lease_id=lease.lease_id,
            lease_epoch=lease.lease_epoch,
            result=result,
        )

    def _dependencies_completed(self, broker_task: BrokerTaskState) -> bool:
        """Dependencies completed. Args: broker_task. Returns: bool."""
        return all(
            self._broker_task_repository.read(broker_task_id=dependency_id).status
            is BrokerTaskStatus.COMPLETED
            for dependency_id in broker_task.depends_on
        )

    @staticmethod
    def _require_external_lease(slot: WorkerSlotState) -> ExternalBrokerLease:
        """Require external lease. Args: slot. Returns: ExternalBrokerLease."""
        if (
            slot.current_broker_task_id is None
            or slot.current_run_id is None
            or slot.broker_term is None
            or slot.broker_group_generation is None
            or slot.broker_lease_id is None
            or slot.broker_lease_epoch is None
        ):
            raise InvalidTransitionError(
                operation="external_broker_lease",
                resource_id=slot.slot_id,
                message="Busy Worker Slot is missing external Broker lease metadata.",
            )
        return ExternalBrokerLease(
            task_id=slot.current_broker_task_id,
            run_id=slot.current_run_id,
            term=slot.broker_term,
            generation=slot.broker_group_generation,
            lease_id=slot.broker_lease_id,
            lease_epoch=slot.broker_lease_epoch,
        )
