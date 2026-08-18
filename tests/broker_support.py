from dataclasses import dataclass
from pathlib import Path

from chattocodex.broker.broker_dto import BrokerAssignment, CreatedWorkerSlot
from chattocodex.broker.broker_service import BrokerService
from chattocodex.broker_client.external_broker_gateway import (
    BrokerClaim as ExternalBrokerClaim,
    BrokerRenewal,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
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


@dataclass(frozen=True, slots=True, kw_only=True)
class BrokerClaim:
    service: BrokerService
    slot: CreatedWorkerSlot
    assignment: BrokerAssignment


@dataclass(frozen=True, slots=True, kw_only=True)
class _FakeLease:
    project_id: str
    run_id: str
    member_id: str
    task_id: str
    objective: str
    lease_id: str
    lease_epoch: int
    lease_expires_at_ms: int


class FakeWorkBrokerGateway:
    """Deterministic test double for the external Broker gateway contract only."""

    def __init__(self) -> None:
        self._published: dict[tuple[str, str], list[tuple[str, str]]] = {}
        self._leases: dict[tuple[str, str], _FakeLease] = {}
        self._completed: set[tuple[str, str]] = set()
        self._lease_sequence = 0

    def publish_task(
        self,
        *,
        project_id: str,
        run_id: str,
        task_id: str,
        objective: str,
    ) -> None:
        queue = self._published.setdefault((project_id, run_id), [])
        candidate = (task_id, objective)
        if candidate not in queue:
            queue.append(candidate)

    def claim_task(
        self, *, project_id: str, run_id: str, member_id: str
    ) -> ExternalBrokerClaim | None:
        queue = self._published.get((project_id, run_id), [])
        for task_id, objective in queue:
            key = (run_id, task_id)
            if key in self._completed or key in self._leases:
                continue
            self._lease_sequence += 1
            lease = _FakeLease(
                project_id=project_id,
                run_id=run_id,
                member_id=member_id,
                task_id=task_id,
                objective=objective,
                lease_id=f"fake_lease_{self._lease_sequence}",
                lease_epoch=1,
                lease_expires_at_ms=90_000 + self._lease_sequence,
            )
            self._leases[key] = lease
            return ExternalBrokerClaim(
                task_id=task_id,
                objective=objective,
                term=1,
                generation=1,
                lease_id=lease.lease_id,
                lease_epoch=lease.lease_epoch,
                lease_expires_at_ms=lease.lease_expires_at_ms,
            )
        return None

    def renew_task(
        self,
        *,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
    ) -> BrokerRenewal:
        lease = self._require_lease(
            project_id=project_id,
            run_id=run_id,
            member_id=member_id,
            task_id=task_id,
            lease_id=lease_id,
            lease_epoch=lease_epoch,
        )
        if term != 1 or generation != 1:
            raise AssertionError("unexpected fake Broker fencing values")
        renewed = _FakeLease(
            project_id=lease.project_id,
            run_id=lease.run_id,
            member_id=lease.member_id,
            task_id=lease.task_id,
            objective=lease.objective,
            lease_id=lease.lease_id,
            lease_epoch=lease.lease_epoch,
            lease_expires_at_ms=lease.lease_expires_at_ms + 90_000,
        )
        self._leases[(run_id, task_id)] = renewed
        return BrokerRenewal(
            term=1,
            generation=1,
            lease_id=renewed.lease_id,
            lease_epoch=renewed.lease_epoch,
            lease_expires_at_ms=renewed.lease_expires_at_ms,
        )

    def complete_task(
        self,
        *,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        term: int,
        generation: int,
        lease_id: str,
        lease_epoch: int,
        result: str,
    ) -> None:
        del result
        self._require_lease(
            project_id=project_id,
            run_id=run_id,
            member_id=member_id,
            task_id=task_id,
            lease_id=lease_id,
            lease_epoch=lease_epoch,
        )
        if term != 1 or generation != 1:
            raise AssertionError("unexpected fake Broker fencing values")
        self._leases.pop((run_id, task_id), None)
        self._completed.add((run_id, task_id))

    def expire_task(self, *, run_id: str, task_id: str) -> None:
        self._leases.pop((run_id, task_id), None)

    def lease_member_id(self, *, run_id: str, task_id: str) -> str:
        return self._leases[(run_id, task_id)].member_id

    def _require_lease(
        self,
        *,
        project_id: str,
        run_id: str,
        member_id: str,
        task_id: str,
        lease_id: str,
        lease_epoch: int,
    ) -> _FakeLease:
        lease = self._leases[(run_id, task_id)]
        if (
            lease.project_id != project_id
            or lease.member_id != member_id
            or lease.lease_id != lease_id
            or lease.lease_epoch != lease_epoch
        ):
            raise AssertionError("fake Broker lease mismatch")
        return lease


def build_broker_service(
    *,
    state_root: Path,
    workspace_root: Path,
    run_repository: RunStateRepository,
    worker_repository: WorkerStateRepository,
    task_repository: TaskStateRepository,
    lock_registry: EntityLockRegistry,
    work_broker_gateway: FakeWorkBrokerGateway | None = None,
) -> BrokerService:
    return BrokerService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        broker_task_repository=BrokerTaskRepository(state_root=state_root),
        slot_repository=WorkerSlotRepository(state_root=state_root),
        work_broker_gateway=work_broker_gateway or FakeWorkBrokerGateway(),
        workspace_project_service=WorkspaceProjectService(
            workspace_root=workspace_root
        ),
        lock_registry=lock_registry,
    )


def claim_broker_task(
    *,
    service: BrokerService,
    run_id: str,
    run_handle: str,
    project_id: str,
    objective: str,
    role: str | None = None,
    label: str | None = None,
    depends_on: tuple[str, ...] = (),
) -> BrokerClaim:
    slot = service.register_slot(project_id=project_id, label=label)
    broker_task = service.create_broker_task(
        run_id=run_id,
        run_handle=run_handle,
        objective=objective,
        role=role,
        depends_on=depends_on,
    )
    assignment = service.claim_task(
        slot_id=slot.state.slot_id,
        slot_handle=slot.slot_handle,
    )
    if assignment is None:
        raise AssertionError(
            f"Broker Task {broker_task.broker_task_id} was not claimable."
        )
    if assignment.broker_task.broker_task_id != broker_task.broker_task_id:
        raise AssertionError(
            "Broker returned a different Task than the one just published."
        )
    return BrokerClaim(service=service, slot=slot, assignment=assignment)
