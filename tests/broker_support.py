from dataclasses import dataclass
from pathlib import Path

from chattocodex.broker.broker_dto import BrokerAssignment, CreatedWorkerSlot
from chattocodex.broker.broker_service import BrokerService
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


def build_broker_service(
    *,
    state_root: Path,
    workspace_root: Path,
    run_repository: RunStateRepository,
    worker_repository: WorkerStateRepository,
    task_repository: TaskStateRepository,
    lock_registry: EntityLockRegistry,
) -> BrokerService:
    return BrokerService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        broker_task_repository=BrokerTaskRepository(state_root=state_root),
        slot_repository=WorkerSlotRepository(state_root=state_root),
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
