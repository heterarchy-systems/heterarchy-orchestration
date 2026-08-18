from hashlib import sha256
from pathlib import Path

from chattocodex.broker.broker_dto import BrokerTaskState, WorkerSlotState
from chattocodex.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)
from chattocodex.schemas.broker_schemas import BrokerTaskRecord, WorkerSlotRecord


def _broker_task_record_from_state(*, state: BrokerTaskState) -> BrokerTaskRecord:
    return BrokerTaskRecord(
        schema_version=1,
        broker_task_id=state.broker_task_id,
        run_id=state.run_id,
        objective=state.objective,
        role=state.role,
        depends_on=state.depends_on,
        status=state.status,
        assigned_slot_id=state.assigned_slot_id,
        assigned_worker_id=state.assigned_worker_id,
        assigned_task_id=state.assigned_task_id,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
        external_published=state.external_published,
    )


def _broker_task_state_from_record(*, record: BrokerTaskRecord) -> BrokerTaskState:
    return BrokerTaskState(
        schema_version=record.schema_version,
        broker_task_id=record.broker_task_id,
        run_id=record.run_id,
        objective=record.objective,
        role=record.role,
        depends_on=record.depends_on,
        status=record.status,
        assigned_slot_id=record.assigned_slot_id,
        assigned_worker_id=record.assigned_worker_id,
        assigned_task_id=record.assigned_task_id,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
        external_published=record.external_published,
    )


def _worker_slot_record_from_state(*, state: WorkerSlotState) -> WorkerSlotRecord:
    return WorkerSlotRecord(
        schema_version=1,
        slot_id=state.slot_id,
        project_id=state.project_id,
        broker_consumer_id=state.broker_consumer_id,
        slot_handle_digest=state.slot_handle_digest,
        label=state.label,
        status=state.status,
        current_broker_task_id=state.current_broker_task_id,
        current_run_id=state.current_run_id,
        current_worker_id=state.current_worker_id,
        current_task_id=state.current_task_id,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
        broker_term=state.broker_term,
        broker_group_generation=state.broker_group_generation,
        broker_lease_id=state.broker_lease_id,
        broker_lease_epoch=state.broker_lease_epoch,
        broker_lease_expires_at_ms=state.broker_lease_expires_at_ms,
    )


def _worker_slot_state_from_record(*, record: WorkerSlotRecord) -> WorkerSlotState:
    return WorkerSlotState(
        schema_version=record.schema_version,
        slot_id=record.slot_id,
        project_id=record.project_id,
        broker_consumer_id=(
            record.broker_consumer_id
            or _legacy_broker_consumer_id(slot_id=record.slot_id)
        ),
        slot_handle_digest=record.slot_handle_digest,
        label=record.label,
        status=record.status,
        current_broker_task_id=record.current_broker_task_id,
        current_run_id=record.current_run_id,
        current_worker_id=record.current_worker_id,
        current_task_id=record.current_task_id,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
        broker_term=record.broker_term,
        broker_group_generation=record.broker_group_generation,
        broker_lease_id=record.broker_lease_id,
        broker_lease_epoch=record.broker_lease_epoch,
        broker_lease_expires_at_ms=record.broker_lease_expires_at_ms,
    )


def _legacy_broker_consumer_id(*, slot_id: str) -> str:
    """Recover a stable Consumer ID for pre-migration Slot records only.

    New Slots always receive an independently generated Consumer ID. This deterministic
    surrogate exists solely so previously persisted schema-v1 records remain readable.
    """
    digest = sha256(f"legacy-broker-consumer:{slot_id}".encode()).hexdigest()
    return f"consumer_{digest[:24]}"


class BrokerTaskRepository:
    """Persist the orchestration-side DAG/task projection, not Broker authority."""

    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "broker" / "tasks"

    def create(self, *, state: BrokerTaskState) -> BrokerTaskState:
        path = self._path(broker_task_id=state.broker_task_id)
        if path.exists():
            raise ResourceAlreadyExistsError(
                operation="broker_task_create",
                resource_id=state.broker_task_id,
            )
        self._write(path=path, state=state, operation="broker_task_create")
        return self.read(broker_task_id=state.broker_task_id)

    def read(self, *, broker_task_id: str) -> BrokerTaskState:
        path = self._path(broker_task_id=broker_task_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="broker_task_read",
                resource_id=broker_task_id,
            )
        try:
            record = read_json_model(path=path, record_type=BrokerTaskRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="broker_task_read",
                resource_id=broker_task_id,
                message=str(error),
            ) from error
        return _broker_task_state_from_record(record=record)

    def write(self, *, state: BrokerTaskState) -> BrokerTaskState:
        path = self._path(broker_task_id=state.broker_task_id)
        self._write(path=path, state=state, operation="broker_task_write")
        return self.read(broker_task_id=state.broker_task_id)

    def list_all(self) -> tuple[BrokerTaskState, ...]:
        return tuple(
            self.read(broker_task_id=path.stem)
            for path in sorted(self._root.glob("btask_*.json"))
        )

    def list_for_run(self, *, run_id: str) -> tuple[BrokerTaskState, ...]:
        return tuple(task for task in self.list_all() if task.run_id == run_id)

    def _write(self, *, path: Path, state: BrokerTaskState, operation: str) -> None:
        try:
            write_json_model(
                path=path, record=_broker_task_record_from_state(state=state)
            )
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation=operation,
                resource_id=state.broker_task_id,
                message=str(error),
            ) from error

    def _path(self, *, broker_task_id: str) -> Path:
        return self._root / f"{broker_task_id}.json"


class WorkerSlotRepository:
    """Persist physical Worker-slot and external-lease projection state."""

    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "broker" / "slots"

    def create(self, *, state: WorkerSlotState) -> WorkerSlotState:
        path = self._path(slot_id=state.slot_id)
        if path.exists():
            raise ResourceAlreadyExistsError(
                operation="worker_slot_register",
                resource_id=state.slot_id,
            )
        self._write(path=path, state=state, operation="worker_slot_register")
        return self.read(slot_id=state.slot_id)

    def read(self, *, slot_id: str) -> WorkerSlotState:
        path = self._path(slot_id=slot_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="worker_slot_read",
                resource_id=slot_id,
            )
        try:
            record = read_json_model(path=path, record_type=WorkerSlotRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_slot_read",
                resource_id=slot_id,
                message=str(error),
            ) from error
        return _worker_slot_state_from_record(record=record)

    def write(self, *, state: WorkerSlotState) -> WorkerSlotState:
        path = self._path(slot_id=state.slot_id)
        self._write(path=path, state=state, operation="worker_slot_write")
        return self.read(slot_id=state.slot_id)

    def list_all(self) -> tuple[WorkerSlotState, ...]:
        return tuple(
            self.read(slot_id=path.stem)
            for path in sorted(self._root.glob("slot_*.json"))
        )

    def _write(self, *, path: Path, state: WorkerSlotState, operation: str) -> None:
        try:
            write_json_model(
                path=path, record=_worker_slot_record_from_state(state=state)
            )
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation=operation,
                resource_id=state.slot_id,
                message=str(error),
            ) from error

    def _path(self, *, slot_id: str) -> Path:
        return self._root / f"{slot_id}.json"
