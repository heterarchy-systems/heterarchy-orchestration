from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_dto import (
    WorkerWakeCapabilityState,
    WorkerWakeCapabilityStatus,
    WorkerWakeTransport,
)
from chattocodex.shared.contracts.worker.worker_wake_capability_schemas import (
    WorkerWakeCapabilityRecord,
)
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)


def _record_from_state(state: WorkerWakeCapabilityState) -> WorkerWakeCapabilityRecord:
    """Record from state. Args: state. Returns: WorkerWakeCapabilityRecord."""
    return WorkerWakeCapabilityRecord(
        schema_version=1,
        wake_target_id=state.wake_target_id,
        project_id=state.project_id,
        run_id=state.run_id,
        registered_connector_epoch=state.registered_connector_epoch,
        wake_revision=state.wake_revision,
        transport=state.transport,
        status=state.status,
        bound_slot_id=state.bound_slot_id,
        revision=state.revision,
        issued_at=state.issued_at,
        expires_at=state.expires_at,
        updated_at=state.updated_at,
    )


def _state_from_record(record: WorkerWakeCapabilityRecord) -> WorkerWakeCapabilityState:
    """State from record. Args: record. Returns: WorkerWakeCapabilityState."""
    return WorkerWakeCapabilityState(
        schema_version=record.schema_version,
        wake_target_id=record.wake_target_id,
        project_id=record.project_id,
        run_id=record.run_id,
        registered_connector_epoch=record.registered_connector_epoch,
        wake_revision=record.wake_revision,
        transport=WorkerWakeTransport(record.transport),
        status=WorkerWakeCapabilityStatus(record.status),
        bound_slot_id=record.bound_slot_id,
        revision=record.revision,
        issued_at=record.issued_at,
        expires_at=record.expires_at,
        updated_at=record.updated_at,
    )


class WorkerWakeCapabilityRepository:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._root = state_root / "wake" / "worker_capabilities"

    def exists(self, wake_target_id: str) -> bool:
        """Exists. Args: wake_target_id. Returns: bool."""
        return self._path(wake_target_id=wake_target_id).exists()

    def create(self, state: WorkerWakeCapabilityState) -> WorkerWakeCapabilityState:
        """Create. Args: state. Returns: WorkerWakeCapabilityState."""
        path = self._path(wake_target_id=state.wake_target_id)
        if path.exists():
            raise ResourceAlreadyExistsError(
                operation="worker_wake_capability_issue",
                resource_id=state.wake_target_id,
            )
        self._write(path=path, state=state, operation="worker_wake_capability_issue")
        return self.read(wake_target_id=state.wake_target_id)

    def read(self, wake_target_id: str) -> WorkerWakeCapabilityState:
        """Read. Args: wake_target_id. Returns: WorkerWakeCapabilityState."""
        path = self._path(wake_target_id=wake_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="worker_wake_capability_read",
                resource_id=wake_target_id,
            )
        try:
            record = read_json_model(path=path, record_type=WorkerWakeCapabilityRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_wake_capability_read",
                resource_id=wake_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def write(self, state: WorkerWakeCapabilityState) -> WorkerWakeCapabilityState:
        """Write. Args: state. Returns: WorkerWakeCapabilityState."""
        path = self._path(wake_target_id=state.wake_target_id)
        self._write(path=path, state=state, operation="worker_wake_capability_write")
        return self.read(wake_target_id=state.wake_target_id)

    def list_all(self) -> tuple[WorkerWakeCapabilityState, ...]:
        """List all. Args: None. Returns: tuple[WorkerWakeCapabilityState, ...]."""
        return tuple(
            self.read(wake_target_id=path.stem)
            for path in sorted(self._root.glob("wtgt_*.json"))
        )

    def _write(
        self,
        path: Path,
        state: WorkerWakeCapabilityState,
        operation: str,
    ) -> None:
        """Write. Args: path, state, operation. Returns: None."""
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation=operation,
                resource_id=state.wake_target_id,
                message=str(error),
            ) from error

    def _path(self, wake_target_id: str) -> Path:
        """Path. Args: wake_target_id. Returns: Path."""
        return self._root / f"{wake_target_id}.json"
