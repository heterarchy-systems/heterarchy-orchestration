from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.wake.wake_dto import (
    WakeTargetState,
    WakeTargetStatus,
)
from chattocodex.shared.contracts.wake.wake_schemas import WakeTargetRecord
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileError,
    AtomicJsonFileNotFoundError,
    delete_json_file,
    read_json_model,
    write_json_model,
)


def _record_from_state(state: WakeTargetState) -> WakeTargetRecord:
    """Record from state. Args: state. Returns: WakeTargetRecord."""
    return WakeTargetRecord(
        schema_version=1,
        wake_target_id=state.wake_target_id,
        project_id=state.project_id,
        registered_connector_epoch=state.registered_connector_epoch,
        status=state.status,
        bound_slot_id=state.bound_slot_id,
        wake_revision=state.wake_revision,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def _state_from_record(record: WakeTargetRecord) -> WakeTargetState:
    """State from record. Args: record. Returns: WakeTargetState."""
    return WakeTargetState(
        schema_version=record.schema_version,
        wake_target_id=record.wake_target_id,
        project_id=record.project_id,
        registered_connector_epoch=record.registered_connector_epoch,
        status=WakeTargetStatus(record.status),
        bound_slot_id=record.bound_slot_id,
        wake_revision=record.wake_revision,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


class WakeTargetRepository:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._root = state_root / "wake" / "targets"

    def exists(self, wake_target_id: str) -> bool:
        """Exists. Args: wake_target_id. Returns: bool."""
        return self._path(wake_target_id=wake_target_id).exists()

    def create(self, state: WakeTargetState) -> WakeTargetState:
        """Create. Args: state. Returns: WakeTargetState."""
        path = self._path(wake_target_id=state.wake_target_id)
        if path.exists():
            raise ResourceAlreadyExistsError(
                operation="wake_target_register",
                resource_id=state.wake_target_id,
            )
        self._write(path=path, state=state, operation="wake_target_register")
        return self.read(wake_target_id=state.wake_target_id)

    def read(self, wake_target_id: str) -> WakeTargetState:
        """Read. Args: wake_target_id. Returns: WakeTargetState."""
        path = self._path(wake_target_id=wake_target_id)
        try:
            record = read_json_model(path=path, record_type=WakeTargetRecord)
        except AtomicJsonFileNotFoundError as error:
            raise ResourceNotFoundError(
                operation="wake_target_read",
                resource_id=wake_target_id,
            ) from error
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_target_read",
                resource_id=wake_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def write(self, state: WakeTargetState) -> WakeTargetState:
        """Write. Args: state. Returns: WakeTargetState."""
        path = self._path(wake_target_id=state.wake_target_id)
        self._write(path=path, state=state, operation="wake_target_write")
        return self.read(wake_target_id=state.wake_target_id)

    def list_all(self) -> tuple[WakeTargetState, ...]:
        """List all. Args: None. Returns: tuple[WakeTargetState, ...]."""
        return tuple(
            self.read(wake_target_id=path.stem)
            for path in sorted(self._root.glob("wtgt_*.json"))
        )

    def delete(self, wake_target_id: str) -> None:
        """Delete. Args: wake_target_id. Returns: None."""
        path = self._path(wake_target_id=wake_target_id)
        try:
            delete_json_file(path=path, missing_ok=False)
        except AtomicJsonFileNotFoundError as error:
            raise ResourceNotFoundError(
                operation="wake_target_delete", resource_id=wake_target_id
            ) from error
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_target_delete",
                resource_id=wake_target_id,
                message=str(error),
            ) from error

    def _write(self, path: Path, state: WakeTargetState, operation: str) -> None:
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
