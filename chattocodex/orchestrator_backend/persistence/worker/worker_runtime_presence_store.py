from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_state import (
    WorkerRuntimePresenceState,
)
from chattocodex.shared.contracts.worker.worker_runtime_schemas import (
    WorkerRuntimePresenceRecord,
)
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileError,
    AtomicJsonFileNotFoundError,
    delete_json_file,
    read_json_model,
    write_json_model,
)


def _record_from_state(
    state: WorkerRuntimePresenceState,
) -> WorkerRuntimePresenceRecord:
    """Record from state. Args: state. Returns: WorkerRuntimePresenceRecord."""
    return WorkerRuntimePresenceRecord(
        schema_version=1,
        wake_target_id=state.wake_target_id,
        last_seen_at=state.last_seen_at,
    )


def _state_from_record(
    record: WorkerRuntimePresenceRecord,
) -> WorkerRuntimePresenceState:
    """State from record. Args: record. Returns: WorkerRuntimePresenceState."""
    return WorkerRuntimePresenceState(
        schema_version=record.schema_version,
        wake_target_id=record.wake_target_id,
        last_seen_at=record.last_seen_at,
    )


class WorkerRuntimePresenceStore:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._root = state_root / "worker_runtime" / "presence"

    def exists(self, wake_target_id: str) -> bool:
        """Exists. Args: wake_target_id. Returns: bool."""
        return self._path(wake_target_id=wake_target_id).is_file()

    def read(self, wake_target_id: str) -> WorkerRuntimePresenceState:
        """Read. Args: wake_target_id. Returns: WorkerRuntimePresenceState."""
        path = self._path(wake_target_id=wake_target_id)
        try:
            record = read_json_model(path=path, record_type=WorkerRuntimePresenceRecord)
        except AtomicJsonFileNotFoundError as error:
            raise ResourceNotFoundError(
                operation="worker_runtime_presence_read",
                resource_id=wake_target_id,
            ) from error
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_runtime_presence_read",
                resource_id=wake_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def upsert(self, state: WorkerRuntimePresenceState) -> WorkerRuntimePresenceState:
        """Upsert. Args: state. Returns: WorkerRuntimePresenceState."""
        path = self._path(wake_target_id=state.wake_target_id)
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_runtime_presence_write",
                resource_id=state.wake_target_id,
                message=str(error),
            ) from error
        return self.read(wake_target_id=state.wake_target_id)

    def delete(self, wake_target_id: str) -> None:
        """Delete. Args: wake_target_id. Returns: None."""
        path = self._path(wake_target_id=wake_target_id)
        try:
            delete_json_file(path=path, missing_ok=True)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_runtime_presence_delete",
                resource_id=wake_target_id,
                message=str(error),
            ) from error

    def _path(self, wake_target_id: str) -> Path:
        """Path. Args: wake_target_id. Returns: Path."""
        return self._root / f"{wake_target_id}.json"
