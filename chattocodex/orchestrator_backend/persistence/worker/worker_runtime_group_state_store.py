from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_group_state import (
    WorkerRuntimeGroupState,
)
from chattocodex.shared.contracts.worker.worker_runtime_group_schemas import (
    WorkerRuntimeGroupRecord,
)
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)


def _record_from_state(state: WorkerRuntimeGroupState) -> WorkerRuntimeGroupRecord:
    """Record from state. Args: state. Returns: WorkerRuntimeGroupRecord."""
    return WorkerRuntimeGroupRecord(
        schema_version=1,
        project_id=state.project_id,
        topology_generation=state.topology_generation,
        wake_target_ids=state.wake_target_ids,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def _state_from_record(record: WorkerRuntimeGroupRecord) -> WorkerRuntimeGroupState:
    """State from record. Args: record. Returns: WorkerRuntimeGroupState."""
    return WorkerRuntimeGroupState(
        schema_version=record.schema_version,
        project_id=record.project_id,
        topology_generation=record.topology_generation,
        wake_target_ids=record.wake_target_ids,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


class WorkerRuntimeGroupStateStore:
    """Persist orchestration-owned Worker runtime topology."""

    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._root = state_root / "worker_runtime_groups"

    def exists(self, project_id: str) -> bool:
        """Exists. Args: project_id. Returns: bool."""
        return self._path(project_id=project_id).is_file()

    def read(self, project_id: str) -> WorkerRuntimeGroupState:
        """Read. Args: project_id. Returns: WorkerRuntimeGroupState."""
        path = self._path(project_id=project_id)
        try:
            record = read_json_model(path=path, record_type=WorkerRuntimeGroupRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_runtime_group_read",
                resource_id=project_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def write(self, state: WorkerRuntimeGroupState) -> WorkerRuntimeGroupState:
        """Write. Args: state. Returns: WorkerRuntimeGroupState."""
        path = self._path(project_id=state.project_id)
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_runtime_group_write",
                resource_id=state.project_id,
                message=str(error),
            ) from error
        return self.read(project_id=state.project_id)

    def _path(self, project_id: str) -> Path:
        """Path. Args: project_id. Returns: Path."""
        return self._root / f"{project_id}.json"
