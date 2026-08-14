from pathlib import Path

from chattocodex.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    delete_json_file,
    read_json_model,
    write_json_model,
)
from chattocodex.schemas.wake_schemas import WakePresenceRecord
from chattocodex.wake.wake_dto import WakePresenceState


def _record_from_state(*, state: WakePresenceState) -> WakePresenceRecord:
    return WakePresenceRecord(
        schema_version=1,
        wake_target_id=state.wake_target_id,
        last_seen_at=state.last_seen_at,
    )


def _state_from_record(*, record: WakePresenceRecord) -> WakePresenceState:
    return WakePresenceState(
        schema_version=record.schema_version,
        wake_target_id=record.wake_target_id,
        last_seen_at=record.last_seen_at,
    )


class WakePresenceRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "wake" / "presence"

    def exists(self, *, wake_target_id: str) -> bool:
        return self._path(wake_target_id=wake_target_id).is_file()

    def read(self, *, wake_target_id: str) -> WakePresenceState:
        path = self._path(wake_target_id=wake_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="wake_presence_read",
                resource_id=wake_target_id,
            )
        try:
            record = read_json_model(path=path, record_type=WakePresenceRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_presence_read",
                resource_id=wake_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def upsert(self, *, state: WakePresenceState) -> WakePresenceState:
        path = self._path(wake_target_id=state.wake_target_id)
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_presence_write",
                resource_id=state.wake_target_id,
                message=str(error),
            ) from error
        return self.read(wake_target_id=state.wake_target_id)

    def delete(self, *, wake_target_id: str) -> None:
        path = self._path(wake_target_id=wake_target_id)
        try:
            delete_json_file(path=path, missing_ok=True)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_presence_delete",
                resource_id=wake_target_id,
                message=str(error),
            ) from error

    def _path(self, *, wake_target_id: str) -> Path:
        return self._root / f"{wake_target_id}.json"
