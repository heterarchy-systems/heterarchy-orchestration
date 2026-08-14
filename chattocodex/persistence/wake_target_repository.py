from pathlib import Path

from chattocodex.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    delete_json_file,
    read_json_model,
    write_json_model,
)
from chattocodex.schemas.wake_schemas import WakeTargetRecord
from chattocodex.wake.wake_dto import WakeTargetState


def _record_from_state(*, state: WakeTargetState) -> WakeTargetRecord:
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


def _state_from_record(*, record: WakeTargetRecord) -> WakeTargetState:
    return WakeTargetState(
        schema_version=record.schema_version,
        wake_target_id=record.wake_target_id,
        project_id=record.project_id,
        registered_connector_epoch=record.registered_connector_epoch,
        status=record.status,
        bound_slot_id=record.bound_slot_id,
        wake_revision=record.wake_revision,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


class WakeTargetRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "wake" / "targets"

    def exists(self, *, wake_target_id: str) -> bool:
        return self._path(wake_target_id=wake_target_id).exists()

    def create(self, *, state: WakeTargetState) -> WakeTargetState:
        path = self._path(wake_target_id=state.wake_target_id)
        if path.exists():
            raise ResourceAlreadyExistsError(
                operation="wake_target_register",
                resource_id=state.wake_target_id,
            )
        self._write(path=path, state=state, operation="wake_target_register")
        return self.read(wake_target_id=state.wake_target_id)

    def read(self, *, wake_target_id: str) -> WakeTargetState:
        path = self._path(wake_target_id=wake_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="wake_target_read",
                resource_id=wake_target_id,
            )
        try:
            record = read_json_model(path=path, record_type=WakeTargetRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_target_read",
                resource_id=wake_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def write(self, *, state: WakeTargetState) -> WakeTargetState:
        path = self._path(wake_target_id=state.wake_target_id)
        self._write(path=path, state=state, operation="wake_target_write")
        return self.read(wake_target_id=state.wake_target_id)

    def list_all(self) -> tuple[WakeTargetState, ...]:
        return tuple(
            self.read(wake_target_id=path.stem)
            for path in sorted(self._root.glob("wtgt_*.json"))
        )

    def delete(self, *, wake_target_id: str) -> None:
        path = self._path(wake_target_id=wake_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="wake_target_delete", resource_id=wake_target_id
            )
        try:
            delete_json_file(path=path, missing_ok=False)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_target_delete",
                resource_id=wake_target_id,
                message=str(error),
            ) from error

    def _write(self, *, path: Path, state: WakeTargetState, operation: str) -> None:
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation=operation,
                resource_id=state.wake_target_id,
                message=str(error),
            ) from error

    def _path(self, *, wake_target_id: str) -> Path:
        return self._root / f"{wake_target_id}.json"
