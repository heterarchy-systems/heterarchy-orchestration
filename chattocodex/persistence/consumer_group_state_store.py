from pathlib import Path

from chattocodex.consumer_group.consumer_group_state import ConsumerGroupState
from chattocodex.orchestration.orchestration_exceptions import StatePersistenceError
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)
from chattocodex.schemas.consumer_group_schemas import ConsumerGroupRecord


def _record_from_state(*, state: ConsumerGroupState) -> ConsumerGroupRecord:
    return ConsumerGroupRecord(
        schema_version=1,
        project_id=state.project_id,
        generation=state.generation,
        member_ids=state.member_ids,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def _state_from_record(*, record: ConsumerGroupRecord) -> ConsumerGroupState:
    return ConsumerGroupState(
        schema_version=record.schema_version,
        project_id=record.project_id,
        generation=record.generation,
        member_ids=record.member_ids,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


class ConsumerGroupStateStore:
    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "consumer_groups"

    def exists(self, *, project_id: str) -> bool:
        return self._path(project_id=project_id).is_file()

    def read(self, *, project_id: str) -> ConsumerGroupState:
        path = self._path(project_id=project_id)
        try:
            record = read_json_model(path=path, record_type=ConsumerGroupRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="consumer_group_read",
                resource_id=project_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def write(self, *, state: ConsumerGroupState) -> ConsumerGroupState:
        path = self._path(project_id=state.project_id)
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="consumer_group_write",
                resource_id=state.project_id,
                message=str(error),
            ) from error
        return self.read(project_id=state.project_id)

    def _path(self, *, project_id: str) -> Path:
        return self._root / f"{project_id}.json"
