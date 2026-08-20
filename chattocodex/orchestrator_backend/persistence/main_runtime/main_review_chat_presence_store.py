from pathlib import Path

from chattocodex.orchestrator_backend.main_runtime.presence.main_review_chat_presence_state import (
    MainReviewChatPresenceState,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.shared.contracts.main_runtime.main_review_chat_presence_schemas import (
    MainReviewChatPresenceRecord,
)
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)


def _record_from_state(
    state: MainReviewChatPresenceState,
) -> MainReviewChatPresenceRecord:
    """Record from state. Args: state. Returns: MainReviewChatPresenceRecord."""
    return MainReviewChatPresenceRecord(
        schema_version=1,
        main_target_id=state.main_target_id,
        last_seen_at=state.last_seen_at,
    )


def _state_from_record(
    record: MainReviewChatPresenceRecord,
) -> MainReviewChatPresenceState:
    """State from record. Args: record. Returns: MainReviewChatPresenceState."""
    return MainReviewChatPresenceState(
        schema_version=record.schema_version,
        main_target_id=record.main_target_id,
        last_seen_at=record.last_seen_at,
    )


class MainReviewChatPresenceStore:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._root = state_root / "main_runtime" / "chat_presence"

    def exists(self, main_target_id: str) -> bool:
        """Exists. Args: main_target_id. Returns: bool."""
        return self._path(main_target_id=main_target_id).is_file()

    def read(self, main_target_id: str) -> MainReviewChatPresenceState:
        """Read. Args: main_target_id. Returns: MainReviewChatPresenceState."""
        path = self._path(main_target_id=main_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="main_review_chat_presence_read",
                resource_id=main_target_id,
            )
        try:
            record = read_json_model(
                path=path,
                record_type=MainReviewChatPresenceRecord,
            )
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="main_review_chat_presence_read",
                resource_id=main_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def upsert(self, state: MainReviewChatPresenceState) -> MainReviewChatPresenceState:
        """Upsert. Args: state. Returns: MainReviewChatPresenceState."""
        path = self._path(main_target_id=state.main_target_id)
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="main_review_chat_presence_write",
                resource_id=state.main_target_id,
                message=str(error),
            ) from error
        return self.read(main_target_id=state.main_target_id)

    def _path(self, main_target_id: str) -> Path:
        """Path. Args: main_target_id. Returns: Path."""
        return self._root / f"{main_target_id}.json"
