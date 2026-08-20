from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from chattocodex.orchestrator_backend.main_runtime.presence.main_review_chat_presence_state import (
    MainReviewChatPresenceState,
)
from chattocodex.orchestrator_backend.persistence.main_runtime.main_review_chat_presence_store import (
    MainReviewChatPresenceStore,
)

_CHAT_HEARTBEAT_WRITE_INTERVAL = timedelta(seconds=5)
_MAIN_REVIEW_SESSION_RECOVERY_WINDOW = timedelta(seconds=120)


class MainReviewChatPresenceTracker:
    def __init__(
        self,
        store: MainReviewChatPresenceStore,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: store, now_provider. Returns: None."""
        self._store = store
        self._now_provider = now_provider or _utc_now

    def heartbeat(self, main_target_id: str) -> MainReviewChatPresenceState:
        """Heartbeat. Args: main_target_id. Returns: MainReviewChatPresenceState."""
        now = self._now_provider()
        if self._store.exists(main_target_id=main_target_id):
            current = self._store.read(main_target_id=main_target_id)
            if now - current.last_seen_at < _CHAT_HEARTBEAT_WRITE_INTERVAL:
                return current
        return self._store.upsert(
            state=MainReviewChatPresenceState(
                schema_version=1,
                main_target_id=main_target_id,
                last_seen_at=now,
            )
        )

    def session_expired(
        self,
        main_target_id: str,
        activated_at: datetime,
    ) -> bool:
        """Session expired. Args: main_target_id, activated_at. Returns: bool."""
        reference = activated_at
        if self._store.exists(main_target_id=main_target_id):
            last_seen_at = self._store.read(main_target_id=main_target_id).last_seen_at
            if last_seen_at > reference:
                reference = last_seen_at
        return self._now_provider() - reference > _MAIN_REVIEW_SESSION_RECOVERY_WINDOW


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
