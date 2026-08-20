from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from chattocodex.orchestrator_backend.persistence.wake.wake_presence_repository import (
    WakePresenceRepository,
)
from chattocodex.orchestrator_backend.wake.wake_dto import WakePresenceState

_ACTIVE_WINDOW = timedelta(seconds=30)
_HEARTBEAT_WRITE_INTERVAL = timedelta(seconds=5)


class WakePresenceService:
    def __init__(
        self,
        repository: WakePresenceRepository,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: repository, now_provider. Returns: None."""
        self._repository = repository
        self._now_provider = now_provider or _utc_now

    def heartbeat(self, wake_target_id: str) -> WakePresenceState:
        """Heartbeat. Args: wake_target_id. Returns: WakePresenceState."""
        now = self._now_provider()
        if self._repository.exists(wake_target_id=wake_target_id):
            current = self._repository.read(wake_target_id=wake_target_id)
            if now - current.last_seen_at < _HEARTBEAT_WRITE_INTERVAL:
                return current
        return self._repository.upsert(
            state=WakePresenceState(
                schema_version=1,
                wake_target_id=wake_target_id,
                last_seen_at=now,
            )
        )

    def is_active(self, wake_target_id: str) -> bool:
        """Is active. Args: wake_target_id. Returns: bool."""
        if not self._repository.exists(wake_target_id=wake_target_id):
            return False
        presence = self._repository.read(wake_target_id=wake_target_id)
        return self._now_provider() - presence.last_seen_at <= _ACTIVE_WINDOW

    def remove(self, wake_target_id: str) -> None:
        """Remove. Args: wake_target_id. Returns: None."""
        self._repository.delete(wake_target_id=wake_target_id)


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
