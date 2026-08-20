from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_state import (
    WorkerRuntimePresenceState,
)

_RUNTIME_ACTIVE_WINDOW = timedelta(seconds=75)
_RUNTIME_HEARTBEAT_WRITE_INTERVAL = timedelta(seconds=10)


class WorkerRuntimePresenceTracker:
    def __init__(
        self,
        store: WorkerRuntimePresenceStore,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: store, now_provider. Returns: None."""
        self._store = store
        self._now_provider = now_provider or _utc_now

    def heartbeat(self, wake_target_id: str) -> WorkerRuntimePresenceState:
        """Heartbeat. Args: wake_target_id. Returns: WorkerRuntimePresenceState."""
        now = self._now_provider()
        if self._store.exists(wake_target_id=wake_target_id):
            current = self._store.read(wake_target_id=wake_target_id)
            if now - current.last_seen_at < _RUNTIME_HEARTBEAT_WRITE_INTERVAL:
                return current
        return self._store.upsert(
            state=WorkerRuntimePresenceState(
                schema_version=1,
                wake_target_id=wake_target_id,
                last_seen_at=now,
            )
        )

    def is_active(self, wake_target_id: str) -> bool:
        """Is active. Args: wake_target_id. Returns: bool."""
        if not self._store.exists(wake_target_id=wake_target_id):
            return False
        presence = self._store.read(wake_target_id=wake_target_id)
        return self._now_provider() - presence.last_seen_at <= _RUNTIME_ACTIVE_WINDOW

    def remove(self, wake_target_id: str) -> None:
        """Remove. Args: wake_target_id. Returns: None."""
        self._store.delete(wake_target_id=wake_target_id)


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
