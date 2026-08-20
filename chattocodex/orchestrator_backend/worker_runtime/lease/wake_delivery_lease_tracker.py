from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from chattocodex.orchestrator_backend.persistence.wake.wake_delivery_lease_store import (
    WakeDeliveryLeaseStore,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.wake_delivery_lease_state import (
    WakeDeliveryLeaseState,
)

_WAKE_LEASE_WINDOW = timedelta(seconds=90)
_FAILURE_BACKOFF_SECONDS = (30, 60, 120, 300)


class WakeDeliveryLeaseTracker:
    def __init__(
        self,
        store: WakeDeliveryLeaseStore,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: store, now_provider. Returns: None."""
        self._store = store
        self._now_provider = now_provider or _utc_now

    def begin(self, wake_target_id: str, wake_revision: int) -> WakeDeliveryLeaseState:
        """Begin. Args: wake_target_id, wake_revision. Returns: WakeDeliveryLeaseState."""
        now = self._now_provider()
        failure_streak = 0
        if self._store.exists(wake_target_id=wake_target_id):
            failure_streak = self._store.read(
                wake_target_id=wake_target_id
            ).failure_streak
        return self._store.upsert(
            state=WakeDeliveryLeaseState(
                schema_version=1,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
                lease_started_at=now,
                cooldown_until=None,
                failure_streak=failure_streak,
                updated_at=now,
            )
        )

    def cancel(
        self, wake_target_id: str, wake_revision: int
    ) -> WakeDeliveryLeaseState | None:
        """Cancel. Args: wake_target_id, wake_revision. Returns: WakeDeliveryLeaseState | None."""
        if not self._store.exists(wake_target_id=wake_target_id):
            return None
        current = self._store.read(wake_target_id=wake_target_id)
        if current.wake_revision != wake_revision:
            return current
        now = self._now_provider()
        return self._store.upsert(
            state=WakeDeliveryLeaseState(
                schema_version=1,
                wake_target_id=current.wake_target_id,
                wake_revision=current.wake_revision,
                lease_started_at=None,
                cooldown_until=None,
                failure_streak=current.failure_streak,
                updated_at=now,
            )
        )

    def mark_success(
        self, wake_target_id: str, wake_revision: int
    ) -> WakeDeliveryLeaseState:
        """Mark success. Args: wake_target_id, wake_revision. Returns: WakeDeliveryLeaseState."""
        now = self._now_provider()
        return self._store.upsert(
            state=WakeDeliveryLeaseState(
                schema_version=1,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
                lease_started_at=None,
                cooldown_until=None,
                failure_streak=0,
                updated_at=now,
            )
        )

    def mark_failure(
        self, wake_target_id: str, wake_revision: int
    ) -> WakeDeliveryLeaseState:
        """Mark failure. Args: wake_target_id, wake_revision. Returns: WakeDeliveryLeaseState."""
        now = self._now_provider()
        failure_streak = 1
        if self._store.exists(wake_target_id=wake_target_id):
            current = self._store.read(wake_target_id=wake_target_id)
            if current.wake_revision > wake_revision:
                return current
            failure_streak = current.failure_streak + 1
        backoff_index = min(failure_streak - 1, len(_FAILURE_BACKOFF_SECONDS) - 1)
        cooldown_until = now + timedelta(
            seconds=_FAILURE_BACKOFF_SECONDS[backoff_index]
        )
        return self._store.upsert(
            state=WakeDeliveryLeaseState(
                schema_version=1,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
                lease_started_at=None,
                cooldown_until=cooldown_until,
                failure_streak=failure_streak,
                updated_at=now,
            )
        )

    def is_expired(self, wake_target_id: str, wake_revision: int) -> bool:
        """Is expired. Args: wake_target_id, wake_revision. Returns: bool."""
        if not self._store.exists(wake_target_id=wake_target_id):
            return False
        current = self._store.read(wake_target_id=wake_target_id)
        if current.wake_revision != wake_revision or current.lease_started_at is None:
            return False
        return self._now_provider() - current.lease_started_at > _WAKE_LEASE_WINDOW

    def is_in_cooldown(self, wake_target_id: str) -> bool:
        """Is in cooldown. Args: wake_target_id. Returns: bool."""
        if not self._store.exists(wake_target_id=wake_target_id):
            return False
        cooldown_until = self._store.read(wake_target_id=wake_target_id).cooldown_until
        return cooldown_until is not None and self._now_provider() < cooldown_until

    def is_claimable(self, wake_target_id: str, wake_revision: int) -> bool:
        """Is claimable. Args: wake_target_id, wake_revision. Returns: bool."""
        if not self._store.exists(wake_target_id=wake_target_id):
            return False
        current = self._store.read(wake_target_id=wake_target_id)
        if current.wake_revision != wake_revision:
            return False
        if current.lease_started_at is None:
            return False
        if self._now_provider() - current.lease_started_at > _WAKE_LEASE_WINDOW:
            return False
        return (
            current.cooldown_until is None
            or self._now_provider() >= current.cooldown_until
        )


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
