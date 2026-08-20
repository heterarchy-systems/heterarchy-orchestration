from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from chattocodex.orchestrator_backend.main_runtime.delivery.main_review_delivery_lease_state import (
    MainReviewDeliveryLeaseState,
)
from chattocodex.orchestrator_backend.persistence.main_runtime.main_review_delivery_lease_store import (
    MainReviewDeliveryLeaseStore,
)

_MAIN_REVIEW_DELIVERY_WINDOW = timedelta(seconds=90)
_MAIN_REVIEW_BACKOFF_SECONDS = (30, 60, 120, 300)


class MainReviewDeliveryLeaseTracker:
    def __init__(
        self,
        store: MainReviewDeliveryLeaseStore,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: store, now_provider. Returns: None."""
        self._store = store
        self._now_provider = now_provider or _utc_now

    def begin(
        self, main_target_id: str, wake_revision: int
    ) -> MainReviewDeliveryLeaseState:
        """Begin. Args: main_target_id, wake_revision. Returns: MainReviewDeliveryLeaseState."""
        now = self._now_provider()
        failure_streak = 0
        if self._store.exists(main_target_id=main_target_id):
            failure_streak = self._store.read(
                main_target_id=main_target_id
            ).failure_streak
        return self._store.upsert(
            state=MainReviewDeliveryLeaseState(
                schema_version=1,
                main_target_id=main_target_id,
                wake_revision=wake_revision,
                lease_started_at=now,
                cooldown_until=None,
                failure_streak=failure_streak,
                updated_at=now,
            )
        )

    def mark_success(
        self, main_target_id: str, wake_revision: int
    ) -> MainReviewDeliveryLeaseState:
        """Mark success. Args: main_target_id, wake_revision. Returns: MainReviewDeliveryLeaseState."""
        now = self._now_provider()
        return self._store.upsert(
            state=MainReviewDeliveryLeaseState(
                schema_version=1,
                main_target_id=main_target_id,
                wake_revision=wake_revision,
                lease_started_at=None,
                cooldown_until=None,
                failure_streak=0,
                updated_at=now,
            )
        )

    def mark_failure(
        self, main_target_id: str, wake_revision: int
    ) -> MainReviewDeliveryLeaseState:
        """Mark failure. Args: main_target_id, wake_revision. Returns: MainReviewDeliveryLeaseState."""
        now = self._now_provider()
        failure_streak = 1
        if self._store.exists(main_target_id=main_target_id):
            current = self._store.read(main_target_id=main_target_id)
            if current.wake_revision > wake_revision:
                return current
            failure_streak = current.failure_streak + 1
        backoff_index = min(
            failure_streak - 1,
            len(_MAIN_REVIEW_BACKOFF_SECONDS) - 1,
        )
        cooldown_until = now + timedelta(
            seconds=_MAIN_REVIEW_BACKOFF_SECONDS[backoff_index]
        )
        return self._store.upsert(
            state=MainReviewDeliveryLeaseState(
                schema_version=1,
                main_target_id=main_target_id,
                wake_revision=wake_revision,
                lease_started_at=None,
                cooldown_until=cooldown_until,
                failure_streak=failure_streak,
                updated_at=now,
            )
        )

    def is_expired(self, main_target_id: str, wake_revision: int) -> bool:
        """Is expired. Args: main_target_id, wake_revision. Returns: bool."""
        if not self._store.exists(main_target_id=main_target_id):
            return False
        current = self._store.read(main_target_id=main_target_id)
        if current.wake_revision != wake_revision or current.lease_started_at is None:
            return False
        return (
            self._now_provider() - current.lease_started_at
            > _MAIN_REVIEW_DELIVERY_WINDOW
        )

    def is_in_cooldown(self, main_target_id: str) -> bool:
        """Is in cooldown. Args: main_target_id. Returns: bool."""
        if not self._store.exists(main_target_id=main_target_id):
            return False
        cooldown_until = self._store.read(main_target_id=main_target_id).cooldown_until
        return cooldown_until is not None and self._now_provider() < cooldown_until

    def is_claimable(self, main_target_id: str, wake_revision: int) -> bool:
        """Is claimable. Args: main_target_id, wake_revision. Returns: bool."""
        if not self._store.exists(main_target_id=main_target_id):
            return False
        current = self._store.read(main_target_id=main_target_id)
        if current.wake_revision != wake_revision:
            return False
        if current.lease_started_at is None:
            return False
        if (
            self._now_provider() - current.lease_started_at
            > _MAIN_REVIEW_DELIVERY_WINDOW
        ):
            return False
        return (
            current.cooldown_until is None
            or self._now_provider() >= current.cooldown_until
        )


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
