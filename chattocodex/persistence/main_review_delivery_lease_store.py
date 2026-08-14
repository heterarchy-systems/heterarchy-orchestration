from pathlib import Path

from chattocodex.main_runtime.main_review_delivery_lease_state import (
    MainReviewDeliveryLeaseState,
)
from chattocodex.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)
from chattocodex.schemas.main_review_delivery_schemas import (
    MainReviewDeliveryLeaseRecord,
)


def _record_from_state(
    *, state: MainReviewDeliveryLeaseState
) -> MainReviewDeliveryLeaseRecord:
    return MainReviewDeliveryLeaseRecord(
        schema_version=1,
        main_target_id=state.main_target_id,
        wake_revision=state.wake_revision,
        lease_started_at=state.lease_started_at,
        cooldown_until=state.cooldown_until,
        failure_streak=state.failure_streak,
        updated_at=state.updated_at,
    )


def _state_from_record(
    *, record: MainReviewDeliveryLeaseRecord
) -> MainReviewDeliveryLeaseState:
    return MainReviewDeliveryLeaseState(
        schema_version=record.schema_version,
        main_target_id=record.main_target_id,
        wake_revision=record.wake_revision,
        lease_started_at=record.lease_started_at,
        cooldown_until=record.cooldown_until,
        failure_streak=record.failure_streak,
        updated_at=record.updated_at,
    )


class MainReviewDeliveryLeaseStore:
    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "main_runtime" / "review_delivery_leases"

    def exists(self, *, main_target_id: str) -> bool:
        return self._path(main_target_id=main_target_id).is_file()

    def read(self, *, main_target_id: str) -> MainReviewDeliveryLeaseState:
        path = self._path(main_target_id=main_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="main_review_delivery_lease_read",
                resource_id=main_target_id,
            )
        try:
            record = read_json_model(
                path=path,
                record_type=MainReviewDeliveryLeaseRecord,
            )
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="main_review_delivery_lease_read",
                resource_id=main_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def upsert(
        self, *, state: MainReviewDeliveryLeaseState
    ) -> MainReviewDeliveryLeaseState:
        path = self._path(main_target_id=state.main_target_id)
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="main_review_delivery_lease_write",
                resource_id=state.main_target_id,
                message=str(error),
            ) from error
        return self.read(main_target_id=state.main_target_id)

    def _path(self, *, main_target_id: str) -> Path:
        return self._root / f"{main_target_id}.json"
