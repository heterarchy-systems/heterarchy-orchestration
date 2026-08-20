from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.worker_runtime.lease.wake_delivery_lease_state import (
    WakeDeliveryLeaseState,
)
from chattocodex.shared.contracts.wake.wake_delivery_schemas import (
    WakeDeliveryLeaseRecord,
)
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)


def _record_from_state(state: WakeDeliveryLeaseState) -> WakeDeliveryLeaseRecord:
    """Record from state. Args: state. Returns: WakeDeliveryLeaseRecord."""
    return WakeDeliveryLeaseRecord(
        schema_version=1,
        wake_target_id=state.wake_target_id,
        wake_revision=state.wake_revision,
        lease_started_at=state.lease_started_at,
        cooldown_until=state.cooldown_until,
        failure_streak=state.failure_streak,
        updated_at=state.updated_at,
    )


def _state_from_record(record: WakeDeliveryLeaseRecord) -> WakeDeliveryLeaseState:
    """State from record. Args: record. Returns: WakeDeliveryLeaseState."""
    return WakeDeliveryLeaseState(
        schema_version=record.schema_version,
        wake_target_id=record.wake_target_id,
        wake_revision=record.wake_revision,
        lease_started_at=record.lease_started_at,
        cooldown_until=record.cooldown_until,
        failure_streak=record.failure_streak,
        updated_at=record.updated_at,
    )


class WakeDeliveryLeaseStore:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._root = state_root / "worker_runtime" / "wake_delivery_leases"

    def exists(self, wake_target_id: str) -> bool:
        """Exists. Args: wake_target_id. Returns: bool."""
        return self._path(wake_target_id=wake_target_id).is_file()

    def read(self, wake_target_id: str) -> WakeDeliveryLeaseState:
        """Read. Args: wake_target_id. Returns: WakeDeliveryLeaseState."""
        path = self._path(wake_target_id=wake_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="wake_delivery_lease_read",
                resource_id=wake_target_id,
            )
        try:
            record = read_json_model(path=path, record_type=WakeDeliveryLeaseRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_delivery_lease_read",
                resource_id=wake_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def upsert(self, state: WakeDeliveryLeaseState) -> WakeDeliveryLeaseState:
        """Upsert. Args: state. Returns: WakeDeliveryLeaseState."""
        path = self._path(wake_target_id=state.wake_target_id)
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="wake_delivery_lease_write",
                resource_id=state.wake_target_id,
                message=str(error),
            ) from error
        return self.read(wake_target_id=state.wake_target_id)

    def _path(self, wake_target_id: str) -> Path:
        """Path. Args: wake_target_id. Returns: Path."""
        return self._root / f"{wake_target_id}.json"
