"""Atomic JSON persistence for project-bound Worker provisioning state."""

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    RevisionConflictError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningAdmissionState,
    ProvisioningDemandState,
    ProvisioningRequestState,
    ProvisioningStoreState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_mapper import (
    store_from_record,
    store_to_record,
)
from chattocodex.shared.contracts.worker.worker_provisioning_storage import (
    ProvisioningStoreRecord,
)
from chattocodex.shared.storage.atomic_json_file import (
    AtomicJsonFileError,
    AtomicJsonFileNotFoundError,
    read_json_model,
    write_json_model,
)


class WorkerProvisioningRepository:
    """Persist one atomic provisioning snapshot with a cross-process file lock."""

    def __init__(self, state_root: Path) -> None:
        """Initialize provisioning state below a durable execution root.

        Args:
            state_root: Global or workspace execution state root. Returns: None."""
        self._root = state_root.expanduser().resolve() / "worker_provisioning"
        self._path = self._root / "state.json"
        self._lock_path = self._root / ".state.lock"

    def exists(self) -> bool:
        """Return whether the atomic provisioning state file exists.

        Returns:
            ``True`` when durable provisioning state has been written. Args: None."""
        return self._path.is_file()

    def read_state(self) -> ProvisioningStoreState:
        """Read the validated atomic provisioning snapshot.

        Returns:
            Immutable provisioning state, or an empty initial snapshot.

        Raises:
            StatePersistenceError: If the persisted state cannot be read or validated. Args: None."""
        with self._hold_lock(operation="worker_provisioning_read"):
            return self._read_unlocked()

    def update(
        self,
        updater: Callable[[ProvisioningStoreState], ProvisioningStoreState],
    ) -> ProvisioningStoreState:
        """Atomically apply one compare-and-swap style state transformation.

        Args:
            updater: Pure function receiving the latest durable state.

        Returns:
            Read-back state after atomic replacement.

        Raises:
            StatePersistenceError: If locking, validation, or atomic persistence fails.
        """
        with self._hold_lock(operation="worker_provisioning_write"):
            current = self._read_unlocked()
            updated = updater(current)
            if updated == current:
                return current
            if updated.revision <= current.revision:
                raise RevisionConflictError(
                    operation="worker_provisioning_write",
                    resource_id="worker_provisioning",
                    current_revision=current.revision,
                )
            self._write_unlocked(state=updated)
            return self._read_unlocked()

    def replace(
        self,
        state: ProvisioningStoreState,
        expected_revision: int,
    ) -> ProvisioningStoreState:
        """Replace the snapshot only when the root revision still matches.

        Args:
            state: New immutable provisioning snapshot.
            expected_revision: Root revision read before the mutation.

        Returns:
            Read-back persisted state.

        Raises:
            RevisionConflictError: If another process advanced the snapshot.
            StatePersistenceError: If persistence fails.
        """
        with self._hold_lock(operation="worker_provisioning_replace"):
            current = self._read_unlocked()
            if current.revision != expected_revision:
                raise RevisionConflictError(
                    operation="worker_provisioning_replace",
                    resource_id="worker_provisioning",
                    current_revision=current.revision,
                )
            if state.revision <= current.revision:
                raise RevisionConflictError(
                    operation="worker_provisioning_replace",
                    resource_id="worker_provisioning",
                    current_revision=current.revision,
                )
            self._write_unlocked(state=state)
            return self._read_unlocked()

    def read_demand(self, project_id: str) -> ProvisioningDemandState:
        """Read one project's durable demand.

        Args:
            project_id: Canonical local project identifier.

        Returns:
            The project's immutable demand state.

        Raises:
            ResourceNotFoundError: If no demand exists for the project.
            StatePersistenceError: If the state is invalid.
        """
        state = self.read_state()
        for demand in state.demands:
            if demand.project_id == project_id:
                return demand
        raise ResourceNotFoundError(
            operation="worker_provisioning_demand_read", resource_id=project_id
        )

    def list_demands(self) -> tuple[ProvisioningDemandState, ...]:
        """List durable demands in canonical project order.

        Returns:
            Immutable demands sorted by project identifier. Args: None."""
        return tuple(
            sorted(self.read_state().demands, key=lambda item: item.project_id)
        )

    def read_request(self, request_id: str) -> ProvisioningRequestState:
        """Read one durable provisioning request.

        Args:
            request_id: Opaque provisioning request identifier.

        Returns:
            The immutable request state.

        Raises:
            ResourceNotFoundError: If no request exists.
            StatePersistenceError: If the state is invalid.
        """
        state = self.read_state()
        for request in state.requests:
            if request.request_id == request_id:
                return request
        raise ResourceNotFoundError(
            operation="worker_provisioning_request_read", resource_id=request_id
        )

    def list_requests(self) -> tuple[ProvisioningRequestState, ...]:
        """List durable provisioning requests in creation order.

        Returns:
            Immutable requests sorted by creation time and request identifier. Args: None."""
        return tuple(
            sorted(
                self.read_state().requests,
                key=lambda item: (item.created_at, item.request_id),
            )
        )

    def read_admission(self) -> ProvisioningAdmissionState:
        """Read the global durable admission record.

        Returns:
            Immutable global admission state. Args: None."""
        return self.read_state().admission

    def _read_unlocked(self) -> ProvisioningStoreState:
        """Read unlocked. Args: None. Returns: ProvisioningStoreState."""
        try:
            record = read_json_model(
                path=self._path,
                record_type=ProvisioningStoreRecord,
            )
        except AtomicJsonFileNotFoundError:
            now = _utc_now()
            return ProvisioningStoreState(
                schema_version=1,
                revision=0,
                demands=(),
                requests=(),
                admission=ProvisioningAdmissionState(
                    schema_version=1,
                    in_flight_request_id=None,
                    last_admitted_at=None,
                    next_admission_at=None,
                    backoff_attempt=0,
                    backoff_until=None,
                    revision=0,
                ),
                updated_at=now,
            )
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_provisioning_read",
                resource_id="worker_provisioning",
                message=str(error),
            ) from error
        return store_from_record(record)

    def _write_unlocked(self, state: ProvisioningStoreState) -> None:
        """Write unlocked. Args: state. Returns: None."""
        try:
            write_json_model(path=self._path, record=store_to_record(state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="worker_provisioning_write",
                resource_id="worker_provisioning",
                message=str(error),
            ) from error

    @contextmanager
    def _hold_lock(self, operation: str) -> Iterator[None]:
        """Hold the state lock across one read/transform/write operation. Args: operation. Returns: Iterator[None]."""
        descriptor: int | None = None
        try:
            self._root.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            flock(descriptor, LOCK_EX)
        except OSError as error:
            if descriptor is not None:
                os.close(descriptor)
            raise StatePersistenceError(
                operation=operation,
                resource_id="worker_provisioning",
                message="Failed to lock durable Worker provisioning state.",
            ) from error
        assert descriptor is not None
        try:
            yield
        finally:
            try:
                flock(descriptor, LOCK_UN)
            finally:
                os.close(descriptor)


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp. Args: None. Returns: datetime."""
    return datetime.now(UTC)
