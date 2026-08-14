"""Atomic filesystem persistence for project binding state."""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from chattocodex.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    RevisionConflictError,
    StatePersistenceError,
)
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)
from chattocodex.project_runtime.project_binding_dto import ProjectBindingState
from chattocodex.project_runtime.project_binding_exceptions import (
    ProjectBindingConflictError,
)
from chattocodex.project_runtime.project_binding_mapper import (
    record_from_state,
    state_from_record,
)
from chattocodex.schemas.project_binding_schemas import (
    LegacyLabelOnlyProjectBindingRecord,
    ProjectBindingAdapterMetadata,
    ProjectBindingRecord,
)
from chattocodex.schemas.project_schemas import ProjectId

_PROJECT_ID_ADAPTER = TypeAdapter(ProjectId)


class ProjectBindingRepository:
    """Persist one canonical binding record per local project identifier."""

    def __init__(self, *, state_root: Path) -> None:
        """Initialize project binding storage below an execution state root.

        Args:
            state_root: Durable execution root shared by local runtime processes.
        """
        self._root = state_root.expanduser().resolve() / "project_bindings"

    def exists(self, *, project_id: str) -> bool:
        """Return whether a binding record exists for a project.

        Args:
            project_id: Canonical local project identifier.

        Returns:
            ``True`` when the durable record exists.

        Raises:
            ValueError: If ``project_id`` is not a valid canonical project ID.
        """
        return self._path(project_id=project_id).is_file()

    def create(self, *, state: ProjectBindingState) -> ProjectBindingState:
        """Create a binding record and reject duplicate creation.

        Args:
            state: Validated internal binding state to persist.

        Returns:
            The read-back persisted state.

        Raises:
            ResourceAlreadyExistsError: If the project already has a record.
            StatePersistenceError: If atomic persistence or read-back fails.
        """
        operation = "project_binding_create"
        path = self._path(project_id=state.project_id)
        with (
            self._hold_identity_lock(operation=operation),
            self._hold_filesystem_lock(
                path=path,
                operation=operation,
                resource_id=state.project_id,
            ),
        ):
            if path.exists():
                raise ResourceAlreadyExistsError(
                    operation=operation,
                    resource_id=state.project_id,
                )
            self._require_unique_adapter_identity(state=state)
            self._write(path=path, state=state, operation=operation)
            return self.read(project_id=state.project_id)

    def read(self, *, project_id: str) -> ProjectBindingState:
        """Read and validate one durable project binding.

        Args:
            project_id: Canonical local project identifier.

        Returns:
            The validated internal binding state.

        Raises:
            ResourceNotFoundError: If no binding exists for the project.
            StatePersistenceError: If persisted JSON is unreadable or invalid.
        """
        path = self._path(project_id=project_id)
        if not path.is_file():
            raise ResourceNotFoundError(
                operation="project_binding_read",
                resource_id=project_id,
            )
        try:
            record = read_json_model(path=path, record_type=ProjectBindingRecord)
        except AtomicJsonFileError as error:
            record = self._read_known_legacy_record(
                path=path,
                project_id=project_id,
                validation_error=error,
            )
        return state_from_record(record)

    @staticmethod
    def _read_known_legacy_record(
        *,
        path: Path,
        project_id: str,
        validation_error: AtomicJsonFileError,
    ) -> ProjectBindingRecord:
        """Read the exact 0.1.11 label-only shape as an unbound connector.

        Args:
            path: Durable project binding record path.
            project_id: Canonical project identifier used for error reporting.
            validation_error: Failure raised by strict current-record validation.

        Returns:
            A current record whose connector app identity is explicitly unbound.

        Raises:
            StatePersistenceError: If the record is not the known legacy shape.
        """
        try:
            legacy = read_json_model(
                path=path,
                record_type=LegacyLabelOnlyProjectBindingRecord,
            )
        except AtomicJsonFileError as legacy_error:
            raise StatePersistenceError(
                operation="project_binding_read",
                resource_id=project_id,
                message=str(validation_error),
            ) from legacy_error
        metadata = legacy.adapter_metadata
        return ProjectBindingRecord(
            schema_version=legacy.schema_version,
            project_id=legacy.project_id,
            project_name=legacy.project_name,
            project_root=legacy.project_root,
            adapter_metadata=ProjectBindingAdapterMetadata(
                provider=metadata.provider,
                project_ref=metadata.project_ref,
                project_label=metadata.project_label,
                project_path=metadata.project_path,
                app_ref=None,
                app_label=None,
                observed_at=metadata.observed_at,
            ),
            connector_epoch=legacy.connector_epoch,
            revision=legacy.revision,
            created_at=legacy.created_at,
            updated_at=legacy.updated_at,
        )

    def write(
        self,
        *,
        state: ProjectBindingState,
        expected_revision: int,
    ) -> ProjectBindingState:
        """Atomically replace a record only when its revision still matches.

        Args:
            state: New validated internal binding state.
            expected_revision: Revision read by the caller before mutation.

        Returns:
            The read-back persisted state.

        Raises:
            ResourceNotFoundError: If the binding was removed before the write.
            RevisionConflictError: If another writer advanced the record.
            StatePersistenceError: If atomic persistence or read-back fails.
        """
        operation = "project_binding_write"
        path = self._path(project_id=state.project_id)
        with (
            self._hold_identity_lock(operation=operation),
            self._hold_filesystem_lock(
                path=path,
                operation=operation,
                resource_id=state.project_id,
            ),
        ):
            current = self.read(project_id=state.project_id)
            if current.revision != expected_revision:
                raise RevisionConflictError(
                    operation=operation,
                    resource_id=state.project_id,
                    current_revision=current.revision,
                )
            if state.revision <= current.revision:
                raise RevisionConflictError(
                    operation=operation,
                    resource_id=state.project_id,
                    current_revision=current.revision,
                )
            self._require_unique_adapter_identity(state=state)
            self._write(path=path, state=state, operation=operation)
            return self.read(project_id=state.project_id)

    def list_all(self) -> tuple[ProjectBindingState, ...]:
        """Read all persisted project binding records in stable order.

        Returns:
            Binding states sorted by canonical project identifier.

        Raises:
            StatePersistenceError: If any persisted record is invalid.
        """
        if not self._root.is_dir():
            return ()
        return tuple(
            self.read(project_id=path.stem)
            for path in sorted(self._root.glob("prj_*.json"))
        )

    def _write(self, *, path: Path, state: ProjectBindingState, operation: str) -> None:
        try:
            write_json_model(path=path, record=record_from_state(state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation=operation,
                resource_id=state.project_id,
                message=str(error),
            ) from error

    def _require_unique_adapter_identity(self, *, state: ProjectBindingState) -> None:
        """Reject assigning one provider project to multiple local projects."""
        metadata = state.adapter_metadata
        if (
            metadata is None
            or metadata.provider is None
            or metadata.project_ref is None
        ):
            return
        duplicate = next(
            (
                existing
                for existing in self.list_all()
                if existing.project_id != state.project_id
                and existing.adapter_metadata is not None
                and existing.adapter_metadata.provider == metadata.provider
                and existing.adapter_metadata.project_ref == metadata.project_ref
            ),
            None,
        )
        if duplicate is not None:
            raise ProjectBindingConflictError(
                project_id=state.project_id,
                message=(
                    "Adapter provider/project reference is already bound to another "
                    "local project. One provider project can have only one local binding."
                ),
            )

    @contextmanager
    def _hold_identity_lock(self, *, operation: str) -> Iterator[None]:
        """Serialize cross-project provider/project-reference uniqueness checks."""
        with self._hold_filesystem_lock(
            path=self._root / "adapter_identity",
            operation=operation,
            resource_id="adapter_identity",
        ):
            yield

    @contextmanager
    def _hold_filesystem_lock(
        self,
        *,
        path: Path,
        operation: str,
        resource_id: str,
    ) -> Iterator[None]:
        """Hold an exclusive cross-process lock for one binding record."""
        lock_path = path.with_name(f".{path.name}.lock")
        file_descriptor: int | None = None
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            file_descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            flock(file_descriptor, LOCK_EX)
        except OSError as error:
            if file_descriptor is not None:
                os.close(file_descriptor)
            raise StatePersistenceError(
                operation=operation,
                resource_id=resource_id,
                message=f"Failed to lock project binding {resource_id!r}.",
            ) from error

        assert file_descriptor is not None
        try:
            yield
        finally:
            try:
                flock(file_descriptor, LOCK_UN)
            finally:
                os.close(file_descriptor)

    def _path(self, *, project_id: str) -> Path:
        try:
            normalized = _PROJECT_ID_ADAPTER.validate_python(project_id)
        except ValidationError as error:
            raise ValueError(f"Invalid project_id: {project_id!r}") from error
        return self._root / f"{normalized}.json"
