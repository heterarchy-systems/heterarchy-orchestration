from pathlib import Path

from chattocodex.main_runtime.main_review_target_state import MainReviewTargetState
from chattocodex.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    StatePersistenceError,
)
from chattocodex.persistence.atomic_json_file import (
    AtomicJsonFileError,
    read_json_model,
    write_json_model,
)
from chattocodex.schemas.main_runtime_schemas import MainReviewTargetRecord


def _record_from_state(*, state: MainReviewTargetState) -> MainReviewTargetRecord:
    return MainReviewTargetRecord(
        schema_version=1,
        main_target_id=state.main_target_id,
        project_id=state.project_id,
        status=state.status,
        pending_workflow_id=state.pending_workflow_id,
        pending_workflow_revision=state.pending_workflow_revision,
        wake_revision=state.wake_revision,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def _state_from_record(*, record: MainReviewTargetRecord) -> MainReviewTargetState:
    return MainReviewTargetState(
        schema_version=record.schema_version,
        main_target_id=record.main_target_id,
        project_id=record.project_id,
        status=record.status,
        pending_workflow_id=record.pending_workflow_id,
        pending_workflow_revision=record.pending_workflow_revision,
        wake_revision=record.wake_revision,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


class MainReviewTargetStore:
    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "main_runtime" / "review_targets"

    def create(self, *, state: MainReviewTargetState) -> MainReviewTargetState:
        path = self._path(main_target_id=state.main_target_id)
        if path.exists():
            raise ResourceAlreadyExistsError(
                operation="main_review_target_create",
                resource_id=state.main_target_id,
            )
        self._write(path=path, state=state, operation="main_review_target_create")
        return self.read(main_target_id=state.main_target_id)

    def read(self, *, main_target_id: str) -> MainReviewTargetState:
        path = self._path(main_target_id=main_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="main_review_target_read",
                resource_id=main_target_id,
            )
        try:
            record = read_json_model(path=path, record_type=MainReviewTargetRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="main_review_target_read",
                resource_id=main_target_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def write(self, *, state: MainReviewTargetState) -> MainReviewTargetState:
        path = self._path(main_target_id=state.main_target_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="main_review_target_write",
                resource_id=state.main_target_id,
            )
        self._write(path=path, state=state, operation="main_review_target_write")
        return self.read(main_target_id=state.main_target_id)

    def list_all(self) -> tuple[MainReviewTargetState, ...]:
        return tuple(
            self.read(main_target_id=path.stem)
            for path in sorted(self._root.glob("mtgt_*.json"))
        )

    def list_for_project(self, *, project_id: str) -> tuple[MainReviewTargetState, ...]:
        return tuple(
            state for state in self.list_all() if state.project_id == project_id
        )

    def _write(
        self,
        *,
        path: Path,
        state: MainReviewTargetState,
        operation: str,
    ) -> None:
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation=operation,
                resource_id=state.main_target_id,
                message=str(error),
            ) from error

    def _path(self, *, main_target_id: str) -> Path:
        return self._root / f"{main_target_id}.json"
