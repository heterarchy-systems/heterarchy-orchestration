from pathlib import Path

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
from chattocodex.run_control.run_control_state import WorkflowRoundState, WorkflowState
from chattocodex.schemas.run_control_schemas import WorkflowRecord, WorkflowRoundRecord


def _record_from_state(*, state: WorkflowState) -> WorkflowRecord:
    return WorkflowRecord(
        schema_version=1,
        workflow_id=state.workflow_id,
        workflow_handle_digest=state.workflow_handle_digest,
        goal=state.goal,
        project_id=state.project_id,
        review_instructions=state.review_instructions,
        max_rework_rounds=state.max_rework_rounds,
        rework_rounds=state.rework_rounds,
        status=state.status,
        rounds=tuple(
            WorkflowRoundRecord(
                round_number=round_state.round_number,
                run_id=round_state.run_id,
                task_keys=round_state.task_keys,
                broker_task_ids=round_state.broker_task_ids,
                created_at=round_state.created_at,
            )
            for round_state in state.rounds
        ),
        last_review_summary=state.last_review_summary,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
        requested_parallelism=state.requested_parallelism,
    )


def _state_from_record(*, record: WorkflowRecord) -> WorkflowState:
    return WorkflowState(
        schema_version=record.schema_version,
        workflow_id=record.workflow_id,
        workflow_handle_digest=record.workflow_handle_digest,
        goal=record.goal,
        project_id=record.project_id,
        review_instructions=record.review_instructions,
        max_rework_rounds=record.max_rework_rounds,
        rework_rounds=record.rework_rounds,
        status=record.status,
        rounds=tuple(
            WorkflowRoundState(
                round_number=item.round_number,
                run_id=item.run_id,
                task_keys=item.task_keys,
                broker_task_ids=item.broker_task_ids,
                created_at=item.created_at,
            )
            for item in record.rounds
        ),
        last_review_summary=record.last_review_summary,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
        requested_parallelism=record.requested_parallelism,
    )


class RunControlStateStore:
    def __init__(self, *, state_root: Path) -> None:
        self._root = state_root / "run_control" / "records"

    def create(self, *, state: WorkflowState) -> WorkflowState:
        path = self._path(workflow_id=state.workflow_id)
        if path.exists():
            raise ResourceAlreadyExistsError(
                operation="run_control_create", resource_id=state.workflow_id
            )
        self._write(path=path, state=state, operation="run_control_create")
        return self.read(workflow_id=state.workflow_id)

    def read(self, *, workflow_id: str) -> WorkflowState:
        path = self._path(workflow_id=workflow_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="run_control_read", resource_id=workflow_id
            )
        try:
            record = read_json_model(path=path, record_type=WorkflowRecord)
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation="run_control_read",
                resource_id=workflow_id,
                message=str(error),
            ) from error
        return _state_from_record(record=record)

    def write(self, *, state: WorkflowState) -> WorkflowState:
        path = self._path(workflow_id=state.workflow_id)
        if not path.exists():
            raise ResourceNotFoundError(
                operation="run_control_write", resource_id=state.workflow_id
            )
        self._write(path=path, state=state, operation="run_control_write")
        return self.read(workflow_id=state.workflow_id)

    def list_all(self) -> tuple[WorkflowState, ...]:
        return tuple(
            self.read(workflow_id=path.stem)
            for path in sorted(self._root.glob("wf_*.json"))
        )

    def list_for_run(self, *, run_id: str) -> tuple[WorkflowState, ...]:
        return tuple(
            state
            for state in self.list_all()
            if any(round_state.run_id == run_id for round_state in state.rounds)
        )

    def _write(self, *, path: Path, state: WorkflowState, operation: str) -> None:
        try:
            write_json_model(path=path, record=_record_from_state(state=state))
        except AtomicJsonFileError as error:
            raise StatePersistenceError(
                operation=operation, resource_id=state.workflow_id, message=str(error)
            ) from error

    def _path(self, *, workflow_id: str) -> Path:
        return self._root / f"{workflow_id}.json"
