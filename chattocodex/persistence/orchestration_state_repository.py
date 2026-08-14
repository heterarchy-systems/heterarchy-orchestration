import os
from collections.abc import Iterator
from contextlib import contextmanager
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from chattocodex.orchestration.orchestration_dto import RunState, TaskState, WorkerState
from chattocodex.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    RevisionConflictError,
    StatePersistenceError,
)
from chattocodex.schemas.orchestration_schemas import (
    RunRecord,
    TaskRecord,
    WorkerRecord,
)


def _atomic_write_bytes(*, path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    file_descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(file_descriptor, "wb") as file_handle:
            file_handle.write(payload)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temporary_path, path)
        _fsync_directory(path=path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)


def _fsync_directory(*, path: Path) -> None:
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, os.O_RDONLY | directory_flag)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _read_record[RecordT: BaseModel](
    *,
    path: Path,
    record_type: type[RecordT],
    operation: str,
    resource_id: str,
) -> RecordT:
    try:
        raw_json = path.read_bytes()
    except FileNotFoundError as exc:
        raise ResourceNotFoundError(
            operation=operation,
            resource_id=resource_id,
        ) from exc
    except OSError as exc:
        raise StatePersistenceError(
            operation=operation,
            resource_id=resource_id,
            message=f"Failed reading state for {resource_id!r}: {exc}",
        ) from exc

    try:
        return record_type.model_validate_json(raw_json)
    except ValidationError as exc:
        raise StatePersistenceError(
            operation=operation,
            resource_id=resource_id,
            message=f"Persisted state for {resource_id!r} failed validation.",
        ) from exc


def _write_record(
    *,
    path: Path,
    record: BaseModel,
    operation: str,
    resource_id: str,
    require_absent: bool,
) -> None:
    if require_absent and path.exists():
        raise ResourceAlreadyExistsError(
            operation=operation,
            resource_id=resource_id,
        )

    try:
        payload = record.model_dump_json(indent=2).encode("utf-8")
        _atomic_write_bytes(path=path, payload=payload)
    except OSError as exc:
        raise StatePersistenceError(
            operation=operation,
            resource_id=resource_id,
            message=f"Failed writing state for {resource_id!r}: {exc}",
        ) from exc


def _run_record_from_state(*, state: RunState) -> RunRecord:
    return RunRecord(
        schema_version=1,
        run_id=state.run_id,
        goal=state.goal,
        project_id=state.project_id,
        project_name=state.project_name,
        project_root=state.project_root,
        run_handle_digest=state.run_handle_digest,
        status=state.status,
        revision=state.revision,
        requested_parallelism=state.requested_parallelism,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def _run_state_from_record(*, record: RunRecord) -> RunState:
    return RunState(
        schema_version=record.schema_version,
        run_id=record.run_id,
        goal=record.goal,
        project_id=record.project_id,
        project_name=record.project_name,
        project_root=record.project_root,
        run_handle_digest=record.run_handle_digest,
        status=record.status,
        revision=record.revision,
        requested_parallelism=record.requested_parallelism,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _worker_record_from_state(*, state: WorkerState) -> WorkerRecord:
    return WorkerRecord(
        schema_version=1,
        run_id=state.run_id,
        worker_id=state.worker_id,
        worker_handle_digest=state.worker_handle_digest,
        status=state.status,
        role=state.role,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def _worker_state_from_record(*, record: WorkerRecord) -> WorkerState:
    return WorkerState(
        schema_version=record.schema_version,
        run_id=record.run_id,
        worker_id=record.worker_id,
        worker_handle_digest=record.worker_handle_digest,
        status=record.status,
        role=record.role,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _task_record_from_state(*, state: TaskState) -> TaskRecord:
    return TaskRecord(
        schema_version=1,
        run_id=state.run_id,
        task_id=state.task_id,
        assigned_worker_id=state.assigned_worker_id,
        objective=state.objective,
        status=state.status,
        revision=state.revision,
        progress_summary=state.progress_summary,
        result=state.result,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def _task_state_from_record(*, record: TaskRecord) -> TaskState:
    return TaskState(
        schema_version=record.schema_version,
        run_id=record.run_id,
        task_id=record.task_id,
        assigned_worker_id=record.assigned_worker_id,
        objective=record.objective,
        status=record.status,
        revision=record.revision,
        progress_summary=record.progress_summary,
        result=record.result,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


class RunStateRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._runs_root = state_root / "runs"

    def create(self, *, state: RunState) -> RunState:
        path = self._run_path(run_id=state.run_id)
        record = _run_record_from_state(state=state)
        _write_record(
            path=path,
            record=record,
            operation="create_run",
            resource_id=state.run_id,
            require_absent=True,
        )
        return self.read(run_id=state.run_id)

    def read(self, *, run_id: str) -> RunState:
        record = _read_record(
            path=self._run_path(run_id=run_id),
            record_type=RunRecord,
            operation="read_run",
            resource_id=run_id,
        )
        return _run_state_from_record(record=record)

    def write(self, *, state: RunState) -> RunState:
        path = self._run_path(run_id=state.run_id)
        record = _run_record_from_state(state=state)
        _write_record(
            path=path,
            record=record,
            operation="write_run",
            resource_id=state.run_id,
            require_absent=False,
        )
        return self.read(run_id=state.run_id)

    def list_all(self) -> tuple[RunState, ...]:
        run_paths = sorted(self._runs_root.glob("run_*/orchestration.json"))
        return tuple(
            _run_state_from_record(
                record=_read_record(
                    path=run_path,
                    record_type=RunRecord,
                    operation="list_runs",
                    resource_id=run_path.parent.name,
                )
            )
            for run_path in run_paths
        )

    def _run_path(self, *, run_id: str) -> Path:
        return self._runs_root / run_id / "orchestration.json"


class WorkerStateRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._runs_root = state_root / "runs"

    def create(self, *, state: WorkerState) -> WorkerState:
        path = self._worker_path(run_id=state.run_id, worker_id=state.worker_id)
        record = _worker_record_from_state(state=state)
        _write_record(
            path=path,
            record=record,
            operation="create_worker",
            resource_id=state.worker_id,
            require_absent=True,
        )
        return self.read(run_id=state.run_id, worker_id=state.worker_id)

    def read(self, *, run_id: str, worker_id: str) -> WorkerState:
        record = _read_record(
            path=self._worker_path(run_id=run_id, worker_id=worker_id),
            record_type=WorkerRecord,
            operation="read_worker",
            resource_id=worker_id,
        )
        return _worker_state_from_record(record=record)

    def write(self, *, state: WorkerState) -> WorkerState:
        path = self._worker_path(run_id=state.run_id, worker_id=state.worker_id)
        record = _worker_record_from_state(state=state)
        _write_record(
            path=path,
            record=record,
            operation="write_worker",
            resource_id=state.worker_id,
            require_absent=False,
        )
        return self.read(run_id=state.run_id, worker_id=state.worker_id)

    def list_for_run(self, *, run_id: str) -> tuple[WorkerState, ...]:
        worker_root = self._runs_root / run_id / "workers"
        worker_paths = sorted(worker_root.glob("worker_*.json"))
        return tuple(
            _worker_state_from_record(
                record=_read_record(
                    path=worker_path,
                    record_type=WorkerRecord,
                    operation="list_workers",
                    resource_id=worker_path.stem,
                )
            )
            for worker_path in worker_paths
        )

    def _worker_path(self, *, run_id: str, worker_id: str) -> Path:
        return self._runs_root / run_id / "workers" / f"{worker_id}.json"


class TaskStateRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._runs_root = state_root / "runs"

    def create(self, *, state: TaskState) -> TaskState:
        path = self._task_path(run_id=state.run_id, task_id=state.task_id)
        record = _task_record_from_state(state=state)
        _write_record(
            path=path,
            record=record,
            operation="create_task",
            resource_id=state.task_id,
            require_absent=True,
        )
        return self.read(run_id=state.run_id, task_id=state.task_id)

    def read(self, *, run_id: str, task_id: str) -> TaskState:
        record = _read_record(
            path=self._task_path(run_id=run_id, task_id=task_id),
            record_type=TaskRecord,
            operation="read_task",
            resource_id=task_id,
        )
        return _task_state_from_record(record=record)

    def write(self, *, state: TaskState, expected_revision: int) -> TaskState:
        path = self._task_path(run_id=state.run_id, task_id=state.task_id)
        with self._hold_write_lock(path=path, task_id=state.task_id):
            current = self.read(run_id=state.run_id, task_id=state.task_id)
            if (
                current.revision != expected_revision
                or state.revision <= current.revision
            ):
                raise RevisionConflictError(
                    operation="write_task",
                    resource_id=state.task_id,
                    current_revision=current.revision,
                )
            record = _task_record_from_state(state=state)
            _write_record(
                path=path,
                record=record,
                operation="write_task",
                resource_id=state.task_id,
                require_absent=False,
            )
            return self.read(run_id=state.run_id, task_id=state.task_id)

    def list_for_run(self, *, run_id: str) -> tuple[TaskState, ...]:
        task_root = self._runs_root / run_id / "tasks"
        task_paths = sorted(task_root.glob("task_*.json"))
        return tuple(
            _task_state_from_record(
                record=_read_record(
                    path=task_path,
                    record_type=TaskRecord,
                    operation="list_tasks",
                    resource_id=task_path.stem,
                )
            )
            for task_path in task_paths
        )

    def list_for_worker(self, *, run_id: str, worker_id: str) -> tuple[TaskState, ...]:
        tasks = self.list_for_run(run_id=run_id)
        return tuple(task for task in tasks if task.assigned_worker_id == worker_id)

    def _task_path(self, *, run_id: str, task_id: str) -> Path:
        return self._runs_root / run_id / "tasks" / f"{task_id}.json"

    @contextmanager
    def _hold_write_lock(self, *, path: Path, task_id: str) -> Iterator[None]:
        lock_path = path.with_suffix(".lock")
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                flock(descriptor, LOCK_EX)
                yield
            finally:
                try:
                    flock(descriptor, LOCK_UN)
                finally:
                    os.close(descriptor)
        except OSError as exc:
            raise StatePersistenceError(
                operation="write_task",
                resource_id=task_id,
                message=f"Failed locking state for {task_id!r}: {exc}",
            ) from exc
