import os
from collections.abc import Iterator
from contextlib import contextmanager
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunState,
    TaskState,
    WorkerState,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    RevisionConflictError,
    StatePersistenceError,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_mapper import (
    run_record_from_state as _run_record_from_state,
    run_state_from_record as _run_state_from_record,
    task_record_from_state as _task_record_from_state,
    task_state_from_record as _task_state_from_record,
    worker_record_from_state as _worker_record_from_state,
    worker_state_from_record as _worker_state_from_record,
)
from chattocodex.shared.contracts.orchestration_schemas import (
    RunRecord,
    TaskRecord,
    WorkerRecord,
)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    """Atomic write bytes. Args: path, payload. Returns: None."""
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


def _fsync_directory(path: Path) -> None:
    """Fsync directory. Args: path. Returns: None."""
    directory_flag = getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(path, os.O_RDONLY | directory_flag)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _read_record[RecordT: BaseModel](
    path: Path,
    record_type: type[RecordT],
    operation: str,
    resource_id: str,
) -> RecordT:
    """Read record. Args: path, record_type, operation, resource_id. Returns: RecordT."""
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
    path: Path,
    record: BaseModel,
    operation: str,
    resource_id: str,
    require_absent: bool,
) -> None:
    """Write record. Args: path, record, operation, resource_id, require_absent. Returns: None."""
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


class RunStateRepository:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._runs_root = state_root / "runs"

    def create(self, state: RunState) -> RunState:
        """Create. Args: state. Returns: RunState."""
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

    def read(self, run_id: str) -> RunState:
        """Read. Args: run_id. Returns: RunState."""
        record = _read_record(
            path=self._run_path(run_id=run_id),
            record_type=RunRecord,
            operation="read_run",
            resource_id=run_id,
        )
        return _run_state_from_record(record=record)

    def write(self, state: RunState) -> RunState:
        """Write. Args: state. Returns: RunState."""
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
        """List all. Args: None. Returns: tuple[RunState, ...]."""
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

    def _run_path(self, run_id: str) -> Path:
        """Run path. Args: run_id. Returns: Path."""
        return self._runs_root / run_id / "orchestration.json"


class WorkerStateRepository:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._runs_root = state_root / "runs"

    def create(self, state: WorkerState) -> WorkerState:
        """Create. Args: state. Returns: WorkerState."""
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

    def read(self, run_id: str, worker_id: str) -> WorkerState:
        """Read. Args: run_id, worker_id. Returns: WorkerState."""
        record = _read_record(
            path=self._worker_path(run_id=run_id, worker_id=worker_id),
            record_type=WorkerRecord,
            operation="read_worker",
            resource_id=worker_id,
        )
        return _worker_state_from_record(record=record)

    def write(self, state: WorkerState) -> WorkerState:
        """Write. Args: state. Returns: WorkerState."""
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

    def list_for_run(self, run_id: str) -> tuple[WorkerState, ...]:
        """List for run. Args: run_id. Returns: tuple[WorkerState, ...]."""
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

    def _worker_path(self, run_id: str, worker_id: str) -> Path:
        """Worker path. Args: run_id, worker_id. Returns: Path."""
        return self._runs_root / run_id / "workers" / f"{worker_id}.json"


class TaskStateRepository:
    def __init__(self, state_root: Path) -> None:
        """Initialize this object. Args: state_root. Returns: None."""
        self._runs_root = state_root / "runs"

    def create(self, state: TaskState) -> TaskState:
        """Create. Args: state. Returns: TaskState."""
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

    def read(self, run_id: str, task_id: str) -> TaskState:
        """Read. Args: run_id, task_id. Returns: TaskState."""
        record = _read_record(
            path=self._task_path(run_id=run_id, task_id=task_id),
            record_type=TaskRecord,
            operation="read_task",
            resource_id=task_id,
        )
        return _task_state_from_record(record=record)

    def write(self, state: TaskState, expected_revision: int) -> TaskState:
        """Write. Args: state, expected_revision. Returns: TaskState."""
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

    def list_for_run(self, run_id: str) -> tuple[TaskState, ...]:
        """List for run. Args: run_id. Returns: tuple[TaskState, ...]."""
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

    def list_for_worker(self, run_id: str, worker_id: str) -> tuple[TaskState, ...]:
        """List for worker. Args: run_id, worker_id. Returns: tuple[TaskState, ...]."""
        tasks = self.list_for_run(run_id=run_id)
        return tuple(task for task in tasks if task.assigned_worker_id == worker_id)

    def _task_path(self, run_id: str, task_id: str) -> Path:
        """Task path. Args: run_id, task_id. Returns: Path."""
        return self._runs_root / run_id / "tasks" / f"{task_id}.json"

    @contextmanager
    def _hold_write_lock(self, path: Path, task_id: str) -> Iterator[None]:
        """Hold write lock. Args: path, task_id. Returns: Iterator[None]."""
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
