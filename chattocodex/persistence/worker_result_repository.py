from pathlib import Path

from chattocodex.git.git_result_dto import WorkerResultCommit
from chattocodex.persistence.atomic_json_file import read_json_model, write_json_model
from chattocodex.schemas.git_result_schemas import WorkerResultCommitRecord


class WorkerResultRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._state_root = state_root.expanduser().resolve()

    def read(self, *, run_id: str, worker_id: str) -> WorkerResultCommit | None:
        path = self._path(run_id=run_id, worker_id=worker_id)
        if not path.is_file():
            return None
        record = read_json_model(path=path, record_type=WorkerResultCommitRecord)
        return _build_result(record=record)

    def save(self, *, result: WorkerResultCommit) -> WorkerResultCommit:
        path = self._path(run_id=result.run_id, worker_id=result.worker_id)
        record = WorkerResultCommitRecord(
            run_id=result.run_id,
            worker_id=result.worker_id,
            base_commit=result.base_commit,
            commit_sha=result.commit_sha,
            changed_paths=result.changed_paths,
            summary=result.summary,
            created_at=result.created_at,
        )
        write_json_model(path=path, record=record)
        persisted = read_json_model(path=path, record_type=WorkerResultCommitRecord)
        return _build_result(record=persisted)

    def list_for_run(self, *, run_id: str) -> tuple[WorkerResultCommit, ...]:
        root = self._state_root / "runs" / run_id / "results"
        if not root.is_dir():
            return ()
        results: list[WorkerResultCommit] = []
        for path in sorted(root.glob("*.json")):
            record = read_json_model(path=path, record_type=WorkerResultCommitRecord)
            results.append(_build_result(record=record))
        return tuple(results)

    def _path(self, *, run_id: str, worker_id: str) -> Path:
        return self._state_root / "runs" / run_id / "results" / f"{worker_id}.json"


def _build_result(*, record: WorkerResultCommitRecord) -> WorkerResultCommit:
    return WorkerResultCommit(
        run_id=record.run_id,
        worker_id=record.worker_id,
        base_commit=record.base_commit,
        commit_sha=record.commit_sha,
        changed_paths=record.changed_paths,
        summary=record.summary,
        created_at=record.created_at,
    )
