from datetime import UTC, datetime
from pathlib import Path

from chattocodex.git.git_worktree_dto import RunGitSnapshot, WorkerGitWorktree
from chattocodex.persistence.atomic_json_file import read_json_model, write_json_model
from chattocodex.schemas.git_worktree_schemas import (
    RunGitSnapshotRecord,
    WorkerGitWorktreeRecord,
)


class GitWorktreeRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._state_root = state_root.expanduser().resolve()

    def read_run_snapshot(self, *, run_id: str) -> RunGitSnapshot | None:
        path = self._run_snapshot_path(run_id=run_id)
        if not path.is_file():
            return None
        record = read_json_model(path=path, record_type=RunGitSnapshotRecord)
        return _build_run_snapshot(record=record)

    def create_run_snapshot(
        self,
        *,
        run_id: str,
        project_root: Path,
        base_commit: str,
    ) -> RunGitSnapshot:
        path = self._run_snapshot_path(run_id=run_id)
        if path.exists():
            existing = self.read_run_snapshot(run_id=run_id)
            if existing is None:
                raise RuntimeError(f"Git snapshot disappeared for Run {run_id}.")
            return existing
        record = RunGitSnapshotRecord(
            run_id=run_id,
            project_root=str(project_root),
            base_commit=base_commit,
            created_at=datetime.now(UTC),
        )
        write_json_model(path=path, record=record)
        return _build_run_snapshot(record=record)

    def read_worker_worktree(
        self,
        *,
        run_id: str,
        worker_id: str,
    ) -> WorkerGitWorktree | None:
        path = self._worker_record_path(run_id=run_id, worker_id=worker_id)
        if not path.is_file():
            return None
        record = read_json_model(path=path, record_type=WorkerGitWorktreeRecord)
        return _build_worker_worktree(record=record)

    def save_worker_worktree(
        self,
        *,
        run_id: str,
        worker_id: str,
        project_root: Path,
        worktree_root: Path,
        base_commit: str,
    ) -> WorkerGitWorktree:
        record = WorkerGitWorktreeRecord(
            run_id=run_id,
            worker_id=worker_id,
            project_root=str(project_root),
            worktree_root=str(worktree_root),
            base_commit=base_commit,
            result_commit=None,
            prepared_at=datetime.now(UTC),
        )
        path = self._worker_record_path(run_id=run_id, worker_id=worker_id)
        write_json_model(path=path, record=record)
        persisted = read_json_model(path=path, record_type=WorkerGitWorktreeRecord)
        return _build_worker_worktree(record=persisted)

    def mark_result_commit(
        self,
        *,
        run_id: str,
        worker_id: str,
        result_commit: str,
    ) -> WorkerGitWorktree:
        path = self._worker_record_path(run_id=run_id, worker_id=worker_id)
        record = read_json_model(path=path, record_type=WorkerGitWorktreeRecord)
        updated = record.model_copy(update={"result_commit": result_commit})
        write_json_model(path=path, record=updated)
        persisted = read_json_model(path=path, record_type=WorkerGitWorktreeRecord)
        return _build_worker_worktree(record=persisted)

    def worktree_root(self, *, run_id: str, worker_id: str) -> Path:
        return self._state_root / "worktrees" / run_id / worker_id

    def _run_snapshot_path(self, *, run_id: str) -> Path:
        return self._state_root / "runs" / run_id / "git-workspace.json"

    def _worker_record_path(self, *, run_id: str, worker_id: str) -> Path:
        return self._state_root / "runs" / run_id / "worktrees" / f"{worker_id}.json"


def _build_run_snapshot(*, record: RunGitSnapshotRecord) -> RunGitSnapshot:
    return RunGitSnapshot(
        run_id=record.run_id,
        project_root=record.project_root,
        base_commit=record.base_commit,
        created_at=record.created_at,
    )


def _build_worker_worktree(*, record: WorkerGitWorktreeRecord) -> WorkerGitWorktree:
    return WorkerGitWorktree(
        run_id=record.run_id,
        worker_id=record.worker_id,
        project_root=record.project_root,
        worktree_root=record.worktree_root,
        base_commit=record.base_commit,
        result_commit=record.result_commit,
        prepared_at=record.prepared_at,
    )
