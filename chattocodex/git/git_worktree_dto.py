from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class RunGitSnapshot:
    run_id: str
    project_root: str
    base_commit: str
    created_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerGitWorktree:
    run_id: str
    worker_id: str
    project_root: str
    worktree_root: str
    base_commit: str
    result_commit: str | None
    prepared_at: datetime
