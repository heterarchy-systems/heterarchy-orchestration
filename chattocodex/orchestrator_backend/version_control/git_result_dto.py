from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class GitCommitExecution:
    commit_sha: str
    parent_sha: str
    changed_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerResultCommit:
    run_id: str
    worker_id: str
    base_commit: str
    commit_sha: str
    changed_paths: tuple[str, ...]
    summary: str
    created_at: datetime
