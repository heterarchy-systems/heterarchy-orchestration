from dataclasses import dataclass


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerFileMutationResult:
    run_id: str
    worker_id: str
    relative_path: str
    sha256: str
    size_bytes: int
