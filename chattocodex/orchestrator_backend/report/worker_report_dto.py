from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerReport:
    run_id: str
    worker_id: str
    task_id: str
    title: str
    summary: str
    content: str
    references: tuple[str, ...]
    created_at: datetime
