from pathlib import Path

from chattocodex.persistence.atomic_json_file import read_json_model, write_json_model
from chattocodex.report.worker_report_dto import WorkerReport
from chattocodex.schemas.worker_report_schemas import WorkerReportRecord


class WorkerReportRepository:
    def __init__(self, *, state_root: Path) -> None:
        self._state_root = state_root.expanduser().resolve()

    def read(self, *, run_id: str, task_id: str) -> WorkerReport | None:
        path = self._path(run_id=run_id, task_id=task_id)
        if not path.is_file():
            return None
        record = read_json_model(path=path, record_type=WorkerReportRecord)
        return _build_report(record=record)

    def save(self, *, report: WorkerReport) -> WorkerReport:
        path = self._path(run_id=report.run_id, task_id=report.task_id)
        record = WorkerReportRecord(
            run_id=report.run_id,
            worker_id=report.worker_id,
            task_id=report.task_id,
            title=report.title,
            summary=report.summary,
            content=report.content,
            references=report.references,
            created_at=report.created_at,
        )
        write_json_model(path=path, record=record)
        persisted = read_json_model(path=path, record_type=WorkerReportRecord)
        return _build_report(record=persisted)

    def delete_if_matches(self, *, report: WorkerReport) -> bool:
        path = self._path(run_id=report.run_id, task_id=report.task_id)
        if not path.is_file():
            return False
        persisted = _build_report(
            record=read_json_model(path=path, record_type=WorkerReportRecord)
        )
        if persisted != report:
            return False
        path.unlink()
        return True

    def list_for_run(self, *, run_id: str) -> tuple[WorkerReport, ...]:
        root = self._state_root / "runs" / run_id / "reports"
        if not root.is_dir():
            return ()
        reports: list[WorkerReport] = []
        for path in sorted(root.glob("task_*.json")):
            record = read_json_model(path=path, record_type=WorkerReportRecord)
            reports.append(_build_report(record=record))
        return tuple(reports)

    def _path(self, *, run_id: str, task_id: str) -> Path:
        return self._state_root / "runs" / run_id / "reports" / f"{task_id}.json"


def _build_report(*, record: WorkerReportRecord) -> WorkerReport:
    return WorkerReport(
        run_id=record.run_id,
        worker_id=record.worker_id,
        task_id=record.task_id,
        title=record.title,
        summary=record.summary,
        content=record.content,
        references=record.references,
        created_at=record.created_at,
    )
