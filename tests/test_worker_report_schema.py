import pytest
from pydantic import ValidationError

from chattocodex.schemas.worker_report_schemas import WorkerReportSubmitRequest


def test_worker_report_references_are_bounded() -> None:
    with pytest.raises(ValidationError, match="at most 32 items"):
        WorkerReportSubmitRequest(
            run_id="run_12345678",
            worker_id="worker_12345678",
            worker_handle="secret-worker-handle",
            task_id="task_12345678",
            expected_revision=0,
            title="Bound report references",
            summary="Prevent one report from retaining an unbounded reference list.",
            content="Evidence",
            references=tuple(f"ref-{index}" for index in range(33)),
        )
