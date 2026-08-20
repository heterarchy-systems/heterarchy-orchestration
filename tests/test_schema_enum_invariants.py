from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskStatus,
    WorkerSlotStatus,
)
from chattocodex.orchestrator_backend.main_runtime.target.main_review_target_state import (
    MainReviewTargetStatus,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowDecision,
)
from chattocodex.shared.contracts.broker.broker_schemas import (
    BrokerTaskRecord,
    WorkerSlotRecord,
)
from chattocodex.shared.contracts.main_runtime.main_runtime_schemas import (
    MainReviewTargetRecord,
)
from chattocodex.shared.contracts.run_control_schemas import (
    ReviewWorkflowRequest,
    WorkflowTaskInput,
)
from chattocodex.shared.contracts.wake.wake_schemas import WakeTargetRecord
from chattocodex.orchestrator_backend.wake.wake_dto import WakeTargetStatus

_BROKER_TASK_ID = f"btask_{'1' * 24}"
_MAIN_TARGET_ID = f"mtgt_{'2' * 32}"
_PROJECT_ID = "prj_sample_12345678"
_RUN_ID = f"run_{'3' * 24}"
_SLOT_ID = f"slot_{'4' * 24}"
_WAKE_TARGET_ID = f"wtgt_{'5' * 32}"
_WORKFLOW_HANDLE = f"wfh_{'a' * 20}"
_WORKFLOW_ID = f"wf_{'6' * 24}"


def _timestamp() -> datetime:
    return datetime.now(tz=UTC)


def test_queued_broker_task_rejects_partial_assignment_after_enum_serialization() -> (
    None
):
    now = _timestamp()
    with pytest.raises(ValidationError, match="cannot retain an assignment"):
        BrokerTaskRecord(
            broker_task_id=_BROKER_TASK_ID,
            run_id=_RUN_ID,
            objective="Validate enum-backed broker task invariants.",
            role=None,
            status=BrokerTaskStatus.QUEUED,
            assigned_slot_id=_SLOT_ID,
            assigned_worker_id=None,
            assigned_task_id=None,
            revision=0,
            created_at=now,
            updated_at=now,
        )


def test_idle_worker_slot_rejects_assignment_after_enum_serialization() -> None:
    now = _timestamp()
    with pytest.raises(ValidationError, match="cannot retain an assignment"):
        WorkerSlotRecord(
            slot_id=_SLOT_ID,
            project_id=_PROJECT_ID,
            slot_handle_digest="7" * 64,
            label=None,
            status=WorkerSlotStatus.IDLE,
            current_broker_task_id=_BROKER_TASK_ID,
            current_run_id=None,
            current_worker_id=None,
            current_task_id=None,
            revision=0,
            created_at=now,
            updated_at=now,
        )


def test_idle_main_target_rejects_pending_workflow_after_enum_serialization() -> None:
    now = _timestamp()
    with pytest.raises(ValidationError, match="cannot retain a pending workflow"):
        MainReviewTargetRecord(
            main_target_id=_MAIN_TARGET_ID,
            project_id=_PROJECT_ID,
            status=MainReviewTargetStatus.IDLE,
            pending_workflow_id=_WORKFLOW_ID,
            pending_workflow_revision=0,
            wake_revision=0,
            revision=0,
            created_at=now,
            updated_at=now,
        )


def test_busy_wake_target_requires_slot_after_enum_serialization() -> None:
    now = _timestamp()
    with pytest.raises(ValidationError, match="require a bound worker slot"):
        WakeTargetRecord(
            wake_target_id=_WAKE_TARGET_ID,
            project_id=_PROJECT_ID,
            status=WakeTargetStatus.BUSY,
            bound_slot_id=None,
            wake_revision=1,
            revision=1,
            created_at=now,
            updated_at=now,
        )


def test_rework_review_requires_tasks_after_enum_serialization() -> None:
    with pytest.raises(ValidationError, match="requires at least one rework task"):
        ReviewWorkflowRequest(
            workflow_id=_WORKFLOW_ID,
            workflow_handle=_WORKFLOW_HANDLE,
            decision=WorkflowDecision.REWORK,
            review_summary="Rework is required.",
        )


def test_non_rework_review_rejects_rework_tasks_after_enum_serialization() -> None:
    with pytest.raises(ValidationError, match="allowed only for a rework decision"):
        ReviewWorkflowRequest(
            workflow_id=_WORKFLOW_ID,
            workflow_handle=_WORKFLOW_HANDLE,
            decision=WorkflowDecision.APPROVE,
            review_summary="Approved.",
            rework_tasks=(
                WorkflowTaskInput(
                    key="fix-boundary",
                    objective="Restore the enum boundary contract.",
                ),
            ),
        )
