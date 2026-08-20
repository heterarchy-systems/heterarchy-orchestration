from datetime import UTC, datetime

from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskState,
    BrokerTaskStatus,
)
from chattocodex.orchestrator_backend.control_panel.domain.execution_projection import (
    ExecutionTaskPhase,
    RunContinuationPhase,
    build_run_execution_projection,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunState,
    RunStatus,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowRoundState,
    WorkflowState,
    WorkflowStatus,
)


def _run(now: datetime) -> RunState:
    return RunState(
        schema_version=1,
        run_id="run_111111111111111111111111",
        goal="Inspect two independent areas",
        project_id="prj_sample_1234abcd",
        project_name="sample",
        project_root="/tmp/sample",
        run_handle_digest="a" * 64,
        status=RunStatus.ACTIVE,
        revision=1,
        created_at=now,
        updated_at=now,
        requested_parallelism=2,
    )


def _broker_task(
    *,
    now: datetime,
    task_id: str,
    objective: str,
    status: BrokerTaskStatus = BrokerTaskStatus.QUEUED,
    depends_on: tuple[str, ...] = (),
) -> BrokerTaskState:
    return BrokerTaskState(
        schema_version=1,
        broker_task_id=task_id,
        run_id="run_111111111111111111111111",
        objective=objective,
        role=None,
        depends_on=depends_on,
        status=status,
        assigned_slot_id=None,
        assigned_worker_id=None,
        assigned_task_id=None,
        revision=0,
        created_at=now,
        updated_at=now,
        external_published=not depends_on,
    )


def test_dependency_projection_distinguishes_ready_and_blocked() -> None:
    now = datetime.now(UTC)
    first = _broker_task(
        now=now,
        task_id="btask_aaaaaaaaaaaaaaaaaaaaaaaa",
        objective="Inspect API boundary",
    )
    dependent = _broker_task(
        now=now,
        task_id="btask_bbbbbbbbbbbbbbbbbbbbbbbb",
        objective="Review combined result",
        depends_on=(first.broker_task_id,),
    )
    projection = build_run_execution_projection(
        run=_run(now),
        broker_tasks=(first, dependent),
        slots=(),
        task_states=(),
        workflow=None,
    )
    assert tuple(item.phase for item in projection.tasks) == (
        ExecutionTaskPhase.READY,
        ExecutionTaskPhase.BLOCKED,
    )
    assert projection.phase is RunContinuationPhase.READY
    assert "ready for Broker claim" in projection.next_step


def test_waiting_main_review_is_visible_after_worker_tasks_finish() -> None:
    now = datetime.now(UTC)
    first = _broker_task(
        now=now,
        task_id="btask_aaaaaaaaaaaaaaaaaaaaaaaa",
        objective="Inspect API boundary",
        status=BrokerTaskStatus.COMPLETED,
    )
    second = _broker_task(
        now=now,
        task_id="btask_bbbbbbbbbbbbbbbbbbbbbbbb",
        objective="Inspect runtime boundary",
        status=BrokerTaskStatus.COMPLETED,
    )
    workflow = WorkflowState(
        schema_version=1,
        workflow_id="wf_aaaaaaaaaaaaaaaaaaaaaaaa",
        workflow_handle_digest="b" * 64,
        goal="Inspect two independent areas",
        project_id="prj_sample_1234abcd",
        review_instructions="Compare both worker reports.",
        max_rework_rounds=1,
        rework_rounds=0,
        status=WorkflowStatus.WAITING_MAIN_REVIEW,
        rounds=(
            WorkflowRoundState(
                round_number=1,
                run_id="run_111111111111111111111111",
                task_keys=("api", "runtime"),
                broker_task_ids=(first.broker_task_id, second.broker_task_id),
                created_at=now,
            ),
        ),
        last_review_summary=None,
        revision=2,
        created_at=now,
        updated_at=now,
        requested_parallelism=2,
    )
    projection = build_run_execution_projection(
        run=_run(now),
        broker_tasks=(first, second),
        slots=(),
        task_states=(),
        workflow=workflow,
    )
    assert projection.phase is RunContinuationPhase.WAITING_MAIN_REVIEW
    assert "Main Review" in projection.next_step
    assert projection.phase_counts[ExecutionTaskPhase.COMPLETED] == 2
