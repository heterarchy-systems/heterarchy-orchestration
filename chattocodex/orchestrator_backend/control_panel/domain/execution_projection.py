from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass

from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskState,
    BrokerTaskStatus,
    WorkerSlotState,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunState,
    RunStatus,
    TaskState,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowState,
    WorkflowStatus,
)
from chattocodex.shared.element_collect.control_panel_enums import (
    ExecutionTaskPhase,
    RunContinuationPhase,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionTaskProjection:
    broker_task: BrokerTaskState
    phase: ExecutionTaskPhase
    task: TaskState | None
    slot: WorkerSlotState | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RunExecutionProjection:
    run: RunState
    workflow: WorkflowState | None
    phase: RunContinuationPhase
    next_step: str
    tasks: tuple[ExecutionTaskProjection, ...]
    phase_counts: Counter[ExecutionTaskPhase]


def build_run_execution_projection(
    run: RunState,
    broker_tasks: tuple[BrokerTaskState, ...],
    slots: tuple[WorkerSlotState, ...],
    task_states: tuple[TaskState, ...],
    workflow: WorkflowState | None,
) -> RunExecutionProjection:
    """Build run execution projection. Args: run, broker_tasks, slots, task_states, workflow. Returns: RunExecutionProjection."""
    broker_task_by_id = {task.broker_task_id: task for task in broker_tasks}
    slot_by_id = {slot.slot_id: slot for slot in slots}
    task_by_id = {task.task_id: task for task in task_states}
    projections = tuple(
        ExecutionTaskProjection(
            broker_task=broker_task,
            phase=_task_phase(
                broker_task=broker_task,
                broker_task_by_id=broker_task_by_id,
            ),
            task=task_by_id.get(broker_task.assigned_task_id)
            if broker_task.assigned_task_id is not None
            else None,
            slot=slot_by_id.get(broker_task.assigned_slot_id)
            if broker_task.assigned_slot_id is not None
            else None,
        )
        for broker_task in sorted(
            broker_tasks,
            key=lambda item: (item.created_at, item.broker_task_id),
        )
    )
    counts: Counter[ExecutionTaskPhase] = Counter(item.phase for item in projections)
    phase, next_step = _continuation(
        run=run,
        workflow=workflow,
        projections=projections,
        counts=counts,
    )
    return RunExecutionProjection(
        run=run,
        workflow=workflow,
        phase=phase,
        next_step=next_step,
        tasks=projections,
        phase_counts=counts,
    )


def _task_phase(
    broker_task: BrokerTaskState,
    broker_task_by_id: Mapping[str, BrokerTaskState],
) -> ExecutionTaskPhase:
    """Task phase. Args: broker_task, broker_task_by_id. Returns: ExecutionTaskPhase."""
    if broker_task.status is BrokerTaskStatus.COMPLETED:
        return ExecutionTaskPhase.COMPLETED
    if broker_task.status is BrokerTaskStatus.FAILED:
        return ExecutionTaskPhase.FAILED
    if broker_task.status is BrokerTaskStatus.CLAIMED:
        return ExecutionTaskPhase.RUNNING
    if not all(
        (dependency := broker_task_by_id.get(dependency_id)) is not None
        and dependency.status is BrokerTaskStatus.COMPLETED
        for dependency_id in broker_task.depends_on
    ):
        return ExecutionTaskPhase.BLOCKED
    return ExecutionTaskPhase.READY


def _continuation(
    run: RunState,
    workflow: WorkflowState | None,
    projections: tuple[ExecutionTaskProjection, ...],
    counts: Counter[ExecutionTaskPhase],
) -> tuple[RunContinuationPhase, str]:
    """Continuation. Args: run, workflow, projections, counts. Returns: tuple[RunContinuationPhase, str]."""
    if workflow is not None:
        if workflow.status is WorkflowStatus.WAITING_MAIN_REVIEW:
            return (
                RunContinuationPhase.WAITING_MAIN_REVIEW,
                "Worker results are complete. Main Review is the next authority transition.",
            )
        if workflow.status is WorkflowStatus.COMPLETED:
            return RunContinuationPhase.COMPLETED, "Workflow approved and complete."
        if workflow.status is WorkflowStatus.FAILED:
            return RunContinuationPhase.FAILED, "Workflow ended in failure."

    if run.status is RunStatus.CANCELLED:
        return RunContinuationPhase.CANCELLED, "Run was cancelled."
    if counts[ExecutionTaskPhase.FAILED]:
        return (
            RunContinuationPhase.FAILED,
            "At least one task failed. Review the failed task before continuing.",
        )
    if counts[ExecutionTaskPhase.RUNNING]:
        return (
            RunContinuationPhase.RUNNING,
            f"{counts[ExecutionTaskPhase.RUNNING]} worker task(s) are executing under Broker leases.",
        )
    if counts[ExecutionTaskPhase.READY]:
        return (
            RunContinuationPhase.READY,
            f"{counts[ExecutionTaskPhase.READY]} task(s) are ready for Broker claim.",
        )
    if counts[ExecutionTaskPhase.BLOCKED]:
        return (
            RunContinuationPhase.BLOCKED,
            "Tasks are waiting for their declared dependencies to complete.",
        )
    if projections and all(
        item.phase is ExecutionTaskPhase.COMPLETED for item in projections
    ):
        if run.status is RunStatus.COMPLETED:
            return RunContinuationPhase.COMPLETED, "All tasks and the Run are complete."
        return (
            RunContinuationPhase.WAITING_MAIN_REVIEW,
            "All worker tasks are complete. The next step is orchestration/Main Review.",
        )
    if run.status is RunStatus.COMPLETED:
        return RunContinuationPhase.COMPLETED, "Run is complete."
    return (
        RunContinuationPhase.PLANNING,
        "The Run is active and waiting for its first orchestration task plan.",
    )
