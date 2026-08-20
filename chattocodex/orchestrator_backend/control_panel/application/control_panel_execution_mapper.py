from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerTaskState,
    WorkerSlotState,
)
from chattocodex.orchestrator_backend.control_panel.domain.execution_projection import (
    ExecutionTaskPhase,
    ExecutionTaskProjection,
    build_run_execution_projection,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunState,
    TaskState,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import WorkflowState
from chattocodex.shared.contracts.control_panel.control_panel_operations_schemas import (
    ExecutionLeaseOperationsView,
    ExecutionTaskOperationsView,
    RunExecutionOperationsView,
)


def build_execution_operations_view(
    run: RunState,
    slots: tuple[WorkerSlotState, ...],
    broker_tasks: tuple[BrokerTaskState, ...],
    task_states: tuple[TaskState, ...],
    workflow: WorkflowState | None,
) -> RunExecutionOperationsView:
    """Map one Run and its durable coordination state into an operations view.

    Args:
        run: Durable Run state to project.
        slots: Worker slots associated with the Run project.
        broker_tasks: Broker tasks owned by the Run.
        task_states: Durable Worker task states for the Run.
        workflow: Latest Main Review workflow associated with the Run, if any.

    Returns:
        Strict Control Panel execution operations view.
    """
    projection = build_run_execution_projection(
        run=run,
        broker_tasks=broker_tasks,
        slots=slots,
        task_states=task_states,
        workflow=workflow,
    )
    counts = projection.phase_counts
    return RunExecutionOperationsView(
        run_id=run.run_id,
        project_id=run.project_id,
        project_name=run.project_name,
        goal=run.goal,
        status=run.status,
        continuation_phase=projection.phase,
        next_step=projection.next_step,
        workflow_id=(
            projection.workflow.workflow_id if projection.workflow is not None else None
        ),
        workflow_status=(
            projection.workflow.status if projection.workflow is not None else None
        ),
        requested_parallelism=run.requested_parallelism,
        total_tasks=len(projection.tasks),
        blocked=counts[ExecutionTaskPhase.BLOCKED],
        ready=counts[ExecutionTaskPhase.READY],
        running=counts[ExecutionTaskPhase.RUNNING],
        completed=counts[ExecutionTaskPhase.COMPLETED],
        failed=counts[ExecutionTaskPhase.FAILED],
        tasks=tuple(_build_execution_task_view(item=item) for item in projection.tasks),
        updated_at=run.updated_at,
    )


def _build_execution_task_view(
    item: ExecutionTaskProjection,
) -> ExecutionTaskOperationsView:
    """Map one projected broker task into its Control Panel task view.

    Args:
        item: Joined broker, Worker task, and slot projection.

    Returns:
        Strict task operations view including an optional lease fence.
    """
    task = item.task
    broker_task = item.broker_task
    return ExecutionTaskOperationsView(
        broker_task_id=broker_task.broker_task_id,
        objective=broker_task.objective,
        role=broker_task.role,
        depends_on=broker_task.depends_on,
        phase=item.phase,
        external_published=broker_task.external_published,
        worker_id=broker_task.assigned_worker_id,
        task_id=task.task_id if task is not None else None,
        progress_summary=task.progress_summary if task is not None else None,
        result=task.result if task is not None else None,
        lease=_build_execution_lease_view(item=item),
        updated_at=max(
            broker_task.updated_at,
            task.updated_at if task is not None else broker_task.updated_at,
            item.slot.updated_at if item.slot is not None else broker_task.updated_at,
        ),
    )


def _build_execution_lease_view(
    item: ExecutionTaskProjection,
) -> ExecutionLeaseOperationsView | None:
    """Build a complete Broker lease fence view when every field is present.

    Args:
        item: Joined task projection whose Worker slot may carry a Broker lease.

    Returns:
        Complete lease operations view, or ``None`` when no complete lease exists.
    """
    slot = item.slot
    if slot is None:
        return None
    values = (
        slot.broker_term,
        slot.broker_group_generation,
        slot.broker_lease_id,
        slot.broker_lease_epoch,
        slot.broker_lease_expires_at_ms,
    )
    if any(value is None for value in values):
        return None
    assert slot.broker_term is not None
    assert slot.broker_group_generation is not None
    assert slot.broker_lease_id is not None
    assert slot.broker_lease_epoch is not None
    assert slot.broker_lease_expires_at_ms is not None
    return ExecutionLeaseOperationsView(
        broker_consumer_id=slot.broker_consumer_id,
        term=slot.broker_term,
        generation=slot.broker_group_generation,
        lease_id=slot.broker_lease_id,
        lease_epoch=slot.broker_lease_epoch,
        lease_expires_at_ms=slot.broker_lease_expires_at_ms,
    )
