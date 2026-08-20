from chattocodex.orchestrator_backend.broker.broker_dto import (
    BrokerAssignment,
    BrokerRunSnapshot,
    BrokerTaskState,
    FinishedAssignment,
    WorkerSlotState,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_run_view,
    build_task_view,
    build_worker_view,
)
from chattocodex.shared.contracts.broker.broker_schemas import (
    BrokerRunStatusSuccess,
    BrokerTaskView,
    WorkerAssignmentFinishSuccess,
    WorkerAssignmentView,
    WorkerSlotView,
)


def build_broker_task_view(state: BrokerTaskState) -> BrokerTaskView:
    """Build broker task view. Args: state. Returns: BrokerTaskView."""
    return BrokerTaskView(
        broker_task_id=state.broker_task_id,
        run_id=state.run_id,
        objective=state.objective,
        role=state.role,
        depends_on=state.depends_on,
        status=state.status,
        assigned_slot_id=state.assigned_slot_id,
        assigned_worker_id=state.assigned_worker_id,
        assigned_task_id=state.assigned_task_id,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def build_slot_view(state: WorkerSlotState) -> WorkerSlotView:
    """Build slot view. Args: state. Returns: WorkerSlotView."""
    return WorkerSlotView(
        slot_id=state.slot_id,
        project_id=state.project_id,
        label=state.label,
        status=state.status,
        current_broker_task_id=state.current_broker_task_id,
        current_run_id=state.current_run_id,
        current_worker_id=state.current_worker_id,
        current_task_id=state.current_task_id,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def build_assignment_view(assignment: BrokerAssignment) -> WorkerAssignmentView:
    """Build assignment view. Args: assignment. Returns: WorkerAssignmentView."""
    return WorkerAssignmentView(
        broker_task=build_broker_task_view(state=assignment.broker_task),
        slot=build_slot_view(state=assignment.slot),
        run=build_run_view(state=assignment.run),
        worker=build_worker_view(state=assignment.worker),
        task=build_task_view(state=assignment.task),
        worker_handle=assignment.worker_handle,
    )


def build_finish_success(finished: FinishedAssignment) -> WorkerAssignmentFinishSuccess:
    """Build finish success. Args: finished. Returns: WorkerAssignmentFinishSuccess."""
    return WorkerAssignmentFinishSuccess(
        broker_task=build_broker_task_view(state=finished.broker_task),
        slot=build_slot_view(state=finished.slot),
        task=build_task_view(state=finished.task),
    )


def build_broker_run_status_success(
    snapshot: BrokerRunSnapshot,
) -> BrokerRunStatusSuccess:
    """Build broker run status success. Args: snapshot. Returns: BrokerRunStatusSuccess."""
    return BrokerRunStatusSuccess(
        run=build_run_view(state=snapshot.run),
        broker_tasks=tuple(
            build_broker_task_view(state=task) for task in snapshot.broker_tasks
        ),
        slots=tuple(build_slot_view(state=slot) for slot in snapshot.slots),
        ready_count=snapshot.ready_count,
        queued_count=snapshot.queued_count,
        claimed_count=snapshot.claimed_count,
        completed_count=snapshot.completed_count,
        failed_count=snapshot.failed_count,
        idle_slot_count=snapshot.idle_slot_count,
        busy_slot_count=snapshot.busy_slot_count,
        is_complete=snapshot.is_complete,
    )
