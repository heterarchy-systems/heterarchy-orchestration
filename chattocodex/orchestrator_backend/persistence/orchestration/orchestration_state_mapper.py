from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    RunState,
    RunStatus,
    TaskState,
    TaskStatus,
    WorkerState,
    WorkerStatus,
)
from chattocodex.shared.contracts.orchestration_schemas import (
    RunRecord,
    TaskRecord,
    WorkerRecord,
)


def run_record_from_state(state: RunState) -> RunRecord:
    """Map a Run domain state into its durable strict record. Args: state. Returns: RunRecord."""
    return RunRecord(
        schema_version=1,
        run_id=state.run_id,
        goal=state.goal,
        project_id=state.project_id,
        project_name=state.project_name,
        project_root=state.project_root,
        run_handle_digest=state.run_handle_digest,
        status=state.status,
        revision=state.revision,
        requested_parallelism=state.requested_parallelism,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def run_state_from_record(record: RunRecord) -> RunState:
    """Map a durable Run record into domain state. Args: record. Returns: RunState."""
    return RunState(
        schema_version=record.schema_version,
        run_id=record.run_id,
        goal=record.goal,
        project_id=record.project_id,
        project_name=record.project_name,
        project_root=record.project_root,
        run_handle_digest=record.run_handle_digest,
        status=RunStatus(record.status),
        revision=record.revision,
        requested_parallelism=record.requested_parallelism,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def worker_record_from_state(state: WorkerState) -> WorkerRecord:
    """Map a Worker domain state into its durable strict record. Args: state. Returns: WorkerRecord."""
    return WorkerRecord(
        schema_version=1,
        run_id=state.run_id,
        worker_id=state.worker_id,
        worker_handle_digest=state.worker_handle_digest,
        status=state.status,
        role=state.role,
        revision=state.revision,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def worker_state_from_record(record: WorkerRecord) -> WorkerState:
    """Map a durable Worker record into domain state. Args: record. Returns: WorkerState."""
    return WorkerState(
        schema_version=record.schema_version,
        run_id=record.run_id,
        worker_id=record.worker_id,
        worker_handle_digest=record.worker_handle_digest,
        status=WorkerStatus(record.status),
        role=record.role,
        revision=record.revision,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def task_record_from_state(state: TaskState) -> TaskRecord:
    """Map a Task domain state into its durable strict record. Args: state. Returns: TaskRecord."""
    return TaskRecord(
        schema_version=1,
        run_id=state.run_id,
        task_id=state.task_id,
        assigned_worker_id=state.assigned_worker_id,
        objective=state.objective,
        status=state.status,
        revision=state.revision,
        progress_summary=state.progress_summary,
        result=state.result,
        created_at=state.created_at,
        updated_at=state.updated_at,
    )


def task_state_from_record(record: TaskRecord) -> TaskState:
    """Map a durable Task record into domain state. Args: record. Returns: TaskState."""
    return TaskState(
        schema_version=record.schema_version,
        run_id=record.run_id,
        task_id=record.task_id,
        assigned_worker_id=record.assigned_worker_id,
        objective=record.objective,
        status=TaskStatus(record.status),
        revision=record.revision,
        progress_summary=record.progress_summary,
        result=record.result,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )
