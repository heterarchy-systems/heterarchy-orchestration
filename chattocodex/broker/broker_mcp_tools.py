import asyncio
from time import monotonic

from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.broker.broker_dto import (
    BrokerAssignment,
    BrokerRunSnapshot,
    BrokerTaskState,
    FinishedAssignment,
    WorkerSlotState,
)
from chattocodex.broker.broker_service import BrokerService
from chattocodex.orchestration.orchestration_exceptions import OrchestrationError
from chattocodex.orchestration.orchestration_view_mapper import (
    build_run_view,
    build_task_view,
    build_tool_error,
    build_validation_error,
    build_worker_view,
)
from chattocodex.schemas.broker_schemas import (
    BrokerRunStatusRequest,
    BrokerRunStatusSuccess,
    BrokerTaskCreateRequest,
    BrokerTaskCreateSuccess,
    BrokerTaskView,
    WorkerAssignmentFinishRequest,
    WorkerAssignmentFinishSuccess,
    WorkerAssignmentView,
    WorkerClaimTaskRequest,
    WorkerClaimTaskSuccess,
    WorkerSlotRegisterRequest,
    WorkerSlotRegisterSuccess,
    WorkerSlotView,
)
from chattocodex.schemas.orchestration_schemas import ToolError


def register_broker_tools(*, server: MCPServer[None], service: BrokerService) -> None:
    @server.tool(
        name="worker_slot_register",
        description=(
            "Worker Chat bootstrap: register this Chat as one generic physical Worker Slot "
            "for a project. The Slot has no fixed role; keep its slot_id and slot_handle in "
            "this Chat and reuse them across dynamic assignments."
        ),
        structured_output=True,
    )
    async def worker_slot_register(
        project_id: str,
        label: str | None = None,
    ) -> WorkerSlotRegisterSuccess | ToolError:
        operation = "worker_slot_register"
        try:
            request = WorkerSlotRegisterRequest(project_id=project_id, label=label)
            created = await asyncio.to_thread(
                service.register_slot,
                project_id=request.project_id,
                label=request.label,
            )
            return WorkerSlotRegisterSuccess(
                slot=_build_slot_view(state=created.state),
                slot_handle=created.slot_handle,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="broker_task_create",
        description=(
            "Main Chat: publish one logical Task to the project Broker without selecting a "
            "physical Chat. role is dynamic per Task; depends_on contains prerequisite Broker "
            "Task IDs that must complete before a generic Worker can claim it."
        ),
        structured_output=True,
    )
    async def broker_task_create(
        run_id: str,
        run_handle: str,
        objective: str,
        role: str | None = None,
        depends_on: tuple[str, ...] = (),
    ) -> BrokerTaskCreateSuccess | ToolError:
        operation = "broker_task_create"
        try:
            request = BrokerTaskCreateRequest(
                run_id=run_id,
                run_handle=run_handle,
                objective=objective,
                role=role,
                depends_on=depends_on,
            )
            broker_task = await asyncio.to_thread(
                service.create_broker_task,
                run_id=request.run_id,
                run_handle=request.run_handle,
                objective=request.objective,
                role=request.role,
                depends_on=request.depends_on,
            )
            return BrokerTaskCreateSuccess(
                broker_task=_build_broker_task_view(state=broker_task)
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="worker_claim_task",
        description=(
            "Generic Worker Chat: pull the next READY logical Task for this Slot's project. "
            "The returned assignment creates a temporary run-scoped Worker identity and Task; "
            "use the returned worker_handle with existing Worker tools. If no work is ready, "
            "returns assignment=null instead of an error. wait_seconds performs bounded polling."
        ),
        structured_output=True,
    )
    async def worker_claim_task(
        slot_id: str,
        slot_handle: str,
        wait_seconds: float = 0,
    ) -> WorkerClaimTaskSuccess | ToolError:
        operation = "worker_claim_task"
        try:
            request = WorkerClaimTaskRequest(
                slot_id=slot_id,
                slot_handle=slot_handle,
                wait_seconds=wait_seconds,
            )
            deadline = monotonic() + request.wait_seconds
            while True:
                assignment = await asyncio.to_thread(
                    service.claim_task,
                    slot_id=request.slot_id,
                    slot_handle=request.slot_handle,
                )
                if assignment is not None:
                    return WorkerClaimTaskSuccess(
                        assignment=_build_assignment_view(assignment=assignment),
                        timed_out=False,
                    )
                if monotonic() >= deadline:
                    return WorkerClaimTaskSuccess(
                        assignment=None,
                        timed_out=request.wait_seconds > 0,
                    )
                await asyncio.sleep(min(0.5, max(0.0, deadline - monotonic())))
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="worker_assignment_finish",
        description=(
            "Generic Worker Chat: after the current run-scoped Task is COMPLETED or FAILED, "
            "commit that terminal state to the Broker and release this physical Slot back to IDLE."
        ),
        structured_output=True,
    )
    async def worker_assignment_finish(
        slot_id: str,
        slot_handle: str,
    ) -> WorkerAssignmentFinishSuccess | ToolError:
        operation = "worker_assignment_finish"
        try:
            request = WorkerAssignmentFinishRequest(
                slot_id=slot_id,
                slot_handle=slot_handle,
            )
            finished = await asyncio.to_thread(
                service.finish_assignment,
                slot_id=request.slot_id,
                slot_handle=request.slot_handle,
            )
            return _build_finish_success(finished=finished)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="broker_run_status",
        description=(
            "Main Chat: inspect the authoritative logical Broker/DAG state for a Run, including "
            "READY/queued/claimed/completed/failed counts and the project's generic Worker Slot "
            "consumer-group state. Use this instead of inferring Broker completion from run_status."
        ),
        structured_output=True,
    )
    async def broker_run_status(
        run_id: str,
    ) -> BrokerRunStatusSuccess | ToolError:
        operation = "broker_run_status"
        try:
            request = BrokerRunStatusRequest(run_id=run_id)
            snapshot = await asyncio.to_thread(
                service.get_run_status,
                run_id=request.run_id,
            )
            return _build_broker_run_status_success(snapshot=snapshot)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)


def _build_broker_task_view(*, state: BrokerTaskState) -> BrokerTaskView:
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


def _build_slot_view(*, state: WorkerSlotState) -> WorkerSlotView:
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


def _build_assignment_view(*, assignment: BrokerAssignment) -> WorkerAssignmentView:
    return WorkerAssignmentView(
        broker_task=_build_broker_task_view(state=assignment.broker_task),
        slot=_build_slot_view(state=assignment.slot),
        run=build_run_view(state=assignment.run),
        worker=build_worker_view(state=assignment.worker),
        task=build_task_view(state=assignment.task),
        worker_handle=assignment.worker_handle,
    )


def _build_finish_success(
    *, finished: FinishedAssignment
) -> WorkerAssignmentFinishSuccess:
    return WorkerAssignmentFinishSuccess(
        broker_task=_build_broker_task_view(state=finished.broker_task),
        slot=_build_slot_view(state=finished.slot),
        task=build_task_view(state=finished.task),
    )


def _build_broker_run_status_success(
    *, snapshot: BrokerRunSnapshot
) -> BrokerRunStatusSuccess:
    return BrokerRunStatusSuccess(
        run=build_run_view(state=snapshot.run),
        broker_tasks=tuple(
            _build_broker_task_view(state=task) for task in snapshot.broker_tasks
        ),
        slots=tuple(_build_slot_view(state=slot) for slot in snapshot.slots),
        ready_count=snapshot.ready_count,
        queued_count=snapshot.queued_count,
        claimed_count=snapshot.claimed_count,
        completed_count=snapshot.completed_count,
        failed_count=snapshot.failed_count,
        idle_slot_count=snapshot.idle_slot_count,
        busy_slot_count=snapshot.busy_slot_count,
        is_complete=snapshot.is_complete,
    )
