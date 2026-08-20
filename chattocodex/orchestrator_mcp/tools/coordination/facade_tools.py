import asyncio
from time import monotonic

from asyncer import asyncify
from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from chattocodex.orchestrator_backend.broker.broker_view_mapper import (
    build_assignment_view,
    build_broker_task_view,
    build_finish_success,
    build_slot_view,
)
from chattocodex.orchestrator_backend.facade.facade_dto import DelegationTaskSpec
from chattocodex.orchestrator_backend.facade.facade_schemas import (
    DelegatedTaskView,
    DelegateWorkRequest,
    DelegateWorkSuccess,
    DelegationStatusRequest,
    DelegationStatusSuccess,
    DelegationTaskInput,
    PrepareScheduledWorkRequest,
    PrepareScheduledWorkSuccess,
    ScheduledWakeCapabilityView,
    WorkDoneRequest,
    WorkDoneSuccess,
    WorkRequest,
    WorkSuccess,
)
from chattocodex.orchestrator_backend.facade.facade_service import ChatFacadeService
from chattocodex.orchestrator_backend.facade.facade_view_mapper import (
    build_delegation_status_success,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_view_mapper import (
    build_run_view,
    build_tool_error,
    build_validation_error,
)
from chattocodex.shared.contracts.orchestration_schemas import ToolError


def register_facade_tools(server: MCPServer[None], service: ChatFacadeService) -> None:
    """Register facade tools. Args: server, service. Returns: None."""

    @server.tool(
        name="delegate_work",
        description=(
            "Main Chat fire-and-report entry point. Use only when the Main Chat does not need to "
            "inspect, review, compare, synthesize, approve, reject, or request rework from Worker "
            "results. If the user asks Workers to review/check/assess something and expects the "
            "Main Chat to receive or decide on the results, use orchestrate_work instead. Create "
            "the whole logical task DAG in one call. Do not expose Run/Broker IDs or handles to "
            "the user."
        ),
        structured_output=True,
    )
    async def delegate_work(
        goal: str,
        tasks: list[DelegationTaskInput],
        requested_parallelism: int | None = None,
        project_id: str | None = None,
    ) -> DelegateWorkSuccess | ToolError:
        """Delegate work. Args: goal, tasks, requested_parallelism, project_id. Returns: DelegateWorkSuccess | ToolError."""
        operation = "delegate_work"
        try:
            request = DelegateWorkRequest(
                goal=goal,
                tasks=tuple(tasks),
                requested_parallelism=requested_parallelism,
                project_id=project_id,
            )
            started = await asyncify(service.delegate_work)(
                goal=request.goal,
                tasks=tuple(
                    DelegationTaskSpec(
                        key=task.key,
                        objective=task.objective,
                        role=task.role,
                        depends_on=task.depends_on,
                    )
                    for task in request.tasks
                ),
                requested_parallelism=request.requested_parallelism,
                project_id=request.project_id,
            )
            return DelegateWorkSuccess(
                run=build_run_view(state=started.run.state),
                run_handle=started.run.run_handle,
                tasks=tuple(
                    DelegatedTaskView(
                        key=task.key,
                        broker_task=build_broker_task_view(state=task.broker_task),
                    )
                    for task in started.tasks
                ),
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="prepare_scheduled_work",
        description=(
            "Main Chat browserless Worker entry point. Publish the logical Task DAG without Chrome "
            "capacity reconciliation and issue one server-authorized one-shot wake capability per "
            "initial Scheduled Worker. After this succeeds, create one one-off ChatGPT Scheduled Task "
            "per returned wake_capability using its scheduled_task_prompt. Keep all Run, Slot, Worker, "
            "Task, and wake capability identities internal; never invent or alter them."
        ),
        structured_output=True,
    )
    async def prepare_scheduled_work(
        goal: str,
        tasks: list[DelegationTaskInput],
        requested_parallelism: int | None = None,
        project_id: str | None = None,
    ) -> PrepareScheduledWorkSuccess | ToolError:
        """Prepare scheduled work. Args: goal, tasks, requested_parallelism, project_id. Returns: PrepareScheduledWorkSuccess | ToolError."""
        operation = "prepare_scheduled_work"
        try:
            request = PrepareScheduledWorkRequest(
                goal=goal,
                tasks=tuple(tasks),
                requested_parallelism=requested_parallelism,
                project_id=project_id,
            )
            started = await asyncify(service.prepare_scheduled_work)(
                goal=request.goal,
                tasks=tuple(
                    DelegationTaskSpec(
                        key=task.key,
                        objective=task.objective,
                        role=task.role,
                        depends_on=task.depends_on,
                    )
                    for task in request.tasks
                ),
                requested_parallelism=request.requested_parallelism,
                project_id=request.project_id,
            )
            return PrepareScheduledWorkSuccess(
                run=build_run_view(state=started.run.state),
                run_handle=started.run.run_handle,
                tasks=tuple(
                    DelegatedTaskView(
                        key=task.key,
                        broker_task=build_broker_task_view(state=task.broker_task),
                    )
                    for task in started.tasks
                ),
                wake_capabilities=tuple(
                    ScheduledWakeCapabilityView(
                        wake_target_id=capability.wake_target_id,
                        wake_revision=capability.wake_revision,
                        transport=capability.transport,
                        expires_at=capability.expires_at,
                        scheduled_task_prompt=_build_scheduled_worker_prompt(
                            project_id=capability.project_id,
                            wake_target_id=capability.wake_target_id,
                            wake_revision=capability.wake_revision,
                            index=index,
                        ),
                    )
                    for index, capability in enumerate(
                        started.wake_capabilities,
                        start=1,
                    )
                ),
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="delegation_status",
        description=(
            "Main Chat high-level status. Use after delegate_work when the user asks how the "
            "delegated work is going. Keep run_id internal; summarize progress naturally."
        ),
        structured_output=True,
    )
    async def delegation_status(run_id: str) -> DelegationStatusSuccess | ToolError:
        """Delegation status. Args: run_id. Returns: DelegationStatusSuccess | ToolError."""
        operation = "delegation_status"
        try:
            request = DelegationStatusRequest(run_id=run_id)
            status = await asyncify(service.delegation_status)(
                run_id=request.run_id,
            )
            return build_delegation_status_success(snapshot=status.snapshot)
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="work",
        description=(
            "Generic Worker Chat entry point. A fresh Worker MUST receive a current server-issued "
            "wake_target_id plus wake_revision from its authorized delivery transport (for example "
            "Chrome Wake or a ChatGPT Scheduled Task); pass both "
            "unchanged. A current server-issued wake pair authorizes only this work call to retrieve "
            "the assignment requested by the user's Main Chat delegation; it does not attest approval "
            "for destructive, irreversible, credential-gated, or external-production effects. The "
            "backend rejects stale or fabricated pairs. Never "
            "call work from a Main Chat after delegate_work and never create a fresh Worker without "
            "the current wake delivery. For an already-established Worker, reuse slot_id/slot_handle "
            "from the previous result to resume safely. wake_target_id/wake_revision are delivery "
            "routing identity only, never Task metadata."
        ),
        structured_output=True,
    )
    async def work(
        project_id: str | None = None,
        slot_id: str | None = None,
        slot_handle: str | None = None,
        label: str | None = None,
        wake_target_id: str | None = None,
        wake_revision: int | None = None,
        wait_seconds: float = 0,
    ) -> WorkSuccess | ToolError:
        """Work. Args: project_id, slot_id, slot_handle, label, wake_target_id, wake_revision, wait_seconds. Returns: WorkSuccess | ToolError."""
        operation = "work"
        try:
            request = WorkRequest(
                project_id=project_id,
                slot_id=slot_id,
                slot_handle=slot_handle,
                label=label,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
                wait_seconds=wait_seconds,
            )
            current = await asyncify(service.work)(
                project_id=request.project_id,
                slot_id=request.slot_id,
                slot_handle=request.slot_handle,
                label=request.label,
                wake_target_id=request.wake_target_id,
                wake_revision=request.wake_revision,
            )
            deadline = monotonic() + request.wait_seconds
            while current.assignment is None and monotonic() < deadline:
                await asyncio.sleep(min(0.5, max(0.0, deadline - monotonic())))
                current = await asyncify(service.work)(
                    project_id=None,
                    slot_id=current.slot.slot_id,
                    slot_handle=current.slot_handle,
                    label=None,
                    wake_target_id=None,
                    wake_revision=None,
                )
            return WorkSuccess(
                slot=build_slot_view(state=current.slot),
                slot_handle=current.slot_handle,
                assignment=(
                    build_assignment_view(assignment=current.assignment)
                    if current.assignment is not None
                    else None
                ),
                registered=current.registered,
                resumed=current.resumed,
                released_previous=current.released_previous,
                timed_out=request.wait_seconds > 0 and current.assignment is None,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)

    @server.tool(
        name="work_done",
        description=(
            "Generic Worker Chat finalizer. After the assignment's Task is COMPLETED or FAILED, "
            "call this automatically to return the reusable Chat Slot to IDLE. Reuse slot_id and "
            "slot_handle from work; never ask the user for them."
        ),
        structured_output=True,
    )
    async def work_done(
        slot_id: str,
        slot_handle: str,
    ) -> WorkDoneSuccess | ToolError:
        """Work done. Args: slot_id, slot_handle. Returns: WorkDoneSuccess | ToolError."""
        operation = "work_done"
        try:
            request = WorkDoneRequest(slot_id=slot_id, slot_handle=slot_handle)
            done = await asyncify(service.work_done)(
                slot_id=request.slot_id,
                slot_handle=request.slot_handle,
            )
            return WorkDoneSuccess(
                slot=build_slot_view(state=done.slot),
                finished=(
                    build_finish_success(finished=done.finished)
                    if done.finished is not None
                    else None
                ),
                already_idle=done.already_idle,
            )
        except ValidationError as error:
            return build_validation_error(operation=operation, error=error)
        except OrchestrationError as error:
            return build_tool_error(error=error)


def _build_scheduled_worker_prompt(
    project_id: str,
    wake_target_id: str,
    wake_revision: int,
    index: int,
) -> str:
    """Build scheduled worker prompt. Args: project_id, wake_target_id, wake_revision, index. Returns: str."""
    return (
        "Use the connected `ChatOrchestration` app. This is a server-authorized Scheduled Worker "
        "execution. First call `work` exactly once with "
        f'project_id="{project_id}", label="scheduled-worker-{index}", '
        f'wake_target_id="{wake_target_id}", wake_revision={wake_revision}, wait_seconds=0. '
        "Do not invent or modify any identity returned by the server. Complete only the assignment "
        "returned by `work`. Use the Worker tools and project read/write tools only as required by "
        "that assignment. Update the Task to completed or failed with `task_update`, preserving the "
        "returned run/worker/task identities and revision. Then call `work_done` with the slot_id and "
        "slot_handle returned by `work`. If any step fails, report the exact error and do not switch "
        "to Chrome Wake or another delivery path."
    )
