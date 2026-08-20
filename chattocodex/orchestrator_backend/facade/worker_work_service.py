from chattocodex.orchestrator_backend.broker.broker_dto import WorkerSlotStatus
from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.facade.facade_dto import (
    WorkerWork,
    WorkerWorkDone,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    OrchestrationError,
)
from chattocodex.orchestrator_backend.run_control.run_control_coordinator import (
    RunControlCoordinator,
)
from chattocodex.orchestrator_backend.wake.capability.worker_wake_capability_service import (
    WorkerWakeCapabilityService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.worker_runtime_coordinator import (
    WorkerRuntimeCoordinator,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_selection_policy import (
    resolve_workspace_project_id,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


class WorkerWorkService:
    """Own Worker wake, slot acquisition, assignment completion, and recovery flow."""

    def __init__(
        self,
        broker_service: BrokerService,
        wake_target_service: WakeTargetService,
        workspace_project_service: WorkspaceProjectService,
        run_control_coordinator: RunControlCoordinator | None = None,
        worker_runtime_coordinator: WorkerRuntimeCoordinator | None = None,
        worker_wake_capability_service: WorkerWakeCapabilityService | None = None,
    ) -> None:
        """Initialize this object. Args: broker_service, wake_target_service, workspace_project_service, run_control_coordinator, worker_runtime_coordinator, worker_wake_capability_service. Returns: None."""
        self._broker_service = broker_service
        self._wake_target_service = wake_target_service
        self._workspace_project_service = workspace_project_service
        self._run_control_coordinator = run_control_coordinator
        self._worker_runtime_coordinator = worker_runtime_coordinator
        self._worker_wake_capability_service = worker_wake_capability_service

    def work(
        self,
        project_id: str | None,
        slot_id: str | None,
        slot_handle: str | None,
        label: str | None,
        wake_target_id: str | None = None,
        wake_revision: int | None = None,
    ) -> WorkerWork:
        """Acquire or resume one assignment for a project-bound Worker slot. Args: project_id, slot_id, slot_handle, label, wake_target_id, wake_revision. Returns: WorkerWork."""

        if (slot_id is None) != (slot_handle is None):
            raise InvalidTransitionError(
                operation="work",
                resource_id="worker-slot",
                message="Worker Slot identity must be supplied completely or omitted completely.",
            )
        if slot_id is None and (wake_target_id is None or wake_revision is None):
            raise InvalidTransitionError(
                operation="work",
                resource_id="wake-target",
                message=(
                    "A new Worker Slot can claim work only from a current server-issued Worker "
                    "Wake delivery. Wait for an authorized delivery transport to supply both "
                    "wake_target_id and wake_revision instead of claiming delegated work from "
                    "the Main Chat."
                ),
            )
        if slot_id is not None and wake_revision is not None:
            raise InvalidTransitionError(
                operation="work",
                resource_id=slot_id,
                message="wake_revision is only valid for a fresh Worker wake claim.",
            )

        if slot_id is None:
            assert wake_target_id is not None
            assert wake_revision is not None
            return self._work_from_fresh_wake(
                project_id=project_id,
                label=label,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )

        assert slot_handle is not None
        acquired = self._broker_service.acquire_task(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        try:
            self._wake_target_service.refresh_bound_slot(
                slot_id=acquired.slot.slot_id,
                project_id=acquired.slot.project_id,
                has_assignment=acquired.assignment is not None,
            )
        except OrchestrationError:
            if acquired.assignment is not None:
                self._broker_service.recover_abandoned_assignment(
                    slot_id=acquired.slot.slot_id,
                    reason="Connector epoch changed during Worker Slot resume.",
                )
            self._wake_target_service.release_slot(slot_id=acquired.slot.slot_id)
            raise
        return WorkerWork(
            slot=acquired.slot,
            slot_handle=slot_handle,
            assignment=acquired.assignment,
            registered=False,
            resumed=acquired.resumed,
            released_previous=acquired.released_previous,
        )

    def work_done(self, slot_id: str, slot_handle: str) -> WorkerWorkDone:
        """Release a terminal assignment and reconcile follow-up capacity/review state. Args: slot_id, slot_handle. Returns: WorkerWorkDone."""

        slot = self._broker_service.get_slot(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        scheduled_slot = (
            self._worker_wake_capability_service is not None
            and self._worker_wake_capability_service.owns_slot(slot_id=slot.slot_id)
        )
        if slot.status is WorkerSlotStatus.IDLE:
            if not scheduled_slot:
                self._wake_target_service.release_slot(slot_id=slot.slot_id)
            return WorkerWorkDone(slot=slot, finished=None, already_idle=True)
        active_run_id = slot.current_run_id
        finished = self._broker_service.finish_assignment(
            slot_id=slot_id,
            slot_handle=slot_handle,
        )
        if not scheduled_slot:
            self._wake_target_service.release_slot(slot_id=slot_id)
        if active_run_id is not None:
            snapshot = self._broker_service.get_run_status(run_id=active_run_id)
            if not scheduled_slot:
                if self._worker_runtime_coordinator is not None:
                    self._worker_runtime_coordinator.reconcile_project(
                        project_id=finished.slot.project_id
                    )
                else:
                    self._wake_target_service.sync_ready_capacity(
                        project_id=finished.slot.project_id,
                        ready_count=snapshot.ready_count,
                    )
            if self._run_control_coordinator is not None:
                self._run_control_coordinator.refresh_by_run(run_id=active_run_id)
        return WorkerWorkDone(
            slot=finished.slot,
            finished=finished,
            already_idle=False,
        )

    def _work_from_fresh_wake(
        self,
        project_id: str | None,
        label: str | None,
        wake_target_id: str,
        wake_revision: int,
    ) -> WorkerWork:
        """Work from fresh wake. Args: project_id, label, wake_target_id, wake_revision. Returns: WorkerWork."""
        if (
            self._worker_wake_capability_service is not None
            and self._worker_wake_capability_service.owns(wake_target_id=wake_target_id)
        ):
            return self._work_from_server_wake_capability(
                project_id=project_id,
                label=label,
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )
        wake_target = self._wake_target_service.reserve_wake_claim(
            wake_target_id=wake_target_id,
            wake_revision=wake_revision,
        )
        acquired = None
        try:
            wake_project_id = wake_target.project_id
            if self._worker_runtime_coordinator is not None:
                self._worker_runtime_coordinator.require_wake_target(
                    project_id=wake_project_id,
                    wake_target_id=wake_target_id,
                )
            if project_id is not None and project_id != wake_project_id:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=wake_target_id,
                    message="Automated wake target belongs to a different project.",
                )
            resolved_project_id = self._resolve_project_id(
                project_id=project_id or wake_project_id,
                prefer_ready=True,
            )
            registered = wake_target.bound_slot_id is None
            if registered:
                created = self._broker_service.register_slot(
                    project_id=resolved_project_id,
                    label=label,
                )
            else:
                assert wake_target.bound_slot_id is not None
                created = self._broker_service.reclaim_slot_for_wake(
                    slot_id=wake_target.bound_slot_id,
                    project_id=resolved_project_id,
                )
            acquired = self._broker_service.acquire_task(
                slot_id=created.state.slot_id,
                slot_handle=created.slot_handle,
            )
            self._wake_target_service.bind_target(
                wake_target_id=wake_target_id,
                project_id=acquired.slot.project_id,
                slot_id=acquired.slot.slot_id,
                has_assignment=acquired.assignment is not None,
                wake_revision=wake_revision,
            )
        except OrchestrationError:
            if acquired is not None and acquired.assignment is not None:
                self._broker_service.recover_abandoned_assignment(
                    slot_id=acquired.slot.slot_id,
                    reason="Wake reservation changed before Worker binding completed.",
                )
            self._wake_target_service.abort_wake_claim(
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )
            raise
        return WorkerWork(
            slot=acquired.slot,
            slot_handle=created.slot_handle,
            assignment=acquired.assignment,
            registered=registered,
            resumed=acquired.resumed,
            released_previous=acquired.released_previous,
        )

    def _work_from_server_wake_capability(
        self,
        project_id: str | None,
        label: str | None,
        wake_target_id: str,
        wake_revision: int,
    ) -> WorkerWork:
        """Work from server wake capability. Args: project_id, label, wake_target_id, wake_revision. Returns: WorkerWork."""
        assert self._worker_wake_capability_service is not None
        capability = self._worker_wake_capability_service.reserve(
            wake_target_id=wake_target_id,
            wake_revision=wake_revision,
        )
        acquired = None
        try:
            if project_id is not None and project_id != capability.project_id:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=wake_target_id,
                    message="Server-issued Worker wake capability belongs to a different project.",
                )
            resolved_project_id = self._resolve_project_id(
                project_id=project_id or capability.project_id,
                prefer_ready=False,
            )
            created = self._broker_service.register_slot(
                project_id=resolved_project_id,
                label=label,
            )
            assignment = self._broker_service.claim_task_for_run(
                slot_id=created.state.slot_id,
                slot_handle=created.slot_handle,
                run_id=capability.run_id,
            )
            if assignment is None:
                raise InvalidTransitionError(
                    operation="work",
                    resource_id=capability.run_id,
                    message="Scheduled Worker wake arrived before its Run had a ready Broker Task.",
                )
            acquired = assignment
            self._worker_wake_capability_service.commit(
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
                slot_id=assignment.slot.slot_id,
            )
        except OrchestrationError:
            if acquired is not None:
                self._broker_service.recover_abandoned_assignment(
                    slot_id=acquired.slot.slot_id,
                    reason="Server-issued Worker wake capability claim did not commit.",
                )
            self._worker_wake_capability_service.abort(
                wake_target_id=wake_target_id,
                wake_revision=wake_revision,
            )
            raise
        return WorkerWork(
            slot=assignment.slot,
            slot_handle=created.slot_handle,
            assignment=assignment,
            registered=True,
            resumed=False,
            released_previous=False,
        )

    def _resolve_project_id(
        self,
        project_id: str | None,
        prefer_ready: bool,
    ) -> str:
        """Resolve project id. Args: project_id, prefer_ready. Returns: str."""
        return resolve_workspace_project_id(
            workspace_project_service=self._workspace_project_service,
            project_id=project_id,
            preferred_project_ids=(
                self._broker_service.ready_project_ids() if prefer_ready else ()
            ),
            operation="resolve_project",
            multiple_projects_message=(
                "The workspace contains multiple projects. Select the intended project internally "
                "with project_list and retry; do not ask the user for Broker or Worker IDs."
            ),
        )
