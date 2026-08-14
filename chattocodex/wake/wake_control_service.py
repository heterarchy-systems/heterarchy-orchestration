from pathlib import Path

from chattocodex.consumer_group.consumer_group_coordinator import (
    ConsumerGroupCoordinator,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.persistence.atomic_json_file import AtomicJsonFileError
from chattocodex.persistence.runtime_config_repository import RuntimeConfigRepository
from chattocodex.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.wake.wake_dto import WakeTargetRetirement, WakeTargetState
from chattocodex.wake.wake_exceptions import (
    WakeConfigurationError,
    WakeTargetConflictError,
)
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.worker_runtime.broker_ready_capacity_projection import (
    BrokerReadyCapacityProjection,
)
from chattocodex.worker_runtime.worker_assignment_lease_reconciler import (
    WorkerAssignmentLeaseReconciler,
)
from chattocodex.worker_runtime.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)
from chattocodex.workspace.workspace_project_dto import WorkspaceProject
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class WakeControlService:
    def __init__(
        self,
        *,
        runtime_config_repository: RuntimeConfigRepository,
        wake_target_service: WakeTargetService,
        wake_presence_service: WakePresenceService,
        runtime_presence_tracker: WorkerRuntimePresenceTracker,
        ready_capacity_projection: BrokerReadyCapacityProjection | None = None,
        assignment_lease_reconciler: WorkerAssignmentLeaseReconciler | None = None,
        consumer_group_coordinator: ConsumerGroupCoordinator | None = None,
        worker_provisioning_service: WorkerProvisioningService | None = None,
        lock_registry: EntityLockRegistry | None = None,
    ) -> None:
        self._runtime_config_repository = runtime_config_repository
        self._wake_target_service = wake_target_service
        self._wake_presence_service = wake_presence_service
        self._runtime_presence_tracker = runtime_presence_tracker
        self._ready_capacity_projection = ready_capacity_projection
        self._assignment_lease_reconciler = assignment_lease_reconciler
        self._consumer_group_coordinator = consumer_group_coordinator
        self._worker_provisioning_service = worker_provisioning_service
        self._lock_registry = lock_registry

    def list_projects(self) -> tuple[WorkspaceProject, ...]:
        return self._workspace_service().list_projects()

    def register_target(
        self,
        *,
        wake_target_id: str,
        project_id: str,
    ) -> WakeTargetState:
        workspace_service = self._workspace_service()
        project = workspace_service.require_project(project_id=project_id)
        target = self._wake_target_service.register_target(
            wake_target_id=wake_target_id,
            project_id=project.project_id,
        )
        if not self._wake_target_service.is_connector_compatible(target=target):
            if self._assignment_lease_reconciler is not None:
                self._assignment_lease_reconciler.reconcile_project(
                    project_id=project.project_id
                )
            if self._consumer_group_coordinator is not None:
                self._consumer_group_coordinator.reconcile_project(
                    project_id=project.project_id
                )
            return self._wake_target_service.get_target(wake_target_id=wake_target_id)
        if self._consumer_group_coordinator is not None:
            self._consumer_group_coordinator.reconcile_membership(
                project_id=project.project_id
            )
        return target

    def get_target(self, *, wake_target_id: str) -> WakeTargetState:
        target = self._wake_target_service.get_target(wake_target_id=wake_target_id)
        self._require_connector_compatible(target=target)
        self._wake_presence_service.heartbeat(wake_target_id=wake_target_id)
        if self._consumer_group_coordinator is not None:
            self._consumer_group_coordinator.reconcile_membership(
                project_id=target.project_id
            )
        return target

    def runtime_heartbeat_target(self, *, wake_target_id: str) -> WakeTargetState:
        target = self._wake_target_service.get_target(wake_target_id=wake_target_id)
        if not self._wake_target_service.is_connector_compatible(target=target):
            if self._assignment_lease_reconciler is not None:
                self._assignment_lease_reconciler.reconcile_project(
                    project_id=target.project_id
                )
            self._require_connector_compatible(
                target=self._wake_target_service.get_target(
                    wake_target_id=wake_target_id
                )
            )
        self._runtime_presence_tracker.heartbeat(wake_target_id=wake_target_id)
        if self._consumer_group_coordinator is not None:
            self._consumer_group_coordinator.reconcile_project(
                project_id=target.project_id
            )
            return self._wake_target_service.get_target(wake_target_id=wake_target_id)
        if self._assignment_lease_reconciler is not None:
            self._assignment_lease_reconciler.reconcile_project(
                project_id=target.project_id
            )
        if self._ready_capacity_projection is not None:
            self._wake_target_service.sync_ready_capacity(
                project_id=target.project_id,
                ready_count=self._ready_capacity_projection.ready_count(
                    project_id=target.project_id
                ),
            )
        return self._wake_target_service.get_target(wake_target_id=wake_target_id)

    def retire_unclaimed_target(
        self,
        *,
        wake_target_id: str,
        expected_revision: int,
    ) -> WakeTargetState:
        """Retire browser and provisioning ownership as one retryable saga.

        Args:
            wake_target_id: Exact server-issued Worker target identity.
            expected_revision: Latest target revision observed by Chrome.

        Returns:
            The exact target removed from recoverable capacity.
        """
        retired = self._wake_target_service.retire_unclaimed_target(
            wake_target_id=wake_target_id,
            expected_revision=expected_revision,
            before_delete=self._retire_provisioning_ownership,
        )
        if self._consumer_group_coordinator is not None:
            self._consumer_group_coordinator.reconcile_project(
                project_id=retired.project_id
            )
        elif self._ready_capacity_projection is not None:
            self._wake_target_service.sync_ready_capacity(
                project_id=retired.project_id,
                ready_count=self._ready_capacity_projection.ready_count(
                    project_id=retired.project_id
                ),
            )
        return retired

    def retire_unclaimed_targets_batch(
        self,
        *,
        project_id: str,
        targets: tuple[WakeTargetRetirement, ...],
    ) -> tuple[str, ...]:
        """Retire one validated project batch and reconcile capacity once.

        Args:
            project_id: Exact canonical project shared by the batch.
            targets: Unique ID/revision pairs in caller order.

        Returns:
            Retired or tombstone-confirmed target IDs in caller order.
        """

        def retire() -> tuple[tuple[str, ...], bool]:
            return self._wake_target_service.retire_unclaimed_targets_batch(
                project_id=project_id,
                targets=targets,
                before_delete=lambda wake_target_ids, missing_target_ids: (
                    self._retire_provisioning_ownership_batch(
                        project_id=project_id,
                        wake_target_ids=wake_target_ids,
                        missing_target_ids=missing_target_ids,
                    )
                ),
            )

        if self._consumer_group_coordinator is not None:
            return self._consumer_group_coordinator.mutate_project_and_reconcile(
                project_id=project_id,
                mutation=retire,
            )
        retired_ids, changed = retire()
        if changed and self._ready_capacity_projection is not None:
            self._wake_target_service.sync_ready_capacity(
                project_id=project_id,
                ready_count=self._ready_capacity_projection.ready_count(
                    project_id=project_id
                ),
            )
        return retired_ids

    def _retire_provisioning_ownership(self, target: WakeTargetState) -> None:
        if self._worker_provisioning_service is None:
            return
        self._worker_provisioning_service.retire_runtime_ownership(
            wake_target_id=target.wake_target_id
        )

    def _retire_provisioning_ownership_batch(
        self,
        *,
        project_id: str,
        wake_target_ids: tuple[str, ...],
        missing_target_ids: tuple[str, ...],
    ) -> bool:
        if self._worker_provisioning_service is None:
            if missing_target_ids:
                raise WakeConfigurationError(
                    message=(
                        "Missing batch retirement targets require Worker provisioning "
                        "tombstones so retries cannot hide unrelated target loss."
                    )
                )
            return False
        _requests, changed = (
            self._worker_provisioning_service.retire_runtime_ownership_batch(
                project_id=project_id,
                wake_target_ids=wake_target_ids,
            )
        )
        return changed

    def _require_connector_compatible(self, *, target: WakeTargetState) -> None:
        if self._wake_target_service.is_connector_compatible(target=target):
            return
        raise WakeTargetConflictError(
            wake_target_id=target.wake_target_id,
            message=(
                "Wake target belongs to a previous connector epoch. Register a "
                "replacement Worker Chat before polling or delivering work."
            ),
        )

    def _workspace_service(self) -> WorkspaceProjectService:
        if not self._runtime_config_repository.exists():
            raise WakeConfigurationError(
                message=(
                    "ChatToCodex runtime is not configured. Save the workspace in the Control "
                    "Panel before registering Worker Chats."
                )
            )
        try:
            config = self._runtime_config_repository.read()
        except AtomicJsonFileError as error:
            raise WakeConfigurationError(
                message=(
                    "ChatToCodex runtime configuration cannot be read. Save the Control Panel "
                    "settings again before registering Worker Chats."
                )
            ) from error
        workspace_root = Path(config.workspace_root).expanduser().resolve()
        if not workspace_root.is_dir():
            raise WakeConfigurationError(
                message=(
                    f"Configured workspace does not exist: {workspace_root}. Update the Control "
                    "Panel workspace before registering Worker Chats."
                )
            )
        return WorkspaceProjectService(workspace_root=workspace_root)
