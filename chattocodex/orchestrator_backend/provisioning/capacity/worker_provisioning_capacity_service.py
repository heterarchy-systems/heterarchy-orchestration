"""Capacity-demand lifecycle for Worker provisioning."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from typing import TYPE_CHECKING

from chattocodex.orchestrator_backend.persistence.worker.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_dto import (
    ProjectBindingState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_dto import (
    ProvisioningDemandState,
    ProvisioningRequestState,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_policy import (
    new_request,
    require_demand,
    retain_terminal_history,
    validate_demand_values,
)
from chattocodex.orchestrator_backend.provisioning.provisioning_state_sync import (
    apply_capacity_decision_state,
    upsert_demand_state,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry

if TYPE_CHECKING:
    from chattocodex.orchestrator_backend.worker_runtime.capacity.capacity_decision import (
        CapacityDecision,
    )


class WorkerProvisioningCapacityService:
    """Own durable capacity demand and reservation synchronization."""

    def __init__(
        self,
        repository_provider: Callable[[], WorkerProvisioningRepository],
        binding_provider: Callable[[str], ProjectBindingState | None],
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime],
        terminal_request_retention: int,
    ) -> None:
        """Initialize this object. Args: repository_provider, binding_provider, lock_registry, now_provider, terminal_request_retention. Returns: None."""
        self._repository_provider = repository_provider
        self._binding_provider = binding_provider
        self._lock_registry = lock_registry
        self._now_provider = now_provider
        self._terminal_request_retention = terminal_request_retention

    @property
    def _repository(self) -> WorkerProvisioningRepository:
        """Repository. Args: None. Returns: WorkerProvisioningRepository."""
        return self._repository_provider()

    def pending_provisioning_count(self, project_id: str | None = None) -> int:
        """Return outstanding reservations for one project or globally. Args: project_id. Returns: int."""
        demands = self._repository.list_demands()
        if project_id is None:
            return sum(item.spawn_needed for item in demands)
        return sum(
            item.spawn_needed for item in demands if item.project_id == project_id
        )

    def upsert_demand(
        self,
        project_id: str,
        desired_capacity: int,
        spawn_needed: int,
        binding_revision: int | None = None,
    ) -> ProvisioningDemandState:
        """Persist one backend-computed project capacity deficit. Args: project_id, desired_capacity, spawn_needed, binding_revision. Returns: ProvisioningDemandState."""
        validate_demand_values(
            desired_capacity=desired_capacity,
            spawn_needed=spawn_needed,
            binding_revision=binding_revision,
        )
        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            updated = self._repository.update(
                updater=lambda state: retain_terminal_history(
                    state=upsert_demand_state(
                        state=state,
                        project_id=project_id,
                        desired_capacity=desired_capacity,
                        spawn_needed=spawn_needed,
                        binding_revision=binding_revision,
                        now=now,
                        request_factory=new_request,
                    ),
                    terminal_request_retention=self._terminal_request_retention,
                    now=now,
                )
            )
            return require_demand(state=updated, project_id=project_id)

    def on_capacity_decision(self, decision: CapacityDecision) -> None:
        """Atomically reserve the remaining deficit from capacity_core. Args: decision. Returns: None."""
        validate_demand_values(
            desired_capacity=decision.desired_capacity,
            spawn_needed=decision.spawn_needed,
            binding_revision=None,
        )
        if decision.capacity_deficit < 0:
            raise ValueError("capacity_deficit cannot be negative")
        binding = self._binding_provider(decision.project_id)

        def request_factory(
            demand: ProvisioningDemandState, now: datetime
        ) -> ProvisioningRequestState:
            """Request factory. Args: demand, now. Returns: ProvisioningRequestState."""
            request = new_request(demand=demand, now=now)
            if binding is None:
                return request
            return replace(
                request,
                binding_revision=binding.revision,
                adapter_metadata=binding.adapter_metadata,
            )

        with self._lock_registry.hold(entity_key="worker-provisioning:capacity"):
            now = self._now_provider()
            updated = self._repository.update(
                updater=lambda state: retain_terminal_history(
                    state=apply_capacity_decision_state(
                        state=state,
                        project_id=decision.project_id,
                        desired_capacity=decision.desired_capacity,
                        capacity_deficit=decision.capacity_deficit,
                        spawn_needed=decision.spawn_needed,
                        project_pending_snapshot=decision.pending_provisioning_count,
                        global_headroom=decision.global_headroom,
                        global_pending_snapshot=(
                            decision.global_pending_provisioning_count
                        ),
                        now=now,
                        request_factory=request_factory,
                    ),
                    terminal_request_retention=self._terminal_request_retention,
                    now=now,
                )
            )
            require_demand(state=updated, project_id=decision.project_id)
