from collections.abc import Callable
from datetime import UTC, datetime

from chattocodex.orchestrator_backend.broker.broker_service import BrokerService
from chattocodex.orchestrator_backend.persistence.orchestration.run_control_state_store import (
    RunControlStateStore,
)
from chattocodex.orchestrator_backend.run_control.run_control_state import (
    WorkflowState,
    WorkflowStatus,
)
from chattocodex.orchestrator_backend.run_control.run_control_state_policy import (
    transition_workflow_state,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


class WorkflowRefreshService:
    """Project Broker round completion into durable Main-review workflow state."""

    def __init__(
        self,
        repository: RunControlStateStore,
        broker_service: BrokerService,
        lock_registry: EntityLockRegistry,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize this object. Args: repository, broker_service, lock_registry, now_provider. Returns: None."""
        self._repository = repository
        self._broker_service = broker_service
        self._lock_registry = lock_registry
        self._now_provider = now_provider or _utc_now

    def refresh_state(self, state: WorkflowState) -> WorkflowState:
        """Persist WAITING_MAIN_REVIEW when the current Broker round is terminal. Args: state. Returns: WorkflowState."""

        if state.status is not WorkflowStatus.RUNNING:
            return state
        snapshot = self._broker_service.get_run_status(
            run_id=state.current_round.run_id
        )
        terminal_count = snapshot.completed_count + snapshot.failed_count
        if not snapshot.broker_tasks or terminal_count != len(snapshot.broker_tasks):
            return state
        updated = transition_workflow_state(
            state=state,
            status=WorkflowStatus.WAITING_MAIN_REVIEW,
            last_review_summary=state.last_review_summary,
            rounds=state.rounds,
            rework_rounds=state.rework_rounds,
            now=self._now_provider(),
        )
        return self._repository.write(state=updated)

    def refresh_by_run(self, run_id: str) -> tuple[WorkflowState, ...]:
        """Refresh active workflows whose current round matches one Run. Args: run_id. Returns: tuple[WorkflowState, ...]."""

        refreshed: list[WorkflowState] = []
        for state in self._repository.list_for_run(run_id=run_id):
            if (
                state.current_round.run_id != run_id
                or state.status is not WorkflowStatus.RUNNING
            ):
                continue
            with self._lock_registry.hold(entity_key=f"workflow:{state.workflow_id}"):
                current = self._repository.read(workflow_id=state.workflow_id)
                if current.current_round.run_id != run_id:
                    continue
                refreshed.append(self.refresh_state(state=current))
        return tuple(refreshed)


def _utc_now() -> datetime:
    """Utc now. Args: None. Returns: datetime."""
    return datetime.now(UTC)
