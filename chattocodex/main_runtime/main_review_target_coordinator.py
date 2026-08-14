from collections.abc import Callable
from datetime import UTC, datetime

from chattocodex.main_runtime.main_review_chat_presence_tracker import (
    MainReviewChatPresenceTracker,
)
from chattocodex.main_runtime.main_review_delivery_lease_tracker import (
    MainReviewDeliveryLeaseTracker,
)
from chattocodex.main_runtime.main_review_target_state import (
    MainReviewTargetState,
    MainReviewTargetStatus,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    ResourceNotFoundError,
)
from chattocodex.persistence.main_review_target_store import MainReviewTargetStore
from chattocodex.persistence.run_control_state_store import RunControlStateStore
from chattocodex.run_control.run_control_state import WorkflowStatus


class MainReviewTargetCoordinator:
    def __init__(
        self,
        *,
        target_store: MainReviewTargetStore,
        run_control_state_store: RunControlStateStore,
        lock_registry: EntityLockRegistry,
        delivery_lease_tracker: MainReviewDeliveryLeaseTracker | None = None,
        chat_presence_tracker: MainReviewChatPresenceTracker | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._target_store = target_store
        self._run_control_state_store = run_control_state_store
        self._delivery_lease_tracker = delivery_lease_tracker
        self._chat_presence_tracker = chat_presence_tracker
        self._lock_registry = lock_registry
        self._now_provider = now_provider or _utc_now

    def register_target(
        self,
        *,
        main_target_id: str,
        project_id: str,
    ) -> MainReviewTargetState:
        with self._lock_registry.hold(entity_key=f"main-review-project:{project_id}"):
            try:
                existing = self._target_store.read(main_target_id=main_target_id)
            except ResourceNotFoundError:
                pass
            else:
                if existing.project_id != project_id:
                    raise InvalidTransitionError(
                        operation="main_review_target_register",
                        resource_id=main_target_id,
                        message="Main review target belongs to a different project.",
                    )
                return existing
            now = self._now_provider()
            return self._target_store.create(
                state=MainReviewTargetState(
                    schema_version=1,
                    main_target_id=main_target_id,
                    project_id=project_id,
                    status=MainReviewTargetStatus.IDLE,
                    pending_workflow_id=None,
                    pending_workflow_revision=None,
                    wake_revision=0,
                    revision=0,
                    created_at=now,
                    updated_at=now,
                )
            )

    def poll_target(self, *, main_target_id: str) -> MainReviewTargetState:
        initial = self._target_store.read(main_target_id=main_target_id)
        with self._lock_registry.hold(
            entity_key=f"main-review-project:{initial.project_id}"
        ):
            state = self._target_store.read(main_target_id=main_target_id)
            return self._refresh_locked(state=state)

    def heartbeat_chat(self, *, main_target_id: str) -> MainReviewTargetState:
        state = self._target_store.read(main_target_id=main_target_id)
        if self._chat_presence_tracker is not None:
            self._chat_presence_tracker.heartbeat(main_target_id=main_target_id)
        return state

    def activate_target(
        self,
        *,
        main_target_id: str,
        main_wake_revision: int,
    ) -> MainReviewTargetState:
        initial = self._target_store.read(main_target_id=main_target_id)
        with self._lock_registry.hold(
            entity_key=f"main-review-project:{initial.project_id}"
        ):
            state = self._refresh_locked(
                state=self._target_store.read(main_target_id=main_target_id)
            )
            if state.status is not MainReviewTargetStatus.WAKING:
                raise InvalidTransitionError(
                    operation="resume_main_review",
                    resource_id=main_target_id,
                    message="Main review target is not waiting to resume a review.",
                )
            if state.wake_revision != main_wake_revision:
                raise InvalidTransitionError(
                    operation="resume_main_review",
                    resource_id=main_target_id,
                    message=(
                        "Main review wake revision is stale. Wait for the current automated Main "
                        "review wake before resuming."
                    ),
                )
            if (
                self._delivery_lease_tracker is not None
                and not self._delivery_lease_tracker.is_claimable(
                    main_target_id=main_target_id,
                    wake_revision=main_wake_revision,
                )
            ):
                raise InvalidTransitionError(
                    operation="resume_main_review",
                    resource_id=main_target_id,
                    message=(
                        "Main review wake delivery expired or is cooling down. Wait for a fresh "
                        "automated review wake."
                    ),
                )
            now = self._now_provider()
            active = self._target_store.write(
                state=MainReviewTargetState(
                    schema_version=state.schema_version,
                    main_target_id=state.main_target_id,
                    project_id=state.project_id,
                    status=MainReviewTargetStatus.ACTIVE,
                    pending_workflow_id=state.pending_workflow_id,
                    pending_workflow_revision=state.pending_workflow_revision,
                    wake_revision=state.wake_revision,
                    revision=state.revision + 1,
                    created_at=state.created_at,
                    updated_at=now,
                )
            )
            if self._delivery_lease_tracker is not None:
                self._delivery_lease_tracker.mark_success(
                    main_target_id=main_target_id,
                    wake_revision=main_wake_revision,
                )
            return active

    def _refresh_locked(self, *, state: MainReviewTargetState) -> MainReviewTargetState:
        if state.status is MainReviewTargetStatus.ACTIVE:
            assert state.pending_workflow_id is not None
            workflow = self._run_control_state_store.read(
                workflow_id=state.pending_workflow_id
            )
            if workflow.status is WorkflowStatus.WAITING_MAIN_REVIEW:
                if self._active_session_expired(state=state):
                    self._mark_delivery_failure(state=state)
                    return self._to_idle(state=state)
                return state
            return self._to_idle(state=state)

        if state.status is MainReviewTargetStatus.WAKING:
            if not self._delivery_expired(state=state):
                return state
            self._mark_delivery_failure(state=state)
            return self._to_idle(state=state)

        if self._in_delivery_cooldown(main_target_id=state.main_target_id):
            return state

        waiting = tuple(
            workflow
            for workflow in self._run_control_state_store.list_all()
            if workflow.project_id == state.project_id
            and workflow.status is WorkflowStatus.WAITING_MAIN_REVIEW
        )
        if not waiting:
            return state
        claimed_workflows = self._claimed_workflows_locked(
            project_id=state.project_id,
            excluding_main_target_id=state.main_target_id,
        )
        waiting = tuple(
            workflow
            for workflow in waiting
            if workflow.workflow_id not in claimed_workflows
        )
        if not waiting:
            return state
        selected = min(waiting, key=lambda item: (item.updated_at, item.workflow_id))
        now = self._now_provider()
        waking = self._target_store.write(
            state=MainReviewTargetState(
                schema_version=state.schema_version,
                main_target_id=state.main_target_id,
                project_id=state.project_id,
                status=MainReviewTargetStatus.WAKING,
                pending_workflow_id=selected.workflow_id,
                pending_workflow_revision=selected.revision,
                wake_revision=state.wake_revision + 1,
                revision=state.revision + 1,
                created_at=state.created_at,
                updated_at=now,
            )
        )
        if self._delivery_lease_tracker is not None:
            self._delivery_lease_tracker.begin(
                main_target_id=waking.main_target_id,
                wake_revision=waking.wake_revision,
            )
        return waking

    def _claimed_workflows_locked(
        self,
        *,
        project_id: str,
        excluding_main_target_id: str,
    ) -> set[str]:
        claimed: set[str] = set()
        for target in self._target_store.list_for_project(project_id=project_id):
            if target.main_target_id == excluding_main_target_id:
                continue
            if (
                target.status is MainReviewTargetStatus.WAKING
                and self._delivery_expired(state=target)
            ):
                self._mark_delivery_failure(state=target)
                self._to_idle(state=target)
                continue
            if (
                target.status is MainReviewTargetStatus.ACTIVE
                and self._active_session_expired(state=target)
            ):
                self._mark_delivery_failure(state=target)
                self._to_idle(state=target)
                continue
            if (
                target.status
                in {MainReviewTargetStatus.WAKING, MainReviewTargetStatus.ACTIVE}
                and target.pending_workflow_id is not None
            ):
                claimed.add(target.pending_workflow_id)
        return claimed

    def _delivery_expired(self, *, state: MainReviewTargetState) -> bool:
        return (
            self._delivery_lease_tracker is not None
            and self._delivery_lease_tracker.is_expired(
                main_target_id=state.main_target_id,
                wake_revision=state.wake_revision,
            )
        )

    def _active_session_expired(self, *, state: MainReviewTargetState) -> bool:
        return (
            self._chat_presence_tracker is not None
            and self._chat_presence_tracker.session_expired(
                main_target_id=state.main_target_id,
                activated_at=state.updated_at,
            )
        )

    def _mark_delivery_failure(self, *, state: MainReviewTargetState) -> None:
        if self._delivery_lease_tracker is not None:
            self._delivery_lease_tracker.mark_failure(
                main_target_id=state.main_target_id,
                wake_revision=state.wake_revision,
            )

    def _in_delivery_cooldown(self, *, main_target_id: str) -> bool:
        return (
            self._delivery_lease_tracker is not None
            and self._delivery_lease_tracker.is_in_cooldown(
                main_target_id=main_target_id
            )
        )

    def _to_idle(self, *, state: MainReviewTargetState) -> MainReviewTargetState:
        now = self._now_provider()
        return self._target_store.write(
            state=MainReviewTargetState(
                schema_version=state.schema_version,
                main_target_id=state.main_target_id,
                project_id=state.project_id,
                status=MainReviewTargetStatus.IDLE,
                pending_workflow_id=None,
                pending_workflow_revision=None,
                wake_revision=state.wake_revision,
                revision=state.revision + 1,
                created_at=state.created_at,
                updated_at=now,
            )
        )


def _utc_now() -> datetime:
    return datetime.now(UTC)
