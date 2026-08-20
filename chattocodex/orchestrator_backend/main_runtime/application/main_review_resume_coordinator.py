from chattocodex.orchestrator_backend.main_runtime.target.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.orchestrator_backend.main_runtime.target.main_review_target_state import (
    MainReviewResume,
)
from chattocodex.orchestrator_backend.run_control.run_control_coordinator import (
    RunControlCoordinator,
)


class MainReviewResumeCoordinator:
    def __init__(
        self,
        target_coordinator: MainReviewTargetCoordinator,
        run_control_coordinator: RunControlCoordinator,
    ) -> None:
        """Initialize this object. Args: target_coordinator, run_control_coordinator. Returns: None."""
        self._target_coordinator = target_coordinator
        self._run_control_coordinator = run_control_coordinator

    def resume_review(
        self,
        main_target_id: str,
        main_wake_revision: int,
    ) -> MainReviewResume:
        """Resume review. Args: main_target_id, main_wake_revision. Returns: MainReviewResume."""
        active = self._target_coordinator.activate_target(
            main_target_id=main_target_id,
            main_wake_revision=main_wake_revision,
        )
        assert active.pending_workflow_id is not None
        issued = self._run_control_coordinator.issue_main_review_capability(
            workflow_id=active.pending_workflow_id
        )
        return MainReviewResume(
            target=active,
            workflow=issued.snapshot,
            workflow_handle=issued.workflow_handle,
        )
