from enum import StrEnum


class WorkflowStatus(StrEnum):
    """Lifecycle states for run-control workflows."""

    RUNNING = "running"
    WAITING_MAIN_REVIEW = "waiting_main_review"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        """Return whether this workflow cannot accept another review transition.

        Args:
            None.

        Returns:
            True for completed or failed workflows.
        """
        return self in {WorkflowStatus.COMPLETED, WorkflowStatus.FAILED}


class WorkflowDecision(StrEnum):
    """Main-review decisions accepted by run control."""

    APPROVE = "approve"
    REWORK = "rework"
    FAIL = "fail"
