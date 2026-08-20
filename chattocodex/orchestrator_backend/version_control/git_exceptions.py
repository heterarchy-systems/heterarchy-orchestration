from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
    OrchestrationErrorCode,
)


class GitRepositoryRequiredError(OrchestrationError):
    def __init__(self, operation: str, project_root: str) -> None:
        """Initialize this object. Args: operation, project_root. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.GIT_REPOSITORY_REQUIRED,
            message=(
                "Isolated Worker Runs require the selected project root to be a top-level "
                f"Git repository with at least one commit: {project_root}"
            ),
            operation=operation,
            resource_id=project_root,
            recoverable=False,
        )


class GitWorktreeError(OrchestrationError):
    def __init__(
        self,
        operation: str,
        resource_id: str,
        message: str,
        recoverable: bool,
    ) -> None:
        """Initialize this object. Args: operation, resource_id, message, recoverable. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.GIT_WORKTREE_ERROR,
            message=message,
            operation=operation,
            resource_id=resource_id,
            recoverable=recoverable,
        )
