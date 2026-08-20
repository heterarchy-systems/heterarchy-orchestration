from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
    OrchestrationErrorCode,
)


class ProjectPathDeniedError(OrchestrationError):
    def __init__(self, operation: str, relative_path: str) -> None:
        """Initialize this object. Args: operation, relative_path. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.PATH_OUTSIDE_PROJECT,
            message=f"Path {relative_path!r} is outside the Run project boundary.",
            operation=operation,
            resource_id=relative_path,
            recoverable=False,
        )


class UnsupportedRepositoryFileError(OrchestrationError):
    def __init__(self, operation: str, relative_path: str, message: str) -> None:
        """Initialize this object. Args: operation, relative_path, message. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.UNSUPPORTED_FILE,
            message=message,
            operation=operation,
            resource_id=relative_path,
            recoverable=False,
        )


class RepositoryReadLimitError(OrchestrationError):
    def __init__(self, operation: str, relative_path: str, message: str) -> None:
        """Initialize this object. Args: operation, relative_path, message. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.READ_LIMIT_EXCEEDED,
            message=message,
            operation=operation,
            resource_id=relative_path,
            recoverable=True,
        )


class RepositoryReadError(OrchestrationError):
    def __init__(self, operation: str, relative_path: str, message: str) -> None:
        """Initialize this object. Args: operation, relative_path, message. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.REPOSITORY_READ_ERROR,
            message=message,
            operation=operation,
            resource_id=relative_path,
            recoverable=True,
        )
