from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
    OrchestrationErrorCode,
)


class ContentHashConflictError(OrchestrationError):
    def __init__(
        self,
        relative_path: str,
        expected_sha256: str,
        current_sha256: str,
    ) -> None:
        """Initialize this object. Args: relative_path, expected_sha256, current_sha256. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.CONTENT_HASH_CONFLICT,
            message=(
                f"Stale content precondition for {relative_path!r}; expected SHA-256 "
                f"{expected_sha256}, current SHA-256 is {current_sha256}. "
                "Read the file again before retrying."
            ),
            operation="worker_file_replace",
            resource_id=relative_path,
            recoverable=True,
        )


class RepositoryWriteError(OrchestrationError):
    def __init__(self, operation: str, relative_path: str, message: str) -> None:
        """Initialize this object. Args: operation, relative_path, message. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.REPOSITORY_WRITE_ERROR,
            message=message,
            operation=operation,
            resource_id=relative_path,
            recoverable=True,
        )
