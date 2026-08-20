from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    OrchestrationError,
    OrchestrationErrorCode,
)


class WakeError(OrchestrationError):
    def __init__(
        self,
        error_code: OrchestrationErrorCode,
        message: str,
        operation: str,
        resource_id: str | None,
        recoverable: bool,
    ) -> None:
        """Initialize this object. Args: error_code, message, operation, resource_id, recoverable. Returns: None."""
        super().__init__(
            error_code=error_code,
            message=message,
            operation=operation,
            resource_id=resource_id,
            recoverable=recoverable,
        )


class WakeConfigurationError(WakeError):
    def __init__(self, message: str) -> None:
        """Initialize this object. Args: message. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.WAKE_CONFIGURATION_ERROR,
            message=message,
            operation="wake_control",
            resource_id="runtime",
            recoverable=True,
        )


class WakeTargetConflictError(WakeError):
    def __init__(self, message: str, wake_target_id: str | None = None) -> None:
        """Initialize this object. Args: message, wake_target_id. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.WAKE_TARGET_CONFLICT,
            message=message,
            operation="wake_target",
            resource_id=wake_target_id,
            recoverable=False,
        )
