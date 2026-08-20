"""Errors raised by project-bound Worker provisioning."""

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    OrchestrationError,
    OrchestrationErrorCode,
)


class ProvisioningConfigurationError(OrchestrationError):
    """Report missing canonical binding or backend provisioning wiring."""

    def __init__(self, project_id: str, message: str) -> None:
        """Create a recoverable provisioning configuration error.

        Args:
            project_id: Canonical local project identifier involved in the error.
            message: Safe local Control Panel explanation. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.PROJECT_BINDING_CONFIGURATION,
            message=message,
            operation="worker_provisioning_configuration",
            resource_id=project_id,
            recoverable=True,
        )


class ProvisioningConflictError(OrchestrationError):
    """Report a project, conversation, or request conflict."""

    def __init__(self, request_id: str, message: str) -> None:
        """Create a recoverable provisioning conflict.

        Args:
            request_id: Durable provisioning request identifier.
            message: Safe local explanation of the conflict. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.WAKE_TARGET_CONFLICT,
            message=message,
            operation="worker_provisioning",
            resource_id=request_id,
            recoverable=True,
        )


class ProvisioningStaleCallbackError(OrchestrationError):
    """Report a callback carrying an obsolete admission or binding revision."""

    def __init__(self, request_id: str, current_revision: int) -> None:
        """Create a recoverable stale-callback error.

        Args:
            request_id: Durable provisioning request identifier.
            current_revision: Current admission revision expected by the server. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.REVISION_CONFLICT,
            message=(
                f"Stale provisioning callback for {request_id!r}; current revision is "
                f"{current_revision}. Refresh the explicitly admitted request."
            ),
            operation="worker_provisioning_callback",
            resource_id=request_id,
            recoverable=True,
            current_revision=current_revision,
        )


class ProvisioningInvalidTransitionError(InvalidTransitionError):
    """Report an invalid provisioning lifecycle transition."""
