"""Exceptions raised by project binding operations."""

from chattocodex.orchestration.orchestration_exceptions import (
    OrchestrationError,
    OrchestrationErrorCode,
)


class ProjectBindingConflictError(OrchestrationError):
    """Report a binding conflict without changing canonical project state."""

    def __init__(self, *, project_id: str, message: str) -> None:
        """Create a recoverable project binding conflict.

        Args:
            project_id: Canonical local project identifier involved in the conflict.
            message: Human-readable explanation safe for the local Control Panel.
        """
        super().__init__(
            error_code=OrchestrationErrorCode.PROJECT_BINDING_CONFLICT,
            message=message,
            operation="project_binding",
            resource_id=project_id,
            recoverable=True,
        )


class ProjectBindingConfigurationError(OrchestrationError):
    """Report that the runtime workspace is unavailable for binding operations."""

    def __init__(self, *, message: str) -> None:
        """Create a recoverable project binding configuration error.

        Args:
            message: Human-readable explanation of the missing runtime configuration.
        """
        super().__init__(
            error_code=OrchestrationErrorCode.PROJECT_BINDING_CONFIGURATION,
            message=message,
            operation="project_binding_configuration",
            resource_id="runtime",
            recoverable=True,
        )
