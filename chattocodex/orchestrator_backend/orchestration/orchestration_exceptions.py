from chattocodex.shared.element_collect.orchestration_enums import (
    OrchestrationErrorCode,
)


class OrchestrationError(Exception):
    def __init__(
        self,
        error_code: OrchestrationErrorCode,
        message: str,
        operation: str,
        resource_id: str | None,
        recoverable: bool,
        current_revision: int | None = None,
    ) -> None:
        """Initialize this object. Args: error_code, message, operation, resource_id, recoverable, current_revision. Returns: None."""
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.operation = operation
        self.resource_id = resource_id
        self.recoverable = recoverable
        self.current_revision = current_revision


class ResourceNotFoundError(OrchestrationError):
    def __init__(self, operation: str, resource_id: str) -> None:
        """Initialize this object. Args: operation, resource_id. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.NOT_FOUND,
            message=f"Resource {resource_id!r} does not exist.",
            operation=operation,
            resource_id=resource_id,
            recoverable=False,
        )


class CapabilityDeniedError(OrchestrationError):
    def __init__(self, operation: str, resource_id: str) -> None:
        """Initialize this object. Args: operation, resource_id. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.CAPABILITY_DENIED,
            message=f"Capability validation failed for {resource_id!r}.",
            operation=operation,
            resource_id=resource_id,
            recoverable=False,
        )


class RevisionConflictError(OrchestrationError):
    def __init__(
        self,
        operation: str,
        resource_id: str,
        current_revision: int,
    ) -> None:
        """Initialize this object. Args: operation, resource_id, current_revision. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.REVISION_CONFLICT,
            message=(
                f"Stale revision for {resource_id!r}; current revision is "
                f"{current_revision}. Refresh state before retrying."
            ),
            operation=operation,
            resource_id=resource_id,
            recoverable=True,
            current_revision=current_revision,
        )


class InvalidTransitionError(OrchestrationError):
    def __init__(self, operation: str, resource_id: str, message: str) -> None:
        """Initialize this object. Args: operation, resource_id, message. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.INVALID_TRANSITION,
            message=message,
            operation=operation,
            resource_id=resource_id,
            recoverable=False,
        )


class AssignmentMismatchError(OrchestrationError):
    def __init__(self, operation: str, resource_id: str) -> None:
        """Initialize this object. Args: operation, resource_id. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.ASSIGNMENT_MISMATCH,
            message=f"Worker is not assigned to task {resource_id!r}.",
            operation=operation,
            resource_id=resource_id,
            recoverable=False,
        )


class StatePersistenceError(OrchestrationError):
    def __init__(self, operation: str, resource_id: str, message: str) -> None:
        """Initialize this object. Args: operation, resource_id, message. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.PERSISTENCE_ERROR,
            message=message,
            operation=operation,
            resource_id=resource_id,
            recoverable=True,
        )


class ResourceAlreadyExistsError(OrchestrationError):
    def __init__(self, operation: str, resource_id: str) -> None:
        """Initialize this object. Args: operation, resource_id. Returns: None."""
        super().__init__(
            error_code=OrchestrationErrorCode.ALREADY_EXISTS,
            message=f"Resource {resource_id!r} already exists.",
            operation=operation,
            resource_id=resource_id,
            recoverable=True,
        )
