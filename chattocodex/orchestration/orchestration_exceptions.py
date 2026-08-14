from enum import StrEnum


class OrchestrationErrorCode(StrEnum):
    VALIDATION_ERROR = "validation_error"
    NOT_FOUND = "not_found"
    CAPABILITY_DENIED = "capability_denied"
    REVISION_CONFLICT = "revision_conflict"
    INVALID_TRANSITION = "invalid_transition"
    ASSIGNMENT_MISMATCH = "assignment_mismatch"
    ALREADY_EXISTS = "already_exists"
    PERSISTENCE_ERROR = "persistence_error"
    PATH_OUTSIDE_PROJECT = "path_outside_project"
    UNSUPPORTED_FILE = "unsupported_file"
    READ_LIMIT_EXCEEDED = "read_limit_exceeded"
    REPOSITORY_READ_ERROR = "repository_read_error"
    GIT_REPOSITORY_REQUIRED = "git_repository_required"
    GIT_WORKTREE_ERROR = "git_worktree_error"
    CONTENT_HASH_CONFLICT = "content_hash_conflict"
    REPOSITORY_WRITE_ERROR = "repository_write_error"
    WAKE_CONFIGURATION_ERROR = "wake_configuration_error"
    WAKE_TARGET_CONFLICT = "wake_target_conflict"
    PROJECT_BINDING_CONFLICT = "project_binding_conflict"
    PROJECT_BINDING_CONFIGURATION = "project_binding_configuration"


class OrchestrationError(Exception):
    def __init__(
        self,
        *,
        error_code: OrchestrationErrorCode,
        message: str,
        operation: str,
        resource_id: str | None,
        recoverable: bool,
        current_revision: int | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.operation = operation
        self.resource_id = resource_id
        self.recoverable = recoverable
        self.current_revision = current_revision


class ResourceNotFoundError(OrchestrationError):
    def __init__(self, *, operation: str, resource_id: str) -> None:
        super().__init__(
            error_code=OrchestrationErrorCode.NOT_FOUND,
            message=f"Resource {resource_id!r} does not exist.",
            operation=operation,
            resource_id=resource_id,
            recoverable=False,
        )


class CapabilityDeniedError(OrchestrationError):
    def __init__(self, *, operation: str, resource_id: str) -> None:
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
        *,
        operation: str,
        resource_id: str,
        current_revision: int,
    ) -> None:
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
    def __init__(self, *, operation: str, resource_id: str, message: str) -> None:
        super().__init__(
            error_code=OrchestrationErrorCode.INVALID_TRANSITION,
            message=message,
            operation=operation,
            resource_id=resource_id,
            recoverable=False,
        )


class AssignmentMismatchError(OrchestrationError):
    def __init__(self, *, operation: str, resource_id: str) -> None:
        super().__init__(
            error_code=OrchestrationErrorCode.ASSIGNMENT_MISMATCH,
            message=f"Worker is not assigned to task {resource_id!r}.",
            operation=operation,
            resource_id=resource_id,
            recoverable=False,
        )


class StatePersistenceError(OrchestrationError):
    def __init__(self, *, operation: str, resource_id: str, message: str) -> None:
        super().__init__(
            error_code=OrchestrationErrorCode.PERSISTENCE_ERROR,
            message=message,
            operation=operation,
            resource_id=resource_id,
            recoverable=True,
        )


class ResourceAlreadyExistsError(OrchestrationError):
    def __init__(self, *, operation: str, resource_id: str) -> None:
        super().__init__(
            error_code=OrchestrationErrorCode.ALREADY_EXISTS,
            message=f"Resource {resource_id!r} already exists.",
            operation=operation,
            resource_id=resource_id,
            recoverable=True,
        )
