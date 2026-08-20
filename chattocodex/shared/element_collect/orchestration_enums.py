from enum import StrEnum


class RunStatus(StrEnum):
    """Lifecycle states for an orchestration run."""

    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Return whether this Run can no longer create capacity demand.

        Args:
            None.

        Returns:
            True when the Run is completed or cancelled.
        """
        return self in {RunStatus.COMPLETED, RunStatus.CANCELLED}


class WorkerStatus(StrEnum):
    """Lifecycle states for logical orchestration workers."""

    ACTIVE = "active"


class TaskStatus(StrEnum):
    """Lifecycle states for logical orchestration tasks."""

    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        """Return whether this Task has reached a terminal state.

        Args:
            None.

        Returns:
            True for completed or failed Tasks.
        """
        return self in {TaskStatus.COMPLETED, TaskStatus.FAILED}


class OrchestrationErrorCode(StrEnum):
    """Stable symbolic error codes shared by orchestration boundaries."""

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
