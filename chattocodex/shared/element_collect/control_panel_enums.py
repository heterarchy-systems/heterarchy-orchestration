from enum import StrEnum


class DatabaseOperationsState(StrEnum):
    """Availability state of the optional PostgreSQL operations read model."""

    DISABLED = "disabled"
    READY = "ready"
    DRIVER_MISSING = "driver_missing"
    UNAVAILABLE = "unavailable"


class WorkerRuntimeTopology(StrEnum):
    """Topology identifiers exposed by Worker runtime operations views."""

    PROJECT_DEFAULT = "project_default"


class RemoteMode(StrEnum):
    """Supported remote exposure modes for the Control Panel."""

    QUICK_TUNNEL = "quick_tunnel"
    CUSTOM_DOMAIN = "custom_domain"


class ExecutionTaskPhase(StrEnum):
    """Derived execution phases exposed by the Control Panel."""

    BLOCKED = "blocked"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class RunContinuationPhase(StrEnum):
    """Derived phases that describe how an orchestration run can continue."""

    PLANNING = "planning"
    READY = "ready"
    RUNNING = "running"
    BLOCKED = "blocked"
    WAITING_MAIN_REVIEW = "waiting_main_review"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
