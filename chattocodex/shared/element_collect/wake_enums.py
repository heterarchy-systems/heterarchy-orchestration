from enum import StrEnum


class WakeTargetStatus(StrEnum):
    """Lifecycle states for reusable worker wake targets."""

    IDLE = "idle"
    WAKING = "waking"
    CLAIMING = "claiming"
    BUSY = "busy"


class WorkerBrowserRuntimeMode(StrEnum):
    """Supported browser runtime modes for Worker ChatGPT sessions."""

    HEADLESS = "headless"
    VISIBLE_BOOTSTRAP = "visible_bootstrap"


class WorkerWakeCapabilityStatus(StrEnum):
    """Lifecycle states for worker wake capabilities."""

    ISSUED = "issued"
    CLAIMING = "claiming"
    CONSUMED = "consumed"


class WorkerWakeTransport(StrEnum):
    """Supported delivery transports for worker wake capabilities."""

    SCHEDULER = "scheduler"
