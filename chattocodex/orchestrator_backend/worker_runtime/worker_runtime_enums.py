from enum import StrEnum


class WorkerRuntimeAvailability(StrEnum):
    """Derived runtime availability for a persistent Worker target."""

    ACTIVE = "active"
    DORMANT = "dormant"
    OFFLINE = "offline"
