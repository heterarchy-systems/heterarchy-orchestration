from enum import StrEnum


class BrokerTaskStatus(StrEnum):
    """Lifecycle states for Broker tasks shared across backend and interfaces."""

    QUEUED = "queued"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        """Return whether this Broker task cannot transition again.

        Args:
            None.

        Returns:
            True for completed or failed Broker tasks.
        """
        return self in {BrokerTaskStatus.COMPLETED, BrokerTaskStatus.FAILED}


class WorkerSlotStatus(StrEnum):
    """Lifecycle states for Broker worker slots."""

    IDLE = "idle"
    BUSY = "busy"
