from enum import StrEnum


class MainReviewTargetStatus(StrEnum):
    """Presence states for the Main Review delivery target."""

    IDLE = "idle"
    WAKING = "waking"
    ACTIVE = "active"
