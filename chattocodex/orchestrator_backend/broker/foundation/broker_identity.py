import secrets
from datetime import UTC, datetime
from uuid import uuid4


def new_broker_task_id() -> str:
    """Create a new Broker task identifier. Args: None. Returns: str."""
    return f"btask_{uuid4().hex[:24]}"


def new_slot_id() -> str:
    """Create a new Worker slot identifier. Args: None. Returns: str."""
    return f"slot_{uuid4().hex[:24]}"


def new_broker_consumer_id() -> str:
    """Create a new Broker consumer identifier. Args: None. Returns: str."""
    return f"consumer_{uuid4().hex[:24]}"


def new_slot_handle() -> str:
    """Create a new high-entropy Worker slot capability. Args: None. Returns: str."""
    return f"slh_{secrets.token_urlsafe(32)}"


def utc_now() -> datetime:
    """Return the current timezone-aware UTC time. Args: None. Returns: datetime."""
    return datetime.now(UTC)
