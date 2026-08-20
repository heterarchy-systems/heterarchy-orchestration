import hashlib
import hmac
import secrets
from uuid import uuid4


def new_run_id() -> str:
    """New run id. Args: None. Returns: str."""
    return f"run_{uuid4().hex[:24]}"


def new_worker_id() -> str:
    """New worker id. Args: None. Returns: str."""
    return f"worker_{uuid4().hex[:24]}"


def new_task_id() -> str:
    """New task id. Args: None. Returns: str."""
    return f"task_{uuid4().hex[:24]}"


def new_run_handle() -> str:
    """New run handle. Args: None. Returns: str."""
    return f"rnh_{secrets.token_urlsafe(32)}"


def new_worker_handle() -> str:
    """New worker handle. Args: None. Returns: str."""
    return f"wkh_{secrets.token_urlsafe(32)}"


def digest_capability(handle: str) -> str:
    """Digest capability. Args: handle. Returns: str."""
    return hashlib.sha256(handle.encode("utf-8")).hexdigest()


def capability_matches(handle: str, expected_digest: str) -> bool:
    """Capability matches. Args: handle, expected_digest. Returns: bool."""
    actual_digest = digest_capability(handle)
    return hmac.compare_digest(actual_digest, expected_digest)
