import hashlib
import hmac
import secrets
from uuid import uuid4


def new_run_id() -> str:
    return f"run_{uuid4().hex[:24]}"


def new_worker_id() -> str:
    return f"worker_{uuid4().hex[:24]}"


def new_task_id() -> str:
    return f"task_{uuid4().hex[:24]}"


def new_run_handle() -> str:
    return f"rnh_{secrets.token_urlsafe(32)}"


def new_worker_handle() -> str:
    return f"wkh_{secrets.token_urlsafe(32)}"


def digest_capability(handle: str) -> str:
    return hashlib.sha256(handle.encode("utf-8")).hexdigest()


def capability_matches(*, handle: str, expected_digest: str) -> bool:
    actual_digest = digest_capability(handle)
    return hmac.compare_digest(actual_digest, expected_digest)
