import secrets
from uuid import uuid4

from chattocodex.orchestration.orchestration_identity import (
    capability_matches,
    digest_capability,
)


def new_workflow_id() -> str:
    return f"wf_{uuid4().hex[:24]}"


def new_workflow_handle() -> str:
    return f"wfh_{secrets.token_urlsafe(32)}"


__all__ = [
    "capability_matches",
    "digest_capability",
    "new_workflow_handle",
    "new_workflow_id",
]
