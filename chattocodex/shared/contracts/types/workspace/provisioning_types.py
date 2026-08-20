"""Reusable Pydantic v2 constrained types for Worker provisioning boundaries."""

from typing import Annotated

from pydantic import StringConstraints

ProvisioningRequestId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^prq_[0-9a-f]{32}$"),
]
