"""Reusable Pydantic v2 constrained types for Worker wake boundaries."""

from typing import Annotated

from pydantic import StringConstraints

WakeTargetId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^wtgt_[0-9a-f]{32}$"),
]
