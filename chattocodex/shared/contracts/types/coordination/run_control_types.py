"""Reusable Pydantic v2 constrained types for workflow control boundaries."""

from typing import Annotated

from pydantic import StringConstraints

WorkflowId = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^wf_[0-9a-f]{24}$"),
]
WorkflowHandle = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^wfh_[A-Za-z0-9_-]{20,}$", max_length=96),
]
ReviewInstructions = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=8_000,
    ),
]
ReviewSummary = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=16_000,
    ),
]
