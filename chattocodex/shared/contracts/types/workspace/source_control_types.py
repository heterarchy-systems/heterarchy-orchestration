"""Reusable Pydantic v2 constrained types for source-control boundaries."""

from typing import Annotated

from pydantic import StringConstraints

ChangedPath = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=4096,
    ),
]
GitCommit = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        pattern=r"^[0-9a-f]{40,64}$",
    ),
]
