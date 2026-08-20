"""Reusable Pydantic v2 constrained types for authentication boundaries."""

from typing import Annotated

from pydantic import StringConstraints

OpaqueToken = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=20,
        max_length=1024,
    ),
]
TokenDigest = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$"),
]
ClientId = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=255,
    ),
]
ScopeName = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=128,
    ),
]
UrlText = Annotated[
    str,
    StringConstraints(
        strict=True,
        strip_whitespace=True,
        min_length=1,
        max_length=4096,
    ),
]
