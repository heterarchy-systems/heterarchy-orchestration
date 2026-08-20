from typing import Annotated, Literal, Self

from pydantic import StringConstraints, model_validator

from chattocodex.shared.contracts.orchestration_schemas import RunId
from chattocodex.shared.contracts.schema_base import StrictSchemaModel, described_field
from chattocodex.shared.element_collect.repository_enums import RepositoryEntryKind

RepositoryReadPath = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, max_length=4096),
]
SearchQuery = Annotated[
    str,
    StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=500),
]


class RepositoryBoundaryModel(StrictSchemaModel):
    pass


class ProjectTreeRequest(RepositoryBoundaryModel):
    run_id: RunId
    relative_path: RepositoryReadPath = ""
    max_depth: Annotated[
        int,
        described_field(
            description="Maximum depth allowed for this project tree request.",
            ge=0,
            le=8,
        ),
    ] = 2
    max_entries: Annotated[
        int,
        described_field(
            description="Maximum entries allowed for this project tree request.",
            ge=1,
            le=1000,
        ),
    ] = 200


class RepositoryTreeEntryView(RepositoryBoundaryModel):
    relative_path: RepositoryReadPath
    kind: RepositoryEntryKind
    size_bytes: Annotated[
        int | None,
        described_field(
            description="Size bytes for this repository tree entry view.", ge=0
        ),
    ] = None


class ProjectTreeSuccess(RepositoryBoundaryModel):
    ok: Literal[True] = True
    root_relative_path: RepositoryReadPath
    entries: tuple[RepositoryTreeEntryView, ...]
    truncated: bool


class ProjectFileReadRequest(RepositoryBoundaryModel):
    run_id: RunId
    relative_path: Annotated[
        str,
        StringConstraints(
            strict=True, strip_whitespace=True, min_length=1, max_length=4096
        ),
    ]
    start_line: Annotated[
        int,
        described_field(
            description="Start line for this project file read request.", ge=1
        ),
    ] = 1
    end_line: Annotated[
        int,
        described_field(
            description="End line for this project file read request.", ge=1
        ),
    ] = 200

    @model_validator(mode="after")
    def validate_line_window(self) -> Self:
        """Validate line window. Args: None. Returns: Self."""
        if self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line.")
        if self.end_line - self.start_line + 1 > 500:
            raise ValueError("A file read may return at most 500 lines.")
        return self


class ProjectFileReadSuccess(RepositoryBoundaryModel):
    ok: Literal[True] = True
    relative_path: RepositoryReadPath
    start_line: Annotated[
        int,
        described_field(
            description="Start line for this project file read success.", ge=1
        ),
    ]
    end_line: Annotated[
        int,
        described_field(
            description="End line for this project file read success.", ge=0
        ),
    ]
    total_lines: Annotated[
        int,
        described_field(
            description="Total lines for this project file read success.", ge=0
        ),
    ]
    content: str


class ProjectTextSearchRequest(RepositoryBoundaryModel):
    run_id: RunId
    query: SearchQuery
    max_results: Annotated[
        int,
        described_field(
            description="Maximum results allowed for this project text search request.",
            ge=1,
            le=100,
        ),
    ] = 50


class RepositoryTextMatchView(RepositoryBoundaryModel):
    relative_path: RepositoryReadPath
    line_number: Annotated[
        int,
        described_field(
            description="Line number for this repository text match view.", ge=1
        ),
    ]
    excerpt: Annotated[
        str,
        StringConstraints(strict=True, max_length=500),
        described_field(description="Excerpt for this repository text match view."),
    ]


class ProjectTextSearchSuccess(RepositoryBoundaryModel):
    ok: Literal[True] = True
    query: SearchQuery
    matches: tuple[RepositoryTextMatchView, ...]
    truncated: bool
