from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from chattocodex.schemas.orchestration_schemas import RunId

RelativePath = Annotated[
    str,
    StringConstraints(strip_whitespace=True, max_length=4096),
]
SearchQuery = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=500),
]


class RepositoryBoundaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )


class ProjectTreeRequest(RepositoryBoundaryModel):
    run_id: RunId
    relative_path: RelativePath = ""
    max_depth: int = Field(default=2, ge=0, le=8)
    max_entries: int = Field(default=200, ge=1, le=1000)


class RepositoryTreeEntryView(RepositoryBoundaryModel):
    relative_path: RelativePath
    kind: Literal["file", "directory", "symlink", "other"]
    size_bytes: int | None = Field(default=None, ge=0)


class ProjectTreeSuccess(RepositoryBoundaryModel):
    ok: Literal[True] = True
    root_relative_path: RelativePath
    entries: tuple[RepositoryTreeEntryView, ...]
    truncated: bool


class ProjectFileReadRequest(RepositoryBoundaryModel):
    run_id: RunId
    relative_path: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=4096),
    ]
    start_line: int = Field(default=1, ge=1)
    end_line: int = Field(default=200, ge=1)

    @model_validator(mode="after")
    def validate_line_window(self) -> Self:
        if self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line.")
        if self.end_line - self.start_line + 1 > 500:
            raise ValueError("A file read may return at most 500 lines.")
        return self


class ProjectFileReadSuccess(RepositoryBoundaryModel):
    ok: Literal[True] = True
    relative_path: RelativePath
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=0)
    total_lines: int = Field(ge=0)
    content: str


class ProjectTextSearchRequest(RepositoryBoundaryModel):
    run_id: RunId
    query: SearchQuery
    max_results: int = Field(default=50, ge=1, le=100)


class RepositoryTextMatchView(RepositoryBoundaryModel):
    relative_path: RelativePath
    line_number: int = Field(ge=1)
    excerpt: str = Field(max_length=500)


class ProjectTextSearchSuccess(RepositoryBoundaryModel):
    ok: Literal[True] = True
    query: SearchQuery
    matches: tuple[RepositoryTextMatchView, ...]
    truncated: bool
