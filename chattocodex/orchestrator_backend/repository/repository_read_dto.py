from dataclasses import dataclass

from chattocodex.shared.element_collect.repository_enums import RepositoryEntryKind


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositoryTreeEntry:
    relative_path: str
    kind: RepositoryEntryKind
    size_bytes: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositoryTreeSnapshot:
    root_relative_path: str
    entries: tuple[RepositoryTreeEntry, ...]
    truncated: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositoryFileSlice:
    relative_path: str
    start_line: int
    end_line: int
    total_lines: int
    content: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositoryTextMatch:
    relative_path: str
    line_number: int
    excerpt: str


@dataclass(frozen=True, slots=True, kw_only=True)
class RepositorySearchResult:
    query: str
    matches: tuple[RepositoryTextMatch, ...]
    truncated: bool
