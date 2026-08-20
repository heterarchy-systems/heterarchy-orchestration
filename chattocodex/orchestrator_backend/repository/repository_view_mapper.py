from chattocodex.orchestrator_backend.repository.repository_read_dto import (
    RepositoryFileSlice,
    RepositorySearchResult,
    RepositoryTreeSnapshot,
)
from chattocodex.shared.contracts.repository.repository_schemas import (
    ProjectFileReadSuccess,
    ProjectTextSearchSuccess,
    ProjectTreeSuccess,
    RepositoryTextMatchView,
    RepositoryTreeEntryView,
)


def build_project_tree_success(snapshot: RepositoryTreeSnapshot) -> ProjectTreeSuccess:
    """Build project tree success. Args: snapshot. Returns: ProjectTreeSuccess."""
    return ProjectTreeSuccess(
        root_relative_path=snapshot.root_relative_path,
        entries=tuple(
            RepositoryTreeEntryView(
                relative_path=entry.relative_path,
                kind=entry.kind,
                size_bytes=entry.size_bytes,
            )
            for entry in snapshot.entries
        ),
        truncated=snapshot.truncated,
    )


def build_project_file_read_success(
    file_slice: RepositoryFileSlice,
) -> ProjectFileReadSuccess:
    """Build project file read success. Args: file_slice. Returns: ProjectFileReadSuccess."""
    return ProjectFileReadSuccess(
        relative_path=file_slice.relative_path,
        start_line=file_slice.start_line,
        end_line=file_slice.end_line,
        total_lines=file_slice.total_lines,
        content=file_slice.content,
    )


def build_project_text_search_success(
    result: RepositorySearchResult,
) -> ProjectTextSearchSuccess:
    """Build project text search success. Args: result. Returns: ProjectTextSearchSuccess."""
    return ProjectTextSearchSuccess(
        query=result.query,
        matches=tuple(
            RepositoryTextMatchView(
                relative_path=match.relative_path,
                line_number=match.line_number,
                excerpt=match.excerpt,
            )
            for match in result.matches
        ),
        truncated=result.truncated,
    )
