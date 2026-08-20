from collections import deque
from collections.abc import Iterator
from pathlib import Path

from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
)
from chattocodex.orchestrator_backend.repository.filesystem_entry_metadata import (
    read_filesystem_entry_metadata,
)
from chattocodex.orchestrator_backend.repository.project_path_resolver import (
    ProjectPathResolver,
)
from chattocodex.orchestrator_backend.repository.repository_exceptions import (
    RepositoryReadError,
    RepositoryReadLimitError,
    UnsupportedRepositoryFileError,
)
from chattocodex.orchestrator_backend.repository.repository_read_dto import (
    RepositoryEntryKind,
    RepositoryFileSlice,
    RepositorySearchResult,
    RepositoryTextMatch,
    RepositoryTreeEntry,
    RepositoryTreeSnapshot,
)

_MAX_TEXT_FILE_BYTES = 1_048_576
_MAX_SEARCH_FILES = 5_000
_IGNORED_DIRECTORIES = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "node_modules",
        "build",
        "dist",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        ".next",
    }
)


class ProjectReadService:
    def __init__(
        self,
        run_repository: RunStateRepository,
        path_resolver: ProjectPathResolver,
    ) -> None:
        """Initialize this object. Args: run_repository, path_resolver. Returns: None."""
        self._run_repository = run_repository
        self._path_resolver = path_resolver

    def tree(
        self,
        run_id: str,
        relative_path: str,
        max_depth: int,
        max_entries: int,
    ) -> RepositoryTreeSnapshot:
        """Tree. Args: run_id, relative_path, max_depth, max_entries. Returns: RepositoryTreeSnapshot."""
        operation = "project_tree"
        project_root, target = self._resolve_run_path(
            run_id=run_id,
            relative_path=relative_path,
            operation=operation,
        )
        if not target.is_dir():
            raise UnsupportedRepositoryFileError(
                operation=operation,
                relative_path=relative_path,
                message=f"Path {relative_path!r} is not a directory.",
            )

        entries: list[RepositoryTreeEntry] = []
        queue: deque[tuple[Path, int]] = deque([(target, 0)])
        truncated = False
        try:
            while queue:
                directory, depth = queue.popleft()
                if depth > max_depth:
                    continue
                children = sorted(
                    directory.iterdir(), key=lambda item: item.name.casefold()
                )
                for child in children:
                    if len(entries) >= max_entries:
                        truncated = True
                        queue.clear()
                        break
                    metadata = read_filesystem_entry_metadata(path=child)
                    kind = (
                        metadata.kind
                        if metadata is not None
                        else RepositoryEntryKind.OTHER
                    )
                    entries.append(
                        RepositoryTreeEntry(
                            relative_path=child.relative_to(project_root).as_posix(),
                            kind=kind,
                            size_bytes=metadata.size_bytes
                            if metadata is not None
                            else None,
                        )
                    )
                    if (
                        kind is RepositoryEntryKind.DIRECTORY
                        and depth < max_depth
                        and child.name not in _IGNORED_DIRECTORIES
                    ):
                        queue.append((child, depth + 1))
        except OSError as error:
            raise RepositoryReadError(
                operation=operation,
                relative_path=relative_path,
                message=f"Failed to enumerate repository path {relative_path!r}.",
            ) from error

        return RepositoryTreeSnapshot(
            root_relative_path=relative_path,
            entries=tuple(entries),
            truncated=truncated,
        )

    def read_file(
        self,
        run_id: str,
        relative_path: str,
        start_line: int,
        end_line: int,
    ) -> RepositoryFileSlice:
        """Read file. Args: run_id, relative_path, start_line, end_line. Returns: RepositoryFileSlice."""
        operation = "project_file_read"
        _project_root, target = self._resolve_run_path(
            run_id=run_id,
            relative_path=relative_path,
            operation=operation,
        )
        if not target.is_file():
            raise UnsupportedRepositoryFileError(
                operation=operation,
                relative_path=relative_path,
                message=f"Path {relative_path!r} is not a regular file.",
            )
        payload = self._read_text_bytes(
            path=target,
            operation=operation,
            relative_path=relative_path,
        )
        lines = payload.splitlines()
        total_lines = len(lines)
        first_index = min(start_line - 1, total_lines)
        last_index = min(end_line, total_lines)
        selected = lines[first_index:last_index]
        actual_end_line = first_index + len(selected)
        return RepositoryFileSlice(
            relative_path=relative_path,
            start_line=start_line,
            end_line=actual_end_line,
            total_lines=total_lines,
            content="\n".join(selected),
        )

    def search_text(
        self,
        run_id: str,
        query: str,
        max_results: int,
    ) -> RepositorySearchResult:
        """Search text. Args: run_id, query, max_results. Returns: RepositorySearchResult."""
        operation = "project_text_search"
        run = self._run_repository.read(run_id=run_id)
        project_root = Path(run.project_root).resolve()
        query_casefold = query.casefold()
        matches: list[RepositoryTextMatch] = []
        scanned_files = 0
        truncated = False

        try:
            for path in _iter_search_files(project_root=project_root):
                if scanned_files >= _MAX_SEARCH_FILES or len(matches) >= max_results:
                    truncated = True
                    break
                scanned_files += 1
                relative_path = path.relative_to(project_root).as_posix()
                text = _read_searchable_text(path=path)
                if text is None:
                    continue
                for line_number, line in enumerate(text.splitlines(), start=1):
                    if query_casefold not in line.casefold():
                        continue
                    matches.append(
                        RepositoryTextMatch(
                            relative_path=relative_path,
                            line_number=line_number,
                            excerpt=line.strip()[:500],
                        )
                    )
                    if len(matches) >= max_results:
                        truncated = True
                        break
        except OSError as error:
            raise RepositoryReadError(
                operation=operation,
                relative_path="",
                message="Failed while searching the Run project.",
            ) from error

        return RepositorySearchResult(
            query=query,
            matches=tuple(matches),
            truncated=truncated,
        )

    def _resolve_run_path(
        self,
        run_id: str,
        relative_path: str,
        operation: str,
    ) -> tuple[Path, Path]:
        """Resolve run path. Args: run_id, relative_path, operation. Returns: tuple[Path, Path]."""
        run = self._run_repository.read(run_id=run_id)
        project_root = Path(run.project_root).resolve()
        target = self._path_resolver.resolve(
            project_root=project_root,
            relative_path=relative_path,
            operation=operation,
        )
        return project_root, target

    def _read_text_bytes(
        self,
        path: Path,
        operation: str,
        relative_path: str,
    ) -> str:
        """Read text bytes. Args: path, operation, relative_path. Returns: str."""
        try:
            size_bytes = path.stat().st_size
            if size_bytes > _MAX_TEXT_FILE_BYTES:
                raise RepositoryReadLimitError(
                    operation=operation,
                    relative_path=relative_path,
                    message=(
                        f"File {relative_path!r} is {size_bytes} bytes; "
                        f"the read-only limit is {_MAX_TEXT_FILE_BYTES} bytes."
                    ),
                )
            payload = path.read_bytes()
        except RepositoryReadLimitError:
            raise
        except OSError as error:
            raise RepositoryReadError(
                operation=operation,
                relative_path=relative_path,
                message=f"Failed to read repository file {relative_path!r}.",
            ) from error
        if b"\x00" in payload:
            raise UnsupportedRepositoryFileError(
                operation=operation,
                relative_path=relative_path,
                message=f"File {relative_path!r} appears to be binary.",
            )
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise UnsupportedRepositoryFileError(
                operation=operation,
                relative_path=relative_path,
                message=f"File {relative_path!r} is not valid UTF-8 text.",
            ) from error


def _iter_search_files(project_root: Path) -> Iterator[Path]:
    """Iter search files. Args: project_root. Returns: Iterator[Path]."""
    directories: list[Path] = [project_root]
    while directories:
        directory = directories.pop()
        for child in sorted(directory.iterdir(), key=lambda item: item.name.casefold()):
            metadata = read_filesystem_entry_metadata(path=child)
            if metadata is None or metadata.kind is RepositoryEntryKind.SYMLINK:
                continue
            if metadata.kind is RepositoryEntryKind.DIRECTORY:
                if child.name not in _IGNORED_DIRECTORIES:
                    directories.append(child)
                continue
            if metadata.kind is RepositoryEntryKind.FILE:
                yield child


def _read_searchable_text(path: Path) -> str | None:
    """Read searchable text. Args: path. Returns: str | None."""
    try:
        if path.stat().st_size > _MAX_TEXT_FILE_BYTES:
            return None
        payload = path.read_bytes()
    except OSError:
        return None
    if b"\x00" in payload:
        return None
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return None
