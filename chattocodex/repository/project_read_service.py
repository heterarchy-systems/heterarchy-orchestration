from collections import deque
from collections.abc import Iterator
from pathlib import Path

from chattocodex.persistence.orchestration_state_repository import RunStateRepository
from chattocodex.repository.project_path_resolver import ProjectPathResolver
from chattocodex.repository.repository_exceptions import (
    RepositoryReadError,
    RepositoryReadLimitError,
    UnsupportedRepositoryFileError,
)
from chattocodex.repository.repository_read_dto import (
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
        *,
        run_repository: RunStateRepository,
        path_resolver: ProjectPathResolver,
    ) -> None:
        self._run_repository = run_repository
        self._path_resolver = path_resolver

    def tree(
        self,
        *,
        run_id: str,
        relative_path: str,
        max_depth: int,
        max_entries: int,
    ) -> RepositoryTreeSnapshot:
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
                    kind = _entry_kind(path=child)
                    entries.append(
                        RepositoryTreeEntry(
                            relative_path=child.relative_to(project_root).as_posix(),
                            kind=kind,
                            size_bytes=_file_size(path=child)
                            if kind == "file"
                            else None,
                        )
                    )
                    if (
                        kind == "directory"
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
        *,
        run_id: str,
        relative_path: str,
        start_line: int,
        end_line: int,
    ) -> RepositoryFileSlice:
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
        *,
        run_id: str,
        query: str,
        max_results: int,
    ) -> RepositorySearchResult:
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
        *,
        run_id: str,
        relative_path: str,
        operation: str,
    ) -> tuple[Path, Path]:
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
        *,
        path: Path,
        operation: str,
        relative_path: str,
    ) -> str:
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


def _entry_kind(*, path: Path) -> RepositoryEntryKind:
    if path.is_symlink():
        return "symlink"
    if path.is_dir():
        return "directory"
    if path.is_file():
        return "file"
    return "other"


def _file_size(*, path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None


def _iter_search_files(*, project_root: Path) -> Iterator[Path]:
    directories: list[Path] = [project_root]
    while directories:
        directory = directories.pop()
        for child in sorted(directory.iterdir(), key=lambda item: item.name.casefold()):
            if child.is_symlink():
                continue
            if child.is_dir():
                if child.name not in _IGNORED_DIRECTORIES:
                    directories.append(child)
                continue
            if child.is_file():
                yield child


def _read_searchable_text(*, path: Path) -> str | None:
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
