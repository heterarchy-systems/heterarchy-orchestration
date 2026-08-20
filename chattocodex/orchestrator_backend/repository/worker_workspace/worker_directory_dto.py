from dataclasses import dataclass

from chattocodex.shared.element_collect.repository_enums import RepositoryEntryKind


@dataclass(frozen=True, slots=True)
class WorkerDirectoryEntry:
    """Describe one entry returned from a Worker directory listing."""

    name: str
    relative_path: str
    kind: RepositoryEntryKind
    size_bytes: int | None


@dataclass(frozen=True, slots=True)
class WorkerDirectoryListing:
    """Describe one bounded Worker worktree directory listing."""

    run_id: str
    worker_id: str
    relative_path: str
    entries: tuple[WorkerDirectoryEntry, ...]


@dataclass(frozen=True, slots=True)
class WorkerWorkspacePathInfo:
    """Describe one resolved path inside a Worker worktree."""

    run_id: str
    worker_id: str
    relative_path: str
    kind: RepositoryEntryKind
    size_bytes: int | None


@dataclass(frozen=True, slots=True)
class WorkerDirectoryMutationResult:
    """Describe a successful Worker directory mutation."""

    run_id: str
    worker_id: str
    relative_path: str
