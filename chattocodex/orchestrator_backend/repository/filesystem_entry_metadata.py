from dataclasses import dataclass
from pathlib import Path
from stat import S_ISDIR, S_ISLNK, S_ISREG

from chattocodex.shared.element_collect.repository_enums import RepositoryEntryKind


@dataclass(frozen=True, slots=True)
class FilesystemEntryMetadata:
    """Describe one filesystem entry from a single non-following stat call."""

    kind: RepositoryEntryKind
    size_bytes: int | None


def read_filesystem_entry_metadata(path: Path) -> FilesystemEntryMetadata | None:
    """Read one path's kind and file size without following symbolic links.

    Args:
        path: Filesystem entry to inspect.

    Returns:
        Typed metadata from one ``lstat`` call, or ``None`` when the entry cannot
        be inspected.
    """
    try:
        status = path.lstat()
    except OSError:
        return None

    mode = status.st_mode
    if S_ISLNK(mode):
        return FilesystemEntryMetadata(
            kind=RepositoryEntryKind.SYMLINK,
            size_bytes=None,
        )
    if S_ISDIR(mode):
        return FilesystemEntryMetadata(
            kind=RepositoryEntryKind.DIRECTORY,
            size_bytes=None,
        )
    if S_ISREG(mode):
        return FilesystemEntryMetadata(
            kind=RepositoryEntryKind.FILE,
            size_bytes=status.st_size,
        )
    return FilesystemEntryMetadata(
        kind=RepositoryEntryKind.OTHER,
        size_bytes=None,
    )
