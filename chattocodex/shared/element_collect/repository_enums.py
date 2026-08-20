from enum import StrEnum


class RepositoryEntryKind(StrEnum):
    """Filesystem entry kinds exposed through repository boundaries."""

    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"
    OTHER = "other"
