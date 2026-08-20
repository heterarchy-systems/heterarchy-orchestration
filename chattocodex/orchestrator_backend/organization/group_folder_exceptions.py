from __future__ import annotations


class GroupFolderRepositoryError(Exception):
    """Base infrastructure-independent failure at the GroupFolder repository port."""


class GroupFolderNameConflictError(GroupFolderRepositoryError):
    """A sibling folder already owns the requested normalized name."""


class GroupFolderStorageError(GroupFolderRepositoryError):
    """The PostgreSQL persistence boundary failed unexpectedly."""
