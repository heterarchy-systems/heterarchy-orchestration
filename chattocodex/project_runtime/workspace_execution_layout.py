from pathlib import Path


class WorkspaceExecutionLayout:
    """Locates project execution artifacts relative to the configured workspace."""

    DIRECTORY_NAME = ".chattocodex"

    @classmethod
    def state_root(cls, *, workspace_root: Path) -> Path:
        return workspace_root.expanduser().resolve() / cls.DIRECTORY_NAME
