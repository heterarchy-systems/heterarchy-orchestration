from pathlib import Path

from chattocodex.orchestrator_backend.repository.repository_exceptions import (
    ProjectPathDeniedError,
)


class ProjectPathResolver:
    def resolve(
        self,
        project_root: Path,
        relative_path: str,
        operation: str,
    ) -> Path:
        """Resolve. Args: project_root, relative_path, operation. Returns: Path."""
        canonical_root = project_root.resolve()
        requested = Path(relative_path or ".")
        if requested.is_absolute() or ".." in requested.parts:
            raise ProjectPathDeniedError(
                operation=operation,
                relative_path=relative_path,
            )
        resolved = (canonical_root / requested).resolve()
        if not resolved.is_relative_to(canonical_root):
            raise ProjectPathDeniedError(
                operation=operation,
                relative_path=relative_path,
            )
        return resolved
