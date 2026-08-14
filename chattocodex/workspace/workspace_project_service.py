import hashlib
import re
from pathlib import Path

from chattocodex.orchestration.orchestration_exceptions import ResourceNotFoundError
from chattocodex.workspace.workspace_project_dto import WorkspaceProject

_PROJECT_MARKERS = (
    ".git",
    "pyproject.toml",
    "package.json",
    "Cargo.toml",
    "go.mod",
    "requirements.txt",
)


def _project_slug(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return normalized[:64] or "project"


def _project_id(*, name: str, root: Path) -> str:
    root_digest = hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:8]
    return f"prj_{_project_slug(name)}_{root_digest}"


class WorkspaceProjectService:
    def __init__(self, *, workspace_root: Path) -> None:
        self._workspace_root = workspace_root.expanduser().resolve()

    def list_projects(self) -> tuple[WorkspaceProject, ...]:
        if not self._workspace_root.is_dir():
            raise ResourceNotFoundError(
                operation="list_workspace_projects",
                resource_id=str(self._workspace_root),
            )

        candidates = [self._workspace_root]
        candidates.extend(
            child
            for child in self._workspace_root.iterdir()
            if child.is_dir() and not child.name.startswith(".")
        )
        projects = tuple(
            project
            for candidate in candidates
            if (project := self._build_project(candidate=candidate)) is not None
        )
        return tuple(sorted(projects, key=lambda item: item.name.casefold()))

    def require_project(self, *, project_id: str) -> WorkspaceProject:
        for project in self.list_projects():
            if project.project_id == project_id:
                return project
        raise ResourceNotFoundError(
            operation="require_workspace_project",
            resource_id=project_id,
        )

    def _build_project(self, *, candidate: Path) -> WorkspaceProject | None:
        resolved = candidate.resolve()
        if not resolved.is_relative_to(self._workspace_root):
            return None
        if not any((resolved / marker).exists() for marker in _PROJECT_MARKERS):
            return None
        return self._to_project(root=resolved)

    @staticmethod
    def _to_project(*, root: Path) -> WorkspaceProject:
        name = root.name or "workspace"
        return WorkspaceProject(
            project_id=_project_id(name=name, root=root),
            name=name,
            root=root,
        )
