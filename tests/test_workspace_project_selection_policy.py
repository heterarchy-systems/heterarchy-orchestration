from pathlib import Path

import pytest

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_selection_policy import (
    resolve_workspace_project_id,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


def _write_project(*, workspace_root: Path, name: str) -> None:
    project_root = workspace_root / name
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        f"[project]\nname='{name}'\nversion='0.1.0'\n",
        encoding="utf-8",
    )


def test_stale_preferred_project_is_not_selected(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    _write_project(workspace_root=workspace_root, name="beta")
    service = WorkspaceProjectService(workspace_root=workspace_root)

    with pytest.raises(InvalidTransitionError, match="multiple projects"):
        resolve_workspace_project_id(
            workspace_project_service=service,
            project_id=None,
            preferred_project_ids=("prj_stale_deadbeef",),
            operation="resolve_project",
            multiple_projects_message="The workspace contains multiple projects.",
        )
