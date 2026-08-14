from pathlib import Path

import pytest

from chattocodex.orchestration.orchestration_exceptions import ResourceNotFoundError
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


def _write_project(*, root: Path, name: str) -> Path:
    project_root = root / name
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        f"[project]\nname = '{name}'\n",
        encoding="utf-8",
    )
    return project_root


def test_workspace_lists_only_direct_recognized_projects_with_stable_ids(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    alpha_root = _write_project(root=workspace_root, name="Alpha Project")
    beta_root = _write_project(root=workspace_root, name="beta")
    (workspace_root / "not-a-project").mkdir()
    hidden_root = workspace_root / ".hidden-project"
    hidden_root.mkdir()
    (hidden_root / "pyproject.toml").write_text("[project]\nname='hidden'\n")

    service = WorkspaceProjectService(workspace_root=workspace_root)
    first = service.list_projects()
    second = service.list_projects()

    assert tuple(project.name for project in first) == ("Alpha Project", "beta")
    assert tuple(project.project_id for project in first) == tuple(
        project.project_id for project in second
    )
    assert {project.root for project in first} == {
        alpha_root.resolve(),
        beta_root.resolve(),
    }
    assert all(project.project_id.startswith("prj_") for project in first)


def test_workspace_require_project_rejects_unknown_id(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(root=workspace_root, name="only-project")
    service = WorkspaceProjectService(workspace_root=workspace_root)

    with pytest.raises(ResourceNotFoundError):
        service.require_project(project_id="prj_missing_00000000")


def test_workspace_without_project_markers_returns_no_projects(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "plain-folder"
    workspace_root.mkdir()
    (workspace_root / "notes.txt").write_text("research only", encoding="utf-8")

    service = WorkspaceProjectService(workspace_root=workspace_root)
    projects = service.list_projects()

    assert projects == ()
