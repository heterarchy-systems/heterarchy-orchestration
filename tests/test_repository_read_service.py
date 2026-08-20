from pathlib import Path
import subprocess

import pytest

from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.orchestrator_backend.orchestration.run_orchestration_service import (
    RunOrchestrationService,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.repository.project_path_resolver import (
    ProjectPathResolver,
)
from chattocodex.orchestrator_backend.repository.project_read_service import (
    ProjectReadService,
)
from chattocodex.orchestrator_backend.repository.repository_exceptions import (
    ProjectPathDeniedError,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


def _build_service(tmp_path: Path) -> tuple[ProjectReadService, str, Path]:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'sample'\n",
        encoding="utf-8",
    )
    (project_root / "README.md").write_text(
        "first line\nORCHESTRATION_SENTINEL\nthird line\n",
        encoding="utf-8",
    )
    nested = project_root / "src"
    nested.mkdir()
    (nested / "module.py").write_text(
        "VALUE = 'ORCHESTRATION_SENTINEL'\n",
        encoding="utf-8",
    )
    for args in (
        ("init",),
        ("config", "user.email", "test@example.com"),
        ("config", "user.name", "Test User"),
        ("add", "."),
        ("commit", "-m", "initial"),
    ):
        subprocess.run(
            ("git", "-C", str(project_root), *args),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )

    run_repository = RunStateRepository(state_root=tmp_path / "state")
    run_service = RunOrchestrationService(
        run_repository=run_repository,
        workspace_project_service=WorkspaceProjectService(
            workspace_root=workspace_root
        ),
        command_runner=GitCommandRunner(),
    )
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    run = run_service.create_run(goal="Read project", project_id=project.project_id)
    return (
        ProjectReadService(
            run_repository=run_repository,
            path_resolver=ProjectPathResolver(),
        ),
        run.state.run_id,
        project_root,
    )


def test_project_read_service_tree_file_and_search_are_run_bound(
    tmp_path: Path,
) -> None:
    service, run_id, _project_root = _build_service(tmp_path)

    tree = service.tree(run_id=run_id, relative_path="", max_depth=2, max_entries=50)
    assert {entry.relative_path for entry in tree.entries} >= {
        "README.md",
        "src",
        "src/module.py",
    }
    assert tree.truncated is False

    file_slice = service.read_file(
        run_id=run_id,
        relative_path="README.md",
        start_line=2,
        end_line=3,
    )
    assert file_slice.content == "ORCHESTRATION_SENTINEL\nthird line"
    assert file_slice.total_lines == 3

    search = service.search_text(
        run_id=run_id,
        query="orchestration_sentinel",
        max_results=10,
    )
    assert {(match.relative_path, match.line_number) for match in search.matches} == {
        ("README.md", 2),
        ("src/module.py", 1),
    }


def test_project_read_service_blocks_parent_and_symlink_escape(tmp_path: Path) -> None:
    service, run_id, project_root = _build_service(tmp_path)
    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("secret", encoding="utf-8")
    (project_root / "outside-link.txt").symlink_to(outside_file)

    with pytest.raises(ProjectPathDeniedError):
        service.read_file(
            run_id=run_id,
            relative_path="../outside.txt",
            start_line=1,
            end_line=20,
        )

    with pytest.raises(ProjectPathDeniedError):
        service.read_file(
            run_id=run_id,
            relative_path="outside-link.txt",
            start_line=1,
            end_line=20,
        )
