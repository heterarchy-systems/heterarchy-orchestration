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
    RepositoryReadLimitError,
    UnsupportedRepositoryFileError,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


def _commit_project(project_root: Path) -> None:
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


def _build_read_service(
    *,
    state_root: Path,
) -> ProjectReadService:
    return ProjectReadService(
        run_repository=RunStateRepository(state_root=state_root),
        path_resolver=ProjectPathResolver(),
    )


def _build_run_service(
    *,
    state_root: Path,
    workspace_root: Path,
) -> tuple[RunOrchestrationService, str]:
    workspace_service = WorkspaceProjectService(workspace_root=workspace_root)
    project_id = workspace_service.list_projects()[0].project_id
    return (
        RunOrchestrationService(
            run_repository=RunStateRepository(state_root=state_root),
            workspace_project_service=workspace_service,
            command_runner=GitCommandRunner(),
        ),
        project_id,
    )


def test_project_read_service_lists_reads_and_searches_bound_run_project(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample-project"
    source_root = project_root / "src"
    source_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'sample-project'\n",
        encoding="utf-8",
    )
    (source_root / "alpha.py").write_text(
        "FIRST = 1\nTARGET_VALUE = 'alpha'\nTHIRD = 3\n",
        encoding="utf-8",
    )
    (source_root / "beta.py").write_text(
        "target_value = 'beta'\n",
        encoding="utf-8",
    )
    _commit_project(project_root)
    run_service, project_id = _build_run_service(
        state_root=state_root,
        workspace_root=workspace_root,
    )
    created_run = run_service.create_run(goal="Read project", project_id=project_id)
    service = _build_read_service(state_root=state_root)

    tree = service.tree(
        run_id=created_run.state.run_id,
        relative_path="",
        max_depth=2,
        max_entries=50,
    )
    tree_paths = {entry.relative_path for entry in tree.entries}
    assert "pyproject.toml" in tree_paths
    assert "src" in tree_paths
    assert "src/alpha.py" in tree_paths
    assert tree.truncated is False

    file_slice = service.read_file(
        run_id=created_run.state.run_id,
        relative_path="src/alpha.py",
        start_line=2,
        end_line=3,
    )
    assert file_slice.content == "TARGET_VALUE = 'alpha'\nTHIRD = 3"
    assert file_slice.total_lines == 3

    search = service.search_text(
        run_id=created_run.state.run_id,
        query="target_value",
        max_results=10,
    )
    assert {(match.relative_path, match.line_number) for match in search.matches} == {
        ("src/alpha.py", 2),
        ("src/beta.py", 1),
    }


def test_project_read_service_rejects_path_escape_and_binary_or_large_file(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "sample-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'sample-project'\n",
        encoding="utf-8",
    )
    outside_file = tmp_path / "outside.txt"
    outside_file.write_text("outside", encoding="utf-8")
    (project_root / "binary.bin").write_bytes(b"abc\x00def")
    (project_root / "large.txt").write_bytes(b"x" * 1_048_577)
    _commit_project(project_root)

    run_service, project_id = _build_run_service(
        state_root=state_root,
        workspace_root=workspace_root,
    )
    created_run = run_service.create_run(goal="Boundary checks", project_id=project_id)
    service = _build_read_service(state_root=state_root)

    with pytest.raises(ProjectPathDeniedError):
        service.read_file(
            run_id=created_run.state.run_id,
            relative_path="../outside.txt",
            start_line=1,
            end_line=10,
        )

    symlink_path = project_root / "outside-link.txt"
    try:
        symlink_path.symlink_to(outside_file)
    except OSError:
        symlink_path = None
    if symlink_path is not None:
        with pytest.raises(ProjectPathDeniedError):
            service.read_file(
                run_id=created_run.state.run_id,
                relative_path="outside-link.txt",
                start_line=1,
                end_line=10,
            )

    with pytest.raises(UnsupportedRepositoryFileError):
        service.read_file(
            run_id=created_run.state.run_id,
            relative_path="binary.bin",
            start_line=1,
            end_line=10,
        )

    with pytest.raises(RepositoryReadLimitError):
        service.read_file(
            run_id=created_run.state.run_id,
            relative_path="large.txt",
            start_line=1,
            end_line=10,
        )
