import subprocess
from pathlib import Path

from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.git.git_exceptions import GitWorktreeError
from chattocodex.project_runtime.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)


def _git_project(tmp_path: Path) -> Path:
    project_root = tmp_path / "target-project"
    project_root.mkdir()
    (project_root / "README.md").write_text("# target\n", encoding="utf-8")
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
    return project_root


def test_workspace_execution_layout_roots_state_below_workspace(tmp_path: Path) -> None:
    workspace_root = tmp_path / "target-project"
    workspace_root.mkdir()

    assert WorkspaceExecutionLayout.state_root(workspace_root=workspace_root) == (
        workspace_root.resolve() / ".chattocodex"
    )


def test_project_execution_directory_is_locally_excluded_from_git_status(
    tmp_path: Path,
) -> None:
    project_root = _git_project(tmp_path)
    execution_root = WorkspaceExecutionLayout.state_root(workspace_root=project_root)
    execution_root.mkdir()
    (execution_root / "runtime.json").write_text("{}\n", encoding="utf-8")

    GitCommandRunner().require_clean_worktree(project_root=project_root)

    exclude = subprocess.run(
        ("git", "-C", str(project_root), "rev-parse", "--git-path", "info/exclude"),
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    exclude_path = Path(exclude)
    if not exclude_path.is_absolute():
        exclude_path = project_root / exclude_path
    assert "/.chattocodex/" in exclude_path.read_text(encoding="utf-8").splitlines()


def test_project_execution_exclusion_does_not_hide_real_source_changes(
    tmp_path: Path,
) -> None:
    project_root = _git_project(tmp_path)
    execution_root = WorkspaceExecutionLayout.state_root(workspace_root=project_root)
    execution_root.mkdir()
    (execution_root / "runtime.json").write_text("{}\n", encoding="utf-8")
    (project_root / "untracked.txt").write_text("real change\n", encoding="utf-8")

    try:
        GitCommandRunner().require_clean_worktree(project_root=project_root)
    except GitWorktreeError:
        pass
    else:
        raise AssertionError(
            "A real untracked source file must still fail the clean-worktree gate."
        )


def test_detached_worker_worktree_can_live_under_project_execution_directory(
    tmp_path: Path,
) -> None:
    project_root = _git_project(tmp_path)
    runner = GitCommandRunner()
    runner.require_clean_worktree(project_root=project_root)
    worktree_root = (
        WorkspaceExecutionLayout.state_root(workspace_root=project_root)
        / "worktrees"
        / "run_test"
        / "worker_test"
    )

    runner.add_detached_worktree(
        project_root=project_root,
        worktree_root=worktree_root,
        base_commit=runner.head_commit(repository_root=project_root),
    )

    assert (worktree_root / "README.md").is_file()
