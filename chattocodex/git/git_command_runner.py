import subprocess
from pathlib import Path

from chattocodex.git.git_exceptions import GitRepositoryRequiredError, GitWorktreeError
from chattocodex.project_runtime.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)

_GIT_TIMEOUT_SECONDS = 30


class GitCommandRunner:
    def require_run_ready_repository(
        self,
        *,
        project_root: Path,
        operation: str,
    ) -> str:
        self.require_top_level_repository(
            project_root=project_root,
            operation=operation,
        )
        self.require_clean_worktree(
            project_root=project_root,
            operation=operation,
        )
        return self.head_commit(
            repository_root=project_root,
            operation=operation,
        )

    def require_top_level_repository(
        self,
        *,
        project_root: Path,
        operation: str = "git_require_top_level",
    ) -> None:
        result = self._run_git(
            project_root=project_root,
            args=("rev-parse", "--show-toplevel"),
            operation=operation,
        )
        if result.returncode != 0:
            raise GitRepositoryRequiredError(
                operation=operation,
                project_root=str(project_root),
            )
        top_level = result.stdout.strip()
        if Path(top_level).resolve() != project_root.resolve():
            raise GitRepositoryRequiredError(
                operation=operation,
                project_root=str(project_root),
            )

    def require_clean_worktree(
        self,
        *,
        project_root: Path,
        operation: str = "git_require_clean_worktree",
    ) -> None:
        self._ensure_project_runtime_excluded(project_root=project_root)
        status = self._run_text(
            project_root=project_root,
            args=("status", "--porcelain=v1", "--untracked-files=normal"),
            operation=operation,
        )
        if status:
            raise GitWorktreeError(
                operation=operation,
                resource_id=str(project_root),
                message=(
                    "The selected Git project has uncommitted or untracked changes. "
                    "Commit, stash, or remove them before creating or preparing an isolated Worker Run."
                ),
                recoverable=True,
            )

    def _ensure_project_runtime_excluded(self, *, project_root: Path) -> None:
        exclude_path_text = self._run_text(
            project_root=project_root,
            args=("rev-parse", "--git-path", "info/exclude"),
            operation="git_project_runtime_exclude_path",
        )
        exclude_path = Path(exclude_path_text)
        if not exclude_path.is_absolute():
            exclude_path = project_root / exclude_path
        pattern = f"/{WorkspaceExecutionLayout.DIRECTORY_NAME}/"
        try:
            existing = (
                exclude_path.read_text(encoding="utf-8")
                if exclude_path.exists()
                else ""
            )
            if pattern in {line.strip() for line in existing.splitlines()}:
                return
            exclude_path.parent.mkdir(parents=True, exist_ok=True)
            separator = "" if not existing or existing.endswith("\n") else "\n"
            exclude_path.write_text(
                f"{existing}{separator}{pattern}\n",
                encoding="utf-8",
            )
        except OSError as error:
            raise GitWorktreeError(
                operation="git_project_runtime_exclude",
                resource_id=str(project_root),
                message=(
                    "ChatToCodex could not exclude its project-local execution state from Git "
                    f"status: {error}"
                ),
                recoverable=True,
            ) from error

    def head_commit(
        self,
        *,
        repository_root: Path,
        operation: str = "git_head_commit",
    ) -> str:
        return self._run_text(
            project_root=repository_root,
            args=("rev-parse", "HEAD"),
            operation=operation,
        )

    def add_detached_worktree(
        self,
        *,
        project_root: Path,
        worktree_root: Path,
        base_commit: str,
    ) -> None:
        worktree_root.parent.mkdir(parents=True, exist_ok=True)
        self._run_text(
            project_root=project_root,
            args=("worktree", "add", "--detach", str(worktree_root), base_commit),
            operation="git_worktree_add",
        )

    def _run_text(
        self,
        *,
        project_root: Path,
        args: tuple[str, ...],
        operation: str,
    ) -> str:
        result = self._run_git(
            project_root=project_root,
            args=args,
            operation=operation,
        )
        if result.returncode != 0:
            detail = (
                result.stderr.strip() or result.stdout.strip() or "git command failed"
            )
            raise GitWorktreeError(
                operation=operation,
                resource_id=str(project_root),
                message=f"Git operation {operation!r} failed: {detail[:500]}",
                recoverable=True,
            )
        return result.stdout.strip()

    def _run_git(
        self,
        *,
        project_root: Path,
        args: tuple[str, ...],
        operation: str,
    ) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                ("git", "-C", str(project_root), *args),
                check=False,
                capture_output=True,
                text=True,
                timeout=_GIT_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitWorktreeError(
                operation=operation,
                resource_id=str(project_root),
                message=f"Git operation {operation!r} could not be executed.",
                recoverable=True,
            ) from error
