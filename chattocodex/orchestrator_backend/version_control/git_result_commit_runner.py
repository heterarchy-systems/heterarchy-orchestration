import os
import subprocess
from collections.abc import Mapping
from pathlib import Path

from chattocodex.orchestrator_backend.version_control.git_exceptions import (
    GitWorktreeError,
)
from chattocodex.orchestrator_backend.version_control.git_result_dto import (
    GitCommitExecution,
)

_GIT_TIMEOUT_SECONDS = 30
_COMMIT_AUTHOR_NAME = "ChatToCodex Worker"
_COMMIT_AUTHOR_EMAIL = "chattocodex-worker@local"


class GitResultCommitRunner:
    def create_commit(
        self,
        worktree_root: Path,
        expected_parent: str,
        summary: str,
    ) -> GitCommitExecution:
        """Create commit. Args: worktree_root, expected_parent, summary. Returns: GitCommitExecution."""
        actual_parent = self._run_text(
            worktree_root=worktree_root,
            args=("rev-parse", "HEAD"),
            operation="worker_result_commit",
        )
        if actual_parent != expected_parent:
            raise GitWorktreeError(
                operation="worker_result_commit",
                resource_id=str(worktree_root),
                message=(
                    "Worker worktree HEAD does not match the expected pre-commit base. "
                    "Do not commit a drifted workspace."
                ),
                recoverable=False,
            )
        status = self._run_bytes(
            worktree_root=worktree_root,
            args=("status", "--porcelain=v1", "-z"),
            operation="worker_result_commit",
        )
        if not status:
            raise GitWorktreeError(
                operation="worker_result_commit",
                resource_id=str(worktree_root),
                message="Worker workspace has no changes to commit.",
                recoverable=True,
            )

        self._run_text(
            worktree_root=worktree_root,
            args=("add", "--all", "--"),
            operation="worker_result_commit",
        )
        tree_sha = self._run_text(
            worktree_root=worktree_root,
            args=("write-tree",),
            operation="worker_result_commit",
        )
        commit_sha = self._run_text(
            worktree_root=worktree_root,
            args=("commit-tree", tree_sha, "-p", expected_parent, "-m", summary),
            operation="worker_result_commit",
            extra_environment={
                "GIT_AUTHOR_NAME": _COMMIT_AUTHOR_NAME,
                "GIT_AUTHOR_EMAIL": _COMMIT_AUTHOR_EMAIL,
                "GIT_COMMITTER_NAME": _COMMIT_AUTHOR_NAME,
                "GIT_COMMITTER_EMAIL": _COMMIT_AUTHOR_EMAIL,
            },
        )
        changed_paths_bytes = self._run_bytes(
            worktree_root=worktree_root,
            args=("diff", "--name-only", "--cached", "-z", expected_parent),
            operation="worker_result_commit",
        )
        changed_paths = tuple(
            path.decode("utf-8") for path in changed_paths_bytes.split(b"\x00") if path
        )
        if not changed_paths:
            raise GitWorktreeError(
                operation="worker_result_commit",
                resource_id=str(worktree_root),
                message="Git result commit contains no changed paths.",
                recoverable=False,
            )
        return GitCommitExecution(
            commit_sha=commit_sha,
            parent_sha=expected_parent,
            changed_paths=changed_paths,
        )

    def activate_commit(
        self,
        worktree_root: Path,
        expected_parent: str,
        commit_sha: str,
    ) -> None:
        """Activate commit. Args: worktree_root, expected_parent, commit_sha. Returns: None."""
        current_head = self.current_head(worktree_root=worktree_root)
        if current_head == commit_sha:
            return
        if current_head != expected_parent:
            raise GitWorktreeError(
                operation="worker_result_commit",
                resource_id=str(worktree_root),
                message="Worker worktree HEAD drifted before result commit activation.",
                recoverable=False,
            )
        self._run_text(
            worktree_root=worktree_root,
            args=("update-ref", "HEAD", commit_sha, expected_parent),
            operation="worker_result_commit",
        )
        post_status = self._run_bytes(
            worktree_root=worktree_root,
            args=("status", "--porcelain=v1", "-z"),
            operation="worker_result_commit",
        )
        if post_status:
            raise GitWorktreeError(
                operation="worker_result_commit",
                resource_id=str(worktree_root),
                message="Worker worktree is not clean after result commit.",
                recoverable=False,
            )

    def current_head(self, worktree_root: Path) -> str:
        """Current head. Args: worktree_root. Returns: str."""
        return self._run_text(
            worktree_root=worktree_root,
            args=("rev-parse", "HEAD"),
            operation="worker_result_commit",
        )

    def _run_text(
        self,
        worktree_root: Path,
        args: tuple[str, ...],
        operation: str,
        extra_environment: Mapping[str, str] | None = None,
    ) -> str:
        """Run text. Args: worktree_root, args, operation, extra_environment. Returns: str."""
        output = self._run(
            worktree_root=worktree_root,
            args=args,
            operation=operation,
            text=True,
            extra_environment=extra_environment,
        )
        assert isinstance(output, str)
        return output.strip()

    def _run_bytes(
        self,
        worktree_root: Path,
        args: tuple[str, ...],
        operation: str,
    ) -> bytes:
        """Run bytes. Args: worktree_root, args, operation. Returns: bytes."""
        output = self._run(
            worktree_root=worktree_root,
            args=args,
            operation=operation,
            text=False,
            extra_environment=None,
        )
        assert isinstance(output, bytes)
        return output

    def _run(
        self,
        worktree_root: Path,
        args: tuple[str, ...],
        operation: str,
        text: bool,
        extra_environment: Mapping[str, str] | None,
    ) -> str | bytes:
        """Run. Args: worktree_root, args, operation, text, extra_environment. Returns: str | bytes."""
        environment = os.environ.copy()
        if extra_environment is not None:
            environment.update(extra_environment)
        try:
            result = subprocess.run(
                ("git", "-C", str(worktree_root), *args),
                check=False,
                capture_output=True,
                text=text,
                timeout=_GIT_TIMEOUT_SECONDS,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitWorktreeError(
                operation=operation,
                resource_id=str(worktree_root),
                message=f"Git operation {operation!r} could not be executed.",
                recoverable=True,
            ) from error
        if result.returncode != 0:
            stderr = result.stderr
            stdout = result.stdout
            detail = (
                _output_text(stderr) or _output_text(stdout) or "git command failed"
            )
            raise GitWorktreeError(
                operation=operation,
                resource_id=str(worktree_root),
                message=f"Git operation {operation!r} failed: {detail[:500]}",
                recoverable=True,
            )
        return result.stdout


def _output_text(output: str | bytes) -> str:
    """Output text. Args: output. Returns: str."""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace").strip()
    return output.strip()
