import os
import subprocess
from pathlib import Path

from chattocodex.git.git_command_runner import GitCommandRunner
from chattocodex.git.git_exceptions import GitWorktreeError
from chattocodex.integration.integration_dto import IntegrationExecution

_GIT_TIMEOUT_SECONDS = 30
_COMMIT_AUTHOR_NAME = "ChatToCodex Integration"
_COMMIT_AUTHOR_EMAIL = "chattocodex-integration@local"


class GitIntegrationRunner:
    def __init__(self, *, command_runner: GitCommandRunner) -> None:
        self._command_runner = command_runner

    def apply(
        self,
        *,
        project_root: Path,
        integration_root: Path,
        base_commit: str,
        result_commits: tuple[str, ...],
        summary: str,
    ) -> IntegrationExecution:
        current_head = self._command_runner.require_run_ready_repository(
            project_root=project_root,
            operation="integration_apply",
        )
        if integration_root.exists():
            raise GitWorktreeError(
                operation="integration_apply",
                resource_id=str(integration_root),
                message="The integration worktree path already exists; do not reuse ambiguous state.",
                recoverable=True,
            )

        self._run_text(
            repository_root=project_root,
            args=("worktree", "add", "--detach", str(integration_root), base_commit),
        )
        try:
            tree_sha = self._prepare_result_tree(
                integration_root=integration_root,
                result_commits=result_commits,
                base_commit=base_commit,
            )
            if current_head != base_commit:
                if self._matches_existing_integration(
                    project_root=project_root,
                    current_head=current_head,
                    base_commit=base_commit,
                    tree_sha=tree_sha,
                    summary=summary,
                ):
                    return IntegrationExecution(
                        integration_commit=current_head,
                        changed_paths=self._changed_paths(
                            repository_root=project_root,
                            base_commit=base_commit,
                            integration_commit=current_head,
                        ),
                    )
                raise GitWorktreeError(
                    operation="integration_apply",
                    resource_id=str(project_root),
                    message=(
                        "The project HEAD moved after the Run base commit. "
                        "Refuse to integrate stale Worker results."
                    ),
                    recoverable=False,
                )
            integration_commit = self._run_text(
                repository_root=integration_root,
                args=("commit-tree", tree_sha, "-p", base_commit, "-m", summary),
                extra_environment={
                    "GIT_AUTHOR_NAME": _COMMIT_AUTHOR_NAME,
                    "GIT_AUTHOR_EMAIL": _COMMIT_AUTHOR_EMAIL,
                    "GIT_COMMITTER_NAME": _COMMIT_AUTHOR_NAME,
                    "GIT_COMMITTER_EMAIL": _COMMIT_AUTHOR_EMAIL,
                },
            )
            self._run_text(
                repository_root=integration_root,
                args=("update-ref", "HEAD", integration_commit, base_commit),
            )
            self._require_clean(repository_root=integration_root)

            before_apply = self._command_runner.require_run_ready_repository(
                project_root=project_root,
                operation="integration_apply",
            )
            if before_apply != base_commit:
                raise GitWorktreeError(
                    operation="integration_apply",
                    resource_id=str(project_root),
                    message="The project HEAD changed while integration was being prepared.",
                    recoverable=False,
                )
            self._run_text(
                repository_root=project_root,
                args=(
                    "-c",
                    "core.hooksPath=/dev/null",
                    "merge",
                    "--ff-only",
                    integration_commit,
                ),
            )
            applied_head = self._command_runner.head_commit(
                repository_root=project_root,
                operation="integration_apply",
            )
            if applied_head != integration_commit:
                raise GitWorktreeError(
                    operation="integration_apply",
                    resource_id=str(project_root),
                    message="The project HEAD does not match the prepared integration commit.",
                    recoverable=False,
                )
            self._command_runner.require_clean_worktree(
                project_root=project_root,
                operation="integration_apply",
            )
            changed_paths = self._changed_paths(
                repository_root=project_root,
                base_commit=base_commit,
                integration_commit=integration_commit,
            )
            return IntegrationExecution(
                integration_commit=integration_commit,
                changed_paths=changed_paths,
            )
        finally:
            self._remove_integration_worktree(
                project_root=project_root,
                integration_root=integration_root,
            )

    def _prepare_result_tree(
        self,
        *,
        integration_root: Path,
        result_commits: tuple[str, ...],
        base_commit: str,
    ) -> str:
        for commit_sha in result_commits:
            self._verify_result_parent(
                integration_root=integration_root,
                commit_sha=commit_sha,
                base_commit=base_commit,
            )
            self._run_text(
                repository_root=integration_root,
                args=(
                    "-c",
                    "core.hooksPath=/dev/null",
                    "cherry-pick",
                    "--no-commit",
                    commit_sha,
                ),
            )
        return self._run_text(
            repository_root=integration_root,
            args=("write-tree",),
        )

    def _matches_existing_integration(
        self,
        *,
        project_root: Path,
        current_head: str,
        base_commit: str,
        tree_sha: str,
        summary: str,
    ) -> bool:
        parent = self._run_text(
            repository_root=project_root,
            args=("rev-parse", f"{current_head}^"),
        )
        if parent != base_commit:
            return False
        current_tree = self._run_text(
            repository_root=project_root,
            args=("rev-parse", f"{current_head}^{{tree}}"),
        )
        if current_tree != tree_sha:
            return False
        message = self._run_text(
            repository_root=project_root,
            args=("show", "-s", "--format=%B", current_head),
        )
        return message == summary

    def _verify_result_parent(
        self,
        *,
        integration_root: Path,
        commit_sha: str,
        base_commit: str,
    ) -> None:
        parent = self._run_text(
            repository_root=integration_root,
            args=("rev-parse", f"{commit_sha}^"),
        )
        if parent != base_commit:
            raise GitWorktreeError(
                operation="integration_apply",
                resource_id=commit_sha,
                message="A Worker result commit is not a direct child of the Run base commit.",
                recoverable=False,
            )

    def _changed_paths(
        self,
        *,
        repository_root: Path,
        base_commit: str,
        integration_commit: str,
    ) -> tuple[str, ...]:
        output = self._run_bytes(
            repository_root=repository_root,
            args=("diff", "--name-only", "-z", base_commit, integration_commit),
        )
        return tuple(path.decode("utf-8") for path in output.split(b"\x00") if path)

    def _require_clean(self, *, repository_root: Path) -> None:
        status = self._run_bytes(
            repository_root=repository_root,
            args=("status", "--porcelain=v1", "-z"),
        )
        if status:
            raise GitWorktreeError(
                operation="integration_apply",
                resource_id=str(repository_root),
                message="The prepared integration worktree is not clean.",
                recoverable=False,
            )

    def _remove_integration_worktree(
        self,
        *,
        project_root: Path,
        integration_root: Path,
    ) -> None:
        if not integration_root.exists():
            return
        self._run_text(
            repository_root=project_root,
            args=("worktree", "remove", "--force", str(integration_root)),
        )

    def _run_text(
        self,
        *,
        repository_root: Path,
        args: tuple[str, ...],
        extra_environment: dict[str, str] | None = None,
    ) -> str:
        output = self._run(
            repository_root=repository_root,
            args=args,
            text=True,
            extra_environment=extra_environment,
        )
        assert isinstance(output, str)
        return output.strip()

    def _run_bytes(
        self,
        *,
        repository_root: Path,
        args: tuple[str, ...],
    ) -> bytes:
        output = self._run(
            repository_root=repository_root,
            args=args,
            text=False,
            extra_environment=None,
        )
        assert isinstance(output, bytes)
        return output

    def _run(
        self,
        *,
        repository_root: Path,
        args: tuple[str, ...],
        text: bool,
        extra_environment: dict[str, str] | None,
    ) -> str | bytes:
        environment = os.environ.copy()
        if extra_environment is not None:
            environment.update(extra_environment)
        try:
            result = subprocess.run(
                ("git", "-C", str(repository_root), *args),
                check=False,
                capture_output=True,
                text=text,
                timeout=_GIT_TIMEOUT_SECONDS,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitWorktreeError(
                operation="integration_apply",
                resource_id=str(repository_root),
                message="Git integration operation could not be executed.",
                recoverable=True,
            ) from error
        if result.returncode != 0:
            detail = _output_text(result.stderr) or _output_text(result.stdout)
            raise GitWorktreeError(
                operation="integration_apply",
                resource_id=str(repository_root),
                message=f"Git integration operation failed: {(detail or 'git command failed')[:500]}",
                recoverable=True,
            )
        return result.stdout


def _output_text(output: str | bytes) -> str:
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace").strip()
    return output.strip()
