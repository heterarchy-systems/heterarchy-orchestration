from datetime import UTC, datetime

from chattocodex.orchestrator_backend.orchestration.orchestration_dto import (
    CreatedRun,
    RunState,
    RunStatus,
)
from chattocodex.orchestrator_backend.orchestration.orchestration_identity import (
    digest_capability,
    new_run_handle,
    new_run_id,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
)
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


class RunOrchestrationService:
    def __init__(
        self,
        run_repository: RunStateRepository,
        workspace_project_service: WorkspaceProjectService,
        command_runner: GitCommandRunner,
    ) -> None:
        """Initialize this object. Args: run_repository, workspace_project_service, command_runner. Returns: None."""
        self._run_repository = run_repository
        self._workspace_project_service = workspace_project_service
        self._command_runner = command_runner

    def create_run(
        self,
        goal: str,
        project_id: str,
        requested_parallelism: int = 1,
    ) -> CreatedRun:
        """Create a durable active Run with its scheduler parallelism contract.

        Args:
            goal: Human-readable Run goal.
            project_id: Canonical local project identity.
            requested_parallelism: Maximum parallel READY work requested by the
                Main/backend caller. Direct legacy callers default to one.

        Returns:
            The persisted Run and its opaque capability handle.

        Raises:
            ValueError: If ``requested_parallelism`` is less than one.
            OrchestrationError: If the project or repository is not runnable.
        """
        if requested_parallelism < 1:
            raise ValueError("requested_parallelism must be at least one")
        project = self._workspace_project_service.require_project(project_id=project_id)
        self._command_runner.require_run_ready_repository(
            project_root=project.root,
            operation="run_create",
        )
        now = datetime.now(UTC)
        run_handle = new_run_handle()
        state = RunState(
            schema_version=1,
            run_id=new_run_id(),
            goal=goal,
            project_id=project.project_id,
            project_name=project.name,
            project_root=str(project.root),
            run_handle_digest=digest_capability(run_handle),
            status=RunStatus.ACTIVE,
            revision=0,
            created_at=now,
            updated_at=now,
            requested_parallelism=requested_parallelism,
        )
        persisted_state = self._run_repository.create(state=state)
        return CreatedRun(state=persisted_state, run_handle=run_handle)
