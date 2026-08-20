from dataclasses import dataclass
from pathlib import Path

from chattocodex.orchestrator_backend.integration.git_integration_runner import (
    GitIntegrationRunner,
)
from chattocodex.orchestrator_backend.integration.integration_apply_service import (
    IntegrationApplyService,
)
from chattocodex.orchestrator_backend.integration.integration_preview_service import (
    IntegrationPreviewService,
)
from chattocodex.orchestrator_backend.persistence.broker.broker_repository import (
    BrokerTaskRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.orchestration_state_repository import (
    RunStateRepository,
    TaskStateRepository,
    WorkerStateRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.run_control_state_store import (
    RunControlStateStore,
)
from chattocodex.orchestrator_backend.persistence.version_control.git_worktree_repository import (
    GitWorktreeRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_report_repository import (
    WorkerReportRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_result_repository import (
    WorkerResultRepository,
)
from chattocodex.orchestrator_backend.report.worker_report_service import (
    WorkerReportService,
)
from chattocodex.orchestrator_backend.repository.project_path_resolver import (
    ProjectPathResolver,
)
from chattocodex.orchestrator_backend.repository.project_read_service import (
    ProjectReadService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_mutation_service import (
    WorkerDirectoryMutationService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_directory_query_service import (
    WorkerDirectoryQueryService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_file_mutation_service import (
    WorkerFileMutationService,
)
from chattocodex.orchestrator_backend.repository.worker_workspace.worker_workspace_access_service import (
    WorkerWorkspaceAccessService,
)
from chattocodex.orchestrator_backend.version_control.git_command_runner import (
    GitCommandRunner,
)
from chattocodex.orchestrator_backend.version_control.git_result_commit_runner import (
    GitResultCommitRunner,
)
from chattocodex.orchestrator_backend.version_control.worker_git_worktree_service import (
    WorkerGitWorktreeService,
)
from chattocodex.orchestrator_backend.version_control.worker_result_commit_service import (
    WorkerResultCommitService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry


@dataclass(frozen=True, slots=True)
class WorkspaceApplicationServices:
    """Group Worker workspace, result, report, and integration application services."""

    project_read_service: ProjectReadService
    worktree_service: WorkerGitWorktreeService
    file_mutation_service: WorkerFileMutationService
    directory_query_service: WorkerDirectoryQueryService
    directory_mutation_service: WorkerDirectoryMutationService
    result_commit_service: WorkerResultCommitService
    worker_report_service: WorkerReportService
    integration_preview_service: IntegrationPreviewService
    integration_apply_service: IntegrationApplyService


def build_workspace_application_services(
    execution_root: Path,
    run_repository: RunStateRepository,
    worker_repository: WorkerStateRepository,
    task_repository: TaskStateRepository,
    broker_task_repository: BrokerTaskRepository,
    run_control_state_store: RunControlStateStore,
    lock_registry: EntityLockRegistry,
    git_command_runner: GitCommandRunner,
) -> WorkspaceApplicationServices:
    """Compose services that operate on isolated Worker workspaces and results.

    Args:
        execution_root: Canonical root for durable orchestration execution state.
        run_repository: Durable Run repository shared with orchestration services.
        worker_repository: Durable Worker repository used for capability checks.
        task_repository: Durable Task repository used by report and integration services.
        broker_task_repository: Broker projection repository used for integration readiness.
        run_control_state_store: Durable workflow review state used by integration apply.
        lock_registry: Shared entity lock registry for workspace linearization.
        git_command_runner: Process boundary used for Git worktree commands.

    Returns:
        Fully composed workspace, result, report, and integration application services.
    """
    project_read_service = ProjectReadService(
        run_repository=run_repository,
        path_resolver=ProjectPathResolver(),
    )
    worktree_repository = GitWorktreeRepository(state_root=execution_root)
    worktree_service = WorkerGitWorktreeService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        worktree_repository=worktree_repository,
        command_runner=git_command_runner,
        lock_registry=lock_registry,
    )
    workspace_access_service = WorkerWorkspaceAccessService(
        worker_repository=worker_repository,
        worktree_repository=worktree_repository,
        path_resolver=ProjectPathResolver(),
    )
    file_mutation_service = WorkerFileMutationService(
        access_service=workspace_access_service,
        lock_registry=lock_registry,
    )
    directory_query_service = WorkerDirectoryQueryService(
        access_service=workspace_access_service
    )
    directory_mutation_service = WorkerDirectoryMutationService(
        access_service=workspace_access_service,
        lock_registry=lock_registry,
    )
    result_repository = WorkerResultRepository(state_root=execution_root)
    result_commit_service = WorkerResultCommitService(
        worker_repository=worker_repository,
        worktree_repository=worktree_repository,
        result_repository=result_repository,
        commit_runner=GitResultCommitRunner(),
        lock_registry=lock_registry,
    )
    worker_report_service = WorkerReportService(
        run_repository=run_repository,
        worker_repository=worker_repository,
        task_repository=task_repository,
        report_repository=WorkerReportRepository(state_root=execution_root),
        lock_registry=lock_registry,
    )
    integration_preview_service = IntegrationPreviewService(
        result_repository=result_repository
    )
    integration_apply_service = IntegrationApplyService(
        run_repository=run_repository,
        task_repository=task_repository,
        broker_task_repository=broker_task_repository,
        result_repository=result_repository,
        preview_service=integration_preview_service,
        runner=GitIntegrationRunner(command_runner=git_command_runner),
        integration_root=execution_root / "integrations",
        lock_registry=lock_registry,
        run_control_state_store=run_control_state_store,
    )
    return WorkspaceApplicationServices(
        project_read_service=project_read_service,
        worktree_service=worktree_service,
        file_mutation_service=file_mutation_service,
        directory_query_service=directory_query_service,
        directory_mutation_service=directory_mutation_service,
        result_commit_service=result_commit_service,
        worker_report_service=worker_report_service,
        integration_preview_service=integration_preview_service,
        integration_apply_service=integration_apply_service,
    )
