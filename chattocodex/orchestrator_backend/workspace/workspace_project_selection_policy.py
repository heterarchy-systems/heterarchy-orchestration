from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


def resolve_workspace_project_id(
    workspace_project_service: WorkspaceProjectService,
    operation: str,
    multiple_projects_message: str,
    project_id: str | None = None,
    preferred_project_ids: tuple[str, ...] = (),
) -> str:
    """Resolve one canonical workspace project with optional validated preference. Args: workspace_project_service, operation, multiple_projects_message, project_id, preferred_project_ids. Returns: str."""

    projects = workspace_project_service.list_projects()
    projects_by_id = {project.project_id: project for project in projects}
    if project_id is not None:
        project = projects_by_id.get(project_id)
        if project is None:
            raise ResourceNotFoundError(
                operation="require_workspace_project",
                resource_id=project_id,
            )
        return project.project_id

    if not projects:
        raise InvalidTransitionError(
            operation=operation,
            resource_id="workspace",
            message="No project is available in the configured workspace.",
        )

    valid_preferred_ids = tuple(
        candidate for candidate in preferred_project_ids if candidate in projects_by_id
    )
    if len(valid_preferred_ids) == 1:
        return valid_preferred_ids[0]
    if len(projects) == 1:
        return projects[0].project_id
    raise InvalidTransitionError(
        operation=operation,
        resource_id="workspace",
        message=multiple_projects_message,
    )
