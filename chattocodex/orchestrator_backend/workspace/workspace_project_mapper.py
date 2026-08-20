from chattocodex.orchestrator_backend.workspace.workspace_project_dto import (
    WorkspaceProject,
)
from chattocodex.shared.contracts.project.project_schemas import ProjectView


def build_project_view(project: WorkspaceProject) -> ProjectView:
    """Build project view. Args: project. Returns: ProjectView."""
    return ProjectView(
        project_id=project.project_id,
        name=project.name,
        root=str(project.root),
    )
