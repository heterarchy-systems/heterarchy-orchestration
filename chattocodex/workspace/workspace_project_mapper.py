from chattocodex.schemas.project_schemas import ProjectView
from chattocodex.workspace.workspace_project_dto import WorkspaceProject


def build_project_view(*, project: WorkspaceProject) -> ProjectView:
    return ProjectView(
        project_id=project.project_id,
        name=project.name,
        root=str(project.root),
    )
