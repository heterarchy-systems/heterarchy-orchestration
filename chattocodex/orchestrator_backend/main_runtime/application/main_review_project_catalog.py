from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    InvalidTransitionError,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)
from chattocodex.shared.storage.atomic_json_file import AtomicJsonFileError


class MainReviewProjectCatalog:
    def __init__(self, runtime_config_repository: RuntimeConfigRepository) -> None:
        """Initialize this object. Args: runtime_config_repository. Returns: None."""
        self._runtime_config_repository = runtime_config_repository

    def require_project_id(self, project_id: str) -> str:
        """Require project id. Args: project_id. Returns: str."""
        return (
            self._workspace_service().require_project(project_id=project_id).project_id
        )

    def _workspace_service(self) -> WorkspaceProjectService:
        """Workspace service. Args: None. Returns: WorkspaceProjectService."""
        if not self._runtime_config_repository.exists():
            raise InvalidTransitionError(
                operation="main_review_target_register",
                resource_id="runtime-config",
                message=(
                    "ChatToCodex runtime is not configured. Save the workspace in the Control "
                    "Panel before registering a Main review Chat."
                ),
            )
        try:
            config = self._runtime_config_repository.read()
        except AtomicJsonFileError as error:
            raise InvalidTransitionError(
                operation="main_review_target_register",
                resource_id="runtime-config",
                message=(
                    "ChatToCodex runtime configuration cannot be read. Save the Control Panel "
                    "settings again before registering a Main review Chat."
                ),
            ) from error
        workspace_root = Path(config.workspace_root).expanduser().resolve()
        if not workspace_root.is_dir():
            raise InvalidTransitionError(
                operation="main_review_target_register",
                resource_id="runtime-config",
                message=(
                    f"Configured workspace does not exist: {workspace_root}. Update the Control "
                    "Panel workspace before registering a Main review Chat."
                ),
            )
        return WorkspaceProjectService(workspace_root=workspace_root)
