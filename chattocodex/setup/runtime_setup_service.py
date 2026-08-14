from pathlib import Path

from chattocodex.auth.owner_token_service import OwnerTokenService
from chattocodex.orchestration.orchestration_exceptions import ResourceNotFoundError
from chattocodex.persistence.atomic_json_file import AtomicJsonFileError
from chattocodex.persistence.runtime_config_repository import RuntimeConfigRepository
from chattocodex.schemas.auth_schemas import RuntimeConfigRecord
from chattocodex.schemas.setup_schemas import DoctorOutput, SetupOutput, SetupRequest
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


class RuntimeSetupService:
    def __init__(self, *, state_root: Path) -> None:
        self._state_root = state_root.expanduser().resolve()
        self._config_repository = RuntimeConfigRepository(state_root=self._state_root)
        self._owner_tokens = OwnerTokenService(state_root=self._state_root)

    def configure(self, *, request: SetupRequest) -> SetupOutput:
        workspace_root = Path(request.workspace_root).expanduser().resolve()
        project_service = WorkspaceProjectService(workspace_root=workspace_root)
        projects = project_service.list_projects()
        if not projects:
            raise ValueError(
                "workspace_root does not contain a recognized project. "
                "Expected a .git, pyproject.toml, package.json, Cargo.toml, go.mod, or requirements.txt marker."
            )

        public_url = str(request.public_url).rstrip("/") if request.public_url else None
        if request.oauth_enabled and public_url is None:
            raise ValueError("OAuth-enabled setup requires public_url.")

        saved = self._config_repository.save(
            record=RuntimeConfigRecord(
                workspace_root=str(workspace_root),
                public_url=public_url,
                oauth_enabled=request.oauth_enabled,
            )
        )
        owner_token = (
            self._owner_tokens.ensure_configured() if saved.oauth_enabled else None
        )
        connector_url = f"{saved.public_url}/mcp" if saved.public_url else None
        return SetupOutput(
            state_root=self._state_root,
            workspace_root=workspace_root,
            public_url=saved.public_url,
            connector_url=connector_url,
            project_count=len(projects),
            owner_token_created=owner_token is not None,
            owner_token=owner_token,
        )

    def doctor(self) -> DoctorOutput:
        issues: list[str] = []
        if not self._config_repository.exists():
            return DoctorOutput(
                ok=False,
                state_root=self._state_root,
                configured=False,
                workspace_root=None,
                public_url=None,
                connector_url=None,
                oauth_enabled=False,
                owner_token_configured=self._owner_tokens.is_configured(),
                project_count=0,
                issues=("Runtime is not configured. Run `chattocodex setup`.",),
            )

        try:
            config = self._config_repository.read()
        except AtomicJsonFileError as error:
            return DoctorOutput(
                ok=False,
                state_root=self._state_root,
                configured=False,
                workspace_root=None,
                public_url=None,
                connector_url=None,
                oauth_enabled=False,
                owner_token_configured=self._owner_tokens.is_configured(),
                project_count=0,
                issues=(str(error),),
            )

        workspace_root = Path(config.workspace_root)
        if not workspace_root.is_dir():
            issues.append(
                "Configured workspace_root does not exist or is not a directory."
            )
            project_count = 0
        else:
            try:
                project_count = len(
                    WorkspaceProjectService(
                        workspace_root=workspace_root
                    ).list_projects()
                )
            except (OSError, ResourceNotFoundError) as error:
                issues.append(f"Project discovery failed: {error}")
                project_count = 0

        if config.oauth_enabled and not self._owner_tokens.is_configured():
            issues.append("OAuth is enabled but Owner Token is not configured.")
        if config.oauth_enabled and config.public_url is None:
            issues.append("OAuth is enabled but public_url is missing.")

        connector_url = f"{config.public_url}/mcp" if config.public_url else None
        return DoctorOutput(
            ok=not issues,
            state_root=self._state_root,
            configured=True,
            workspace_root=workspace_root,
            public_url=config.public_url,
            connector_url=connector_url,
            oauth_enabled=config.oauth_enabled,
            owner_token_configured=self._owner_tokens.is_configured(),
            project_count=project_count,
            issues=tuple(issues),
        )
