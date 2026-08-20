import asyncio
from pathlib import Path

from asyncer import asyncify
from pydantic import AnyHttpUrl

from chattocodex.orchestrator_backend.control_panel.domain.control_panel_dto import (
    ControlPanelPreferences,
    ControlPanelSnapshot,
    RemoteMode,
)
from chattocodex.orchestrator_backend.control_panel.domain.control_panel_exceptions import (
    ControlPanelError,
    OwnerTokenRotationConflictError,
    RuntimeLifecycleError,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.cloudflare_process_supervisor import (
    CloudflareProcessSupervisor,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.control_panel_database_bridge import (
    ControlPanelDatabaseBridge,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.control_panel_preferences_repository import (
    ControlPanelPreferencesRepository,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.mcp_process_supervisor import (
    McpProcessSupervisor,
)
from chattocodex.orchestrator_backend.control_panel.infrastructure.worker_browser_process_supervisor import (
    WorkerBrowserProcessSupervisor,
)
from chattocodex.orchestrator_backend.database.database_config import DatabaseConfig
from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
)
from chattocodex.orchestrator_backend.setup.runtime_setup_service import (
    RuntimeSetupService,
)
from chattocodex.orchestrator_backend.wake.browser.worker_browser_runtime_dto import (
    WorkerBrowserRuntimeMode,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from chattocodex.shared.contracts.control_panel.control_panel_operations_schemas import (
    ControlPanelOperationsResponse,
)
from chattocodex.shared.contracts.control_panel.control_panel_schemas import (
    ControlPanelPreferencesRequest,
    normalize_custom_origin,
    normalize_workspace_path,
)
from chattocodex.shared.contracts.setup_schemas import SetupRequest
from chattocodex.shared.security.owner_token_service import (
    OwnerTokenRevisionConflictError,
    OwnerTokenService,
)
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)
from chattocodex.shared.storage.atomic_json_file import AtomicJsonFileError


class ControlPanelService:
    def __init__(
        self,
        state_root: Path,
        mcp_host: str = "127.0.0.1",
    ) -> None:
        """Initialize this object. Args: state_root, mcp_host. Returns: None."""
        self._state_root = state_root.expanduser().resolve()
        self._preferences_repository = ControlPanelPreferencesRepository(
            state_root=self._state_root
        )
        self._runtime_config_repository = RuntimeConfigRepository(
            state_root=self._state_root
        )
        self._setup_service = RuntimeSetupService(state_root=self._state_root)
        self._owner_tokens = OwnerTokenService(state_root=self._state_root)
        self._mcp_process = McpProcessSupervisor(
            state_root=self._state_root,
            host=mcp_host,
        )
        self._tunnel_process = CloudflareProcessSupervisor(local_host=mcp_host)
        self._worker_browser = WorkerBrowserProcessSupervisor(
            state_root=self._state_root
        )
        self._database_bridge = ControlPanelDatabaseBridge(config=DatabaseConfig())
        self._lifecycle_lock = asyncio.Lock()
        self._visible_owner_token: str | None = None

    async def project_operations_read_model(
        self,
        snapshot: ControlPanelOperationsResponse,
    ) -> ControlPanelOperationsResponse:
        """Project the Control Panel snapshot through the async database boundary.

        Args:
            snapshot: File-backed operations snapshot that remains runtime authority.

        Returns:
            PostgreSQL-projected snapshot when available, otherwise the fallback snapshot.
        """
        return await self._database_bridge.project_or_fallback(snapshot)

    async def save_preferences(
        self,
        request: ControlPanelPreferencesRequest,
    ) -> ControlPanelSnapshot:
        """Save preferences. Args: request. Returns: ControlPanelSnapshot."""
        workspace_root = normalize_workspace_path(value=request.workspace_root)
        project_count = await asyncify(_validate_workspace_projects)(
            workspace_root=workspace_root,
        )
        if project_count == 0:
            raise RuntimeLifecycleError(
                message="The selected workspace does not contain a recognized project."
            )
        custom_origin = (
            normalize_custom_origin(value=request.custom_origin)
            if request.custom_origin is not None
            else None
        )
        preferences = ControlPanelPreferences(
            workspace_root=workspace_root,
            remote_mode=RemoteMode(request.remote_mode),
            custom_origin=custom_origin,
            mcp_port=request.mcp_port,
        )
        async with self._lifecycle_lock:
            if self._mcp_process.is_running() or self._tunnel_process.is_running():
                raise RuntimeLifecycleError(
                    message="Stop MCP and the tunnel before changing saved settings."
                )
            await asyncify(self._preferences_repository.save)(
                preferences=preferences,
            )
            return await self.snapshot()

    async def start(self) -> ControlPanelSnapshot:
        """Start. Args: None. Returns: ControlPanelSnapshot."""
        async with self._lifecycle_lock:
            mcp_was_running = self._mcp_process.is_running()
            tunnel_was_running = self._tunnel_process.is_running()
            if mcp_was_running and tunnel_was_running:
                return await self.snapshot()
            preferences = await asyncify(self._require_preferences)()
            try:
                if not mcp_was_running:
                    await self._mcp_process.ensure_port_available(
                        port=preferences.mcp_port
                    )
                if preferences.remote_mode is RemoteMode.QUICK_TUNNEL:
                    if tunnel_was_running:
                        public_url = self._tunnel_process.public_url()
                        if public_url is None:
                            raise RuntimeLifecycleError(
                                message=(
                                    "The Quick Tunnel is running without a known public URL. "
                                    "Stop the runtime and start it again."
                                )
                            )
                    else:
                        public_url = await self._tunnel_process.start_quick(
                            local_port=preferences.mcp_port
                        )
                    await self._configure_runtime(
                        preferences=preferences,
                        public_url=public_url,
                    )
                    if not mcp_was_running:
                        await self._mcp_process.start(port=preferences.mcp_port)
                else:
                    if preferences.custom_origin is None:
                        raise RuntimeLifecycleError(
                            message="Custom-domain mode requires a saved HTTPS origin."
                        )
                    await self._configure_runtime(
                        preferences=preferences,
                        public_url=preferences.custom_origin,
                    )
                    if not mcp_was_running:
                        await self._mcp_process.start(port=preferences.mcp_port)
                    if not tunnel_was_running:
                        await self._tunnel_process.start_custom(
                            public_origin=preferences.custom_origin,
                            local_port=preferences.mcp_port,
                        )
            except ControlPanelError:
                await self._rollback_started_processes(
                    mcp_was_running=mcp_was_running,
                    tunnel_was_running=tunnel_was_running,
                )
                raise
            except (AtomicJsonFileError, OSError, ValueError) as error:
                await self._rollback_started_processes(
                    mcp_was_running=mcp_was_running,
                    tunnel_was_running=tunnel_was_running,
                )
                raise RuntimeLifecycleError(
                    message="Runtime setup failed. Check the saved workspace and remote settings."
                ) from error
            return await self.snapshot()

    async def stop(self) -> ControlPanelSnapshot:
        """Stop. Args: None. Returns: ControlPanelSnapshot."""
        async with self._lifecycle_lock:
            await self._stop_processes()
            return await self.snapshot()

    async def rotate_owner_token(self) -> str:
        """Rotate owner token. Args: None. Returns: str."""
        try:
            token = await asyncify(self._owner_tokens.generate)()
        except OwnerTokenRevisionConflictError as error:
            raise OwnerTokenRotationConflictError from error
        self._visible_owner_token = token
        return token

    async def start_worker_browser_setup(self) -> None:
        """Open the dedicated Worker browser profile for interactive ChatGPT setup. Args: None. Returns: None."""
        async with self._lifecycle_lock:
            await self._worker_browser.start(
                mode=WorkerBrowserRuntimeMode.VISIBLE_BOOTSTRAP
            )

    async def start_worker_browser_headless(self) -> None:
        """Start the reusable Worker browser with its persistent profile. Args: None. Returns: None."""
        async with self._lifecycle_lock:
            await self._worker_browser.start(mode=WorkerBrowserRuntimeMode.HEADLESS)

    async def stop_worker_browser(self) -> None:
        """Stop the Control Panel-managed Worker browser process. Args: None. Returns: None."""
        async with self._lifecycle_lock:
            await self._worker_browser.stop()

    def worker_browser_running(self) -> bool:
        """Worker browser running. Args: None. Returns: bool."""
        return self._worker_browser.is_running()

    def worker_browser_mode(self) -> WorkerBrowserRuntimeMode | None:
        """Worker browser mode. Args: None. Returns: WorkerBrowserRuntimeMode | None."""
        return self._worker_browser.mode()

    def worker_browser_profile_initialized(self) -> bool:
        """Worker browser profile initialized. Args: None. Returns: bool."""
        return self._worker_browser.profile_initialized()

    async def snapshot(self) -> ControlPanelSnapshot:
        """Snapshot. Args: None. Returns: ControlPanelSnapshot."""
        preferences = await asyncify(self._load_preferences)()
        project_count = 0
        if preferences is not None and preferences.workspace_root.is_dir():
            try:
                project_count = await asyncify(_validate_workspace_projects)(
                    workspace_root=preferences.workspace_root,
                )
            except OSError, ResourceNotFoundError, RuntimeLifecycleError:
                project_count = 0

        runtime_public_url = await asyncify(self._runtime_public_url)()
        if (
            preferences is not None
            and preferences.remote_mode is RemoteMode.QUICK_TUNNEL
        ):
            public_url = self._tunnel_process.public_url()
        else:
            public_url = self._tunnel_process.public_url() or runtime_public_url
        connector_url = f"{public_url}/mcp" if public_url is not None else None
        return ControlPanelSnapshot(
            configured=preferences is not None,
            preferences=preferences,
            project_count=project_count,
            owner_token_configured=await asyncify(self._owner_tokens.is_configured)(),
            visible_owner_token=self._visible_owner_token,
            public_url=public_url,
            connector_url=connector_url,
            mcp_process=self._mcp_process.snapshot(),
            tunnel_process=self._tunnel_process.snapshot(),
        )

    async def shutdown(self) -> None:
        """Release managed browser, process, and database resources.

        Args:
            None.

        Returns:
            None after managed resources have been stopped and disposed.
        """
        async with self._lifecycle_lock:
            await self._worker_browser.stop()
            await self._stop_processes()
            await self._database_bridge.close()

    async def _configure_runtime(
        self,
        preferences: ControlPanelPreferences,
        public_url: str,
    ) -> None:
        """Configure runtime. Args: preferences, public_url. Returns: None."""
        result = await asyncify(self._setup_service.configure)(
            request=SetupRequest(
                workspace_root=preferences.workspace_root,
                public_url=AnyHttpUrl(public_url),
                oauth_enabled=True,
            ),
        )
        if result.owner_token is not None:
            self._visible_owner_token = result.owner_token

    async def _stop_processes(self) -> None:
        """Stop processes. Args: None. Returns: None."""
        await self._mcp_process.stop()
        await self._tunnel_process.stop()

    async def _rollback_started_processes(
        self,
        mcp_was_running: bool,
        tunnel_was_running: bool,
    ) -> None:
        """Rollback started processes. Args: mcp_was_running, tunnel_was_running. Returns: None."""
        if not mcp_was_running:
            await self._mcp_process.stop()
        if not tunnel_was_running:
            await self._tunnel_process.stop()

    def _require_preferences(self) -> ControlPanelPreferences:
        """Require preferences. Args: None. Returns: ControlPanelPreferences."""
        preferences = self._load_preferences()
        if preferences is None:
            raise RuntimeLifecycleError(
                message="Save the workspace and remote settings before starting MCP."
            )
        return preferences

    def _load_preferences(self) -> ControlPanelPreferences | None:
        """Load preferences. Args: None. Returns: ControlPanelPreferences | None."""
        if self._preferences_repository.exists():
            return self._preferences_repository.read()
        if not self._runtime_config_repository.exists():
            return None
        config = self._runtime_config_repository.read()
        if config.public_url is None:
            return None
        remote_mode = (
            RemoteMode.QUICK_TUNNEL
            if ".trycloudflare.com" in config.public_url
            else RemoteMode.CUSTOM_DOMAIN
        )
        return ControlPanelPreferences(
            workspace_root=Path(config.workspace_root),
            remote_mode=remote_mode,
            custom_origin=(
                config.public_url if remote_mode is RemoteMode.CUSTOM_DOMAIN else None
            ),
            mcp_port=8765,
        )

    def _runtime_public_url(self) -> str | None:
        """Runtime public url. Args: None. Returns: str | None."""
        if not self._runtime_config_repository.exists():
            return None
        return self._runtime_config_repository.read().public_url


def _validate_workspace_projects(workspace_root: Path) -> int:
    """Validate workspace projects. Args: workspace_root. Returns: int."""
    if not workspace_root.is_dir():
        raise RuntimeLifecycleError(
            message=f"Workspace folder does not exist: {workspace_root}"
        )
    return len(WorkspaceProjectService(workspace_root=workspace_root).list_projects())
