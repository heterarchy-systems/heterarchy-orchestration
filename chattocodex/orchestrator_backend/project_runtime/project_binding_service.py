"""Application service for canonical local project bindings."""

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from chattocodex.orchestrator_backend.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    RevisionConflictError,
)
from chattocodex.orchestrator_backend.persistence.project.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_dto import (
    ProjectBindingAdapterMetadataState,
    ProjectBindingState,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_exceptions import (
    ProjectBindingConfigurationError,
    ProjectBindingConflictError,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_mapper import (
    metadata_state_from_schema,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_dto import (
    WorkspaceProject,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.shared.contracts.project.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)


class ProjectBindingService:
    """Own canonical project binding validation, idempotency, and lifecycle."""

    def __init__(
        self,
        repository: ProjectBindingRepository,
        workspace_project_service: WorkspaceProjectService | None = None,
        runtime_config_repository: RuntimeConfigRepository | None = None,
        lock_registry: EntityLockRegistry | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize the project binding application service.

        Args:
            repository: Durable project binding repository.
            workspace_project_service: Optional fixed project catalog for tests or embedded use.
            runtime_config_repository: Optional repository used to resolve the configured catalog.
            lock_registry: Optional in-process coordination registry.
            now_provider: Optional timezone-aware clock used for deterministic tests.

        Raises:
            ValueError: If neither a fixed catalog nor runtime configuration is supplied. Returns: None."""
        if workspace_project_service is None and runtime_config_repository is None:
            raise ValueError(
                "ProjectBindingService requires a workspace project service or runtime config."
            )
        self._repository = repository
        self._workspace_project_service = workspace_project_service
        self._runtime_config_repository = runtime_config_repository
        self._lock_registry = lock_registry or EntityLockRegistry()
        self._now_provider = now_provider or _utc_now

    def upsert_binding(
        self,
        project_id: str,
        metadata: ProjectBindingAdapterMetadata | None = None,
        expected_revision: int | None = None,
    ) -> ProjectBindingState:
        """Create or update one local project's adapter observation.

        The local project catalog supplies both the canonical project ID and root. The
        browser cannot submit or replace either value. Existing stable adapter provider
        and project-reference values cannot be changed, while labels and observed paths
        may be refreshed as routing metadata. Observation timestamps are diagnostic only:
        an otherwise identical re-registration is a no-op so it cannot invalidate an
        in-flight provisioning callback by advancing the binding revision.

        Args:
            project_id: Canonical local project identifier selected by the Control Panel.
            metadata: Optional browser-observed provider metadata.
            expected_revision: Optional revision for a compare-and-swap update.

        Returns:
            The newly created or idempotently updated binding state.

        Raises:
            ProjectBindingConfigurationError: If runtime project configuration is missing.
            ProjectBindingConflictError: If canonical state or stable metadata conflicts.
            RevisionConflictError: If ``expected_revision`` is stale.
        """
        project = self._require_project(project_id=project_id)
        incoming_metadata = metadata_state_from_schema(metadata)
        with self._lock_registry.hold(entity_key=f"project-binding:{project_id}"):
            try:
                current = self._repository.read(project_id=project.project_id)
            except ResourceNotFoundError as error:
                if expected_revision is not None:
                    raise ProjectBindingConflictError(
                        project_id=project.project_id,
                        message=(
                            "A new project binding cannot use an expected revision before "
                            "the first binding is created."
                        ),
                    ) from error
                now = self._now_provider()
                return self._repository.create(
                    state=ProjectBindingState(
                        schema_version=1,
                        project_id=project.project_id,
                        project_name=project.name,
                        project_root=project.root,
                        adapter_metadata=incoming_metadata,
                        connector_epoch=0,
                        revision=0,
                        created_at=now,
                        updated_at=now,
                    )
                )

            self._require_canonical_match(project=project, state=current)
            if expected_revision is not None and expected_revision != current.revision:
                raise RevisionConflictError(
                    operation="project_binding_upsert",
                    resource_id=project.project_id,
                    current_revision=current.revision,
                )
            next_metadata = self._merge_metadata(
                current=current.adapter_metadata,
                incoming=incoming_metadata,
            )
            self._require_metadata_update_allowed(
                project_id=project.project_id,
                current=current.adapter_metadata,
                incoming=next_metadata,
            )
            if _routing_metadata_equal(
                current=current.adapter_metadata,
                incoming=next_metadata,
            ):
                return current
            updated = replace(
                current,
                adapter_metadata=next_metadata,
                connector_epoch=(
                    current.connector_epoch + 1
                    if _connector_identity_changed(
                        current=current.adapter_metadata,
                        incoming=next_metadata,
                    )
                    else current.connector_epoch
                ),
                revision=current.revision + 1,
                updated_at=self._now_provider(),
            )
            return self._repository.write(
                state=updated,
                expected_revision=current.revision,
            )

    def get_binding(self, project_id: str) -> ProjectBindingState:
        """Read one binding after revalidating the current local project catalog.

        Args:
            project_id: Canonical local project identifier.

        Returns:
            The validated durable binding state.

        Raises:
            ProjectBindingConfigurationError: If runtime project configuration is missing.
            ProjectBindingConflictError: If persisted root no longer matches the catalog.
            ResourceNotFoundError: If the project or binding does not exist.
        """
        project = self._require_project(project_id=project_id)
        state = self._repository.read(project_id=project.project_id)
        self._require_canonical_match(project=project, state=state)
        return state

    def require_connector_binding(self, project_id: str) -> ProjectBindingState:
        """Return a binding only when it has a complete stable connector identity.

        Args:
            project_id: Canonical local project identifier selected for Main registration.

        Returns:
            The validated binding carrying the provider project and plugin app identity.

        Raises:
            ProjectBindingConfigurationError: If no binding exists or its connector
                metadata lacks a stable ChatGPT plugin reference.
            ProjectBindingConflictError: If persisted canonical project state conflicts.
        """
        try:
            state = self.get_binding(project_id=project_id)
        except ResourceNotFoundError as error:
            raise ProjectBindingConfigurationError(
                message=(
                    "Bind the Main Chat to a selected connector with a stable "
                    "plugin app_ref before registering it."
                )
            ) from error
        metadata = state.adapter_metadata
        if (
            metadata is None
            or metadata.provider != "chatgpt"
            or metadata.project_ref is None
            or metadata.project_path is None
            or metadata.app_ref is None
            or metadata.app_label is None
        ):
            raise ProjectBindingConfigurationError(
                message=(
                    "Bind the Main Chat to a selected connector with a stable "
                    "plugin app_ref before registering it."
                )
            )
        return state

    def resolve_connector_epoch(self, project_id: str) -> int:
        """Resolve the current connector generation for wake compatibility.

        A known local project without a binding remains in the legacy generation zero.
        Provisioning still requires a complete binding; this compatibility resolver only
        lets pre-binding wake records remain readable until a connector identity is bound.
        Unknown projects continue to fail closed through the canonical project catalog. Args: project_id. Returns: int."""
        project = self._require_project(project_id=project_id)
        try:
            state = self._repository.read(project_id=project.project_id)
        except ResourceNotFoundError:
            return 0
        self._require_canonical_match(project=project, state=state)
        return state.connector_epoch

    def list_bindings(self) -> tuple[ProjectBindingState, ...]:
        """List bindings belonging to currently discovered local projects.

        Returns:
            Valid durable bindings sorted by project identifier.

        Raises:
            ProjectBindingConfigurationError: If runtime project configuration is missing.
            ProjectBindingConflictError: If a persisted binding has a changed canonical root. Args: None."""
        projects = {
            project.project_id: project
            for project in self._workspace_service().list_projects()
        }
        bindings: list[ProjectBindingState] = []
        for state in self._repository.list_all():
            project = projects.get(state.project_id)
            if project is None:
                continue
            self._require_canonical_match(project=project, state=state)
            bindings.append(state)
        return tuple(bindings)

    def _workspace_service(self) -> WorkspaceProjectService:
        """Workspace service. Args: None. Returns: WorkspaceProjectService."""
        if self._workspace_project_service is not None:
            return self._workspace_project_service
        assert self._runtime_config_repository is not None
        if not self._runtime_config_repository.exists():
            raise ProjectBindingConfigurationError(
                message=(
                    "ChatToCodex runtime is not configured. Save the workspace in the "
                    "Control Panel before creating a project binding."
                )
            )
        config = self._runtime_config_repository.read()
        return WorkspaceProjectService(workspace_root=Path(config.workspace_root))

    def _require_project(self, project_id: str) -> WorkspaceProject:
        """Require project. Args: project_id. Returns: WorkspaceProject."""
        return self._workspace_service().require_project(project_id=project_id)

    @staticmethod
    def _require_canonical_match(
        project: WorkspaceProject, state: ProjectBindingState
    ) -> None:
        """Require canonical match. Args: project, state. Returns: None."""
        if (
            state.project_id != project.project_id
            or state.project_root.resolve() != project.root.resolve()
            or state.project_name != project.name
        ):
            raise ProjectBindingConflictError(
                project_id=project.project_id,
                message=(
                    "Persisted project binding does not match the current canonical local "
                    "project identity or root. Reconcile the binding before retrying."
                ),
            )

    @staticmethod
    def _require_metadata_update_allowed(
        project_id: str,
        current: ProjectBindingAdapterMetadataState | None,
        incoming: ProjectBindingAdapterMetadataState | None,
    ) -> None:
        """Require metadata update allowed. Args: project_id, current, incoming. Returns: None."""
        if current is None or incoming is None:
            return
        for field_name in ("provider", "project_ref"):
            current_value = getattr(current, field_name)
            incoming_value = getattr(incoming, field_name)
            if (
                current_value is not None
                and incoming_value is not None
                and current_value != incoming_value
            ):
                raise ProjectBindingConflictError(
                    project_id=project_id,
                    message=(
                        f"Adapter metadata field {field_name!r} cannot be rebound after "
                        "the local project binding is established."
                    ),
                )

    @staticmethod
    def _merge_metadata(
        current: ProjectBindingAdapterMetadataState | None,
        incoming: ProjectBindingAdapterMetadataState | None,
    ) -> ProjectBindingAdapterMetadataState | None:
        """Merge an adapter observation without treating ``None`` as deletion. Args: current, incoming. Returns: ProjectBindingAdapterMetadataState | None."""
        if current is None:
            return incoming
        if incoming is None:
            return current
        return ProjectBindingAdapterMetadataState(
            provider=(
                current.provider if incoming.provider is None else incoming.provider
            ),
            project_ref=(
                current.project_ref
                if incoming.project_ref is None
                else incoming.project_ref
            ),
            project_label=(
                current.project_label
                if incoming.project_label is None
                else incoming.project_label
            ),
            project_path=(
                current.project_path
                if incoming.project_path is None
                else incoming.project_path
            ),
            app_ref=(current.app_ref if incoming.app_ref is None else incoming.app_ref),
            app_label=(
                current.app_label if incoming.app_label is None else incoming.app_label
            ),
            observed_at=(
                current.observed_at
                if incoming.observed_at is None
                else incoming.observed_at
            ),
        )


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC timestamp. Args: None. Returns: datetime."""
    return datetime.now(UTC)


def _routing_metadata_equal(
    current: ProjectBindingAdapterMetadataState | None,
    incoming: ProjectBindingAdapterMetadataState | None,
) -> bool:
    """Compare only metadata that changes provider routing semantics.

    ``observed_at`` records when the browser saw a project page; it does not change
    which provider project is bound or how that project is opened. Keeping it out of
    the routing revision prevents harmless Main heartbeat/re-registration traffic from
    making an already admitted Worker callback stale. Args: current, incoming. Returns: bool."""
    if current is None or incoming is None:
        return current is incoming
    return all(
        getattr(current, field_name) == getattr(incoming, field_name)
        for field_name in (
            "provider",
            "project_ref",
            "project_label",
            "project_path",
            "app_ref",
            "app_label",
        )
    )


def _connector_identity_changed(
    current: ProjectBindingAdapterMetadataState | None,
    incoming: ProjectBindingAdapterMetadataState | None,
) -> bool:
    """Return whether the selected connector application identity changed. Args: current, incoming. Returns: bool."""
    current_ref = None if current is None else current.app_ref
    incoming_ref = None if incoming is None else incoming.app_ref
    return current_ref != incoming_ref
