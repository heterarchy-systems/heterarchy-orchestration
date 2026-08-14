import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest
from pydantic import ValidationError

from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestration.orchestration_exceptions import (
    ResourceNotFoundError,
    RevisionConflictError,
)
from chattocodex.persistence.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.project_runtime.project_binding_dto import ProjectBindingState
from chattocodex.project_runtime.project_binding_exceptions import (
    ProjectBindingConfigurationError,
    ProjectBindingConflictError,
)
from chattocodex.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.schemas.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)
from chattocodex.workspace.workspace_project_service import WorkspaceProjectService


def _write_project(*, workspace_root: Path, name: str) -> Path:
    project_root = workspace_root / name
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        f"[project]\nname = '{name}'\n", encoding="utf-8"
    )
    return project_root


def _service(*, workspace_root: Path, state_root: Path) -> ProjectBindingService:
    return ProjectBindingService(
        repository=ProjectBindingRepository(state_root=state_root),
        workspace_project_service=WorkspaceProjectService(
            workspace_root=workspace_root
        ),
        lock_registry=EntityLockRegistry(),
    )


def test_upsert_derives_canonical_root_and_is_idempotent(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = _write_project(workspace_root=workspace_root, name="alpha")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    metadata = ProjectBindingAdapterMetadata(
        provider="chatgpt",
        project_ref="gpt-project-alpha",
        project_label="Alpha",
        project_path="/g/gpt-project-alpha",
    )

    first = service.upsert_binding(project_id=project.project_id, metadata=metadata)
    repeated = service.upsert_binding(project_id=project.project_id, metadata=metadata)

    assert first == repeated
    assert first.project_id == project.project_id
    assert first.project_root == project_root.resolve()
    assert first.revision == 0
    assert first.adapter_metadata is not None
    assert first.adapter_metadata.provider == metadata.provider
    assert first.adapter_metadata.project_ref == metadata.project_ref


def test_connector_epoch_defaults_to_zero_only_for_known_unbound_project(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]

    assert service.resolve_connector_epoch(project_id=project.project_id) == 0
    with pytest.raises(ResourceNotFoundError):
        service.resolve_connector_epoch(project_id="prj_unknown_deadbeef")


def test_metadata_can_update_without_rebinding_canonical_project(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = _write_project(workspace_root=workspace_root, name="alpha")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]

    first = service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-alpha",
            project_path="/g/gpt-project-alpha",
            project_label="Old label",
        ),
    )
    updated = service.upsert_binding(
        project_id=project.project_id,
        expected_revision=first.revision,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-alpha",
            project_label="New label",
            project_path="/g/gpt-project-alpha",
        ),
    )

    assert updated.project_root == project_root.resolve()
    assert updated.revision == first.revision + 1
    assert updated.adapter_metadata is not None
    assert updated.adapter_metadata.project_label == "New label"


def test_observation_only_reregistration_does_not_advance_routing_revision(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    first_observation = datetime(2026, 8, 12, tzinfo=UTC)
    metadata = {
        "provider": "chatgpt",
        "project_ref": "gpt-project-alpha",
        "project_label": "Alpha",
        "project_path": "/g/gpt-project-alpha",
    }
    first = service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            **metadata,
            observed_at=first_observation,
        ),
    )

    repeated = service.upsert_binding(
        project_id=project.project_id,
        expected_revision=first.revision,
        metadata=ProjectBindingAdapterMetadata(
            **metadata,
            observed_at=first_observation + timedelta(minutes=5),
        ),
    )

    assert repeated == first
    assert repeated.revision == first.revision
    assert repeated.adapter_metadata is not None
    assert repeated.adapter_metadata.observed_at == first_observation


@pytest.mark.parametrize(
    ("project_ref", "project_path"),
    [
        ("project-alpha", None),
        (None, "/g/project-alpha"),
        ("project-alpha", "/g/different"),
        ("project-alpha", "/g/project-alpha/extra"),
        ("project-alpha", "/g/project-alpha?query=1"),
        ("project-alpha", "/g/project-alpha#fragment"),
    ],
)
def test_chatgpt_metadata_requires_exact_complete_project_identity(
    project_ref: str | None,
    project_path: str | None,
) -> None:
    with pytest.raises(ValidationError, match="chatgpt"):
        ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref=project_ref,
            project_path=project_path,
        )


@pytest.mark.parametrize(
    ("app_ref", "app_label"),
    [
        ("plugin_asdk_app_alpha", None),
        ("not-a-plugin-reference", "my-local-mcp"),
    ],
)
def test_chatgpt_app_routing_identity_is_complete_and_provider_observed(
    app_ref: str | None,
    app_label: str | None,
) -> None:
    with pytest.raises(ValidationError, match=r"app|plugin"):
        ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="project-alpha",
            project_path="/g/project-alpha",
            app_ref=app_ref,
            app_label=app_label,
        )


def test_chatgpt_app_display_name_is_not_fixed_to_one_name() -> None:
    metadata = ProjectBindingAdapterMetadata(
        provider="chatgpt",
        project_ref="project-alpha",
        project_path="/g/project-alpha",
        app_ref="plugin_asdk_app_alpha",
        app_label="my-local-mcp",
    )

    assert metadata.app_ref == "plugin_asdk_app_alpha"
    assert metadata.app_label == "my-local-mcp"

    with pytest.raises(ValidationError, match="both app_ref and app_label"):
        ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="project-alpha",
            project_path="/g/project-alpha",
            app_label="another-mcp-name",
        )


def test_metadata_identity_change_is_rejected(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-alpha",
            project_path="/g/gpt-project-alpha",
        ),
    )

    with pytest.raises(ProjectBindingConflictError):
        service.upsert_binding(
            project_id=project.project_id,
            metadata=ProjectBindingAdapterMetadata(
                provider="chatgpt",
                project_ref="gpt-project-other",
                project_path="/g/gpt-project-other",
            ),
        )


def test_adapter_provider_project_reference_is_unique_across_local_projects(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    _write_project(workspace_root=workspace_root, name="beta")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    alpha, beta = WorkspaceProjectService(workspace_root=workspace_root).list_projects()
    shared_identity = ProjectBindingAdapterMetadata(
        provider="chatgpt",
        project_ref="gpt-project-shared",
        project_path="/g/gpt-project-shared",
    )
    service.upsert_binding(project_id=alpha.project_id, metadata=shared_identity)
    beta_binding = service.upsert_binding(project_id=beta.project_id)

    with pytest.raises(ProjectBindingConflictError, match="already bound"):
        service.upsert_binding(
            project_id=beta.project_id,
            expected_revision=beta_binding.revision,
            metadata=shared_identity,
        )

    assert service.get_binding(project_id=beta.project_id) == beta_binding


def test_partial_metadata_update_preserves_unspecified_values(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    first = service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-alpha",
            project_label="Old label",
            project_path="/g/gpt-project-alpha",
        ),
    )

    updated = service.upsert_binding(
        project_id=project.project_id,
        expected_revision=first.revision,
        metadata=ProjectBindingAdapterMetadata(project_label="New label"),
    )

    assert updated.adapter_metadata is not None
    assert updated.adapter_metadata.provider == "chatgpt"
    assert updated.adapter_metadata.project_ref == "gpt-project-alpha"
    assert updated.adapter_metadata.project_label == "New label"
    assert updated.adapter_metadata.project_path == "/g/gpt-project-alpha"


def test_metadata_identity_cannot_be_cleared_then_rebound(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    first = service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-alpha",
            project_path="/g/gpt-project-alpha",
        ),
    )

    cleared = service.upsert_binding(
        project_id=project.project_id,
        expected_revision=first.revision,
        metadata=ProjectBindingAdapterMetadata(
            provider=None,
            project_ref=None,
            project_label="Refreshed label",
        ),
    )

    assert cleared.adapter_metadata is not None
    assert cleared.adapter_metadata.provider == "chatgpt"
    assert cleared.adapter_metadata.project_ref == "gpt-project-alpha"
    with pytest.raises(ProjectBindingConflictError):
        service.upsert_binding(
            project_id=project.project_id,
            expected_revision=cleared.revision,
            metadata=ProjectBindingAdapterMetadata(
                provider="other-adapter", project_ref="other-project"
            ),
        )


def test_expected_revision_and_project_isolation_are_enforced(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    _write_project(workspace_root=workspace_root, name="beta")
    service = _service(workspace_root=workspace_root, state_root=tmp_path / "state")
    projects = WorkspaceProjectService(workspace_root=workspace_root).list_projects()
    alpha, beta = projects
    first = service.upsert_binding(project_id=alpha.project_id)

    with pytest.raises(RevisionConflictError):
        service.upsert_binding(
            project_id=alpha.project_id,
            expected_revision=first.revision + 1,
            metadata=ProjectBindingAdapterMetadata(project_label="stale"),
        )
    with pytest.raises(ResourceNotFoundError):
        service.get_binding(project_id=beta.project_id)

    assert (
        service.get_binding(project_id=alpha.project_id).project_id == alpha.project_id
    )


def test_persisted_root_mismatch_is_rejected_before_any_update(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    state_root = tmp_path / "state"
    service = _service(workspace_root=workspace_root, state_root=state_root)
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    first = service.upsert_binding(project_id=project.project_id)
    tampered = replace(
        first,
        project_root=tmp_path / "forged-project-root",
        revision=first.revision + 1,
    )
    ProjectBindingRepository(state_root=state_root).write(
        state=tampered,
        expected_revision=first.revision,
    )

    with pytest.raises(ProjectBindingConflictError):
        service.get_binding(project_id=project.project_id)


def test_binding_persists_across_service_restart(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = _write_project(workspace_root=workspace_root, name="alpha")
    state_root = tmp_path / "state"
    project_service = WorkspaceProjectService(workspace_root=workspace_root)
    project = project_service.list_projects()[0]
    first_service = _service(workspace_root=workspace_root, state_root=state_root)
    first = first_service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-alpha",
            project_path="/g/gpt-project-alpha",
        ),
    )

    restarted = _service(workspace_root=workspace_root, state_root=state_root)
    recovered = restarted.get_binding(project_id=project.project_id)

    assert recovered.project_id == first.project_id
    assert recovered.project_root == project_root.resolve()
    assert recovered.adapter_metadata == first.adapter_metadata
    assert recovered.revision == first.revision


def test_legacy_label_only_record_is_unbound_until_stable_rebind(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    project_root = _write_project(workspace_root=workspace_root, name="alpha")
    state_root = tmp_path / "state"
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    record_path = state_root / "project_bindings" / f"{project.project_id}.json"
    record_path.parent.mkdir(parents=True)
    timestamp = datetime(2026, 8, 12, tzinfo=UTC)
    record_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "project_id": project.project_id,
                "project_name": project.name,
                "project_root": str(project_root.resolve()),
                "adapter_metadata": {
                    "provider": "chatgpt",
                    "project_ref": "gpt-project-alpha",
                    "project_label": "Alpha",
                    "project_path": "/g/gpt-project-alpha",
                    "app_ref": None,
                    "app_label": "ChatOrchestration",
                    "observed_at": timestamp.isoformat(),
                },
                "connector_epoch": 4,
                "revision": 11,
                "created_at": timestamp.isoformat(),
                "updated_at": timestamp.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    service = _service(workspace_root=workspace_root, state_root=state_root)

    degraded = service.get_binding(project_id=project.project_id)

    assert degraded.adapter_metadata is not None
    assert degraded.adapter_metadata.app_ref is None
    assert degraded.adapter_metadata.app_label is None
    with pytest.raises(ProjectBindingConfigurationError, match="stable plugin app_ref"):
        service.require_connector_binding(project_id=project.project_id)

    repaired = service.upsert_binding(
        project_id=project.project_id,
        expected_revision=degraded.revision,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="gpt-project-alpha",
            project_label="Alpha",
            project_path="/g/gpt-project-alpha",
            app_ref="plugin_chat_orchestration",
            app_label="ChatOrchestration",
        ),
    )

    assert repaired.revision == degraded.revision + 1
    assert repaired.connector_epoch == degraded.connector_epoch + 1
    assert repaired.adapter_metadata is not None
    assert repaired.adapter_metadata.app_ref == "plugin_chat_orchestration"
    assert repaired.adapter_metadata.app_label == "ChatOrchestration"
    assert service.require_connector_binding(project_id=project.project_id) == repaired
    persisted = ProjectBindingRepository(state_root=state_root).read(
        project_id=project.project_id
    )
    assert persisted == repaired


def test_concurrent_repository_instances_allow_exactly_one_cas_writer(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace"
    _write_project(workspace_root=workspace_root, name="alpha")
    state_root = tmp_path / "state"
    project = WorkspaceProjectService(workspace_root=workspace_root).list_projects()[0]
    initial_service = _service(workspace_root=workspace_root, state_root=state_root)
    initial = initial_service.upsert_binding(project_id=project.project_id)
    services = (
        _service(workspace_root=workspace_root, state_root=state_root),
        _service(workspace_root=workspace_root, state_root=state_root),
    )
    start = Barrier(2)

    def update(
        service: ProjectBindingService, label: str
    ) -> ProjectBindingState | RevisionConflictError:
        start.wait()
        try:
            return service.upsert_binding(
                project_id=project.project_id,
                expected_revision=initial.revision,
                metadata=ProjectBindingAdapterMetadata(project_label=label),
            )
        except RevisionConflictError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(update, services, ("writer-one", "writer-two")))

    successes = tuple(
        result for result in results if not isinstance(result, RevisionConflictError)
    )
    conflicts = tuple(
        result for result in results if isinstance(result, RevisionConflictError)
    )
    assert len(successes) == 1
    assert len(conflicts) == 1
    winner = successes[0]
    assert isinstance(winner, ProjectBindingState)
    final = ProjectBindingRepository(state_root=state_root).read(
        project_id=project.project_id
    )
    assert final == winner
    assert final.revision == initial.revision + 1
