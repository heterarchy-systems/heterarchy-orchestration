from pathlib import Path

from starlette.testclient import TestClient

from chattocodex.control_panel.control_panel_routes import create_control_panel_app
from chattocodex.control_panel.control_panel_service import ControlPanelService
from chattocodex.main_runtime.main_review_chat_presence_tracker import (
    MainReviewChatPresenceTracker,
)
from chattocodex.main_runtime.main_review_project_catalog import (
    MainReviewProjectCatalog,
)
from chattocodex.main_runtime.main_review_route_adapter import MainReviewRouteAdapter
from chattocodex.main_runtime.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.persistence.main_review_chat_presence_store import (
    MainReviewChatPresenceStore,
)
from chattocodex.persistence.main_review_target_store import MainReviewTargetStore
from chattocodex.persistence.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.persistence.run_control_state_store import RunControlStateStore
from chattocodex.persistence.runtime_config_repository import RuntimeConfigRepository
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.persistence.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.schemas.auth_schemas import RuntimeConfigRecord
from chattocodex.wake.wake_control_service import WakeControlService
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService
from chattocodex.worker_runtime.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    project = workspace / "sample"
    project.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        "[project]\nname='sample'\n", encoding="utf-8"
    )
    return workspace


def _wake_service(*, state_root: Path) -> WakeControlService:
    lock_registry = EntityLockRegistry()
    presence_service = WakePresenceService(
        repository=WakePresenceRepository(state_root=state_root)
    )
    runtime_presence_tracker = WorkerRuntimePresenceTracker(
        store=WorkerRuntimePresenceStore(state_root=state_root)
    )
    return WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(state_root=state_root),
        wake_target_service=WakeTargetService(
            repository=WakeTargetRepository(state_root=state_root),
            presence_service=presence_service,
            runtime_presence_tracker=runtime_presence_tracker,
            lock_registry=lock_registry,
        ),
        wake_presence_service=presence_service,
        runtime_presence_tracker=runtime_presence_tracker,
        lock_registry=lock_registry,
    )


def _main_review_adapter(
    *, state_root: Path, project_binding_service: ProjectBindingService
) -> MainReviewRouteAdapter:
    return MainReviewRouteAdapter(
        target_coordinator=MainReviewTargetCoordinator(
            target_store=MainReviewTargetStore(state_root=state_root),
            run_control_state_store=RunControlStateStore(state_root=state_root),
            chat_presence_tracker=MainReviewChatPresenceTracker(
                store=MainReviewChatPresenceStore(state_root=state_root)
            ),
            lock_registry=EntityLockRegistry(),
        ),
        project_catalog=MainReviewProjectCatalog(
            runtime_config_repository=RuntimeConfigRepository(state_root=state_root)
        ),
        project_binding_service=project_binding_service,
    )


def _app(*, workspace: Path, state_root: Path):
    RuntimeConfigRepository(state_root=state_root).save(
        record=RuntimeConfigRecord(
            workspace_root=str(workspace.resolve()),
            public_url=None,
            oauth_enabled=False,
        )
    )
    project_binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=workspace / ".chattocodex"),
        runtime_config_repository=RuntimeConfigRepository(state_root=state_root),
        lock_registry=EntityLockRegistry(),
    )
    return create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        wake_service=_wake_service(state_root=state_root),
        main_review_adapter=_main_review_adapter(
            state_root=state_root,
            project_binding_service=project_binding_service,
        ),
        project_binding_service=project_binding_service,
    )


def test_binding_put_uses_server_canonical_root_and_opaque_response(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    app = _app(workspace=workspace, state_root=state_root)

    with TestClient(app) as client:
        project = client.get("/api/wake/projects").json()["projects"][0]
        response = client.put(
            f"/api/project-bindings/{project['project_id']}",
            json={
                "adapter_metadata": {
                    "provider": "chatgpt",
                    "project_ref": "gpt-alpha",
                    "project_path": "/g/gpt-alpha",
                }
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["binding"]["project_id"] == project["project_id"]
        assert payload["binding"]["project_root"] == str(
            (workspace / "sample").resolve()
        )
        serialized = str(payload).lower()
        for forbidden in (
            "task",
            "role",
            "objective",
            "run",
            "dag",
            "capability",
        ):
            assert forbidden not in serialized


def test_binding_put_rejects_browser_canonical_root_field(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    app = _app(workspace=workspace, state_root=state_root)

    with TestClient(app) as client:
        project = client.get("/api/wake/projects").json()["projects"][0]
        response = client.put(
            f"/api/project-bindings/{project['project_id']}",
            json={"project_root": "/browser/forged/root"},
        )

        assert response.status_code == 422
        assert not (workspace / ".chattocodex" / "project_bindings").exists()


def test_binding_get_is_persistent_and_missing_project_isolated(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    app = _app(workspace=workspace, state_root=state_root)

    with TestClient(app) as client:
        project = client.get("/api/wake/projects").json()["projects"][0]
        put = client.put(
            f"/api/project-bindings/{project['project_id']}",
            json={
                "adapter_metadata": {
                    "provider": "chatgpt",
                    "project_ref": "gpt-alpha",
                    "project_path": "/g/gpt-alpha",
                }
            },
        )
        assert put.status_code == 200
        fetched = client.get(f"/api/project-bindings/{project['project_id']}")
        assert fetched.status_code == 200
        assert fetched.json()["binding"] == put.json()["binding"]

        unknown = client.get("/api/project-bindings/prj_missing_00000000")
        assert unknown.status_code == 404


def test_connector_binding_get_exposes_only_browser_safe_identity(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    app = _app(workspace=workspace, state_root=state_root)

    with TestClient(app) as client:
        project = client.get("/api/wake/projects").json()["projects"][0]
        created = client.put(
            f"/api/project-bindings/{project['project_id']}",
            json={
                "adapter_metadata": {
                    "provider": "chatgpt",
                    "project_ref": "gpt-alpha",
                    "project_path": "/g/gpt-alpha",
                    "app_ref": "plugin_alpha",
                    "app_label": "ChatToCodex",
                }
            },
        )
        assert created.status_code == 200

        response = client.get(
            f"/api/project-bindings/{project['project_id']}/connector"
        )

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "connector": {
            "project_id": project["project_id"],
            "provider": "chatgpt",
            "project_ref": "gpt-alpha",
            "project_path": "/g/gpt-alpha",
            "app_identity": {
                "app_ref": "plugin_alpha",
                "app_label": "ChatToCodex",
            },
            "connector_epoch": created.json()["binding"]["connector_epoch"],
            "revision": created.json()["binding"]["revision"],
        },
    }
    serialized = response.text.lower()
    assert "project_root" not in serialized
    assert "project_name" not in serialized
    assert str((workspace / "sample").resolve()).lower() not in serialized


def test_main_registration_rejects_label_only_binding_and_accepts_stable_ref(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    app = _app(workspace=workspace, state_root=state_root)

    with TestClient(app) as client:
        project = client.get("/api/wake/projects").json()["projects"][0]
        label_only = client.put(
            f"/api/project-bindings/{project['project_id']}",
            json={
                "adapter_metadata": {
                    "provider": "chatgpt",
                    "project_ref": "gpt-alpha",
                    "project_path": "/g/gpt-alpha",
                    "app_label": "an arbitrary MCP display name",
                }
            },
        )
        assert label_only.status_code == 422

        main_target_id = f"mtgt_{9:032x}"
        unbound_registration = client.post(
            "/api/main-runtime/review-targets/register",
            json={
                "main_target_id": main_target_id,
                "project_id": project["project_id"],
            },
        )
        assert unbound_registration.status_code == 409
        assert "app_ref" in unbound_registration.json()["message"]

        bound = client.put(
            f"/api/project-bindings/{project['project_id']}",
            json={
                "adapter_metadata": {
                    "provider": "chatgpt",
                    "project_ref": "gpt-alpha",
                    "project_path": "/g/gpt-alpha",
                    "app_ref": "plugin_arbitrary_connector_123",
                    "app_label": "an arbitrary MCP display name",
                }
            },
        )
        assert bound.status_code == 200

        registered = client.post(
            "/api/main-runtime/review-targets/register",
            json={
                "main_target_id": main_target_id,
                "project_id": project["project_id"],
            },
        )
        assert registered.status_code == 200
        connector = client.get(
            f"/api/project-bindings/{project['project_id']}/connector"
        )
        assert connector.status_code == 200
        assert connector.json()["connector"]["app_identity"] == {
            "app_ref": "plugin_arbitrary_connector_123",
            "app_label": "an arbitrary MCP display name",
        }


def test_connector_binding_get_fails_closed_for_missing_metadata(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    app = _app(workspace=workspace, state_root=state_root)

    with TestClient(app) as client:
        project = client.get("/api/wake/projects").json()["projects"][0]
        created = client.put(f"/api/project-bindings/{project['project_id']}", json={})
        assert created.status_code == 200

        response = client.get(
            f"/api/project-bindings/{project['project_id']}/connector"
        )

    assert response.status_code == 409
    assert response.json()["error_code"] == "project_binding_configuration"
    assert "project_root" not in response.text.lower()
    assert "project_name" not in response.text.lower()
