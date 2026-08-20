from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_routes import (
    create_control_panel_app,
)
from chattocodex.orchestrator_backend.control_panel.application.control_panel_service import (
    ControlPanelService,
)
from chattocodex.orchestrator_backend.main_runtime.presence.main_review_chat_presence_tracker import (
    MainReviewChatPresenceTracker,
)
from chattocodex.orchestrator_backend.main_runtime.application.main_review_project_catalog import (
    MainReviewProjectCatalog,
)
from chattocodex.orchestrator_backend.main_runtime.interface.main_review_route_adapter import (
    MainReviewRouteAdapter,
)
from chattocodex.orchestrator_backend.main_runtime.target.main_review_target_coordinator import (
    MainReviewTargetCoordinator,
)
from chattocodex.shared.concurrency.entity_lock_registry import EntityLockRegistry
from chattocodex.orchestrator_backend.persistence.main_runtime.main_review_chat_presence_store import (
    MainReviewChatPresenceStore,
)
from chattocodex.orchestrator_backend.persistence.main_runtime.main_review_target_store import (
    MainReviewTargetStore,
)
from chattocodex.orchestrator_backend.persistence.project.project_binding_repository import (
    ProjectBindingRepository,
)
from chattocodex.orchestrator_backend.persistence.orchestration.run_control_state_store import (
    RunControlStateStore,
)
from chattocodex.shared.settings.runtime_config_repository import (
    RuntimeConfigRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_presence_repository import (
    WakePresenceRepository,
)
from chattocodex.orchestrator_backend.persistence.wake.wake_target_repository import (
    WakeTargetRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_provisioning_repository import (
    WorkerProvisioningRepository,
)
from chattocodex.orchestrator_backend.persistence.worker.worker_runtime_presence_store import (
    WorkerRuntimePresenceStore,
)
from chattocodex.orchestrator_backend.project_runtime.project_binding_service import (
    ProjectBindingService,
)
from chattocodex.orchestrator_backend.provisioning.worker_provisioning_service import (
    WorkerProvisioningService,
)
from chattocodex.shared.contracts.auth_schemas import RuntimeConfigRecord
from chattocodex.shared.contracts.project.project_binding_schemas import (
    ProjectBindingAdapterMetadata,
)
from chattocodex.orchestrator_backend.wake.wake_control_service import (
    WakeControlService,
)
from chattocodex.orchestrator_backend.wake.wake_exceptions import (
    WakeTargetConflictError,
)
from chattocodex.orchestrator_backend.wake.presence.wake_presence_service import (
    WakePresenceService,
)
from chattocodex.orchestrator_backend.wake.target.wake_target_service import (
    WakeTargetService,
)
from chattocodex.orchestrator_backend.worker_runtime.presence.worker_runtime_presence_tracker import (
    WorkerRuntimePresenceTracker,
)
from chattocodex.orchestrator_backend.workspace.workspace_project_service import (
    WorkspaceProjectService,
)


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    project = workspace / "sample"
    project.mkdir(parents=True)
    (project / "pyproject.toml").write_text(
        "[project]\nname='sample'\n", encoding="utf-8"
    )
    return workspace


class _Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 13, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def _wake_service(
    *, state_root: Path, registered_connector_epoch: int = 0
) -> WakeControlService:
    lock_registry = EntityLockRegistry()
    presence_service = WakePresenceService(
        repository=WakePresenceRepository(state_root=state_root)
    )
    runtime_presence_tracker = WorkerRuntimePresenceTracker(
        store=WorkerRuntimePresenceStore(state_root=state_root)
    )
    wake_target_service = WakeTargetService(
        repository=WakeTargetRepository(state_root=state_root),
        presence_service=presence_service,
        runtime_presence_tracker=runtime_presence_tracker,
        lock_registry=lock_registry,
    )
    wake_target_service.attach_connector_epoch_resolver(
        lambda _project_id: registered_connector_epoch
    )
    return WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(state_root=state_root),
        wake_target_service=wake_target_service,
        wake_presence_service=presence_service,
        runtime_presence_tracker=runtime_presence_tracker,
        lock_registry=lock_registry,
    )


def _main_review_adapter(*, state_root: Path) -> MainReviewRouteAdapter:
    project_binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=state_root),
        runtime_config_repository=RuntimeConfigRepository(state_root=state_root),
    )
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


def test_control_panel_serves_settings_and_persists_preferences(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    service = ControlPanelService(state_root=state_root)
    app = create_control_panel_app(
        service=service,
        wake_service=_wake_service(state_root=state_root),
        main_review_adapter=_main_review_adapter(state_root=state_root),
    )

    with TestClient(app) as client:
        index = client.get("/")
        assert index.status_code == 200
        assert "ChatToCodex 설정" in index.text

        saved = client.post(
            "/api/preferences",
            json={
                "workspace_root": str(workspace),
                "remote_mode": "quick_tunnel",
                "custom_origin": None,
                "mcp_port": 8765,
            },
        )
        assert saved.status_code == 200
        payload = saved.json()
        assert payload["configured"] is True
        assert payload["project_count"] == 1
        assert payload["preferences"]["remote_mode"] == "quick_tunnel"

        status = client.get("/api/status")
        assert status.status_code == 200
        assert status.json()["preferences"]["workspace_root"] == str(
            workspace.resolve()
        )


def test_control_panel_registers_and_polls_opaque_wake_target(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    RuntimeConfigRepository(state_root=state_root).save(
        record=RuntimeConfigRecord(
            workspace_root=str(workspace.resolve()),
            public_url=None,
            oauth_enabled=False,
        )
    )
    app = create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        wake_service=_wake_service(state_root=state_root, registered_connector_epoch=5),
        main_review_adapter=_main_review_adapter(state_root=state_root),
    )

    with TestClient(app) as client:
        projects_response = client.get("/api/wake/projects")
        assert projects_response.status_code == 200
        projects = projects_response.json()["projects"]
        assert len(projects) == 1
        project_id = projects[0]["project_id"]

        target_id = "wtgt_33333333333333333333333333333333"
        registered = client.post(
            "/api/wake/targets/register",
            json={"wake_target_id": target_id, "project_id": project_id},
        )
        assert registered.status_code == 200
        assert registered.json()["target"]["status"] == "idle"
        assert registered.json()["target"]["registered_connector_epoch"] == 5

        polled = client.get(f"/api/wake/targets/{target_id}")
        assert polled.status_code == 200
        assert polled.json()["target"]["wake_target_id"] == target_id
        assert polled.json()["target"]["wake_revision"] == 0
        assert polled.json()["target"]["registered_connector_epoch"] == 5

        runtime_heartbeat = client.post(
            f"/api/wake/targets/{target_id}/runtime-heartbeat"
        )
        assert runtime_heartbeat.status_code == 200
        assert runtime_heartbeat.json()["target"]["wake_target_id"] == target_id
        assert runtime_heartbeat.json()["target"]["registered_connector_epoch"] == 5

        retired = client.post(
            f"/api/wake/targets/{target_id}/retire",
            json={"expected_revision": 0},
        )
        assert retired.status_code == 200
        assert retired.json() == {
            "ok": True,
            "wake_target_id": target_id,
            "retired_revision": 0,
        }
        missing = client.get(f"/api/wake/targets/{target_id}")
        assert missing.status_code == 400
        assert missing.json()["error_code"] == "not_found"


def test_control_panel_retire_rejects_stale_or_busy_target(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    RuntimeConfigRepository(state_root=state_root).save(
        record=RuntimeConfigRecord(
            workspace_root=str(workspace.resolve()),
            public_url=None,
            oauth_enabled=False,
        )
    )
    wake_service = _wake_service(state_root=state_root)
    app = create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        wake_service=wake_service,
        main_review_adapter=_main_review_adapter(state_root=state_root),
    )

    with TestClient(app) as client:
        project_id = client.get("/api/wake/projects").json()["projects"][0][
            "project_id"
        ]
        target_id = "wtgt_44444444444444444444444444444444"
        assert (
            client.post(
                "/api/wake/targets/register",
                json={"wake_target_id": target_id, "project_id": project_id},
            ).status_code
            == 200
        )

        stale = client.post(
            f"/api/wake/targets/{target_id}/retire",
            json={"expected_revision": 1},
        )
        assert stale.status_code == 400
        assert stale.json()["error_code"] == "wake_target_conflict"

        target_service = wake_service._wake_target_service
        target_service.sync_ready_capacity(project_id=project_id, ready_count=1)
        waking = target_service.get_target(wake_target_id=target_id)
        claiming = target_service.reserve_wake_claim(
            wake_target_id=target_id,
            wake_revision=waking.wake_revision,
        )
        busy = client.post(
            f"/api/wake/targets/{target_id}/retire",
            json={"expected_revision": claiming.revision},
        )
        assert busy.status_code == 400
        assert busy.json()["error_code"] == "wake_target_conflict"


def test_control_panel_batch_retirement_route_returns_ids_in_request_order(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    RuntimeConfigRepository(state_root=state_root).save(
        record=RuntimeConfigRecord(
            workspace_root=str(workspace.resolve()),
            public_url=None,
            oauth_enabled=False,
        )
    )
    wake_service = _wake_service(state_root=state_root)
    app = create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        wake_service=wake_service,
        main_review_adapter=_main_review_adapter(state_root=state_root),
    )

    with TestClient(app) as client:
        project_id = client.get("/api/wake/projects").json()["projects"][0][
            "project_id"
        ]
        target_ids = (
            "wtgt_55555555555555555555555555555555",
            "wtgt_66666666666666666666666666666666",
        )
        for target_id in target_ids:
            registered = client.post(
                "/api/wake/targets/register",
                json={"wake_target_id": target_id, "project_id": project_id},
            )
            assert registered.status_code == 200

        response = client.post(
            "/api/wake/targets/retire-batch",
            json={
                "project_id": project_id,
                "targets": [
                    {"wake_target_id": target_id, "expected_revision": 0}
                    for target_id in reversed(target_ids)
                ],
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "ok": True,
            "project_id": project_id,
            "retired_wake_target_ids": list(reversed(target_ids)),
        }


def test_incompatible_connector_target_cannot_renew_presence(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    clock = _Clock()
    locks = EntityLockRegistry()
    presence_repository = WakePresenceRepository(state_root=state_root)
    presence = WakePresenceService(repository=presence_repository, now_provider=clock)
    runtime_store = WorkerRuntimePresenceStore(state_root=state_root)
    runtime_presence = WorkerRuntimePresenceTracker(
        store=runtime_store,
        now_provider=clock,
    )
    targets = WakeTargetService(
        repository=WakeTargetRepository(state_root=state_root),
        presence_service=presence,
        runtime_presence_tracker=runtime_presence,
        lock_registry=locks,
        now_provider=clock,
    )
    target_id = "wtgt_99999999999999999999999999999999"
    targets.register_target(
        wake_target_id=target_id,
        project_id="prj_sample_12345678",
        registered_connector_epoch=0,
    )
    initial_presence = presence_repository.read(wake_target_id=target_id)
    targets.attach_connector_epoch_resolver(lambda _project_id: 1)
    service = WakeControlService(
        runtime_config_repository=RuntimeConfigRepository(state_root=state_root),
        wake_target_service=targets,
        wake_presence_service=presence,
        runtime_presence_tracker=runtime_presence,
        lock_registry=locks,
    )
    clock.now += timedelta(seconds=30)

    with pytest.raises(WakeTargetConflictError):
        service.get_target(wake_target_id=target_id)
    with pytest.raises(WakeTargetConflictError):
        service.runtime_heartbeat_target(wake_target_id=target_id)

    assert presence_repository.read(wake_target_id=target_id) == initial_presence
    assert not runtime_store.exists(wake_target_id=target_id)


def test_control_panel_main_review_target_boundary_is_opaque(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    RuntimeConfigRepository(state_root=state_root).save(
        record=RuntimeConfigRecord(
            workspace_root=str(workspace.resolve()),
            public_url=None,
            oauth_enabled=False,
        )
    )
    app = create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        wake_service=_wake_service(state_root=state_root),
        main_review_adapter=_main_review_adapter(state_root=state_root),
    )

    with TestClient(app) as client:
        project_id = client.get("/api/wake/projects").json()["projects"][0][
            "project_id"
        ]
        ProjectBindingService(
            repository=ProjectBindingRepository(state_root=state_root),
            runtime_config_repository=RuntimeConfigRepository(state_root=state_root),
        ).upsert_binding(
            project_id=project_id,
            metadata=ProjectBindingAdapterMetadata(
                provider="chatgpt",
                project_ref="gpt-project-sample",
                project_path="/g/gpt-project-sample",
                app_ref="plugin_control_panel_test",
                app_label="Control Panel Test Connector",
            ),
        )
        main_target_id = f"mtgt_{1:032x}"
        registered = client.post(
            "/api/main-runtime/review-targets/register",
            json={"main_target_id": main_target_id, "project_id": project_id},
        )
        assert registered.status_code == 200
        payload = registered.json()
        assert payload["target"]["main_target_id"] == main_target_id
        assert payload["target"]["status"] == "idle"

        serialized = str(payload).lower()
        assert "workflow_id" not in serialized
        assert "workflow_handle" not in serialized
        assert "run_id" not in serialized

        polled = client.get(f"/api/main-runtime/review-targets/{main_target_id}")
        assert polled.status_code == 200
        assert polled.json()["target"]["main_target_id"] == main_target_id

        heartbeat = client.post(
            f"/api/main-runtime/review-targets/{main_target_id}/chat-heartbeat"
        )
        assert heartbeat.status_code == 200
        persisted = MainReviewChatPresenceStore(state_root=state_root).read(
            main_target_id=main_target_id
        )
        assert persisted.main_target_id == main_target_id


def test_worker_provisioning_routes_use_exact_opaque_browser_contract(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    provisioning = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=state_root),
        minimum_admission_interval=timedelta(0),
    )
    provisioning.upsert_demand(
        project_id="prj_a_12345678", desired_capacity=1, spawn_needed=1
    )
    app = create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        wake_service=_wake_service(state_root=state_root),
        main_review_adapter=_main_review_adapter(state_root=state_root),
        worker_provisioning_service=provisioning,
    )

    with TestClient(app) as client:
        next_response = client.post("/api/worker-provisioning/next")
        assert next_response.status_code == 200
        next_payload = next_response.json()
        assert set(next_payload) == {"ok", "request"}
        request_payload = next_payload["request"]
        assert request_payload["wake_target_id"].startswith("wtgt_")
        serialized = str(next_payload).lower()
        for forbidden in (
            "task_id",
            "run_id",
            "role",
            "objective",
            "dag",
            "capability",
        ):
            assert forbidden not in serialized

        registered = client.post(
            f"/api/worker-provisioning/{request_payload['request_id']}/registered",
            json={
                "conversation_id": "conversation-a",
                "wake_target_id": request_payload["wake_target_id"],
                "admission_revision": request_payload["admission_revision"],
            },
        )
        assert registered.status_code == 200
        assert set(registered.json()) == {"ok", "request"}
        assert registered.json()["request"]["status"] == "active"

        provisioning.upsert_demand(
            project_id="prj_a_12345678", desired_capacity=1, spawn_needed=1
        )
        failed_request = client.post("/api/worker-provisioning/next").json()["request"]
        failed = client.post(
            f"/api/worker-provisioning/{failed_request['request_id']}/failed",
            json={
                "admission_revision": failed_request["admission_revision"],
                "reason": "browser_open_failed",
                "rate_limited": False,
            },
        )
        assert failed.status_code == 200
        assert set(failed.json()) == {"ok", "retry_after_ms"}

        rejected_metadata = client.post(
            f"/api/worker-provisioning/{failed_request['request_id']}/failed",
            json={
                "admission_revision": failed_request["admission_revision"],
                "reason": "browser_open_failed",
                "rate_limited": False,
                "adapter_metadata": {},
            },
        )
        assert rejected_metadata.status_code == 422


def test_worker_provisioning_operator_retry_route_requires_request_cas(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    state_root = tmp_path / "state"
    current = [datetime(2026, 8, 13, tzinfo=UTC)]
    lock_registry = EntityLockRegistry(lock_directory=state_root / "locks")
    workspace_service = WorkspaceProjectService(workspace_root=workspace)
    project = workspace_service.list_projects()[0]
    binding_service = ProjectBindingService(
        repository=ProjectBindingRepository(state_root=state_root),
        workspace_project_service=workspace_service,
        lock_registry=lock_registry,
        now_provider=lambda: current[0],
    )
    binding = binding_service.upsert_binding(
        project_id=project.project_id,
        metadata=ProjectBindingAdapterMetadata(
            provider="chatgpt",
            project_ref="project-sample",
            project_path="/g/project-sample",
            app_ref="plugin_sample",
            app_label="ChatOrchestration",
        ),
    )
    provisioning = WorkerProvisioningService(
        repository=WorkerProvisioningRepository(state_root=state_root),
        project_binding_service=binding_service,
        lock_registry=lock_registry,
        now_provider=lambda: current[0],
    )
    provisioning.upsert_demand(
        project_id=project.project_id,
        desired_capacity=1,
        spawn_needed=1,
        binding_revision=binding.revision,
    )
    app = create_control_panel_app(
        service=ControlPanelService(state_root=state_root),
        wake_service=_wake_service(state_root=state_root),
        main_review_adapter=_main_review_adapter(state_root=state_root),
        project_binding_service=binding_service,
        worker_provisioning_service=provisioning,
    )

    with TestClient(app) as client:
        admitted = client.post("/api/worker-provisioning/next").json()["request"]
        failed = client.post(
            f"/api/worker-provisioning/{admitted['request_id']}/failed",
            json={
                "admission_revision": admitted["admission_revision"],
                "reason": "app_menu_unavailable",
                "rate_limited": False,
            },
        )
        assert failed.status_code == 200
        blocked = client.get("/api/worker-provisioning/status").json()["requests"][0]

        retried = client.post(
            f"/api/worker-provisioning/{admitted['request_id']}/retry",
            json={"expected_revision": blocked["revision"]},
        )

        assert retried.status_code == 200
        retried_request = retried.json()["request"]
        assert retried_request["status"] == "waiting_admission"
        assert retried_request["admission_revision"] is None
        assert retried_request["failure_reason"] is None

        stale = client.post(
            f"/api/worker-provisioning/{admitted['request_id']}/retry",
            json={"expected_revision": blocked["revision"]},
        )
        assert stale.status_code == 409
        assert stale.json()["error_code"] == "revision_conflict"

        invalid = client.post(
            f"/api/worker-provisioning/{admitted['request_id']}/retry",
            json={"expected_revision": retried_request["revision"], "extra": True},
        )
        assert invalid.status_code == 422
