from pathlib import Path

from starlette.testclient import TestClient

from chattocodex.control_panel.control_panel_routes import create_control_panel_app
from chattocodex.control_panel.control_panel_service import ControlPanelService


def _client(tmp_path: Path) -> TestClient:
    return TestClient(
        create_control_panel_app(
            service=ControlPanelService(state_root=tmp_path / "state")
        ),
        base_url="http://127.0.0.1:8790",
    )


def test_control_panel_rejects_dns_rebinding_host(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.get(
            "/api/status",
            headers={"Host": "attacker.example"},
        )

    assert response.status_code == 400
    assert response.json()["error_code"] == "invalid_host"


def test_control_panel_rejects_cross_origin_mutation(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.post(
            "/api/start",
            headers={"Origin": "https://attacker.example"},
        )

    assert response.status_code == 403
    assert response.json()["error_code"] == "cross_origin_request_denied"


def test_runtime_extension_origin_reaches_loopback_routes(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.post(
            "/api/start",
            headers={"Origin": "chrome-extension://klgggcfgnnmcjpefhcgnlhoicakbipbk"},
        )

    assert response.status_code == 409
    assert response.json()["error_code"] == "runtime_lifecycle_error"
