import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from starlette.testclient import TestClient

from chattocodex.orchestrator_backend.control_panel.application.control_panel_service import (
    ControlPanelService,
)
from chattocodex.orchestrator_backend.control_panel.interface.routes.control_panel_routes import (
    create_control_panel_app,
)
from chattocodex.shared.security.runtime_access_service import (
    OwnerTokenRevisionConflictError,
    OwnerTokenService,
    _derive_digest,
)


def _race_digest(*, barrier: threading.Barrier):
    def derive_digest(*, token: str, salt: bytes) -> bytes:
        barrier.wait(timeout=2)
        return _derive_digest(token=token, salt=salt)

    return derive_digest


def test_independent_owner_token_services_allow_one_concurrent_rotation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    initial_service = OwnerTokenService(state_root=state_root)
    initial_token = initial_service.generate()
    left = OwnerTokenService(state_root=state_root)
    right = OwnerTokenService(state_root=state_root)
    barrier = threading.Barrier(2)

    with monkeypatch.context() as context:
        context.setattr(
            "chattocodex.shared.security.runtime_access_service._derive_digest",
            _race_digest(barrier=barrier),
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = (executor.submit(left.generate), executor.submit(right.generate))
            outcomes: list[str | OwnerTokenRevisionConflictError] = []
            for future in futures:
                try:
                    outcomes.append(future.result(timeout=3))
                except OwnerTokenRevisionConflictError as error:
                    outcomes.append(error)

    issued_tokens = [outcome for outcome in outcomes if isinstance(outcome, str)]
    conflicts = [
        outcome
        for outcome in outcomes
        if isinstance(outcome, OwnerTokenRevisionConflictError)
    ]
    assert len(issued_tokens) == 1
    assert len(conflicts) == 1
    assert not initial_service.verify(candidate=initial_token)
    assert OwnerTokenService(state_root=state_root).verify(candidate=issued_tokens[0])


def test_verify_rejects_old_token_when_rotation_commits_during_kdf(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    verifier = OwnerTokenService(state_root=state_root)
    old_token = verifier.generate()
    rotation = OwnerTokenService(state_root=state_root)
    verification_kdf_started = threading.Event()
    allow_verification_kdf_to_finish = threading.Event()

    def paused_old_token_digest(*, token: str, salt: bytes) -> bytes:
        if token == old_token:
            verification_kdf_started.set()
            assert allow_verification_kdf_to_finish.wait(timeout=3)
        return _derive_digest(token=token, salt=salt)

    with monkeypatch.context() as context:
        context.setattr(
            "chattocodex.shared.security.runtime_access_service._derive_digest",
            paused_old_token_digest,
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            verification = executor.submit(verifier.verify, candidate=old_token)
            assert verification_kdf_started.wait(timeout=3)
            new_token = rotation.generate()
            allow_verification_kdf_to_finish.set()
            assert verification.result(timeout=3) is False

    assert verifier.verify(candidate=new_token)


def test_concurrent_ensure_configured_creates_once_without_rotating(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    left = OwnerTokenService(state_root=state_root)
    right = OwnerTokenService(state_root=state_root)
    barrier = threading.Barrier(2)

    with monkeypatch.context() as context:
        context.setattr(
            "chattocodex.shared.security.runtime_access_service._derive_digest",
            _race_digest(barrier=barrier),
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = tuple(
                future.result(timeout=3)
                for future in (
                    executor.submit(left.ensure_configured),
                    executor.submit(right.ensure_configured),
                )
            )

    issued_tokens = [outcome for outcome in outcomes if outcome is not None]
    assert len(issued_tokens) == 1
    assert outcomes.count(None) == 1
    assert OwnerTokenService(state_root=state_root).verify(candidate=issued_tokens[0])


def test_control_panel_returns_one_success_for_concurrent_owner_token_rotation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_root = tmp_path / "state"
    OwnerTokenService(state_root=state_root).generate()
    left = ControlPanelService(state_root=state_root)
    right = ControlPanelService(state_root=state_root)
    barrier = threading.Barrier(2)
    left_client = TestClient(create_control_panel_app(service=left))
    right_client = TestClient(create_control_panel_app(service=right))

    with left_client, right_client, monkeypatch.context() as context:
        context.setattr(
            "chattocodex.shared.security.runtime_access_service._derive_digest",
            _race_digest(barrier=barrier),
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = tuple(
                future.result(timeout=3)
                for future in (
                    executor.submit(left_client.post, "/api/owner-token/rotate"),
                    executor.submit(right_client.post, "/api/owner-token/rotate"),
                )
            )

    successes = [response for response in responses if response.status_code == 200]
    conflicts = [response for response in responses if response.status_code == 409]
    assert len(successes) == 1
    assert len(conflicts) == 1
    assert conflicts[0].json() == {
        "ok": False,
        "error_code": "owner_token_rotation_conflict",
        "message": "Owner Token changed during rotation. Retry the operation.",
        "recoverable": True,
    }
    issued_token = successes[0].json()["owner_token"]
    assert OwnerTokenService(state_root=state_root).verify(candidate=issued_token)
