import asyncio
import base64
import hashlib
import os
import secrets
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from chattocodex.orchestration.entity_lock_registry import EntityLockRegistry
from chattocodex.persistence.wake_presence_repository import WakePresenceRepository
from chattocodex.persistence.wake_target_repository import WakeTargetRepository
from chattocodex.project_runtime.workspace_execution_layout import (
    WorkspaceExecutionLayout,
)
from chattocodex.schemas.setup_schemas import SetupRequest
from chattocodex.setup.runtime_setup_service import RuntimeSetupService
from chattocodex.wake.wake_presence_service import WakePresenceService
from chattocodex.wake.wake_target_service import WakeTargetService


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind(("127.0.0.1", 0))
        return int(server_socket.getsockname()[1])


def _wait_for_port(*, port: int, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.settimeout(0.1)
            if client_socket.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"OAuth MCP server did not open port {port}.")


@contextmanager
def _running_oauth_server(*, state_root: Path, port: int) -> Iterator[None]:
    environment = os.environ.copy()
    environment["CHATTOCODEX_STATE_ROOT"] = str(state_root)
    environment["CHATTOCODEX_PORT"] = str(port)
    process = subprocess.Popen(
        [sys.executable, "-m", "chattocodex.mcp_server"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_port(port=port)
        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.returncode not in {0, -15}:
            stderr = process.stderr.read() if process.stderr is not None else ""
            raise AssertionError(f"OAuth MCP server exited unexpectedly: {stderr}")


def _workspace_with_project(tmp_path: Path) -> Path:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "remote-project"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'remote-project'\n",
        encoding="utf-8",
    )
    (project_root / "README.md").write_text(
        "# Remote Project\n\nORCHESTRATION_SENTINEL\n",
        encoding="utf-8",
    )
    subprocess.run(
        ("git", "-C", str(project_root), "init"), check=True, capture_output=True
    )
    subprocess.run(
        ("git", "-C", str(project_root), "config", "user.email", "test@example.com"),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ("git", "-C", str(project_root), "config", "user.name", "Test User"),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ("git", "-C", str(project_root), "add", "."),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ("git", "-C", str(project_root), "commit", "-m", "initial"),
        check=True,
        capture_output=True,
    )
    return workspace_root


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


def _authorize_client(
    *,
    client: httpx2.Client,
    base_url: str,
    endpoint: str,
    owner_token: str,
) -> tuple[str, str, str]:
    metadata = client.get(f"{base_url}/.well-known/oauth-authorization-server")
    assert metadata.status_code == 200
    metadata_payload = metadata.json()
    assert set(metadata_payload["scopes_supported"]) == {
        "chattocodex",
        "offline_access",
    }
    assert metadata_payload["registration_endpoint"] == f"{base_url}/register"

    protected = client.get(f"{base_url}/.well-known/oauth-protected-resource/mcp")
    assert protected.status_code == 200
    assert protected.json()["resource"] == endpoint

    registration = client.post(
        f"{base_url}/register",
        json={
            "redirect_uris": ["http://localhost:9999/callback"],
            "token_endpoint_auth_method": "client_secret_post",
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "scope": "chattocodex offline_access",
            "client_name": "ChatToCodex integration client",
        },
    )
    assert registration.status_code == 201
    registered = registration.json()
    client_id = str(registered["client_id"])
    client_secret = str(registered["client_secret"])

    verifier, challenge = _pkce_pair()
    authorization = client.get(
        f"{base_url}/authorize",
        params={
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": "http://localhost:9999/callback",
            "scope": "chattocodex offline_access",
            "state": "integration-state",
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "resource": endpoint,
        },
        follow_redirects=False,
    )
    assert authorization.status_code == 302
    approval_url = authorization.headers["location"]
    pending_id = parse_qs(urlparse(approval_url).query)["pending_id"][0]

    approval_page = client.get(approval_url)
    assert approval_page.status_code == 200
    assert "Authorize ChatToCodex" in approval_page.text

    approval = client.post(
        f"{base_url}/oauth/approve",
        data={"pending_id": pending_id, "owner_token": owner_token},
        follow_redirects=False,
    )
    assert approval.status_code == 302
    callback = urlparse(approval.headers["location"])
    callback_query = parse_qs(callback.query)
    assert callback_query["state"] == ["integration-state"]
    authorization_code = callback_query["code"][0]

    token_response = client.post(
        f"{base_url}/token",
        data={
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "code": authorization_code,
            "code_verifier": verifier,
            "redirect_uri": "http://localhost:9999/callback",
            "resource": endpoint,
        },
    )
    assert token_response.status_code == 200
    token_payload = token_response.json()
    assert token_payload["token_type"] == "Bearer"
    assert "offline_access" in token_payload["scope"].split()
    return (
        str(token_payload["access_token"]),
        str(token_payload["refresh_token"]),
        f"{client_id}:{client_secret}",
    )


async def _list_projects_with_access_token(*, endpoint: str, access_token: str) -> int:
    async with (
        httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {access_token}"}
        ) as http_client,
        streamable_http_client(endpoint, http_client=http_client) as (
            read_stream,
            write_stream,
        ),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        result = await session.call_tool("project_list", {})
        assert result.structured_content is not None
        payload = result.structured_content["result"]
        projects = payload["projects"]
        assert projects[0]["name"] == "remote-project"
        return len(projects)


async def _remote_worker_flow(
    *,
    endpoint: str,
    access_token: str,
    state_root: Path,
    run_id: str,
    project_id: str,
    worker_number: int,
) -> tuple[str, str]:
    async with (
        httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {access_token}"}
        ) as http_client,
        streamable_http_client(endpoint, http_client=http_client) as (
            read_stream,
            write_stream,
        ),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        wake_target_id = f"wtgt_{worker_number:032x}"
        wake_revision = (
            WakeTargetRepository(state_root=state_root)
            .read(wake_target_id=wake_target_id)
            .wake_revision
        )
        claimed = await session.call_tool(
            "work",
            {
                "project_id": project_id,
                "label": f"oauth-consumer-{worker_number}",
                "wake_target_id": wake_target_id,
                "wake_revision": wake_revision,
                "wait_seconds": 2,
            },
        )
        assert claimed.structured_content is not None
        claimed_payload = claimed.structured_content["result"]
        slot = claimed_payload["slot"]
        assert isinstance(slot, dict)
        slot_id = str(slot["slot_id"])
        slot_handle = str(claimed_payload["slot_handle"])
        assignment = claimed_payload["assignment"]
        assert isinstance(assignment, dict)
        worker_view = assignment["worker"]
        task_view = assignment["task"]
        assert isinstance(worker_view, dict)
        assert isinstance(task_view, dict)
        worker_handle = str(assignment["worker_handle"])

        project_read = await session.call_tool(
            "project_file_read",
            {
                "run_id": run_id,
                "relative_path": "README.md",
                "start_line": 1,
                "end_line": 20,
            },
        )
        assert project_read.structured_content is not None
        assert (
            "ORCHESTRATION_SENTINEL"
            in project_read.structured_content["result"]["content"]
        )
        workspace = await session.call_tool(
            "worker_workspace_prepare",
            {
                "run_id": run_id,
                "worker_id": str(worker_view["worker_id"]),
                "worker_handle": worker_handle,
            },
        )
        assert workspace.structured_content is not None
        worktree_root = str(workspace.structured_content["result"]["worktree_root"])
        replaced = await session.call_tool(
            "worker_file_replace",
            {
                "run_id": run_id,
                "worker_id": str(worker_view["worker_id"]),
                "worker_handle": worker_handle,
                "relative_path": "README.md",
                "expected_sha256": hashlib.sha256(
                    b"# Remote Project\n\nORCHESTRATION_SENTINEL\n"
                ).hexdigest(),
                "content": f"# Remote Worker {worker_number}\n",
            },
        )
        assert replaced.is_error is False
        assert (Path(worktree_root) / "README.md").read_text(encoding="utf-8") == (
            f"# Remote Worker {worker_number}\n"
        )
        committed = await session.call_tool(
            "worker_result_commit",
            {
                "run_id": run_id,
                "worker_id": str(worker_view["worker_id"]),
                "worker_handle": worker_handle,
                "summary": f"remote worker {worker_number} result",
            },
        )
        assert committed.structured_content is not None
        commit_sha = str(committed.structured_content["result"]["commit_sha"])
        completed = await session.call_tool(
            "task_update",
            {
                "run_id": run_id,
                "worker_id": str(worker_view["worker_id"]),
                "worker_handle": worker_handle,
                "task_id": str(task_view["task_id"]),
                "expected_revision": 0,
                "status": "completed",
                "progress_summary": f"remote worker {worker_number} completed",
                "result": f"remote-result-{worker_number}",
            },
        )
        assert completed.is_error is False
        released = await session.call_tool(
            "work_done",
            {"slot_id": slot_id, "slot_handle": slot_handle},
        )
        assert released.structured_content is not None
        assert released.structured_content["result"]["slot"]["status"] == "idle"
        return worktree_root, commit_sha


def _register_remote_wake_targets(*, state_root: Path, project_id: str) -> None:
    presence = WakePresenceService(
        repository=WakePresenceRepository(state_root=state_root)
    )
    targets = WakeTargetService(
        repository=WakeTargetRepository(state_root=state_root),
        presence_service=presence,
        lock_registry=EntityLockRegistry(),
    )
    for worker_number in range(1, 4):
        wake_target_id = f"wtgt_{worker_number:032x}"
        targets.register_target(wake_target_id=wake_target_id, project_id=project_id)
        presence.heartbeat(wake_target_id=wake_target_id)


async def _complete_remote_three_worker_run(
    *, endpoint: str, access_token: str, state_root: Path
) -> str:
    async with (
        httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {access_token}"}
        ) as http_client,
        streamable_http_client(endpoint, http_client=http_client) as (
            read_stream,
            write_stream,
        ),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        projects_result = await session.call_tool("project_list", {})
        assert projects_result.structured_content is not None
        projects = projects_result.structured_content["result"]["projects"]
        project_id = str(projects[0]["project_id"])
        _register_remote_wake_targets(state_root=state_root, project_id=project_id)
        delegated = await session.call_tool(
            "delegate_work",
            {
                "goal": "OAuth remote three-consumer run",
                "project_id": project_id,
                "tasks": [
                    {
                        "key": f"lane-{worker_number}",
                        "objective": f"Remote lane {worker_number}",
                        "role": f"lane-{worker_number}",
                    }
                    for worker_number in range(1, 4)
                ],
            },
        )
        assert delegated.structured_content is not None
        run_payload = delegated.structured_content["result"]
        run_id = str(run_payload["run"]["run_id"])
        delegated_tasks = run_payload["tasks"]
        assert isinstance(delegated_tasks, list)
        assert len(delegated_tasks) == 3

    worker_outputs = await asyncio.gather(
        *(
            _remote_worker_flow(
                endpoint=endpoint,
                access_token=access_token,
                state_root=state_root,
                run_id=run_id,
                project_id=project_id,
                worker_number=index,
            )
            for index in range(1, 4)
        )
    )
    worktree_roots = [worktree_root for worktree_root, _commit_sha in worker_outputs]
    commit_shas = [commit_sha for _worktree_root, commit_sha in worker_outputs]
    assert len(set(worktree_roots)) == 3
    assert len(set(commit_shas)) == 3

    async with (
        httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {access_token}"}
        ) as status_client,
        streamable_http_client(endpoint, http_client=status_client) as (
            status_read,
            status_write,
        ),
        ClientSession(status_read, status_write) as status_session,
    ):
        await status_session.initialize()
        status_result = await status_session.call_tool(
            "delegation_status", {"run_id": run_id}
        )
        assert status_result.structured_content is not None
        status = status_result.structured_content["result"]
        assert status["is_complete"] is True
        assert status["completed_count"] == 3
        assert status["busy_slot_count"] == 0
        assert status["idle_slot_count"] == 3
        results_response = await status_session.call_tool(
            "run_results", {"run_id": run_id}
        )
        assert results_response.structured_content is not None
        results = results_response.structured_content["result"]["results"]
        assert len(results) == 3
        assert {result["commit_sha"] for result in results} == set(commit_shas)
        assert len({result["base_commit"] for result in results}) == 1
        integration_response = await status_session.call_tool(
            "integration_preview", {"run_id": run_id}
        )
        assert integration_response.structured_content is not None
        integration = integration_response.structured_content["result"]
        assert integration["result_count"] == 3
        assert integration["ready_to_integrate"] is False
        assert len(integration["conflicts"]) == 1
        assert integration["conflicts"][0]["relative_path"] == "README.md"
        assert len(integration["conflicts"][0]["worker_ids"]) == 3
    return run_id


def test_oauth_remote_mcp_survives_restart_and_refreshes_token(tmp_path: Path) -> None:
    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"
    endpoint = f"{base_url}/mcp"
    state_root = tmp_path / "state"
    workspace_root = _workspace_with_project(tmp_path)
    execution_state_root = WorkspaceExecutionLayout.state_root(
        workspace_root=workspace_root
    )
    setup = RuntimeSetupService(state_root=state_root).configure(
        request=SetupRequest(
            workspace_root=workspace_root,
            public_url=base_url,
            oauth_enabled=True,
        )
    )
    assert setup.owner_token is not None

    with _running_oauth_server(state_root=state_root, port=port):
        with httpx2.Client() as client:
            unauthorized = client.post(endpoint, content=b"{}")
            assert unauthorized.status_code == 401
            access_token, refresh_token, client_credentials = _authorize_client(
                client=client,
                base_url=base_url,
                endpoint=endpoint,
                owner_token=setup.owner_token,
            )
        assert (
            asyncio.run(
                _list_projects_with_access_token(
                    endpoint=endpoint,
                    access_token=access_token,
                )
            )
            == 1
        )
        remote_run_id = asyncio.run(
            _complete_remote_three_worker_run(
                endpoint=endpoint,
                access_token=access_token,
                state_root=execution_state_root,
            )
        )
        assert remote_run_id.startswith("run_")
        assert (workspace_root / "remote-project" / "README.md").read_text(
            encoding="utf-8"
        ) == "# Remote Project\n\nORCHESTRATION_SENTINEL\n"

    client_id, client_secret = client_credentials.split(":", maxsplit=1)
    with _running_oauth_server(state_root=state_root, port=port):
        with httpx2.Client() as client:
            refresh = client.post(
                f"{base_url}/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                    "scope": "chattocodex offline_access",
                },
            )
            assert refresh.status_code == 200
            refreshed_payload = refresh.json()
            refreshed_access_token = str(refreshed_payload["access_token"])
            assert refreshed_access_token != access_token
        assert (
            asyncio.run(
                _list_projects_with_access_token(
                    endpoint=endpoint,
                    access_token=refreshed_access_token,
                )
            )
            == 1
        )
