import subprocess
import sys
from pathlib import Path


def _workspace_with_project(tmp_path: Path) -> Path:
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "demo"
    project_root.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text(
        "[project]\nname = 'demo'\n",
        encoding="utf-8",
    )
    return workspace_root


def test_cli_setup_and_doctor_use_selected_state_root(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    workspace_root = _workspace_with_project(tmp_path)
    setup = subprocess.run(
        [
            sys.executable,
            "-m",
            "chattocodex.cli",
            "--state-root",
            str(state_root),
            "setup",
            "--workspace-root",
            str(workspace_root),
            "--public-url",
            "https://multi.example.test",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert setup.returncode == 0
    assert '"connector_url":"https://multi.example.test/mcp"' in setup.stdout.replace(
        " ", ""
    )
    assert "Store the Owner Token securely" in setup.stdout

    doctor = subprocess.run(
        [
            sys.executable,
            "-m",
            "chattocodex.cli",
            "--state-root",
            str(state_root),
            "doctor",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert doctor.returncode == 0
    compact_output = doctor.stdout.replace(" ", "")
    assert '"ok":true' in compact_output
    assert '"project_count":1' in compact_output
    assert '"connector_url":"https://multi.example.test/mcp"' in compact_output
