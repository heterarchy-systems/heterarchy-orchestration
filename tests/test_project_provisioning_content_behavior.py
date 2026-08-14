import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_HARNESS = _ROOT / "tests" / "project_provisioning_content_harness.js"


def test_project_provisioning_accepts_current_slugged_project_landing() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required to execute the Chrome extension harness.")

    completed = subprocess.run(
        (node, str(_HARNESS)),
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
