import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_HARNESS = _ROOT / "tests" / "project_binding_extension_harness.js"


@pytest.mark.parametrize(
    "scenario",
    (
        "valid-project",
        "slugged-project",
        "plain-conversation",
        "label-only-connector",
        "popup-label-only-source",
        "reregistration",
    ),
)
def test_project_binding_extension_behavior(scenario: str) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip(
            "Node.js is required to execute the Chrome extension contract harness."
        )

    completed = subprocess.run(
        (node, str(_HARNESS), scenario),
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
