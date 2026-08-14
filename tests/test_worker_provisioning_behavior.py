import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_HARNESS = _ROOT / "tests" / "worker_provisioning_harness.js"


@pytest.mark.parametrize(
    "scenario",
    (
        "n1-order",
        "slugged-project-landing",
        "provisioned-tab-first-wake",
        "binding-change-during-finalize",
        "n2-sequential",
        "unsupported-project",
        "mismatched-project-identity",
        "pending-finalize-restart",
        "registration-conflict-cleanup",
        "registration-conflict-claimed-tab",
        "shared-open-cooldown",
        "terminal-admission",
        "duplicate-stale",
        "rate-limit",
        "claimed-tab",
        "bootstrap-claimed-tab",
        "claimed-then-deactivated-tab",
        "failed-tab-cleanup",
        "bounded-admission-history",
        "payload-opaque",
        "retirement-admission-gap",
    ),
)
def test_worker_provisioning_behavior(scenario: str) -> None:
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
