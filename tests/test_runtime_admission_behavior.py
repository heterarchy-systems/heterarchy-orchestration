import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_HARNESS = _ROOT / "tests" / "runtime_admission_harness.js"


@pytest.mark.parametrize(
    "scenario",
    (
        "stale-no-ready",
        "registration-map-race",
        "park-user-claim-race",
        "composer-human-race",
        "bootstrap-composer-race",
        "generation-in-progress",
        "project-new-chat-race",
        "one-explicit-wake",
        "retirement-runtime-open-gap",
        "lower-stale-revision",
        "registration-rebind",
        "stale-cleanup-user-claim",
        "concurrent-candidates",
        "recovery-and-cooldown",
        "existing-main-binding-refresh",
        "existing-main-binding-refresh-fetches-localhost",
        "main-binding-refresh-identity-only",
        "main-binding-refresh-preserves-unrelated",
        "main-binding-refresh-no-auto-main",
        "main-binding-refresh-fail-closed",
        "poll-main-refresh-before-waking-target",
        "poll-main-delayed-old-binding",
        "poll-worker-same-ref-label-refresh",
        "poll-worker-binding-mismatch",
        "poll-worker-delayed-old-binding",
        "poll-worker-missing-epoch-proof",
        "runtime-owned-provisioning-draft",
        "final-send-admission",
        "wake-adapter-version-handshake",
        "wake-adapter-content-lifecycle",
        "runtime-owned-draft-confirmation",
        "corrupted-stored-draft-capability",
        "stranded-human-draft",
        "durable-draft-reserve-commit",
        "durable-draft-reserve-semantic-cas",
        "durable-draft-confirm-semantic-cas",
        "resume-attempt-queued-merge",
        "durable-reserve-rejection-stage",
        "pre-candidate-provider-draft-mismatch",
        "pre-candidate-composer-changed",
        "pre-candidate-document-claimed",
        "pre-candidate-empty-without-selected-app",
        "selected-app-pill-ancestor-scope",
        "provider-normalized-registration-draft",
        "stale-owned-wake-cleanup-retry",
        "stale-owned-wake-cleanup-leading-feff",
        "stale-cleanup-provider-turn-persistence",
        "stale-owned-wake-cleanup-zero-width-rejections",
        "stale-owned-wake-cleanup-fail-closed",
        "stale-wake-cleanup-pill-provenance",
        "stale-wake-cleanup-consumed-capability-conflict",
        "controlled-editor-stale-cleanup",
        "controlled-editor-split-residual",
        "controlled-editor-failure-rollback",
        "controlled-editor-native-input",
        "controlled-editor-rejected-transactions",
        "exact-current-normalization-near-misses",
        "exact-current-entry-poll-suppression",
        "exact-current-wake-recovery-commit",
        "exact-current-wake-recovery-restart-replay",
        "exact-current-wake-recovery-concurrency",
        "exact-current-wake-recovery-near-misses",
        "provider-normalized-wait-human-edit",
        "contenteditable-react-reconciliation",
        "provider-shadow-two-poll-live-signature",
        "durable-worker-retirement-restart",
        "concurrent-worker-retirement",
        "batch-worker-retirement-drain",
        "batch-worker-retirement-revision-refresh",
        "worker-retirement-preserves-human-main",
        "worker-retirement-suppresses-heartbeat",
        "content-disposition-message-contract",
        "content-disposition-durable-validation",
        "content-disposition-nonauthoritative-liveness",
        "content-disposition-read-and-race-boundary",
        "content-disposition-storage-queue-boundary",
    ),
)
def test_runtime_admission_behavior(scenario: str) -> None:
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
