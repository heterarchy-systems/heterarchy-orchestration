import json
import re
import shutil
import subprocess
from pathlib import Path

_EXTENSION_ROOT = (
    Path(__file__).resolve().parents[1]
    / "chattocodex"
    / "orchestrator_backend"
    / "wake"
    / "chrome_extension"
)
_TYPESCRIPT_ROOT = (
    Path(__file__).resolve().parents[1]
    / "chattocodex"
    / "orchestrator_backend"
    / "wake"
    / "chrome_extension_ts"
)


def test_wake_extension_has_minimal_permissions_and_loopback_control_boundary() -> None:
    manifest = json.loads(
        (_EXTENSION_ROOT / "manifest.json").read_text(encoding="utf-8")
    )

    assert manifest["manifest_version"] == 3
    assert manifest["version"] == "0.1.29"
    assert set(manifest["permissions"]) == {
        "storage",
        "activeTab",
        "scripting",
        "alarms",
    }
    assert set(manifest["host_permissions"]) == {
        "http://127.0.0.1:8790/*",
        "https://chatgpt.com/*",
    }
    assert manifest["background"]["service_worker"] == "service_worker.js"


def test_service_worker_imported_scripts_are_packaged_and_parseable() -> None:
    service_worker = (_EXTENSION_ROOT / "service_worker.js").read_text(encoding="utf-8")
    imported_names = tuple(
        name
        for call in re.findall(r"importScripts\(([^)]+)\);", service_worker)
        for name in re.findall(r'"([^"]+\.js)"', call)
    )

    assert imported_names == (
        "runtime_conversation_identity.js",
        "runtime_tab_reopen_policy.js",
        "persistent_worker_runtime_policy.js",
        "worker_provisioning_adapter.js",
    )
    node = shutil.which("node")
    for name in imported_names:
        imported_path = _EXTENSION_ROOT / name
        assert imported_path.is_file(), f"missing service-worker import: {name}"
        if node is not None:
            completed = subprocess.run(
                (node, "--check", str(imported_path)),
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert completed.returncode == 0, completed.stdout + completed.stderr


def test_persistent_worker_typescript_source_matches_packaged_javascript(
    tmp_path: Path,
) -> None:
    tsc = shutil.which("tsc")
    if tsc is None:
        return
    completed = subprocess.run(
        (
            tsc,
            "-p",
            str(_TYPESCRIPT_ROOT / "tsconfig.json"),
            "--outDir",
            str(tmp_path),
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    generated = (tmp_path / "persistent_worker_runtime_policy.js").read_text(
        encoding="utf-8"
    )
    packaged = (_EXTENSION_ROOT / "persistent_worker_runtime_policy.js").read_text(
        encoding="utf-8"
    )
    assert generated == packaged


def test_browser_wake_payload_contains_only_opaque_routing_identity() -> None:
    manifest = json.loads(
        (_EXTENSION_ROOT / "manifest.json").read_text(encoding="utf-8")
    )
    content = (_EXTENSION_ROOT / "content.js").read_text(encoding="utf-8")
    service_worker = (_EXTENSION_ROOT / "service_worker.js").read_text(encoding="utf-8")
    provisioning = (_EXTENSION_ROOT / "worker_provisioning_adapter.js").read_text(
        encoding="utf-8"
    )
    project_provisioning_content = (
        _EXTENSION_ROOT / "project_bound_provisioning_content.js"
    ).read_text(encoding="utf-8")
    conversation_identity = (
        _EXTENSION_ROOT / "runtime_conversation_identity.js"
    ).read_text(encoding="utf-8")
    reopen_policy = (_EXTENSION_ROOT / "runtime_tab_reopen_policy.js").read_text(
        encoding="utf-8"
    )
    browser_source = (
        f"{content}\n{service_worker}\n{provisioning}\n"
        f"{project_provisioning_content}\n{conversation_identity}\n{reopen_policy}"
    ).lower()

    assert "@testcodex" not in content.lower()
    assert "appIdentity" in content
    assert "appMentionText" in content
    assert "작업해" in content
    assert "작업 계속해" in content
    assert "검수해" in content
    assert "검수 계속해" in content
    assert "wake_target_id" in content
    assert "wake_revision:" in content
    assert "현재 배정을 조회하기 위한 work 호출만 승인합니다" in content
    assert "외부 프로덕션 작업의 별도 승인을 뜻하지 않습니다" in content
    assert "위험을 확인했으며 실행을 승인합니다" not in content
    assert "main_target_id" in content
    assert "main_wake_revision:" in content
    assert "__chatToCodexWakeLoaded" in content
    assert "chrome.alarms" in service_worker
    assert "runtime-heartbeat" in service_worker
    assert "chrome.tabs.create" in service_worker
    assert "chrome.tabs.remove" in service_worker
    assert "chrome.tabs.onActivated" in service_worker
    assert "tab.active || tab.pinned" in service_worker
    assert "runtime:canPark" in content
    assert "runtime:consumeRegistrationDraft" in content
    assert "runtime:canPark" in service_worker
    assert "invalidateExtensionContext" in content
    assert "clearInterval(wakePollTimer)" in content
    popup = (_EXTENSION_ROOT / "popup.js").read_text(encoding="utf-8")
    popup_html = (_EXTENSION_ROOT / "popup.html").read_text(encoding="utf-8")
    assert "chrome.scripting.executeScript" in popup
    assert "consumeRegistrationDraft" in popup
    assert "kindElement" not in popup
    assert 'value="worker"' not in popup_html
    assert 'kind: "main"' in popup
    register_start = popup.index("async function registerCurrentChat")
    register_end = popup.index("function renderProjects")
    popup_registration = popup[register_start:register_end]
    assert 'kind: "worker"' not in popup_registration
    assert "자동 등록된 Worker" in popup
    assert "wake:markResumeAttempt" in service_worker
    assert "wake:markMainResumeAttempt" in service_worker
    assert "chat-heartbeat" in service_worker
    assert "worker_provisioning_adapter.js" in service_worker
    assert "processNextAdmittedWorkerProvision" in service_worker
    assert "WORKER_PROVISIONING_ENDPOINTS" in provisioning
    assert 'next: "/api/worker-provisioning/next"' in provisioning
    assert "reserveRuntimeOpenAdmission" in provisioning
    assert "recordRuntimeOpen" in provisioning
    assert "releaseRuntimeOpenAdmission" in provisioning
    assert "workerProvisioningInFlight" in provisioning
    assert "Promise.all" not in provisioning
    assert "explicitProjectBoundProvisioningPlan" in provisioning
    assert "unsupported_project_identity" in provisioning
    assert "provision:openProjectNewChat" in provisioning
    assert "provision:bootstrap" in provisioning
    assert "provision:providerState" in provisioning
    assert "provider_rate_limited" in provisioning
    assert "MIN_PROVIDER_RATE_LIMIT_BACKOFF_MS = 30_000" in provisioning
    assert "MAX_PROVIDER_RATE_LIMIT_BACKOFF_MS = 120_000" in provisioning
    assert "conversation_id" in provisioning
    assert "wake_target_id" in provisioning
    assert "admission_revision" in provisioning
    assert (
        "project_bound_provisioning_content.js" in manifest["content_scripts"][0]["js"]
    )
    assert "bootstrapProvisionedWorker" in content
    assert "@testcodex" not in content.lower()
    assert "app_identity_unavailable" in content
    assert "composer_not_safe" in content
    assert "composer_changed" in content
    assert "generation_in_progress" in content
    assert "hasProviderRateLimitSignal" in content
    bootstrap_start = content.index("async function bootstrapProvisionedWorker")
    bootstrap_end = content.index("function findComposer")
    bootstrap_body = content[bootstrap_start:bootstrap_end]
    assert "작업해" not in bootstrap_body
    assert "wake_target_id" not in bootstrap_body
    assert "PROJECT_NEW_CHAT_SELECTORS" in project_provisioning_content
    assert "isExplicitProjectContext" in project_provisioning_content
    for forbidden in (
        "broker_task_id",
        "assigned_task_id",
        "run_id",
        "workflow_id",
        "workflow_handle",
        "depends_on",
        "objective",
        "worker_handle",
    ):
        assert forbidden not in browser_source


def test_runtime_admission_policy_prevents_sweep_reopen_storms() -> None:
    service_worker = (_EXTENSION_ROOT / "service_worker.js").read_text(encoding="utf-8")
    conversation_identity = (
        _EXTENSION_ROOT / "runtime_conversation_identity.js"
    ).read_text(encoding="utf-8")
    reopen_policy = (_EXTENSION_ROOT / "runtime_tab_reopen_policy.js").read_text(
        encoding="utf-8"
    )

    assert "chatConversationId" in conversation_identity
    assert 'segments.lastIndexOf("c")' in conversation_identity
    assert "MAX_RUNTIME_OPEN_IN_FLIGHT = 1" in reopen_policy
    assert "MIN_RUNTIME_OPEN_INTERVAL_MS = 12_000" in reopen_policy
    assert "runtimeOpeningConversationIds" in reopen_policy
    assert "runtimeOpenInFlightCount" in reopen_policy
    assert "runtimeTargetIdentity" in reopen_policy
    assert "hasRuntimeRecoveryAdmission" in reopen_policy
    assert "runtimeRegistrationBindingMatches" in reopen_policy
    assert "hasNewerRuntimeAdmission" in reopen_policy
    assert "runtimeTargetPriority" in service_worker
    assert "chatConversationId(conversationUrl)" in service_worker
    assert "expectedId === candidateId" in service_worker
    assert "runtimeOpeningConversationIds.has(conversationId)" in service_worker
    assert "MAX_RUNTIME_OPEN_IN_FLIGHT" in service_worker
    assert "MIN_RUNTIME_OPEN_INTERVAL_MS" in service_worker
    assert "RUNTIME_OPEN_ADMISSION_STORAGE_KEY" in service_worker
    assert "reserveRuntimeOpenAdmission" in service_worker
    assert "recordRuntimeOpen" in service_worker
    assert "deliveryAdmissionForRuntimeTarget" in service_worker
    assert "dispatchBackendAuthorizedRuntimeAdmissions" in service_worker
    assert "deliverBackendAuthorizedRuntimeAdmission" in service_worker
    assert "currentRegistrationForAdmissionCandidate" in service_worker
    assert "currentRuntimeAdmissionForCandidate" in service_worker
    assert "updateRegistrationIfBindingMatches" in service_worker
    assert "registrationStorageKey" in service_worker
    assert "normalizeStoredRegistration" in service_worker
    assert "conversation:${candidate.conversationId}" in service_worker
    assert "conversationUrl" in service_worker
    assert "restoreRuntimeTargetRegistration" not in service_worker

    sweep_start = service_worker.index("async function sweepRuntimeTargets")
    sweep_end = service_worker.index("async function pollRuntimeTarget")
    sweep_body = service_worker[sweep_start:sweep_end]
    assert "ensureConversationTab" not in sweep_body
    assert "chrome.tabs.create" not in sweep_body
    assert "dispatchBackendAuthorizedRuntimeAdmissions" in sweep_body

    admission_start = service_worker.index(
        "async function deliverBackendAuthorizedRuntimeAdmission"
    )
    admission_end = service_worker.index("async function reconcileRuntimeTab")
    admission_body = service_worker[admission_start:admission_end]
    assert "currentRuntimeAdmissionForCandidate(candidate)" in admission_body
    assert "recordRuntimeDeliveryAdmission" in admission_body
    assert "admissionBeforeTabAccess" in admission_body
    assert "admissionBeforeRecord" in admission_body

    ensure_start = service_worker.index("async function ensureConversationTab")
    ensure_end = service_worker.index("async function reserveRuntimeOpenAdmission")
    ensure_body = service_worker[ensure_start:ensure_end]
    assert "candidate" in ensure_body
    assert "admissionAfterLocatedTabLookup" in ensure_body
    assert "admissionAfterFinalTabLookup" in ensure_body
    assert "currentRuntimeAdmissionForCandidate(candidate)" in ensure_body
    assert ensure_body.index("admissionAfterFinalTabLookup") < ensure_body.index(
        "chrome.tabs.create"
    )

    admission_context_start = service_worker.index(
        "async function currentRuntimeAdmissionForCandidate"
    )
    admission_context_end = service_worker.index(
        "function registrationMatchesAdmissionCandidate"
    )
    admission_context = service_worker[admission_context_start:admission_context_end]
    assert "registrationBeforeTargetRead" in admission_context
    assert "readRuntimeTarget(registrationBeforeTargetRead)" in admission_context
    assert "latestRegistration" in admission_context
    assert "runtimeRegistrationBindingMatches" in admission_context
    assert "registration: latestRegistration" in admission_context

    policy_start = reopen_policy.index("function hasNewerRuntimeAdmission")
    policy_end = reopen_policy.index("function hasRuntimeRecoveryAdmission")
    monotonic_policy = reopen_policy[policy_start:policy_end]
    assert "lastAdmittedWakeRevision" in monotonic_policy
    assert "> identity?.wakeRevision" in monotonic_policy

    recovery_start = service_worker.index("function deliveryAdmissionForRuntimeTarget")
    recovery_end = service_worker.index(
        "async function dispatchBackendAuthorizedRuntimeAdmissions"
    )
    recovery_body = service_worker[recovery_start:recovery_end]
    assert 'target.status === "busy"' in recovery_body
    assert 'target.status === "active"' in recovery_body
    assert "hasRuntimeRecoveryAdmission" in recovery_body

    register_start = service_worker.index("async function registerTarget")
    register_end = service_worker.index("async function registerWorkerTarget")
    register_body = service_worker[register_start:register_end]
    assert "sweepRuntimeTargets()" not in register_body
    assert "pollRuntimeTarget(registration)" in register_body


def test_main_registration_binds_only_from_explicit_project_conversation_route() -> (
    None
):
    popup = (_EXTENSION_ROOT / "popup.js").read_text(encoding="utf-8")
    popup_html = (_EXTENSION_ROOT / "popup.html").read_text(encoding="utf-8")
    conversation_identity = (
        _EXTENSION_ROOT / "runtime_conversation_identity.js"
    ).read_text(encoding="utf-8")
    service_worker = (_EXTENSION_ROOT / "service_worker.js").read_text(encoding="utf-8")

    assert "explicitChatGPTProjectContext" in popup
    assert "chatGPTProjectConversationContext" in popup
    assert 'segments[0] !== "g"' in conversation_identity
    assert 'tail[0] === "c"' in conversation_identity
    assert 'src="runtime_conversation_identity.js"' in popup_html
    assert "...conversationProjectContext" in popup
    assert "explicitChatGPTProjectContext" in service_worker
    assert (
        "Main registration requires a current ChatGPT Project conversation"
        in service_worker
    )
    assert 'method: "PUT"' in service_worker
    assert "/api/project-bindings/" in service_worker
    assert "provider: projectContext.provider" in service_worker
    assert "project_ref: projectContext.projectRef" in service_worker
    assert "project_path: projectContext.projectPath" in service_worker
    assert "observed_at" in service_worker

    register_start = service_worker.index("async function registerTarget")
    register_end = service_worker.index("async function registerWorkerTarget")
    register_body = service_worker[register_start:register_end]
    assert register_body.index("bindProjectBinding") < register_body.index(
        "registerMainTarget"
    )
    assert "project_root" not in register_body
    assert "broker_task_id" not in register_body
    assert "run_id" not in register_body
