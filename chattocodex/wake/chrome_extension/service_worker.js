importScripts("runtime_conversation_identity.js", "runtime_tab_reopen_policy.js");
importScripts("worker_provisioning_adapter.js");

const CONTROL_ORIGIN = "http://127.0.0.1:8790";
const STORAGE_KEY = "wakeTargets";
const WAKE_CONTENT_DISPOSITION_STORAGE_KEY = "wakeContentDisposition";
const RUNTIME_OPEN_ADMISSION_STORAGE_KEY = "runtimeOpenAdmission";
const RUNTIME_SWEEP_ALARM = "chattocodex-runtime-sweep";
const RUNTIME_SWEEP_PERIOD_MINUTES = 0.5;
const PARK_IDLE_DELAY_MS = 30000;
const WAKE_ADAPTER_VERSION = chrome.runtime.getManifest().version;
let runtimeSweepInFlight = false;
let pendingWorkerRetirementAdmissionGeneration = 0;
// Chrome storage writes this registration map as one value. Keep every
// read/modify/write transaction in one local queue so updates for unrelated
// conversations cannot overwrite one another with stale map snapshots.
let registrationStorageMutationTail = Promise.resolve();
let wakeContentDispositionMutationTail = Promise.resolve();
let pendingWorkerRetirementDrain = null;
const runtimeCreatedTabIds = new Set();
const runtimeCreatedTabsEverClaimed = new Set();
const pendingFinalSendAdmissions = new Map();
const WAKE_SEND_FAILURE_REASONS = new Set([
  "initial_guard_rejected",
  "initial_generating",
  "prepare_rejected",
  "composer_missing",
  "durable_reserve_rejected",
  "reselection_precondition_rejected",
  "mention_injection_failed",
  "mention_ownership_rejected",
  "app_selection_rejected",
  "post_200_generating",
  "post_200_unclaimed",
  "post_200_ownership_rejected",
  "marker_insertion_failed",
  "send_button_absent",
  "pre_confirm_generating",
  "pre_confirm_unclaimed",
  "pre_confirm_ownership_rejected",
  "confirm_rejected",
  "final_url_changed",
  "final_unclaimed",
  "final_composer_changed",
  "final_raw_changed",
  "final_app_changed",
  "final_button_changed",
  "final_generating",
  "final_wake_ownership_rejected",
]);
const DURABLE_RESERVE_REJECTION_STAGES = new Set([
  "pre_candidate",
  "background_admission",
  "background_identity",
  "background_registration",
  "background_draft_capability",
  "background_tab",
  "background_proof",
  "background_snapshot",
  "post_unclaimed",
  "post_composer",
  "post_raw",
  "post_generating",
  "post_candidate",
]);
const PRE_CANDIDATE_REJECTION_SUBSTAGES = new Set([
  "document_claimed",
  "composer_changed",
  "provider_draft_mismatch",
  "empty_without_selected_app",
]);
const WAKE_CONTENT_DISPOSITION_RANKS = Object.freeze({
  poll_received: 1,
  exact_current_entry: 2,
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  void handleMessage(message, sender)
    .then((payload) => sendResponse({ ok: true, ...payload }))
    .catch((error) =>
      sendResponse({
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      }),
    );
  return true;
});

chrome.runtime.onInstalled.addListener(() => {
  void bootstrapRuntimeSweep();
});

chrome.runtime.onStartup.addListener(() => {
  void bootstrapRuntimeSweep();
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === RUNTIME_SWEEP_ALARM) {
    void sweepRuntimeTargets();
  }
});

chrome.tabs.onActivated.addListener(({ tabId }) => {
  revokeFinalSendAdmissionsForTab(tabId);
  noteProvisioningTabActivation(tabId);
  noteRuntimeCreatedTabActivation(tabId);
  void transferRuntimeOwnership(tabId);
});

chrome.tabs.onUpdated?.addListener((tabId, changeInfo, tab) => {
  if (
    typeof changeInfo.url === "string" ||
    changeInfo.status === "loading" ||
    changeInfo.pinned === true ||
    changeInfo.active === true ||
    tab?.pinned ||
    tab?.active
  ) {
    revokeFinalSendAdmissionsForTab(tabId);
  }
});

chrome.tabs.onRemoved.addListener((tabId) => {
  void clearRemovedRuntimeTab(tabId);
});

void bootstrapRuntimeSweep();

async function handleMessage(message, sender = {}) {
  switch (message?.type) {
    case "wake:listProjects":
      return listProjects();
    case "wake:getRegistration":
      return getRegistration(message.conversationUrl);
    case "wake:register":
      return registerTarget({
        conversationUrl: message.conversationUrl,
        projectContext: message.projectContext,
        projectId: message.projectId,
        projectName: message.projectName,
        kind: message.kind,
      });
    case "wake:poll":
      return pollTarget(message.conversationUrl, sender.tab);
    case "wake:recordContentDisposition":
      return recordWakeContentDisposition({
        senderTab: sender.tab,
        conversationUrl: message.conversationUrl,
        delivery: message.delivery,
        code: message.code,
      });
    case "wake:markAttempt":
      return markAttempt({
        conversationUrl: message.conversationUrl,
        targetId: message.targetId,
        wakeRevision: message.wakeRevision,
      });
    case "wake:markResumeAttempt":
      return markResumeAttempt({
        conversationUrl: message.conversationUrl,
        wakeTargetId: message.wakeTargetId,
      });
    case "wake:markMainResumeAttempt":
      return markMainResumeAttempt({
        conversationUrl: message.conversationUrl,
        mainTargetId: message.mainTargetId,
      });
    case "wake:prepareFinalSend":
      return prepareFinalSendAdmission({
        tabId: sender.tab?.id,
        conversationUrl: message.conversationUrl,
        delivery: message.delivery,
      });
    case "wake:confirmFinalSend":
      return confirmFinalSendAdmission({
        tabId: sender.tab?.id,
        conversationUrl: message.conversationUrl,
        admissionToken: message.admissionToken,
        delivery: message.delivery,
      });
    case "wake:recordSendFailure":
      return recordWakeSendFailure({
        tabId: sender.tab?.id,
        conversationUrl: message.conversationUrl,
        delivery: message.delivery,
        reason: message.reason,
        rejectionStage: message.rejectionStage,
        rejectionSubstage: message.rejectionSubstage,
      });
    case "runtime:consumeOwnedRegistrationDraft":
      return consumeRuntimeOwnedRegistrationDraft({
        tabId: sender.tab?.id,
        conversationUrl: message.conversationUrl,
        appIdentity: message.appIdentity,
        draft: message.draft,
        admissionToken: message.admissionToken,
        delivery: message.delivery,
      });
    case "runtime:reserveExactCurrentWakeDraft":
      return reserveExactCurrentWakeDraft({
        tabId: sender.tab?.id,
        conversationUrl: message.conversationUrl,
        appIdentity: message.appIdentity,
        draft: message.draft,
        admissionToken: message.admissionToken,
        delivery: message.delivery,
      });
    case "runtime:cleanupStaleWakeDraft":
      return cleanupStaleRuntimeWakeDraft({
        tabId: sender.tab?.id,
        conversationUrl: message.conversationUrl,
        appIdentity: message.appIdentity,
        draft: message.draft,
        admissionToken: message.admissionToken,
        delivery: message.delivery,
      });
    case "provision:confirmBootstrapSubmit":
      return confirmProvisioningBootstrapSubmit({
        tabId: sender.tab?.id,
        bootstrapToken: message.bootstrapToken,
      });
    case "provision:confirmProjectOpen":
      return confirmProvisioningProjectOpen({
        tabId: sender.tab?.id,
        ownershipToken: message.ownershipToken,
      });
    default:
      throw new Error("Unsupported runtime wake adapter message.");
  }
}

async function bootstrapRuntimeSweep() {
  await ensureRuntimeSweepAlarm();
  await sweepRuntimeTargets();
}

async function ensureRuntimeSweepAlarm() {
  const existing = await chrome.alarms.get(RUNTIME_SWEEP_ALARM);
  if (existing) {
    return;
  }
  await chrome.alarms.create(RUNTIME_SWEEP_ALARM, {
    delayInMinutes: RUNTIME_SWEEP_PERIOD_MINUTES,
    periodInMinutes: RUNTIME_SWEEP_PERIOD_MINUTES,
  });
}

async function sweepRuntimeTargets() {
  if (runtimeSweepInFlight) {
    return;
  }
  runtimeSweepInFlight = true;
  try {
    // Pending Worker retirement is a fail-closed admission barrier. Retire all
    // exact targets project-by-project before reading any delivery candidates
    // or asking the provisioning adapter for work that could create a tab.
    if (!(await resumePendingWorkerRetirements())) {
      return;
    }
    const registrations = await readAllRegistrations();
    const admissionCandidates = [];
    for (const rawRegistration of Object.values(registrations)) {
      let registration = normalizeRegistration(rawRegistration);
      const conversationUrl = registration?.conversationUrl;
      if (!registration || !conversationUrl) {
        continue;
      }
      try {
        if (registration.retirementPending) {
          return;
        }
        registration = await syncMainRegistrationFromBinding(registration);
        if (!registration) {
          continue;
        }
        const target = await pollRuntimeTarget(registration);
        registration = await syncWorkerRegistrationFromBinding(
          registration,
          target,
        );
        if (!registration) {
          continue;
        }
        // A normal sweep is deliberately limited to presence/state sync and
        // safe parking; it only collects explicit backend delivery candidates.
        await reconcileRuntimeTab(conversationUrl, registration, target);
        const admission = deliveryAdmissionForRuntimeTarget(registration, target);
        if (admission) {
          admissionCandidates.push({
            conversationUrl,
            registration,
            target,
            admission,
          });
        }
      } catch (_error) {
        // Unknown/stale local registrations are inert. In particular, do not
        // recreate a backend target merely because it remains in local storage.
      }
    }

    admissionCandidates.sort((left, right) => {
      const priorityDelta =
        runtimeTargetPriority(left.target) - runtimeTargetPriority(right.target);
      if (priorityDelta !== 0) {
        return priorityDelta;
      }
      return left.conversationUrl.localeCompare(right.conversationUrl);
    });

    if (await hasPendingWorkerRetirements()) {
      return;
    }
    await dispatchBackendAuthorizedRuntimeAdmissions(admissionCandidates);
    // This is not registration-driven reopening: one separate server-admitted
    // provisioning request may be consumed after the regular presence sweep.
    // The adapter shares the same global open reservation as wake recovery.
    if (!(await hasPendingWorkerRetirements())) {
      await processNextAdmittedWorkerProvision();
    }
  } finally {
    runtimeSweepInFlight = false;
  }
}

async function pollRuntimeTarget(registration) {
  return readRuntimeTarget(registration);
}

async function readRuntimeTarget(registration) {
  if (registration.kind === "main") {
    const payload = await requestJson(
      `/api/main-runtime/review-targets/${registration.mainTargetId}`,
    );
    return payload.target;
  }
  const payload = await requestJson(
    `/api/wake/targets/${registration.wakeTargetId}/runtime-heartbeat`,
    { method: "POST" },
  );
  return payload.target;
}

function deliveryAdmissionForRuntimeTarget(registration, target) {
  if (!target || typeof target.status !== "string") {
    return null;
  }
  const identity = runtimeTargetIdentity(registration, target);
  if (!isRuntimeTargetBoundToRegistration(registration, target, identity)) {
    return null;
  }
  if (hasNewerRuntimeAdmission(registration, identity)) {
    return null;
  }
  if (target.status === "waking") {
    // One backend delivery epoch is admitted at most once. A retry requires a
    // fresh backend-selected wake revision after the durable lease changes.
    if (registration.lastAdmittedDeliveryKey === identity.key) {
      return null;
    }
    return { kind: "wake", identity };
  }
  if (
    target.status === "busy" &&
    identity.kind === "worker" &&
    hasRuntimeRecoveryAdmission(registration, identity)
  ) {
    return { kind: "worker-recovery", identity };
  }
  if (
    target.status === "active" &&
    identity.kind === "main" &&
    hasRuntimeRecoveryAdmission(registration, identity)
  ) {
    return { kind: "main-recovery", identity };
  }
  return null;
}

async function dispatchBackendAuthorizedRuntimeAdmissions(candidates) {
  for (const candidate of candidates) {
    const outcome = await deliverBackendAuthorizedRuntimeAdmission(candidate);
    // A single background opening attempt is the upper bound for one dispatch.
    // The global lock and persisted cooldown also protect concurrent dispatches.
    if (outcome.openAttempted) {
      return;
    }
  }
}

async function deliverBackendAuthorizedRuntimeAdmission(candidate) {
  try {
    // Candidate collection is only a hint. Rebinds are legal, so every
    // side-effect path must use the current registration, never that snapshot.
    const currentAdmissionContext =
      await currentRuntimeAdmissionForCandidate(candidate);
    if (!currentAdmissionContext) {
      return { openedNewTab: false, openAttempted: false };
    }

    // The target read is asynchronous; recheck the binding again before any
    // Chrome tab lookup or creation can observe a now-rebound registration.
    const admissionBeforeTabAccess =
      await currentRuntimeAdmissionForCandidate(candidate);
    if (!admissionBeforeTabAccess) {
      return { openedNewTab: false, openAttempted: false };
    }
    const outcome = await ensureConversationTab(
      candidate.conversationUrl,
      admissionBeforeTabAccess.registration,
      candidate,
    );
    if (outcome.admitted) {
      const admissionBeforeRecord =
        await currentRuntimeAdmissionForCandidate(candidate);
      if (!admissionBeforeRecord) {
        return outcome;
      }
      await recordRuntimeDeliveryAdmission(
        candidate.conversationUrl,
        admissionBeforeRecord.registration,
        admissionBeforeRecord.admission.identity,
      );
    }
    return outcome;
  } catch (error) {
    return {
      openedNewTab: false,
      openAttempted: error?.runtimeOpenAttempted === true,
    };
  }
}

async function currentRegistrationForAdmissionCandidate(candidate) {
  const registration = normalizeRegistration(
    await readRegistration(candidate.conversationUrl),
  );
  return registrationMatchesAdmissionCandidate(registration, candidate)
    ? registration
    : null;
}

async function currentRuntimeAdmissionForCandidate(candidate) {
  const storedRegistrationBeforeTargetRead =
    await currentRegistrationForAdmissionCandidate(candidate);
  const registrationBeforeTargetRead = await syncMainRegistrationFromBinding(
    storedRegistrationBeforeTargetRead,
  );
  if (!registrationBeforeTargetRead) {
    return null;
  }
  const target = await readRuntimeTarget(registrationBeforeTargetRead);
  const registrationAfterWorkerBindingRefresh =
    await syncWorkerRegistrationFromBinding(
      registrationBeforeTargetRead,
      target,
    );
  if (!registrationAfterWorkerBindingRefresh) {
    return null;
  }
  // A target read awaits the backend. Rebinds during that await must not be
  // carried forward into a tab side effect under the earlier registration.
  const latestRegistration = await currentRegistrationForAdmissionCandidate(
    candidate,
  );
  if (
    !latestRegistration ||
    !runtimeRegistrationBindingMatches(
      latestRegistration,
      registrationAfterWorkerBindingRefresh,
    )
  ) {
    return null;
  }
  const admission = deliveryAdmissionForRuntimeTarget(
    latestRegistration,
    target,
  );
  if (!admissionMatchesCandidate(admission, candidate)) {
    return null;
  }
  return { registration: latestRegistration, target, admission };
}

function registrationMatchesAdmissionCandidate(registration, candidate) {
  const identity = candidate?.admission?.identity;
  const registrationTargetId =
    registration?.kind === "main"
      ? registration.mainTargetId
      : registration?.wakeTargetId;
  return Boolean(
    identity &&
      runtimeRegistrationBindingMatches(registration, candidate?.registration) &&
      registration?.kind === identity.kind &&
      registrationTargetId === identity.targetId,
  );
}

function admissionMatchesCandidate(admission, candidate) {
  return (
    admission?.identity?.key === candidate?.admission?.identity?.key &&
    admission?.kind === candidate?.admission?.kind
  );
}

async function reconcileRuntimeTab(conversationUrl, registration, target) {
  if (!target || typeof target.status !== "string") {
    return;
  }
  if (target.status === "idle") {
    await parkRuntimeOwnedTabWhenSafe(conversationUrl, registration);
    return;
  }
  if (registration.runtimeOpenedTabId) {
    await updateRegistration(conversationUrl, { idleSince: 0 });
  }
}

async function ensureConversationTab(conversationUrl, registration, candidate) {
  const conversationId = chatConversationId(conversationUrl);
  if (!conversationId) {
    throw new Error("Registered Chat URL is missing a persistent conversation ID.");
  }

  const located = await findConversationTab(
    conversationUrl,
    registration.runtimeOpenedTabId,
  );
  const admissionAfterLocatedTabLookup =
    await currentRuntimeAdmissionForCandidate(candidate);
  if (!admissionAfterLocatedTabLookup) {
    return { openedNewTab: false, openAttempted: false, admitted: false };
  }
  if (located.tab) {
    if (located.runtimeOwned) {
      const adapterReady = await ensureCurrentWakeAdapter(
        conversationUrl,
        admissionAfterLocatedTabLookup.registration,
        candidate,
        located,
      );
      if (!adapterReady) {
        return { openedNewTab: false, openAttempted: false, admitted: false };
      }
    }
    await reconcileLocatedConversationTab(
      conversationUrl,
      admissionAfterLocatedTabLookup.registration,
      located,
    );
    return {
      openedNewTab: false,
      openAttempted: false,
      admitted: true,
    };
  }
  if (runtimeOpeningConversationIds.has(conversationId)) {
    return { openedNewTab: false, openAttempted: false, admitted: false };
  }

  runtimeOpeningConversationIds.add(conversationId);
  let openReservation = null;
  let openAttempted = false;
  try {
    openReservation = await reserveRuntimeOpenAdmission();
    if (!openReservation) {
      return { openedNewTab: false, openAttempted: false, admitted: false };
    }
    const rechecked = await findConversationTab(
      conversationUrl,
      registration.runtimeOpenedTabId,
    );
    const admissionAfterFinalTabLookup =
      await currentRuntimeAdmissionForCandidate(candidate);
    if (!admissionAfterFinalTabLookup) {
      return { openedNewTab: false, openAttempted: false, admitted: false };
    }
    if (rechecked.tab) {
      if (rechecked.runtimeOwned) {
        const adapterReady = await ensureCurrentWakeAdapter(
          conversationUrl,
          admissionAfterFinalTabLookup.registration,
          candidate,
          rechecked,
        );
        if (!adapterReady) {
          return { openedNewTab: false, openAttempted: false, admitted: false };
        }
      }
      await reconcileLocatedConversationTab(
        conversationUrl,
        admissionAfterFinalTabLookup.registration,
        rechecked,
      );
      return {
        openedNewTab: false,
        openAttempted: false,
        admitted: true,
      };
    }

    if (!(await runtimeSideEffectsAllowedWithoutPendingRetirement())) {
      return { openedNewTab: false, openAttempted: false, admitted: false };
    }
    openAttempted = true;
    // No await is permitted between the global retirement barrier and the
    // first tab-creation side effect. A durable retirement started in any
    // conversation revokes this admission generation before Chrome can open.
    const created = await chrome.tabs.create({
      url: conversationUrl,
      active: false,
    });
    if (!Number.isInteger(created.id)) {
      throw new Error("Chrome did not return a tab ID for the reopened Chat.");
    }
    runtimeCreatedTabIds.add(created.id);
    const openedAt = Date.now();
    await recordRuntimeOpen(openedAt);
    const updated = await updateRegistrationIfBindingMatches(
      conversationUrl,
      admissionAfterFinalTabLookup.registration,
      {
        runtimeOpenedTabId: created.id,
        runtimeOpenedAt: openedAt,
        idleSince: 0,
      },
    );
    if (!updated) {
      await removeRuntimeCreatedTabWhenStillOwned(created, conversationUrl);
      return { openedNewTab: false, openAttempted: true, admitted: false };
    }
    runtimeCreatedTabIds.delete(created.id);
    runtimeCreatedTabsEverClaimed.delete(created.id);
    return {
      openedNewTab: true,
      openAttempted: true,
      admitted: true,
    };
  } catch (error) {
    if (error && typeof error === "object") {
      error.runtimeOpenAttempted = openAttempted;
    }
    throw error;
  } finally {
    if (openReservation) {
      releaseRuntimeOpenAdmission();
    }
    runtimeOpeningConversationIds.delete(conversationId);
  }
}

async function ensureCurrentWakeAdapter(
  conversationUrl,
  registration,
  candidate,
  located,
) {
  const tab = located?.tab;
  if (
    !located?.runtimeOwned ||
    !Number.isInteger(tab?.id) ||
    registration.runtimeOpenedTabId !== tab.id ||
    tab.active !== false ||
    tab.pinned !== false ||
    !tabMatchesConversation(tab, conversationUrl)
  ) {
    return false;
  }
  const currentDiagnosis = await diagnoseWakeAdapter(tab.id);
  if (wakeAdapterDiagnosisIsCurrent(currentDiagnosis)) {
    return true;
  }
  if (currentDiagnosis?.everClaimed === true) {
    return false;
  }
  const admissionBeforeInjection =
    await currentRuntimeAdmissionForCandidate(candidate);
  let tabBeforeInjection;
  try {
    tabBeforeInjection = await chrome.tabs.get(tab.id);
  } catch (_error) {
    return false;
  }
  if (
    !admissionBeforeInjection ||
    admissionBeforeInjection.registration.runtimeOpenedTabId !== tab.id ||
    tabBeforeInjection.active !== false ||
    tabBeforeInjection.pinned !== false ||
    !tabMatchesConversation(tabBeforeInjection, conversationUrl)
  ) {
    return false;
  }
  await chrome.scripting.executeScript({
    target: { tabId: tab.id },
    func: () => {
      try {
        globalThis.__chatToCodexWakeAdapter?.dispose?.();
      } catch (_error) {
        // Invalidated adapters must not block replacement injection.
      }
      delete globalThis.__chatToCodexWakeAdapter;
      globalThis.__chatToCodexWakeLoaded = false;
    },
  });
  await chrome.scripting.executeScript({
    target: { tabId: tab.id },
    files: ["content.js"],
  });
  const diagnosisAfterInjection = await diagnoseWakeAdapter(tab.id);
  const admissionAfterInjection =
    await currentRuntimeAdmissionForCandidate(candidate);
  let tabAfterInjection;
  try {
    tabAfterInjection = await chrome.tabs.get(tab.id);
  } catch (_error) {
    return false;
  }
  return Boolean(
    wakeAdapterDiagnosisIsCurrent(diagnosisAfterInjection) &&
      admissionAfterInjection &&
      admissionAfterInjection.registration.runtimeOpenedTabId === tab.id &&
      tabAfterInjection.active === false &&
      tabAfterInjection.pinned === false &&
      tabMatchesConversation(tabAfterInjection, conversationUrl)
  );
}

async function diagnoseWakeAdapter(tabId) {
  try {
    return await chrome.tabs.sendMessage(tabId, {
      type: "runtime:diagnoseWakeAdapter",
    });
  } catch (_error) {
    return null;
  }
}

function wakeAdapterDiagnosisIsCurrent(diagnosis) {
  return Boolean(
    diagnosis?.version === WAKE_ADAPTER_VERSION &&
      diagnosis.extensionContextAvailable === true &&
      diagnosis.pollTimerActive === true &&
      diagnosis.everClaimed === false
  );
}

function noteRuntimeCreatedTabActivation(tabId) {
  if (runtimeCreatedTabIds.has(tabId)) {
    runtimeCreatedTabsEverClaimed.add(tabId);
  }
}

async function removeRuntimeCreatedTabWhenStillOwned(created, conversationUrl) {
  try {
    const current = await chrome.tabs.get(created.id);
    if (
      runtimeCreatedTabsEverClaimed.has(created.id) ||
      current.id !== created.id ||
      current.active !== false ||
      current.pinned !== false ||
      normalizedChatConversationUrl(current.url || current.pendingUrl || "") !==
        normalizedChatConversationUrl(conversationUrl)
    ) {
      return false;
    }
    await chrome.tabs.remove(created.id);
    return true;
  } catch (_error) {
    return false;
  } finally {
    runtimeCreatedTabIds.delete(created.id);
    runtimeCreatedTabsEverClaimed.delete(created.id);
  }
}

async function reserveRuntimeOpenAdmission() {
  if (runtimeOpenInFlightCount >= MAX_RUNTIME_OPEN_IN_FLIGHT) {
    return null;
  }
  runtimeOpenInFlightCount += 1;
  let reserved = false;
  try {
    const stored = await chrome.storage.local.get(
      RUNTIME_OPEN_ADMISSION_STORAGE_KEY,
    );
    const persistedLastOpenedAt = Number(
      stored[RUNTIME_OPEN_ADMISSION_STORAGE_KEY]?.lastOpenedAt || 0,
    );
    const lastOpenedAt = Math.max(runtimeLastOpenedAt, persistedLastOpenedAt);
    if (
      lastOpenedAt > 0 &&
      Date.now() - lastOpenedAt < MIN_RUNTIME_OPEN_INTERVAL_MS
    ) {
      return null;
    }
    reserved = true;
    return { reservedAt: Date.now() };
  } finally {
    if (!reserved) {
      runtimeOpenInFlightCount -= 1;
    }
  }
}

function releaseRuntimeOpenAdmission() {
  runtimeOpenInFlightCount = Math.max(0, runtimeOpenInFlightCount - 1);
}

async function recordRuntimeOpen(openedAt) {
  runtimeLastOpenedAt = openedAt;
  await chrome.storage.local.set({
    [RUNTIME_OPEN_ADMISSION_STORAGE_KEY]: { lastOpenedAt: openedAt },
  });
}

async function recordRuntimeDeliveryAdmission(
  conversationUrl,
  expectedRegistration,
  identity,
) {
  return updateRegistrationIfBindingMatches(
    conversationUrl,
    expectedRegistration,
    {
      lastAdmittedDeliveryKey: identity.key,
      lastAdmittedWakeRevision: identity.wakeRevision,
      lastAdmittedAt: Date.now(),
      idleSince: 0,
    },
    { rejectNewerAdmissionThan: identity.wakeRevision },
  );
}

async function reconcileLocatedConversationTab(
  conversationUrl,
  registration,
  located,
) {
  const { tab } = located;
  if (
    registration.runtimeOpenedTabId &&
    tab.id !== registration.runtimeOpenedTabId
  ) {
    await retireProvisionedWorkerOrClearOwnership(
      registration,
      "runtime_conversation_tab_rebound",
    );
  }
  if (located.runtimeOwned && tab.discarded && Number.isInteger(tab.id)) {
    await chrome.tabs.reload(tab.id);
  }
}

async function findConversationTab(conversationUrl, preferredTabId) {
  if (Number.isInteger(preferredTabId)) {
    try {
      const preferred = await chrome.tabs.get(preferredTabId);
      if (tabMatchesConversation(preferred, conversationUrl)) {
        return { tab: preferred, runtimeOwned: true };
      }
    } catch (_error) {
      // The runtime-owned tab may have been closed manually.
    }
  }

  const tabs = await chrome.tabs.query({ url: "https://chatgpt.com/*" });
  const matching = tabs.find((tab) => tabMatchesConversation(tab, conversationUrl));
  return { tab: matching || null, runtimeOwned: false };
}

function tabMatchesConversation(tab, conversationUrl) {
  const expectedId = chatConversationId(conversationUrl);
  const candidateId = chatConversationId(tab?.url || tab?.pendingUrl || "");
  return Boolean(expectedId && candidateId && expectedId === candidateId);
}

async function parkRuntimeOwnedTabWhenSafe(conversationUrl, registration) {
  const runtimeTabId = registration.runtimeOpenedTabId;
  if (!Number.isInteger(runtimeTabId)) {
    return;
  }

  let tab;
  try {
    tab = await chrome.tabs.get(runtimeTabId);
  } catch (_error) {
    await retireProvisionedWorkerOrClearOwnership(registration, "runtime_tab_missing");
    return;
  }
  if (!tabMatchesConversation(tab, conversationUrl)) {
    await retireProvisionedWorkerOrClearOwnership(
      registration,
      "runtime_tab_conversation_changed",
    );
    return;
  }
  if (tab.active || tab.pinned) {
    await retireProvisionedWorkerOrClearOwnership(registration, "runtime_tab_claimed");
    return;
  }

  const idleSince = Number(registration.idleSince || 0);
  if (!Number.isFinite(idleSince) || idleSince <= 0) {
    await updateRegistration(conversationUrl, { idleSince: Date.now() });
    return;
  }
  if (Date.now() - idleSince < PARK_IDLE_DELAY_MS) {
    return;
  }

  const canPark = tab.discarded ? true : await canParkRuntimeTab(runtimeTabId);
  if (!canPark) {
    return;
  }

  // canPark asks a content script and therefore yields. A person can activate
  // or pin the tab while that request is in flight, so use a fresh Chrome read
  // immediately before the destructive side effect.
  let tabBeforeRemove;
  try {
    tabBeforeRemove = await chrome.tabs.get(runtimeTabId);
  } catch (_error) {
    await retireProvisionedWorkerOrClearOwnership(registration, "runtime_tab_missing");
    return;
  }
  if (!tabMatchesConversation(tabBeforeRemove, conversationUrl)) {
    await retireProvisionedWorkerOrClearOwnership(
      registration,
      "runtime_tab_conversation_changed",
    );
    return;
  }
  if (tabBeforeRemove.active || tabBeforeRemove.pinned) {
    await retireProvisionedWorkerOrClearOwnership(registration, "runtime_tab_claimed");
    return;
  }

  try {
    // Do not await any storage operation between this final state read and
    // remove: that would recreate the user-activation race we just checked.
    await chrome.tabs.remove(runtimeTabId);
  } catch (_error) {
    await retireProvisionedWorkerOrClearOwnership(
      registration,
      "runtime_tab_remove_failed",
    );
    return;
  }
  await updateRegistration(conversationUrl, {
    runtimeOpenedTabId: null,
    runtimeOpenedAt: 0,
    idleSince: 0,
    lastParkedAt: Date.now(),
  });
}

async function canParkRuntimeTab(tabId) {
  try {
    const response = await chrome.tabs.sendMessage(tabId, {
      type: "runtime:canPark",
    });
    return response?.ok === true && response.canPark === true;
  } catch (_error) {
    return false;
  }
}

async function transferRuntimeOwnership(tabId) {
  if (!Number.isInteger(tabId)) {
    return;
  }
  const registrations = await readAllRegistrations();
  for (const rawRegistration of Object.values(registrations)) {
    const registration = normalizeRegistration(rawRegistration);
    if (
      registration?.runtimeOpenedTabId === tabId &&
      registration.conversationUrl
    ) {
      await retireProvisionedWorkerOrClearOwnership(registration, "runtime_tab_claimed");
    }
  }
}

async function clearRemovedRuntimeTab(tabId) {
  if (!Number.isInteger(tabId)) {
    return;
  }
  const registrations = await readAllRegistrations();
  for (const rawRegistration of Object.values(registrations)) {
    const registration = normalizeRegistration(rawRegistration);
    if (
      registration?.runtimeOpenedTabId === tabId &&
      registration.conversationUrl
    ) {
      await retireProvisionedWorkerOrClearOwnership(registration, "runtime_tab_removed");
    }
  }
}

async function consumeRuntimeOwnedRegistrationDraft({
  tabId,
  conversationUrl,
  appIdentity: rawAppIdentity,
  draft,
  admissionToken,
  delivery,
}) {
  const identity = normalizeFinalSendIdentity(conversationUrl, delivery);
  const pending = pendingFinalSendAdmissions.get(admissionToken);
  if (
    !Number.isInteger(tabId) ||
    !pending ||
    pending.confirming ||
    pending.tabId !== tabId ||
    pending.expiresAt < Date.now() ||
    pending.runtimeOwnedDraftReservation
  ) {
    return { safe: false, rejectionStage: "background_admission" };
  }
  if (!identity || !finalSendIdentityMatches(pending, identity)) {
    return { safe: false, rejectionStage: "background_identity" };
  }
  let expectedIdentity;
  try {
    expectedIdentity = requireProviderAppIdentity(rawAppIdentity);
    requireConversationUrl(conversationUrl);
  } catch (_error) {
    return { safe: false, rejectionStage: "background_identity" };
  }
  if (!(await finalSendIdentityIsCurrent(tabId, identity))) {
    return { safe: false, rejectionStage: "background_admission" };
  }
  const storageKey = registrationStorageKey(conversationUrl);
  const registration = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  const capability = registration?.runtimeOwnedRegistrationDraft;
  if (
    !registration ||
    registration.retirementPending ||
    registration.runtimeOpenedTabId !== tabId ||
    Number(registration.lastAttemptRevision || 0) !== 0 ||
    registration.appIdentity?.appRef !== expectedIdentity.appRef ||
    registration.appIdentity?.appLabel !== expectedIdentity.appLabel
  ) {
    return { safe: false, rejectionStage: "background_registration" };
  }
  const currentCapabilityMatches = Boolean(
    capability &&
      capability.tabId === tabId &&
      capability.conversationId === registration.conversationId &&
      runtimeOwnedAppDraftsMatch(capability.draft, draft, expectedIdentity) &&
      capability.appRef === expectedIdentity.appRef &&
      capability.appLabel === expectedIdentity.appLabel,
  );
  const strandedCapabilityFirstWakeRecovery = Boolean(
    !capability &&
      registration.kind === "worker" &&
      Number(registration.runtimeOpenedAt || 0) > 0 &&
      Number(registration.runtimeOwnedRegistrationDraftConsumedAt || 0) > 0 &&
      runtimeOwnedFirstWakeDraftMatches(draft, expectedIdentity),
  );
  if (!currentCapabilityMatches && !strandedCapabilityFirstWakeRecovery) {
    return { safe: false, rejectionStage: "background_draft_capability" };
  }
  let tab;
  try {
    tab = await chrome.tabs.get(tabId);
  } catch (_error) {
    return { safe: false, rejectionStage: "background_tab" };
  }
  if (
    tab.active !== false ||
    tab.pinned !== false ||
    !tabMatchesConversation(tab, conversationUrl)
  ) {
    return { safe: false, rejectionStage: "background_tab" };
  }
  if (!(await finalSendIdentityIsCurrent(tabId, identity))) {
    return { safe: false, rejectionStage: "background_admission" };
  }
  const reservation = {
    storageKey,
    conversationId: registration.conversationId,
    appRef: expectedIdentity.appRef,
    appLabel: expectedIdentity.appLabel,
    draft,
    stranded: strandedCapabilityFirstWakeRecovery,
    consumedAt: Number(
      registration.runtimeOwnedRegistrationDraftConsumedAt || 0,
    ),
    provisionedConnectorProof: {
      ...registration.provisionedConnectorProof,
    },
  };
  const proofMatches = await provisionedConnectorProofMatchesCurrentBinding(
    storageKey,
    expectedIdentity,
  );
  const latestRegistration = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  if (!proofMatches) {
    return { safe: false, rejectionStage: "background_proof" };
  }
  if (
    pendingFinalSendAdmissions.get(admissionToken) !== pending ||
    pending.runtimeOwnedDraftReservation
  ) {
    return { safe: false, rejectionStage: "background_admission" };
  }
  if (!registrationSnapshotsMatch(latestRegistration, registration)) {
    return { safe: false, rejectionStage: "background_snapshot" };
  }
  pending.runtimeOwnedDraftReservation = reservation;
  return { safe: true };
}

async function reserveExactCurrentWakeDraft({
  tabId,
  conversationUrl,
  appIdentity: rawAppIdentity,
  draft,
  admissionToken,
  delivery,
}) {
  const identity = normalizeFinalSendIdentity(conversationUrl, delivery);
  const pending = pendingFinalSendAdmissions.get(admissionToken);
  if (
    !Number.isInteger(tabId) ||
    !identity ||
    identity.kind !== "worker" ||
    identity.mode !== "wake" ||
    !pending ||
    pending.confirming ||
    pending.tabId !== tabId ||
    pending.expiresAt < Date.now() ||
    pending.runtimeOwnedDraftReservation ||
    pending.staleWakeCleanupClaimed ||
    !finalSendIdentityMatches(pending, identity)
  ) {
    return { safe: false };
  }
  let expectedIdentity;
  try {
    expectedIdentity = requireProviderAppIdentity(rawAppIdentity);
    requireConversationUrl(conversationUrl);
  } catch (_error) {
    return { safe: false };
  }
  if (
    !exactSerializedCurrentWorkerWakeDraft(
      draft,
      expectedIdentity,
      identity.targetId,
      identity.targetRevision,
    ) ||
    !(await finalSendIdentityIsCurrent(tabId, identity))
  ) {
    return { safe: false };
  }

  const storageKey = registrationStorageKey(conversationUrl);
  const registration = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  const capability = registration?.runtimeOwnedRegistrationDraft;
  if (
    !exactCurrentWakeRegistrationMatches(
      registration,
      capability,
      tabId,
      identity,
      expectedIdentity,
      draft,
    ) ||
    !hasExactStaleCleanupConnectorProof(registration, expectedIdentity)
  ) {
    return { safe: false };
  }
  let tab;
  try {
    tab = await chrome.tabs.get(tabId);
  } catch (_error) {
    return { safe: false };
  }
  if (
    tab.active !== false ||
    tab.pinned !== false ||
    normalizedChatConversationUrl(tab.url) !== identity.conversationUrl
  ) {
    return { safe: false };
  }

  const proofMatches = await provisionedConnectorProofMatchesCurrentBinding(
    storageKey,
    expectedIdentity,
  );
  const latestRegistration = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  let latestTab;
  try {
    latestTab = await chrome.tabs.get(tabId);
  } catch (_error) {
    return { safe: false };
  }
  if (
    !proofMatches ||
    pendingFinalSendAdmissions.get(admissionToken) !== pending ||
    pending.confirming ||
    pending.runtimeOwnedDraftReservation ||
    pending.staleWakeCleanupClaimed ||
    !registrationSnapshotsMatch(latestRegistration, registration) ||
    latestTab.active !== false ||
    latestTab.pinned !== false ||
    normalizedChatConversationUrl(latestTab.url) !== identity.conversationUrl
  ) {
    return { safe: false };
  }

  const reservation = {
    type: "exact_current_wake_draft",
    storageKey,
    conversationId: registration.conversationId,
    appRef: expectedIdentity.appRef,
    appLabel: expectedIdentity.appLabel,
    draft,
    targetId: identity.targetId,
    targetRevision: identity.targetRevision,
    provisionedConnectorProof: { ...registration.provisionedConnectorProof },
  };
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      pendingFinalSendAdmissions.get(admissionToken) !== pending ||
      pending.confirming ||
      pending.runtimeOwnedDraftReservation ||
      pending.staleWakeCleanupClaimed ||
      !registrationSnapshotsMatch(current, registration) ||
      !exactCurrentWakeRegistrationMatches(
        current,
        current?.runtimeOwnedRegistrationDraft,
        tabId,
        identity,
        expectedIdentity,
        draft,
      )
    ) {
      return { safe: false };
    }
    pending.runtimeOwnedDraftReservation = reservation;
    return { safe: true };
  });
}

function exactCurrentWakeRegistrationMatches(
  registration,
  capability,
  tabId,
  identity,
  appIdentity,
  draft,
) {
  return Boolean(
    registration &&
      !registration.retirementPending &&
      registration.kind === "worker" &&
      registration.wakeTargetId === identity.targetId &&
      registration.runtimeOpenedTabId === tabId &&
      Number(registration.runtimeOpenedAt || 0) > 0 &&
      Number(registration.lastAttemptRevision || 0) === 0 &&
      Number(registration.runtimeOwnedRegistrationDraftConsumedAt || 0) === 0 &&
      registration.appIdentity?.appRef === appIdentity.appRef &&
      registration.appIdentity?.appLabel === appIdentity.appLabel &&
      capability &&
      capability.tabId === tabId &&
      capability.conversationId === registration.conversationId &&
      capability.appRef === appIdentity.appRef &&
      capability.appLabel === appIdentity.appLabel &&
      runtimeOwnedProviderAppDraftMatches(capability.draft, appIdentity) &&
      exactSerializedCurrentWorkerWakeDraft(
        draft,
        appIdentity,
        identity.targetId,
        identity.targetRevision,
      )
  );
}

async function cleanupStaleRuntimeWakeDraft({
  tabId,
  conversationUrl,
  appIdentity: rawAppIdentity,
  draft,
  admissionToken,
  delivery,
}) {
  const identity = normalizeFinalSendIdentity(conversationUrl, delivery);
  const pending = pendingFinalSendAdmissions.get(admissionToken);
  if (
    !Number.isInteger(tabId) ||
    !identity ||
    identity.kind !== "worker" ||
    identity.mode !== "wake" ||
    !pending ||
    pending.confirming ||
    pending.runtimeOwnedDraftReservation ||
    pending.staleWakeCleanupClaimed ||
    pending.tabId !== tabId ||
    pending.expiresAt < Date.now() ||
    !finalSendIdentityMatches(pending, identity)
  ) {
    return { safe: false };
  }
  let expectedIdentity;
  try {
    expectedIdentity = requireProviderAppIdentity(rawAppIdentity);
    requireConversationUrl(conversationUrl);
  } catch (_error) {
    return { safe: false };
  }
  const staleRevision = exactSerializedWorkerWakeRevision(
    draft,
    expectedIdentity,
    identity.targetId,
  );
  if (
    !Number.isSafeInteger(staleRevision) ||
    staleRevision < 1 ||
    staleRevision >= identity.targetRevision
  ) {
    return { safe: false };
  }
  if (!(await finalSendIdentityIsCurrent(tabId, identity))) {
    return { safe: false };
  }
  const storageKey = registrationStorageKey(conversationUrl);
  const registration = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  const capability = registration?.runtimeOwnedRegistrationDraft;
  if (
    !registration ||
    registration.retirementPending ||
    registration.kind !== "worker" ||
    registration.wakeTargetId !== identity.targetId ||
    registration.runtimeOpenedTabId !== tabId ||
    Number(registration.runtimeOpenedAt || 0) <= 0 ||
    Number(registration.lastAttemptRevision || 0) !== 0 ||
    Number(registration.runtimeOwnedRegistrationDraftConsumedAt || 0) !== 0 ||
    registration.appIdentity?.appRef !== expectedIdentity.appRef ||
    registration.appIdentity?.appLabel !== expectedIdentity.appLabel ||
    !capability ||
    capability.tabId !== tabId ||
    capability.conversationId !== registration.conversationId ||
    capability.appRef !== expectedIdentity.appRef ||
    capability.appLabel !== expectedIdentity.appLabel ||
    !runtimeOwnedProviderAppDraftMatches(capability.draft, expectedIdentity) ||
    !hasExactStaleCleanupConnectorProof(registration, expectedIdentity)
  ) {
    return { safe: false };
  }
  let tab;
  try {
    tab = await chrome.tabs.get(tabId);
  } catch (_error) {
    return { safe: false };
  }
  if (
    tab.active !== false ||
    tab.pinned !== false ||
    runtimeCreatedTabsEverClaimed.has(tabId) ||
    normalizedChatConversationUrl(tab.url) !== identity.conversationUrl
  ) {
    return { safe: false };
  }
  const proofMatches = await provisionedConnectorProofMatchesCurrentBinding(
    storageKey,
    expectedIdentity,
  );
  const latestRegistration = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  let latestTab;
  try {
    latestTab = await chrome.tabs.get(tabId);
  } catch (_error) {
    return { safe: false };
  }
  if (
    !proofMatches ||
    pendingFinalSendAdmissions.get(admissionToken) !== pending ||
    pending.confirming ||
    pending.runtimeOwnedDraftReservation ||
    pending.staleWakeCleanupClaimed ||
    !registrationSnapshotsMatch(latestRegistration, registration) ||
    latestTab.active !== false ||
    latestTab.pinned !== false ||
    runtimeCreatedTabsEverClaimed.has(tabId) ||
    normalizedChatConversationUrl(latestTab.url) !== identity.conversationUrl
  ) {
    return { safe: false };
  }
  pending.staleWakeCleanupClaimed = true;
  return { safe: true, draft: capability.draft };
}

function exactSerializedWorkerWakeRevision(
  rawDraft,
  appIdentity,
  wakeTargetId,
) {
  const draft = typeof rawDraft === "string" ? rawDraft : "";
  const revisionMatch = /wake_revision: ([1-9][0-9]*)$/.exec(draft);
  if (!revisionMatch) {
    return null;
  }
  const revision = Number(revisionMatch[1]);
  if (!Number.isSafeInteger(revision)) {
    return null;
  }
  const label = appIdentity.appLabel;
  const iconInitial = Array.from(label)[0] || "";
  const prefixes = [
    `@${label}`,
    label,
    iconInitial,
    `@${iconInitial}`,
    `${iconInitial}${label}`,
    `@${iconInitial}${label}`,
  ].filter(Boolean);
  const command = workerWakeCommand(wakeTargetId, revision);
  const providerFlattenedCommand = command.replace(/\n/g, " ");
  const liveProviderSerializedDraft = `\uFEFF${iconInitial}${label} ${providerFlattenedCommand}`;
  return prefixes.some(
    (prefix) =>
      draft === `${prefix}${command}` ||
      draft === `${prefix}${providerFlattenedCommand}`,
  ) || draft === liveProviderSerializedDraft
    ? revision
    : null;
}

function exactSerializedCurrentWorkerWakeDraft(
  rawDraft,
  appIdentity,
  wakeTargetId,
  wakeRevision,
) {
  if (
    typeof rawDraft !== "string" ||
    !/^[A-Za-z0-9_-]{1,255}$/.test(wakeTargetId || "") ||
    !Number.isSafeInteger(wakeRevision) ||
    wakeRevision <= 0
  ) {
    return false;
  }
  const label = appIdentity?.appLabel;
  if (typeof label !== "string" || !label) {
    return false;
  }
  const iconInitial = Array.from(label)[0] || "";
  const command = workerWakeCommand(wakeTargetId, wakeRevision);
  return (
    rawDraft === `\uFEFF${iconInitial}${label} ${command.replace(/\n/g, " ")}`
  );
}

function workerWakeCommand(wakeTargetId, wakeRevision) {
  return ` 작업해. 사용자가 Main Chat에서 Worker 위임을 요청했습니다. 이 서버 발급 wake pair는 현재 배정을 조회하기 위한 work 호출만 승인합니다. 파괴적·비가역적·자격증명 필요·외부 프로덕션 작업의 별도 승인을 뜻하지 않습니다.\nwake_target_id: ${wakeTargetId}\nwake_revision: ${wakeRevision}`;
}

function hasExactStaleCleanupConnectorProof(registration, appIdentity) {
  const proof = registration?.provisionedConnectorProof;
  return Boolean(
    proof &&
      /^prq_[0-9a-f]{32}$/.test(proof.requestId || "") &&
      proof.wakeTargetId === registration.wakeTargetId &&
      proof.conversationId === registration.conversationId &&
      proof.projectId === registration.projectId &&
      proof.appRef === appIdentity.appRef &&
      proof.appLabel === appIdentity.appLabel &&
      Number.isSafeInteger(proof.admissionRevision) &&
      proof.admissionRevision > 0 &&
      Number.isSafeInteger(proof.bindingRevision) &&
      Number.isSafeInteger(proof.connectorEpoch) &&
      Number.isFinite(Number(proof.finalizedAt)) &&
      Number(proof.finalizedAt) >= Number(registration.registeredAt || 0)
  );
}

async function provisionedConnectorProofMatchesCurrentBinding(
  storageKey,
  expectedIdentity,
) {
  const registration = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  const proof = registration?.provisionedConnectorProof;
  if (
    !registration ||
    registration.kind !== "worker" ||
    !proof ||
    proof.wakeTargetId !== registration.wakeTargetId ||
    proof.conversationId !== registration.conversationId ||
    proof.projectId !== registration.projectId ||
    proof.appRef !== expectedIdentity.appRef ||
    proof.appLabel !== expectedIdentity.appLabel
  ) {
    return false;
  }
  const payload = await requestJson(
    `/api/project-bindings/${encodeURIComponent(registration.projectId)}/connector`,
  );
  const binding = payload?.connector;
  return Boolean(
    binding?.project_id === registration.projectId &&
      binding?.provider === proof.provider &&
      binding?.project_ref === proof.projectRef &&
      binding?.project_path === proof.projectPath &&
      binding?.app_identity?.app_ref === expectedIdentity.appRef &&
      binding?.app_identity?.app_label === expectedIdentity.appLabel &&
      Number(binding?.revision) === Number(proof.bindingRevision) &&
      Number(binding?.connector_epoch) === Number(proof.connectorEpoch) &&
      Number(registration.bindingRevision) === Number(proof.bindingRevision) &&
      Number(registration.connectorEpoch) === Number(proof.connectorEpoch)
  );
}

function runtimeOwnedFirstWakeDraftMatches(currentDraft, appIdentity) {
  return runtimeOwnedProviderAppDraftMatches(currentDraft, appIdentity, {
    allowEmpty: true,
  });
}

function runtimeOwnedAppDraftsMatch(expectedDraft, currentDraft, appIdentity) {
  return Boolean(
    runtimeOwnedProviderAppDraftMatches(expectedDraft, appIdentity) &&
      runtimeOwnedProviderAppDraftMatches(currentDraft, appIdentity, {
        allowEmpty: true,
      })
  );
}

function runtimeOwnedProviderAppDraftMatches(
  draft,
  appIdentity,
  { allowEmpty = false } = {},
) {
  const normalized = normalizeRuntimeOwnedAppDraft(draft);
  const label = normalizeRuntimeOwnedAppDraft(appIdentity?.appLabel);
  const iconInitial = Array.from(label)[0] || "";
  const appOnlyDrafts = [
    `@${label}`,
    label,
    iconInitial,
    `@${iconInitial}`,
    `${iconInitial}${label}`,
    `@${iconInitial}${label}`,
  ].filter(Boolean);
  return (allowEmpty && !normalized) || appOnlyDrafts.includes(normalized);
}

function normalizeRuntimeOwnedAppDraft(value) {
  return String(value || "")
    .replace(/[\u200B-\u200D\u2060\uFEFF]/g, "")
    .replace(/\s+/g, "")
    .toLocaleLowerCase();
}

async function clearRuntimeOwnership(conversationUrl) {
  await updateRegistration(conversationUrl, {
    runtimeOpenedTabId: null,
    runtimeOpenedAt: 0,
    idleSince: 0,
    runtimeOwnedRegistrationDraft: null,
  });
}

function hasExactProvisionedWorkerProvenance(registration) {
  if (
    registration?.kind !== "worker" ||
    !Number.isInteger(registration.runtimeOpenedTabId) ||
    Number(registration.runtimeOpenedAt || 0) <= 0 ||
    Number(registration.lastAttemptRevision || 0) !== 0
  ) {
    return false;
  }
  const draft = registration.runtimeOwnedRegistrationDraft;
  const unconsumedDraftMatches = Boolean(
    draft &&
      draft.tabId === registration.runtimeOpenedTabId &&
      draft.conversationId === registration.conversationId,
  );
  const consumedDraftMatches =
    !draft && Number(registration.runtimeOwnedRegistrationDraftConsumedAt || 0) > 0;
  return unconsumedDraftMatches || consumedDraftMatches;
}

function hasProvisionedConnectorProvenance(registration) {
  const proof = registration?.provisionedConnectorProof;
  return Boolean(
    registration?.kind === "worker" &&
      proof &&
      /^prq_[0-9a-f]{32}$/.test(proof.requestId || "") &&
      proof.wakeTargetId === registration.wakeTargetId &&
      proof.conversationId === registration.conversationId &&
      proof.projectId === registration.projectId
  );
}

async function retireProvisionedWorkerOrClearOwnership(registration, reason = null) {
  if (
    registration?.retirementPending ||
    hasExactProvisionedWorkerProvenance(registration) ||
    hasProvisionedConnectorProvenance(registration)
  ) {
    await beginOrResumePendingWorkerRetirement(registration, reason);
    return;
  }
  if (registration?.conversationUrl) {
    await clearRuntimeOwnership(registration.conversationUrl);
  }
}

async function beginOrResumePendingWorkerRetirement(registration, reason = null) {
  const conversationUrl = registration?.conversationUrl;
  if (!conversationUrl) {
    return false;
  }
  const storageKey = registrationStorageKey(conversationUrl);
  const pendingRegistration = await mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (!runtimeRegistrationBindingMatches(current, registration)) {
      return null;
    }
    if (current.retirementPending) {
      return current;
    }
    if (
      !hasExactProvisionedWorkerProvenance(current) &&
      !hasProvisionedConnectorProvenance(current)
    ) {
      return null;
    }
    const updated = {
      ...current,
      retirementPending: {
        wakeTargetId: current.wakeTargetId,
        projectId: current.projectId,
        conversationId: current.conversationId,
        runtimeOpenedTabId: current.runtimeOpenedTabId,
        markedAt: Date.now(),
        ...(typeof reason === "string" && reason ? { reason } : {}),
      },
    };
    registrations[storageKey] = updated;
    // This durable write must complete before either target read or retirement.
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    pendingWorkerRetirementAdmissionGeneration += 1;
    return updated;
  });
  if (!pendingRegistration) {
    return false;
  }
  // The durable marker is the ownership boundary. Start the project-scoped
  // drain immediately; a sweep will retry it before any future admission.
  return resumePendingWorkerRetirement(pendingRegistration);
}

async function resumePendingWorkerRetirement(registration) {
  if (!isExactPendingWorkerRetirement(registration)) {
    return false;
  }
  return resumePendingWorkerRetirements();
}

function isExactPendingWorkerRetirement(registration) {
  const marker = registration?.retirementPending;
  return Boolean(
    registration?.kind === "worker" &&
      marker &&
      marker.wakeTargetId === registration.wakeTargetId &&
      marker.projectId === registration.projectId &&
      marker.conversationId === registration.conversationId &&
      Number.isFinite(Number(marker.markedAt)),
  );
}

function pendingWorkerRetirementMarkersMatch(currentMarker, expectedMarker) {
  return Boolean(
    currentMarker &&
      expectedMarker &&
      currentMarker.wakeTargetId === expectedMarker.wakeTargetId &&
      currentMarker.projectId === expectedMarker.projectId &&
      currentMarker.conversationId === expectedMarker.conversationId &&
      currentMarker.runtimeOpenedTabId === expectedMarker.runtimeOpenedTabId &&
      currentMarker.markedAt === expectedMarker.markedAt &&
      currentMarker.reason === expectedMarker.reason &&
      currentMarker.expectedRevision === expectedMarker.expectedRevision,
  );
}

async function hasPendingWorkerRetirements() {
  const registrations = await readAllRegistrations();
  return Object.values(registrations).some((rawRegistration) =>
    Boolean(normalizeRegistration(rawRegistration)?.retirementPending),
  );
}

async function runtimeSideEffectsAllowedWithoutPendingRetirement() {
  const expectedGeneration = pendingWorkerRetirementAdmissionGeneration;
  if (await hasPendingWorkerRetirements()) {
    return false;
  }
  return expectedGeneration === pendingWorkerRetirementAdmissionGeneration;
}

async function performPendingWorkerRetirementDrain() {
  const registrations = await readAllRegistrations();
  const byProject = new Map();
  for (const rawRegistration of Object.values(registrations)) {
    const registration = normalizeRegistration(rawRegistration);
    if (!registration?.retirementPending) {
      continue;
    }
    if (!isExactPendingWorkerRetirement(registration)) {
      return false;
    }
    const projectRegistrations = byProject.get(registration.projectId) || [];
    projectRegistrations.push(registration);
    byProject.set(registration.projectId, projectRegistrations);
  }
  if (byProject.size === 0) {
    return true;
  }

  for (const projectId of [...byProject.keys()].sort()) {
    const projectRegistrations = byProject.get(projectId).sort((left, right) =>
      left.retirementPending.wakeTargetId.localeCompare(
        right.retirementPending.wakeTargetId,
      ),
    );
    if (projectRegistrations.length > 32) {
      return false;
    }
    const targetIds = new Set(
      projectRegistrations.map(
        (registration) => registration.retirementPending.wakeTargetId,
      ),
    );
    if (targetIds.size !== projectRegistrations.length) {
      return false;
    }

    const targets = [];
    const exactRegistrations = [];
    try {
      for (const registration of projectRegistrations) {
        const marker = registration.retirementPending;
        let revision =
          typeof marker.expectedRevision === "number"
            ? marker.expectedRevision
            : Number.NaN;
        try {
          const payload = await requestJson(
            `/api/wake/targets/${marker.wakeTargetId}`,
          );
          const target = payload?.target;
          revision = Number(target?.revision);
          if (
            target?.wake_target_id !== marker.wakeTargetId ||
            target?.project_id !== projectId ||
            !Number.isSafeInteger(revision) ||
            revision < 0
          ) {
            return false;
          }
          // Batch retirement is intentionally narrower than the legacy
          // single-target endpoint. A WAKING revision is transient and must not
          // become the durable CAS that strands this marker after recovery.
          if (target?.status !== "idle") {
            return false;
          }
        } catch (error) {
          // A mixed-version backend may have completed retirement before this
          // service worker restarted. Preserve and submit the durable expected
          // revision so the batch endpoint can reconcile its tombstone.
          if (
            error?.errorCode !== "not_found" ||
            !Number.isSafeInteger(revision) ||
            revision < 0
          ) {
            return false;
          }
        }
        const exactRegistration = await persistPendingWorkerExpectedRevision(
          registration,
          revision,
        );
        if (!exactRegistration) {
          return false;
        }
        exactRegistrations.push(exactRegistration);
        targets.push({
          wake_target_id: marker.wakeTargetId,
          expected_revision: revision,
        });
      }
      const payload = await requestJson("/api/wake/targets/retire-batch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ project_id: projectId, targets }),
      });
      const retiredIds = Array.isArray(payload?.retired_wake_target_ids)
        ? payload.retired_wake_target_ids
        : [];
      if (
        payload?.ok !== true ||
        payload?.project_id !== projectId ||
        retiredIds.length !== targets.length ||
        retiredIds.some(
          (wakeTargetId, index) =>
            wakeTargetId !== targets[index].wake_target_id,
        )
      ) {
        return false;
      }
    } catch (_error) {
      return false;
    }

    for (const registration of exactRegistrations) {
      await deleteExactPendingWorkerRegistration(registration);
    }
  }
  return !(await hasPendingWorkerRetirements());
}

async function persistPendingWorkerExpectedRevision(
  expectedRegistration,
  expectedRevision,
) {
  const storageKey = registrationStorageKey(expectedRegistration.conversationUrl);
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    const expectedMarker = expectedRegistration.retirementPending;
    const currentMarker = current?.retirementPending;
    if (
      !runtimeRegistrationBindingMatches(current, expectedRegistration) ||
      !pendingWorkerRetirementMarkersMatch(currentMarker, expectedMarker) ||
      (typeof currentMarker.expectedRevision === "number" &&
        Number.isSafeInteger(currentMarker.expectedRevision) &&
        currentMarker.expectedRevision > expectedRevision)
    ) {
      return null;
    }
    const updated = {
      ...current,
      retirementPending: { ...currentMarker, expectedRevision },
    };
    registrations[storageKey] = updated;
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return updated;
  });
}

async function deleteExactPendingWorkerRegistration(expectedRegistration) {
  const storageKey = registrationStorageKey(expectedRegistration.conversationUrl);
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    const expectedMarker = expectedRegistration.retirementPending;
    const currentMarker = current?.retirementPending;
    if (
      !runtimeRegistrationBindingMatches(current, expectedRegistration) ||
      !pendingWorkerRetirementMarkersMatch(currentMarker, expectedMarker)
    ) {
      return false;
    }
    delete registrations[storageKey];
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return true;
  });
}

async function resumePendingWorkerRetirements() {
  if (pendingWorkerRetirementDrain) {
    return pendingWorkerRetirementDrain;
  }
  const pending = performPendingWorkerRetirementDrain().finally(() => {
    if (pendingWorkerRetirementDrain === pending) {
      pendingWorkerRetirementDrain = null;
    }
  });
  pendingWorkerRetirementDrain = pending;
  return pending;
}

async function listProjects() {
  const payload = await requestJson("/api/wake/projects");
  return { projects: payload.projects };
}

async function getRegistration(conversationUrl) {
  const registration = await readRegistration(conversationUrl);
  return { registration: normalizeRegistration(registration) };
}

async function registerTarget({
  conversationUrl,
  projectContext: suppliedProjectContext,
  projectId,
  projectName,
  kind,
}) {
  requireConversationUrl(conversationUrl);
  requireProjectId(projectId);
  const normalizedKind = requireTargetKind(kind);
  const projectContext =
    normalizedKind === "main"
      ? requireMainProjectContext({ conversationUrl, suppliedProjectContext })
      : null;
  const existing = normalizeRegistration(await readRegistration(conversationUrl));
  if (existing) {
    if (existing.projectId !== projectId) {
      throw new Error(
        "This Chat is already registered to another project. Use a new Chat for a different project.",
      );
    }
    if (existing.kind !== normalizedKind) {
      throw new Error(
        "This Chat already has a different ChatToCodex runtime role. Use a separate Chat for Main and Worker roles.",
      );
    }
    if (existing.kind === "main") {
      await bindProjectBinding({ projectId, projectContext });
      const refreshed = await updateRegistration(conversationUrl, {
        appIdentity: projectContext.appIdentity,
        // Reopening the Main registration popup is an explicit, local retry:
        // the selected app pill is revalidated by the content script before a
        // current backend wake can be sent. Clear only the delivery throttle;
        // the backend wake revision still prevents stale replay.
        lastAttemptRevision: 0,
        lastAttemptAt: 0,
      });
      await verifyRegistration(refreshed || existing);
      return { registration: refreshed || existing };
    }
    await verifyRegistration(existing);
    return { registration: existing };
  }

  if (normalizedKind === "main") {
    // Binding is deliberately completed before the opaque Main target is
    // registered.  The local project/root remain server-owned; this payload
    // carries provider observation metadata only.
    await bindProjectBinding({ projectId, projectContext });
  }
  const registration =
    normalizedKind === "main"
      ? await registerMainTarget({
          projectId,
          projectName,
          appIdentity: projectContext.appIdentity,
        })
      : await registerWorkerTarget({ projectId, projectName });
  await writeRegistration(conversationUrl, registration);
  if (registration.kind === "worker") {
    await pollRuntimeTarget(registration);
  }
  return { registration };
}

function requireMainProjectContext({ conversationUrl, suppliedProjectContext }) {
  const derived = explicitChatGPTProjectContext(conversationUrl);
  if (!derived) {
    throw new Error(
      "Main registration requires a current ChatGPT Project conversation at /g/.../c/....",
    );
  }
  if (
    suppliedProjectContext &&
    (suppliedProjectContext.provider !== derived.provider ||
      suppliedProjectContext.projectRef !== derived.projectRef ||
      suppliedProjectContext.projectPath !== derived.projectPath ||
      suppliedProjectContext.conversationUrl !== derived.conversationUrl)
  ) {
    throw new Error("ChatGPT Project context changed before Main registration.");
  }
  const observedAt =
    typeof suppliedProjectContext?.observedAt === "string" &&
    ISO_TIMESTAMP_PATTERN.test(suppliedProjectContext.observedAt)
      ? suppliedProjectContext.observedAt
      : null;
  const appIdentity = requireProviderAppIdentity(
    suppliedProjectContext?.appIdentity,
  );
  return { ...derived, appIdentity, observedAt };
}

function requireProviderAppIdentity(rawIdentity) {
  const appRef = String(rawIdentity?.appRef || "").trim();
  const appLabel = String(rawIdentity?.appLabel || "").trim();
  if (
    !/^plugin_[A-Za-z0-9_-]+$/.test(appRef) ||
    !appLabel ||
    appLabel.length > 255
  ) {
    throw new Error(
      "Main registration requires the app selected in the current ChatGPT Work composer.",
    );
  }
  return { appRef, appLabel };
}

const ISO_TIMESTAMP_PATTERN =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/;

// Only the exact, current provider route proves a project.  A plain /c/<id>
// conversation, UI text, or a caller-supplied project guess is not enough.
function explicitChatGPTProjectContext(rawUrl) {
  return chatGPTProjectConversationContext(rawUrl);
}

async function bindProjectBinding({ projectId, projectContext }) {
  if (!projectContext) {
    throw new Error("ChatGPT Project context is required before binding.");
  }
  await requestJson(
    `/api/project-bindings/${encodeURIComponent(projectId)}`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        adapter_metadata: {
          provider: projectContext.provider,
          project_ref: projectContext.projectRef,
          project_path: projectContext.projectPath,
          app_ref: projectContext.appIdentity.appRef,
          app_label: projectContext.appIdentity.appLabel,
          observed_at: projectContext.observedAt || adapterObservedAt(),
        },
      }),
    },
  );
}

function adapterObservedAt() {
  const epochMilliseconds = Number(Date.now());
  if (typeof Date === "function") {
    return new Date(epochMilliseconds).toISOString();
  }
  // Small harnesses may replace Date with a clock object.  Keep the adapter
  // contract timezone-aware without making project binding depend on that
  // test/runtime detail.
  const parts = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "UTC",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(epochMilliseconds);
  const valueFor = (type) => parts.find((part) => part.type === type)?.value;
  const milliseconds = String(
    ((epochMilliseconds % 1000) + 1000) % 1000,
  ).padStart(3, "0");
  return `${valueFor("year")}-${valueFor("month")}-${valueFor("day")}T${valueFor("hour")}:${valueFor("minute")}:${valueFor("second")}.${milliseconds}Z`;
}

async function registerWorkerTarget({ projectId, projectName }) {
  const wakeTargetId = `wtgt_${crypto.randomUUID().replaceAll("-", "")}`;
  await requestJson("/api/wake/targets/register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      wake_target_id: wakeTargetId,
      project_id: projectId,
    }),
  });
  return {
    kind: "worker",
    wakeTargetId,
    projectId,
    projectName,
    registeredAt: Date.now(),
    lastAttemptRevision: 0,
    lastAttemptAt: 0,
    lastResumeAttemptAt: 0,
    lastAdmittedDeliveryKey: null,
    lastAdmittedWakeRevision: 0,
    lastAdmittedAt: 0,
    runtimeOpenedTabId: null,
    runtimeOpenedAt: 0,
    idleSince: 0,
    lastParkedAt: 0,
  };
}

async function registerMainTarget({ projectId, projectName, appIdentity }) {
  const mainTargetId = `mtgt_${crypto.randomUUID().replaceAll("-", "")}`;
  await requestJson("/api/main-runtime/review-targets/register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      main_target_id: mainTargetId,
      project_id: projectId,
    }),
  });
  return {
    kind: "main",
    mainTargetId,
    projectId,
    projectName,
    appIdentity,
    registeredAt: Date.now(),
    lastAttemptRevision: 0,
    lastAttemptAt: 0,
    lastResumeAttemptAt: 0,
    lastAdmittedDeliveryKey: null,
    lastAdmittedWakeRevision: 0,
    lastAdmittedAt: 0,
    runtimeOpenedTabId: null,
    runtimeOpenedAt: 0,
    idleSince: 0,
    lastParkedAt: 0,
  };
}

async function verifyRegistration(registration) {
  if (registration.kind === "main") {
    await requestJson(
      `/api/main-runtime/review-targets/${registration.mainTargetId}`,
    );
    return;
  }
  await requestJson(`/api/wake/targets/${registration.wakeTargetId}`);
}

async function prepareFinalSendAdmission({ tabId, conversationUrl, delivery }) {
  const identity = normalizeFinalSendIdentity(conversationUrl, delivery);
  if (!Number.isInteger(tabId) || !identity) {
    return { safe: false };
  }
  const safe = await finalSendIdentityIsCurrent(tabId, identity);
  if (!safe) {
    revokeFinalSendAdmissionsForTab(tabId);
    return { safe: false };
  }
  revokeFinalSendAdmissionsForTab(tabId);
  const admissionToken = crypto.randomUUID();
  pendingFinalSendAdmissions.set(admissionToken, {
    ...identity,
    tabId,
    expiresAt: Date.now() + 15000,
  });
  return { safe: true, admissionToken };
}

async function confirmFinalSendAdmission({
  tabId,
  conversationUrl,
  admissionToken,
  delivery,
}) {
  const identity = normalizeFinalSendIdentity(conversationUrl, delivery);
  const pending = pendingFinalSendAdmissions.get(admissionToken);
  if (pending?.confirming) {
    return { safe: false };
  }
  if (
    !pending ||
    !identity ||
    pending.tabId !== tabId ||
    pending.expiresAt < Date.now() ||
    !finalSendIdentityMatches(pending, identity)
  ) {
    pendingFinalSendAdmissions.delete(admissionToken);
    return { safe: false };
  }
  // Mark before yielding so a concurrent confirmation cannot reuse the token,
  // but retain it in the map so activation/pinning listeners can revoke the
  // in-flight confirmation while its Chrome/backend reads are pending.
  pending.confirming = true;
  const safe = await finalSendIdentityIsCurrent(tabId, identity);
  const reservedDraftCurrent = pending.runtimeOwnedDraftReservation
    ? await provisionedConnectorProofMatchesCurrentBinding(
        pending.runtimeOwnedDraftReservation.storageKey,
        {
          appRef: pending.runtimeOwnedDraftReservation.appRef,
          appLabel: pending.runtimeOwnedDraftReservation.appLabel,
        },
      )
    : true;
  const attemptRecorded =
    safe && reservedDraftCurrent && identity.mode === "wake"
      ? await commitConfirmedWakeAttempt({
          tabId,
          identity,
          admissionToken,
          pending,
          draftReservation: pending.runtimeOwnedDraftReservation || null,
        })
      : safe && reservedDraftCurrent
        ? await clearConfirmedWakeSendFailure({
            identity,
            admissionToken,
            pending,
          })
        : false;
  const stillAdmitted = pendingFinalSendAdmissions.get(admissionToken) === pending;
  pendingFinalSendAdmissions.delete(admissionToken);
  return { safe: Boolean(attemptRecorded && stillAdmitted) };
}

async function commitConfirmedWakeAttempt({
  tabId,
  identity,
  admissionToken,
  pending,
  draftReservation,
}) {
  const storageKey = registrationStorageKey(identity.conversationUrl);
  const registrationSnapshot = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  let tabSnapshot;
  try {
    tabSnapshot = await chrome.tabs.get(tabId);
  } catch (_error) {
    return false;
  }
  if (
    !confirmedAttemptSnapshotIsCurrent(
      registrationSnapshot,
      tabSnapshot,
      tabId,
      identity,
      draftReservation,
    ) ||
    pendingFinalSendAdmissions.get(admissionToken) !== pending
  ) {
    return false;
  }
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      pendingFinalSendAdmissions.get(admissionToken) !== pending ||
      pending.confirming !== true ||
      (draftReservation
        ? pending.runtimeOwnedDraftReservation !== draftReservation
        : Boolean(pending.runtimeOwnedDraftReservation)) ||
      !registrationSnapshotsMatch(current, registrationSnapshot)
    ) {
      return false;
    }
    registrations[storageKey] = {
      ...current,
      lastAttemptRevision: identity.targetRevision,
      lastAttemptAt: Date.now(),
      ...(draftReservation
        ? {
            runtimeOwnedRegistrationDraft: null,
            runtimeOwnedRegistrationDraftConsumedAt: Date.now(),
          }
        : {}),
      lastWakeSendFailure: null,
    };
    // This write is the local attempt-ownership commit. It must finish before
    // content receives a safe response and is allowed to click. Consequently,
    // a post-click activation can no longer classify the tab as never attempted.
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return true;
  });
}

async function clearConfirmedWakeSendFailure({
  identity,
  admissionToken,
  pending,
}) {
  const storageKey = registrationStorageKey(identity.conversationUrl);
  const registrationSnapshot = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  if (
    !registrationSnapshot ||
    pendingFinalSendAdmissions.get(admissionToken) !== pending
  ) {
    return false;
  }
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      pendingFinalSendAdmissions.get(admissionToken) !== pending ||
      pending.confirming !== true ||
      !registrationSnapshotsMatch(current, registrationSnapshot)
    ) {
      return false;
    }
    registrations[storageKey] = {
      ...current,
      lastWakeSendFailure: null,
    };
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return true;
  });
}

async function recordWakeSendFailure({
  tabId,
  conversationUrl,
  delivery,
  reason,
  rejectionStage,
  rejectionSubstage,
}) {
  const identity = normalizeFinalSendIdentity(conversationUrl, delivery);
  if (
    !Number.isInteger(tabId) ||
    !identity ||
    !WAKE_SEND_FAILURE_REASONS.has(reason) ||
    (rejectionStage !== undefined &&
      (reason !== "durable_reserve_rejected" ||
        !DURABLE_RESERVE_REJECTION_STAGES.has(rejectionStage))) ||
    (rejectionStage === "pre_candidate" &&
      !PRE_CANDIDATE_REJECTION_SUBSTAGES.has(rejectionSubstage)) ||
    (rejectionSubstage !== undefined &&
      (reason !== "durable_reserve_rejected" ||
        rejectionStage !== "pre_candidate" ||
        !PRE_CANDIDATE_REJECTION_SUBSTAGES.has(rejectionSubstage)))
  ) {
    return { safe: false };
  }
  const storageKey = registrationStorageKey(identity.conversationUrl);
  const registrationSnapshot = normalizeRegistration(
    (await readAllRegistrations())[storageKey],
  );
  let tabSnapshot;
  try {
    tabSnapshot = await chrome.tabs.get(tabId);
  } catch (_error) {
    return { safe: false };
  }
  if (
    !registrationSnapshot ||
    registrationSnapshot.retirementPending ||
    registrationSnapshot.kind !== identity.kind ||
    registrationTargetId(registrationSnapshot) !== identity.targetId ||
    registrationSnapshot.runtimeOpenedTabId !== tabId ||
    Number(registrationSnapshot.lastAttemptRevision || 0) >
      identity.targetRevision ||
    normalizedChatConversationUrl(tabSnapshot.url) !== identity.conversationUrl
  ) {
    return { safe: false };
  }
  const failure = {
    conversationUrl: identity.conversationUrl,
    kind: identity.kind,
    targetId: identity.targetId,
    targetRevision: identity.targetRevision,
    mode: identity.mode,
    reason,
    ...(rejectionStage === undefined ? {} : { rejectionStage }),
    ...(rejectionSubstage === undefined ? {} : { rejectionSubstage }),
    recordedAt: Date.now(),
  };
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (!registrationSnapshotsMatch(current, registrationSnapshot)) {
      return { safe: false };
    }
    registrations[storageKey] = {
      ...current,
      lastWakeSendFailure: failure,
    };
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return { safe: true };
  });
}

function confirmedAttemptSnapshotIsCurrent(
  registration,
  tab,
  tabId,
  identity,
  draftReservation,
) {
  return Boolean(
    registration &&
      !registration.retirementPending &&
      registration.kind === identity.kind &&
      registrationTargetId(registration) === identity.targetId &&
      registration.runtimeOpenedTabId === tabId &&
      Number(registration.lastAttemptRevision || 0) <= identity.targetRevision &&
      tab.active === false &&
      tab.pinned === false &&
      normalizedChatConversationUrl(tab.url) === identity.conversationUrl &&
      (!draftReservation ||
        runtimeOwnedDraftReservationMatches(
          registration,
          draftReservation,
          tabId,
        ))
  );
}

function registrationSnapshotsMatch(current, expected) {
  if (
    !current ||
    !expected ||
    current.lastAttemptRevision !== expected.lastAttemptRevision ||
    current.lastAdmittedDeliveryKey !== expected.lastAdmittedDeliveryKey ||
    current.lastAdmittedWakeRevision !== expected.lastAdmittedWakeRevision
  ) {
    return false;
  }
  return (
    JSON.stringify(registrationSafetyCriticalSnapshot(current)) ===
    JSON.stringify(registrationSafetyCriticalSnapshot(expected))
  );
}

function registrationSafetyCriticalSnapshot(registration) {
  const {
    idleSince: _idleSince,
    lastParkedAt: _lastParkedAt,
    lastWakeSendFailure: _lastWakeSendFailure,
    lastResumeAttemptAt: _lastResumeAttemptAt,
    lastAttemptAt: _lastAttemptAt,
    lastAdmittedAt: _lastAdmittedAt,
    ...safetyCritical
  } = registration;
  return safetyCritical;
}

function runtimeOwnedDraftReservationMatches(current, reservation, tabId) {
  if (
    reservation.storageKey !== registrationStorageKey(current.conversationUrl) ||
    reservation.conversationId !== current.conversationId ||
    current.runtimeOpenedTabId !== tabId ||
    current.appIdentity?.appRef !== reservation.appRef ||
    current.appIdentity?.appLabel !== reservation.appLabel ||
    !provisionedConnectorProofSnapshotsMatch(
      current.provisionedConnectorProof,
      reservation.provisionedConnectorProof,
    )
  ) {
    return false;
  }
  const capability = current.runtimeOwnedRegistrationDraft;
  if (reservation.type === "exact_current_wake_draft") {
    return Boolean(
      capability &&
        Number(current.lastAttemptRevision || 0) === 0 &&
        Number(current.runtimeOwnedRegistrationDraftConsumedAt || 0) === 0 &&
        capability.tabId === tabId &&
        capability.conversationId === current.conversationId &&
        capability.appRef === reservation.appRef &&
        capability.appLabel === reservation.appLabel &&
        runtimeOwnedProviderAppDraftMatches(capability.draft, {
          appRef: reservation.appRef,
          appLabel: reservation.appLabel,
        }) &&
        current.wakeTargetId === reservation.targetId &&
        exactSerializedCurrentWorkerWakeDraft(
          reservation.draft,
          { appRef: reservation.appRef, appLabel: reservation.appLabel },
          reservation.targetId,
          reservation.targetRevision,
        )
    );
  }
  if (reservation.stranded) {
    return Boolean(
      !capability &&
        Number(current.lastAttemptRevision || 0) === 0 &&
        Number(current.runtimeOwnedRegistrationDraftConsumedAt || 0) ===
          reservation.consumedAt &&
        reservation.consumedAt > 0 &&
        runtimeOwnedFirstWakeDraftMatches(reservation.draft, {
          appRef: reservation.appRef,
          appLabel: reservation.appLabel,
        })
    );
  }
  return Boolean(
    capability &&
      capability.tabId === tabId &&
      capability.conversationId === current.conversationId &&
      capability.appRef === reservation.appRef &&
      capability.appLabel === reservation.appLabel &&
      runtimeOwnedAppDraftsMatch(capability.draft, reservation.draft, {
        appRef: reservation.appRef,
        appLabel: reservation.appLabel,
      })
  );
}

function provisionedConnectorProofSnapshotsMatch(current, expected) {
  const keys = [
    "requestId",
    "wakeTargetId",
    "conversationId",
    "admissionRevision",
    "projectId",
    "provider",
    "projectRef",
    "projectPath",
    "appRef",
    "appLabel",
    "bindingRevision",
    "connectorEpoch",
    "finalizedAt",
  ];
  return Boolean(
    current && expected && keys.every((key) => current[key] === expected[key])
  );
}

function revokeFinalSendAdmissionsForTab(tabId) {
  for (const [token, admission] of pendingFinalSendAdmissions) {
    if (admission.tabId === tabId) {
      pendingFinalSendAdmissions.delete(token);
    }
  }
}

function normalizeFinalSendIdentity(conversationUrl, delivery) {
  const normalizedConversation = normalizedChatConversationUrl(conversationUrl);
  const kind = delivery?.kind;
  const mode = delivery?.mode;
  const targetId = String(delivery?.targetId || "");
  const targetRevision = Number(delivery?.targetRevision);
  if (
    !normalizedConversation ||
    (kind !== "worker" && kind !== "main") ||
    (mode !== "wake" && mode !== "resume") ||
    !/^[A-Za-z0-9_-]{1,255}$/.test(targetId) ||
    !Number.isSafeInteger(targetRevision) ||
    targetRevision <= 0
  ) {
    return null;
  }
  return {
    conversationUrl: normalizedConversation,
    kind,
    mode,
    targetId,
    targetRevision,
  };
}

function finalSendIdentityMatches(left, right) {
  return (
    left.conversationUrl === right.conversationUrl &&
    left.kind === right.kind &&
    left.mode === right.mode &&
    left.targetId === right.targetId &&
    left.targetRevision === right.targetRevision
  );
}

async function finalSendIdentityIsCurrent(tabId, identity) {
  const registration = normalizeRegistration(
    await readRegistration(identity.conversationUrl),
  );
  if (
    !registration ||
    registration.retirementPending ||
    registration.kind !== identity.kind ||
    registrationTargetId(registration) !== identity.targetId ||
    registration.runtimeOpenedTabId !== tabId
  ) {
    return false;
  }
  let tab;
  try {
    tab = await chrome.tabs.get(tabId);
  } catch (_error) {
    return false;
  }
  if (
    tab.active !== false ||
    tab.pinned !== false ||
    normalizedChatConversationUrl(tab.url) !== identity.conversationUrl
  ) {
    return false;
  }
  const path =
    identity.kind === "main"
      ? `/api/main-runtime/review-targets/${identity.targetId}`
      : `/api/wake/targets/${identity.targetId}`;
  const payload = await requestJson(path);
  const target = payload?.target;
  const targetId =
    identity.kind === "main" ? target?.main_target_id : target?.wake_target_id;
  const revision = Number(
    identity.mode === "wake" ? target?.wake_revision : target?.revision,
  );
  const expectedStatus =
    identity.mode === "wake"
      ? "waking"
      : identity.kind === "main"
        ? "active"
        : "busy";
  if (
    targetId !== identity.targetId ||
    target?.status !== expectedStatus ||
    revision !== identity.targetRevision
  ) {
    return false;
  }
  // All preceding reads yield. Finish with fresh local ownership reads so a
  // retirement or Chrome claim during backend validation revokes admission.
  const latestRegistration = normalizeRegistration(
    await readRegistration(identity.conversationUrl),
  );
  let latestTab;
  try {
    latestTab = await chrome.tabs.get(tabId);
  } catch (_error) {
    return false;
  }
  return Boolean(
    latestRegistration &&
      !latestRegistration.retirementPending &&
      latestRegistration.runtimeOpenedTabId === tabId &&
      runtimeRegistrationBindingMatches(latestRegistration, registration) &&
      latestTab.active === false &&
      latestTab.pinned === false &&
      normalizedChatConversationUrl(latestTab.url) === identity.conversationUrl
  );
}

async function pollTarget(conversationUrl, senderTab) {
  const senderIdentity = normalizeRuntimeSenderTab(senderTab);
  if (!senderIdentity) {
    return { registration: null, target: null };
  }
  const storedRegistration = normalizeRegistration(
    await readRegistration(conversationUrl),
  );
  if (storedRegistration?.retirementPending) {
    await resumePendingWorkerRetirement(storedRegistration);
    return { registration: null, target: null };
  }
  let registration = await syncMainRegistrationFromBinding(storedRegistration);
  if (!registration) {
    return { registration: null, target: null };
  }
  if (registration.kind === "main") {
    await requestJson(
      `/api/main-runtime/review-targets/${registration.mainTargetId}/chat-heartbeat`,
      { method: "POST" },
    );
  }
  const path =
    registration.kind === "main"
      ? `/api/main-runtime/review-targets/${registration.mainTargetId}`
      : `/api/wake/targets/${registration.wakeTargetId}`;
  const payload = await requestJson(path);
  registration = await syncWorkerRegistrationFromBinding(
    registration,
    payload.target,
  );
  if (!registration) {
    return { registration: null, target: null };
  }
  const latestRegistration = normalizeRegistration(
    await readRegistration(conversationUrl),
  );
  if (
    latestRegistration?.retirementPending ||
    !runtimeRegistrationBindingMatches(latestRegistration, registration)
  ) {
    return { registration: null, target: null };
  }
  if (
    !runtimeOwnedSenderTabMatches(senderIdentity, latestRegistration, {
      conversationUrl: latestRegistration.conversationUrl,
    })
  ) {
    return { registration: null, target: null };
  }
  const identity = currentAdmittedWakingIdentity(
    latestRegistration,
    payload.target,
  );
  if (identity) {
    const observedAt = Date.now();
    void commitWakeContentDisposition({
      senderIdentity,
      identity,
      code: "poll_received",
      observedAt,
      expectedRegistration: latestRegistration,
      requirePriorPoll: false,
    }).catch(() => {});
  }
  return {
    registration: latestRegistration,
    target: payload.target,
  };
}

function currentAdmittedWakingIdentity(registration, target) {
  if (target?.status !== "waking") {
    return null;
  }
  const targetIdentity = runtimeTargetIdentity(registration, target);
  if (
    !isRuntimeTargetBoundToRegistration(registration, target, targetIdentity) ||
    registration.lastAdmittedDeliveryKey !== targetIdentity.key ||
    Number(registration.lastAdmittedWakeRevision) !== targetIdentity.wakeRevision
  ) {
    return null;
  }
  return normalizeFinalSendIdentity(registration.conversationUrl, {
    kind: targetIdentity.kind,
    mode: "wake",
    targetId: targetIdentity.targetId,
    targetRevision: targetIdentity.wakeRevision,
  });
}

function registrationHasCurrentAdmittedDelivery(registration, identity) {
  return Boolean(
    registration &&
      !registration.retirementPending &&
      registration.kind === identity.kind &&
      registrationTargetId(registration) === identity.targetId &&
      registration.lastAdmittedDeliveryKey ===
        `${identity.kind}:${identity.targetId}:${identity.targetRevision}` &&
      Number(registration.lastAdmittedWakeRevision) === identity.targetRevision
  );
}

function wakeContentDispositionMatchesIdentity(disposition, identity) {
  return Boolean(
    disposition &&
      disposition.conversationUrl === identity.conversationUrl &&
      disposition.kind === identity.kind &&
      disposition.mode === identity.mode &&
      disposition.targetId === identity.targetId &&
      Number(disposition.targetRevision) === identity.targetRevision
  );
}

function normalizeRuntimeSenderTab(senderTab) {
  const conversationUrl = normalizedChatConversationUrl(senderTab?.url);
  if (!Number.isInteger(senderTab?.id) || !conversationUrl) {
    return null;
  }
  return { tabId: senderTab.id, conversationUrl };
}

function runtimeOwnedSenderTabMatches(senderIdentity, registration, identity) {
  return Boolean(
    senderIdentity &&
      registration?.runtimeOpenedTabId === senderIdentity.tabId &&
      registration?.conversationUrl === identity.conversationUrl &&
      senderIdentity.conversationUrl === identity.conversationUrl
  );
}

async function commitWakeContentDisposition({
  senderIdentity,
  identity,
  code,
  observedAt,
  expectedRegistration,
  requirePriorPoll,
  expectedAdmission = null,
}) {
  const rank = WAKE_CONTENT_DISPOSITION_RANKS[code];
  if (
    !identity ||
    !rank ||
    !Number.isFinite(observedAt) ||
    observedAt <= 0 ||
    !expectedRegistration
  ) {
    return { safe: false };
  }
  const storageKey = registrationStorageKey(identity.conversationUrl);
  return mutateWakeContentDisposition(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      !registrationSnapshotsMatch(current, expectedRegistration) ||
      !registrationHasCurrentAdmittedDelivery(current, identity) ||
      !runtimeOwnedSenderTabMatches(senderIdentity, current, identity) ||
      (expectedAdmission &&
        matchingPendingFinalSendAdmission(senderIdentity, identity) !==
          expectedAdmission)
    ) {
      return { safe: false };
    }
    const stored = await chrome.storage.local.get(
      WAKE_CONTENT_DISPOSITION_STORAGE_KEY,
    );
    const dispositions = pruneWakeContentDispositionMap(
      stored[WAKE_CONTENT_DISPOSITION_STORAGE_KEY],
      registrations,
    );
    const latestRegistrations = (await readCanonicalRegistrationMap())
      .registrations;
    const latest = normalizeRegistration(latestRegistrations[storageKey]);
    if (
      !registrationSnapshotsMatch(latest, current) ||
      !registrationHasCurrentAdmittedDelivery(latest, identity) ||
      !runtimeOwnedSenderTabMatches(senderIdentity, latest, identity) ||
      (expectedAdmission &&
        matchingPendingFinalSendAdmission(senderIdentity, identity) !==
          expectedAdmission)
    ) {
      return { safe: false };
    }
    const previous = dispositions[storageKey];
    if (wakeContentDispositionMatchesIdentity(previous, identity)) {
      const previousRank = WAKE_CONTENT_DISPOSITION_RANKS[previous.code];
      if (!previousRank) {
        return { safe: false };
      }
      if (previousRank >= rank) {
        return { safe: true };
      }
      if (requirePriorPoll && previous.code !== "poll_received") {
        return { safe: false };
      }
    } else if (requirePriorPoll) {
      return { safe: false };
    }
    const disposition = {
      conversationUrl: identity.conversationUrl,
      kind: identity.kind,
      mode: identity.mode,
      targetId: identity.targetId,
      targetRevision: identity.targetRevision,
      code,
      observedAt,
    };
    dispositions[storageKey] = disposition;
    await chrome.storage.local.set({
      [WAKE_CONTENT_DISPOSITION_STORAGE_KEY]: dispositions,
    });
    return { safe: true };
  });
}

function mutateWakeContentDisposition(mutator) {
  const scheduled = wakeContentDispositionMutationTail.then(mutator, mutator);
  wakeContentDispositionMutationTail = scheduled.catch(() => {});
  return scheduled;
}

function pruneWakeContentDispositionMap(rawDispositions, registrations) {
  const raw =
    rawDispositions && typeof rawDispositions === "object"
      ? rawDispositions
      : {};
  const bounded = {};
  for (const [storageKey, registration] of Object.entries(registrations)) {
    const record = normalizeWakeContentDisposition(
      raw[storageKey],
      normalizeRegistration(registration),
    );
    if (record) {
      bounded[storageKey] = record;
    }
  }
  return bounded;
}

function normalizeWakeContentDisposition(record, registration) {
  if (
    !record ||
    Object.keys(record).length !== 7 ||
    !WAKE_CONTENT_DISPOSITION_RANKS[record.code] ||
    record.conversationUrl !== registration?.conversationUrl ||
    record.kind !== registration?.kind ||
    record.mode !== "wake" ||
    record.targetId !== registrationTargetId(registration) ||
    !Number.isSafeInteger(record.targetRevision) ||
    record.targetRevision <= 0 ||
    !Number.isFinite(record.observedAt) ||
    record.observedAt <= 0
  ) {
    return null;
  }
  return {
    conversationUrl: record.conversationUrl,
    kind: record.kind,
    mode: record.mode,
    targetId: record.targetId,
    targetRevision: record.targetRevision,
    code: record.code,
    observedAt: record.observedAt,
  };
}

function matchingPendingFinalSendAdmission(senderIdentity, identity) {
  const now = Date.now();
  for (const pending of pendingFinalSendAdmissions.values()) {
    if (
      pending.tabId === senderIdentity.tabId &&
      pending.expiresAt >= now &&
      finalSendIdentityMatches(pending, identity)
    ) {
      return pending;
    }
  }
  return null;
}

async function recordWakeContentDisposition({
  senderTab,
  conversationUrl,
  delivery,
  code,
}) {
  const identity = normalizeFinalSendIdentity(conversationUrl, delivery);
  const senderIdentity = normalizeRuntimeSenderTab(senderTab);
  const pendingAdmission =
    identity && senderIdentity
      ? matchingPendingFinalSendAdmission(senderIdentity, identity)
      : null;
  if (
    code !== "exact_current_entry" ||
    identity?.kind !== "worker" ||
    identity?.mode !== "wake" ||
    !senderIdentity ||
    !pendingAdmission
  ) {
    return { safe: false };
  }
  const storageKey = registrationStorageKey(identity.conversationUrl);
  const registration = normalizeRegistration(
    (await readCanonicalRegistrationMap()).registrations[storageKey],
  );
  if (
    !registrationHasCurrentAdmittedDelivery(registration, identity) ||
    !runtimeOwnedSenderTabMatches(senderIdentity, registration, identity)
  ) {
    return { safe: false };
  }
  const observedAt = Date.now();
  const result = await commitWakeContentDisposition({
    senderIdentity,
    identity,
    code,
    observedAt,
    expectedRegistration: registration,
    requirePriorPoll: true,
    expectedAdmission: pendingAdmission,
  });
  return { safe: result.safe };
}

async function syncWorkerRegistrationFromBinding(registration, target) {
  if (!registration || registration.kind !== "worker") {
    return registration;
  }
  // Only Workers provisioned with an explicit provider identity participate
  // in connector refresh. Legacy/manual registrations remain inert rather
  // than inferring an app from DOM state or writing a project binding.
  let currentIdentity = null;
  try {
    currentIdentity = requireProviderAppIdentity(registration.appIdentity);
  } catch (_error) {
    return registration;
  }
  const hasRegisteredConnectorEpoch = Object.prototype.hasOwnProperty.call(
    target || {},
    "registered_connector_epoch",
  );
  const rawRegisteredConnectorEpoch = target?.registered_connector_epoch;
  const registeredConnectorEpoch = Number(rawRegisteredConnectorEpoch);
  if (
    target?.wake_target_id !== registration.wakeTargetId ||
    target?.project_id !== registration.projectId ||
    !["idle", "waking", "claiming", "busy"].includes(target?.status)
  ) {
    await retireWorkerAfterBindingMismatch(registration, "target_binding_mismatch");
    return null;
  }
  const payload = await requestJson(
    `/api/project-bindings/${encodeURIComponent(registration.projectId)}/connector`,
  );
  const binding = payload?.connector;
  const bindingRevision = Number(binding?.revision);
  const connectorEpoch = Number(binding?.connector_epoch);
  let appIdentity = null;
  try {
    appIdentity = requireProviderAppIdentity({
      appRef: binding?.app_identity?.app_ref,
      appLabel: binding?.app_identity?.app_label,
    });
  } catch (_error) {
    await retireWorkerAfterBindingMismatch(registration, "connector_identity_invalid");
    return null;
  }
  const missingEpochAccepted =
    !hasRegisteredConnectorEpoch &&
    freshProvisionedConnectorProofMatches(
      registration,
      target,
      binding,
      appIdentity,
      bindingRevision,
      connectorEpoch,
    );
  if (
    binding?.project_id !== registration.projectId ||
    !Number.isSafeInteger(bindingRevision) ||
    bindingRevision < 0 ||
    !Number.isSafeInteger(connectorEpoch) ||
    connectorEpoch < 0 ||
    (!missingEpochAccepted &&
      (!hasRegisteredConnectorEpoch ||
        typeof rawRegisteredConnectorEpoch !== "number" ||
        !Number.isSafeInteger(registeredConnectorEpoch) ||
        registeredConnectorEpoch < 0 ||
        connectorEpoch !== registeredConnectorEpoch)) ||
    appIdentity.appRef !== currentIdentity.appRef
  ) {
    await retireWorkerAfterBindingMismatch(
      registration,
      !hasRegisteredConnectorEpoch
        ? "missing_registered_connector_epoch"
        : "connector_binding_mismatch",
    );
    return null;
  }
  const currentBindingRevision = Number(registration.bindingRevision || 0);
  const currentConnectorEpoch = Number(registration.connectorEpoch || 0);
  if (
    connectorEpoch < currentConnectorEpoch ||
    (connectorEpoch === currentConnectorEpoch &&
      bindingRevision < currentBindingRevision)
  ) {
    return registration;
  }
  if (
    currentIdentity.appLabel === appIdentity.appLabel &&
    currentBindingRevision === bindingRevision &&
    currentConnectorEpoch === connectorEpoch
  ) {
    return registration;
  }
  return updateRegistrationFromBinding(registration.conversationUrl, registration, {
    appIdentity,
    bindingRevision,
    connectorEpoch,
  });
}

function freshProvisionedConnectorProofMatches(
  registration,
  target,
  binding,
  appIdentity,
  bindingRevision,
  connectorEpoch,
) {
  const proof = registration.provisionedConnectorProof;
  return Boolean(
    proof &&
      typeof proof.requestId === "string" &&
      /^prq_[0-9a-f]{32}$/.test(proof.requestId) &&
      proof.wakeTargetId === registration.wakeTargetId &&
      proof.conversationId === registration.conversationId &&
      Number.isSafeInteger(proof.admissionRevision) &&
      proof.admissionRevision > 0 &&
      proof.projectId === registration.projectId &&
      proof.provider === binding?.provider &&
      proof.projectRef === binding?.project_ref &&
      proof.projectPath === binding?.project_path &&
      proof.appRef === appIdentity.appRef &&
      proof.appLabel === appIdentity.appLabel &&
      proof.bindingRevision === bindingRevision &&
      proof.connectorEpoch === connectorEpoch &&
      Number(registration.bindingRevision) === bindingRevision &&
      Number(registration.connectorEpoch) === connectorEpoch &&
      target?.wake_target_id === registration.wakeTargetId &&
      target?.project_id === registration.projectId &&
      Number.isFinite(Number(proof.finalizedAt)) &&
      Number(proof.finalizedAt) >= Number(registration.registeredAt || 0)
  );
}

async function retireWorkerAfterBindingMismatch(registration, reason) {
  if (
    registration?.retirementPending ||
    hasExactProvisionedWorkerProvenance(registration) ||
    hasProvisionedConnectorProvenance(registration) ||
    Number.isInteger(registration?.runtimeOpenedTabId)
  ) {
    await retireProvisionedWorkerOrClearOwnership(registration, reason);
  }
}

async function syncMainRegistrationFromBinding(registration) {
  if (!registration || registration.kind !== "main") {
    return registration;
  }
  const projectContext = chatGPTProjectConversationContext(
    registration.conversationUrl,
  );
  if (!projectContext) {
    return null;
  }
  const payload = await requestJson(
    `/api/project-bindings/${encodeURIComponent(registration.projectId)}/connector`,
  );
  const binding = payload?.connector;
  const bindingRevision = Number(binding?.revision);
  const connectorEpoch = Number(binding?.connector_epoch);
  let appIdentity = null;
  try {
    appIdentity = requireProviderAppIdentity({
      appRef: binding?.app_identity?.app_ref,
      appLabel: binding?.app_identity?.app_label,
    });
  } catch (_error) {
    return null;
  }
  if (
    binding?.project_id !== registration.projectId ||
    binding?.provider !== projectContext.provider ||
    binding?.project_ref !== projectContext.projectRef ||
    binding?.project_path !== projectContext.projectPath ||
    !Number.isSafeInteger(bindingRevision) ||
    bindingRevision < 0 ||
    !Number.isSafeInteger(connectorEpoch) ||
    connectorEpoch < 0
  ) {
    return null;
  }
  const currentBindingRevision = Number(registration.bindingRevision || 0);
  const currentConnectorEpoch = Number(registration.connectorEpoch || 0);
  if (
    connectorEpoch < currentConnectorEpoch ||
    (connectorEpoch === currentConnectorEpoch &&
      bindingRevision < currentBindingRevision)
  ) {
    return registration;
  }
  const identityChanged =
    registration.appIdentity?.appRef !== appIdentity.appRef ||
    registration.appIdentity?.appLabel !== appIdentity.appLabel;
  const bindingChanged =
    currentBindingRevision !== bindingRevision ||
    currentConnectorEpoch !== connectorEpoch;
  if (!identityChanged && !bindingChanged) {
    return registration;
  }
  return updateRegistrationFromBinding(
    registration.conversationUrl,
    registration,
    {
      appIdentity,
      bindingRevision,
      connectorEpoch,
      ...(identityChanged
        ? { lastAttemptRevision: 0, lastAttemptAt: 0 }
        : {}),
    },
  );
}

async function updateRegistrationFromBinding(
  conversationUrl,
  expectedRegistration,
  patch,
) {
  requireConversationUrl(conversationUrl);
  const storageKey = registrationStorageKey(conversationUrl);
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      current?.kind !== expectedRegistration.kind ||
      !runtimeRegistrationBindingMatches(current, expectedRegistration)
    ) {
      return null;
    }
    const currentConnectorEpoch = Number(current.connectorEpoch || 0);
    const currentBindingRevision = Number(current.bindingRevision || 0);
    if (
      currentConnectorEpoch > patch.connectorEpoch ||
      (currentConnectorEpoch === patch.connectorEpoch &&
        currentBindingRevision > patch.bindingRevision)
    ) {
      return current;
    }
    if (
      current.kind === "worker" &&
      current.appIdentity?.appRef !== patch.appIdentity?.appRef
    ) {
      return null;
    }
    const updated = { ...current, ...patch };
    registrations[storageKey] = updated;
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return updated;
  });
}

async function markAttempt({ conversationUrl, targetId, wakeRevision }) {
  requireConversationUrl(conversationUrl);
  const revision = Number(wakeRevision);
  if (!Number.isSafeInteger(revision) || revision <= 0) {
    throw new Error("Runtime attempt revision is invalid.");
  }
  const storageKey = registrationStorageKey(conversationUrl);
  const updated = await mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      !current ||
      current.retirementPending ||
      registrationTargetId(current) !== targetId
    ) {
      throw new Error("Runtime registration changed before the attempt could be recorded.");
    }
    if (Number(current.lastAttemptRevision || 0) > revision) {
      return current;
    }
    const next = {
      ...current,
      lastAttemptRevision: revision,
      lastAttemptAt: Date.now(),
    };
    registrations[storageKey] = next;
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return next;
  });
  return { registration: updated };
}

async function markResumeAttempt({ conversationUrl, wakeTargetId }) {
  requireConversationUrl(conversationUrl);
  const storageKey = registrationStorageKey(conversationUrl);
  const updated = await mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      !current ||
      current.retirementPending ||
      current.kind !== "worker" ||
      current.wakeTargetId !== wakeTargetId
    ) {
      throw new Error("Worker runtime registration changed before resume was recorded.");
    }
    const next = { ...current, lastResumeAttemptAt: Date.now() };
    registrations[storageKey] = next;
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return next;
  });
  return { registration: updated };
}

async function markMainResumeAttempt({ conversationUrl, mainTargetId }) {
  requireConversationUrl(conversationUrl);
  const storageKey = registrationStorageKey(conversationUrl);
  const updated = await mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (
      !current ||
      current.retirementPending ||
      current.kind !== "main" ||
      current.mainTargetId !== mainTargetId
    ) {
      throw new Error("Main runtime registration changed before resume was recorded.");
    }
    const next = { ...current, lastResumeAttemptAt: Date.now() };
    registrations[storageKey] = next;
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return next;
  });
  return { registration: updated };
}

async function requestJson(path, options = {}) {
  const response = await fetch(`${CONTROL_ORIGIN}${path}`, {
    cache: "no-store",
    ...options,
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch (_error) {
    throw new Error(`Runtime control returned HTTP ${response.status}.`);
  }
  if (!response.ok || payload?.ok === false) {
    const error = new Error(
      payload?.message || `Runtime control returned HTTP ${response.status}.`,
    );
    error.status = response.status;
    error.errorCode = payload?.error_code || null;
    throw error;
  }
  return payload;
}

async function readRegistration(conversationUrl) {
  requireConversationUrl(conversationUrl);
  const registrations = await readAllRegistrations();
  return registrations[registrationStorageKey(conversationUrl)] || null;
}

async function readAllRegistrations() {
  const snapshot = await readCanonicalRegistrationMap();
  if (!snapshot.changed) {
    return snapshot.registrations;
  }

  // Legacy-key cleanup is also a whole-map mutation. Re-read while holding
  // the same queue so a concurrent registration update is never overwritten.
  return mutateRegistrationMap(async () => {
    const latest = await readCanonicalRegistrationMap();
    if (latest.changed) {
      await chrome.storage.local.set({ [STORAGE_KEY]: latest.registrations });
    }
    return latest.registrations;
  });
}

async function mutateRegistrationMap(mutator) {
  const scheduled = registrationStorageMutationTail.then(mutator, mutator);
  registrationStorageMutationTail = scheduled.catch(() => {});
  return scheduled;
}

async function readCanonicalRegistrationMap() {
  const stored = await chrome.storage.local.get(STORAGE_KEY);
  const rawRegistrations = stored[STORAGE_KEY] || {};
  const canonical = {};
  let changed = false;
  for (const [legacyKey, rawRegistration] of Object.entries(rawRegistrations)) {
    if (
      Object.prototype.hasOwnProperty.call(
        rawRegistration || {},
        "lastWakeContentDisposition",
      )
    ) {
      changed = true;
    }
    const candidate = normalizeStoredRegistration(legacyKey, rawRegistration);
    if (!candidate) {
      changed = true;
      continue;
    }
    const storageKey = `conversation:${candidate.conversationId}`;
    const existing = canonical[storageKey];
    if (!existing || registrationFreshness(candidate) > registrationFreshness(existing)) {
      canonical[storageKey] = candidate;
    }
    if (legacyKey !== storageKey || existing) {
      changed = true;
    }
  }
  return { registrations: canonical, changed };
}

async function writeRegistration(conversationUrl, registration) {
  requireConversationUrl(conversationUrl);
  const normalizedUrl = normalizedChatConversationUrl(conversationUrl);
  const conversationId = chatConversationId(conversationUrl);
  if (!normalizedUrl || !conversationId) {
    throw new Error("ChatGPT conversation identity is unavailable.");
  }
  await mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    registrations[`conversation:${conversationId}`] = {
      ...registration,
      conversationId,
      conversationUrl: normalizedUrl,
    };
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
  });
}

async function updateRegistration(conversationUrl, patch) {
  requireConversationUrl(conversationUrl);
  const storageKey = registrationStorageKey(conversationUrl);
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (!current) {
      return null;
    }
    const updated = { ...current, ...patch };
    registrations[storageKey] = updated;
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return updated;
  });
}

async function updateRegistrationIfBindingMatches(
  conversationUrl,
  expectedRegistration,
  patch,
  { rejectNewerAdmissionThan = null } = {},
) {
  requireConversationUrl(conversationUrl);
  const storageKey = registrationStorageKey(conversationUrl);
  return mutateRegistrationMap(async () => {
    const { registrations } = await readCanonicalRegistrationMap();
    const current = normalizeRegistration(registrations[storageKey]);
    if (!runtimeRegistrationBindingMatches(current, expectedRegistration)) {
      return null;
    }
    if (
      Number.isSafeInteger(rejectNewerAdmissionThan) &&
      Number(current.lastAdmittedWakeRevision || 0) > rejectNewerAdmissionThan
    ) {
      return null;
    }
    const updated = { ...current, ...patch };
    registrations[storageKey] = updated;
    await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
    return updated;
  });
}

function normalizeStoredRegistration(storageKey, rawRegistration) {
  const registration = normalizeRegistration(rawRegistration);
  if (!registration) {
    return null;
  }
  const legacyUrl = storageKey.startsWith("https://") ? storageKey : null;
  const conversationUrl = normalizedChatConversationUrl(
    registration.conversationUrl || legacyUrl || "",
  );
  const conversationId =
    registration.conversationId || chatConversationId(conversationUrl || "");
  if (!conversationUrl || !conversationId) {
    return null;
  }
  return {
    ...registration,
    conversationId,
    conversationUrl,
  };
}

function registrationStorageKey(conversationUrl) {
  const conversationId = chatConversationId(conversationUrl);
  if (!conversationId) {
    throw new Error("ChatGPT conversation identity is unavailable.");
  }
  return `conversation:${conversationId}`;
}

function registrationFreshness(registration) {
  return Math.max(
    Number(registration.registeredAt || 0),
    Number(registration.lastAttemptAt || 0),
    Number(registration.lastResumeAttemptAt || 0),
    Number(registration.lastAdmittedAt || 0),
    Number(registration.runtimeOpenedAt || 0),
  );
}

function normalizeRegistration(registration) {
  if (!registration) {
    return null;
  }
  const {
    lastWakeContentDisposition: _lastWakeContentDisposition,
    ...storedRegistration
  } = registration;
  return {
    ...storedRegistration,
    kind: registration.kind || "worker",
    lastResumeAttemptAt: Number(registration.lastResumeAttemptAt || 0),
    lastAdmittedDeliveryKey:
      typeof registration.lastAdmittedDeliveryKey === "string"
        ? registration.lastAdmittedDeliveryKey
        : null,
    lastAdmittedWakeRevision: Number(registration.lastAdmittedWakeRevision || 0),
    lastAdmittedAt: Number(registration.lastAdmittedAt || 0),
  };
}

function registrationTargetId(registration) {
  return registration.kind === "main"
    ? registration.mainTargetId
    : registration.wakeTargetId;
}

function requireConversationUrl(value) {
  if (!normalizedChatConversationUrl(value)) {
    throw new Error("Open a persistent ChatGPT conversation before registering it.");
  }
}

function requireProjectId(value) {
  if (!/^prj_[a-z0-9-]{1,64}_[0-9a-f]{8}$/.test(String(value))) {
    throw new Error("Invalid ChatToCodex project ID.");
  }
}

function requireTargetKind(value) {
  if (value === "worker" || value === "main") {
    return value;
  }
  throw new Error("Select whether this Chat is a Main reviewer or Worker.");
}
