(() => {
const WAKE_ADAPTER_VERSION = chrome.runtime.getManifest().version;
const priorWakeAdapter = globalThis.__chatToCodexWakeAdapter;
if (
  priorWakeAdapter?.version === WAKE_ADAPTER_VERSION &&
  priorWakeAdapter.disposed !== true
) {
  return;
}
try {
  priorWakeAdapter?.dispose?.();
} catch (_error) {
  // An invalidated extension context can make old Chrome listener cleanup
  // throw. Replacement must still proceed with a fresh adapter instance.
}
delete globalThis.__chatToCodexWakeAdapter;
globalThis.__chatToCodexWakeLoaded = true;
globalThis.__chatToCodexWakeAdapter = {
  version: WAKE_ADAPTER_VERSION,
  disposed: false,
  dispose: null,
};

const POLL_INTERVAL_MS = 2000;
const RETRY_AFTER_MS = 90000;
let inFlightWakeKey = null;
let appSelectionFailureCode = "app_mention_unavailable";
let extensionContextAvailable = true;
let wakePollTimer = null;
let runtimeOwnedRegistrationDraft = null;
let provisioningDocumentEverClaimed =
  document.visibilityState === "visible" ||
  (typeof document.hasFocus === "function" && document.hasFocus());
function noteProvisioningDocumentVisibility() {
  if (document.visibilityState === "visible") {
    provisioningDocumentEverClaimed = true;
  }
}
function noteProvisioningDocumentFocus() {
  provisioningDocumentEverClaimed = true;
}
document.addEventListener?.(
  "visibilitychange",
  noteProvisioningDocumentVisibility,
);
window.addEventListener?.("focus", noteProvisioningDocumentFocus);

function handleRuntimeWakeMessage(message, _sender, sendResponse) {
  if (message?.type === "runtime:diagnoseWakeAdapter") {
    sendResponse({
      version: WAKE_ADAPTER_VERSION,
      extensionContextAvailable,
      pollTimerActive: wakePollTimer !== null,
      everClaimed: provisioningDocumentEverClaimed,
      composerPresent: Boolean(findComposer()),
      generating: isGeneratingResponse(),
    });
    return;
  }
  if (message?.type === "runtime:canPark") {
    const composer = findComposer();
    const canPark = Boolean(
      !inFlightWakeKey &&
        !isGeneratingResponse() &&
        composer &&
        !readComposerText(composer).trim(),
    );
    sendResponse({ ok: true, canPark });
    return;
  }
  if (message?.type === "provision:providerState") {
    sendResponse({ ok: true, rateLimited: hasProviderRateLimitSignal() });
    return;
  }
  if (message?.type === "runtime:consumeRegistrationDraft") {
    sendResponse(consumeRegistrationDraft(message.appIdentity));
    return;
  }
  if (message?.type === "provision:bootstrap") {
    void bootstrapProvisionedWorker(message.bootstrapToken, message.appIdentity)
      .then(sendResponse)
      .catch(() => sendResponse({ ok: false, code: "bootstrap_unavailable" }));
    return true;
  }
}
chrome.runtime.onMessage.addListener(handleRuntimeWakeMessage);

wakePollTimer = setInterval(() => {
  void pollWake();
}, POLL_INTERVAL_MS);
void pollWake();

globalThis.__chatToCodexWakeAdapter.dispose = () => {
  globalThis.__chatToCodexWakeAdapter.disposed = true;
  try {
    if (wakePollTimer !== null) {
      clearInterval(wakePollTimer);
    }
  } catch (_error) {
    // Continue removing the remaining owned listeners.
  } finally {
    wakePollTimer = null;
  }
  try {
    document.removeEventListener?.(
      "visibilitychange",
      noteProvisioningDocumentVisibility,
    );
  } catch (_error) {
    // Best-effort cleanup for an invalidated document context.
  }
  try {
    window.removeEventListener?.("focus", noteProvisioningDocumentFocus);
  } catch (_error) {
    // Best-effort cleanup for an invalidated document context.
  }
  try {
    chrome.runtime.onMessage.removeListener?.(handleRuntimeWakeMessage);
  } catch (_error) {
    // Best-effort cleanup for an invalidated extension context.
  }
};

async function pollWake() {
  const conversationUrl = normalizeConversationUrl(location.href);
  if (!conversationUrl || inFlightWakeKey) {
    return;
  }

  const response = await sendExtensionMessage({
    type: "wake:poll",
    conversationUrl,
  });
  if (!response?.ok || !response.registration || !response.target) {
    return;
  }

  const { registration, target } = response;
  const kind = registration.kind || "worker";
  if (
    target.status === "busy" &&
    kind === "worker" &&
    Number.isInteger(registration.runtimeOpenedTabId)
  ) {
    const lastResumeAttemptAt = Number(registration.lastResumeAttemptAt || 0);
    if (Date.now() - lastResumeAttemptAt >= RETRY_AFTER_MS) {
      const targetId = target.wake_target_id || null;
      if (targetId) {
        const resumeKey = `resume:${targetId}`;
        inFlightWakeKey = resumeKey;
        try {
          const sent = await sendWorkerResumeMessage(
            registration.appIdentity,
            targetId,
            Number(target.revision),
          );
          if (sent) {
            await sendExtensionMessage({
              type: "wake:markResumeAttempt",
              conversationUrl,
              wakeTargetId: targetId,
            });
          }
        } finally {
          if (inFlightWakeKey === resumeKey) {
            inFlightWakeKey = null;
          }
        }
      }
    }
    return;
  }
  if (
    target.status === "active" &&
    kind === "main" &&
    Number.isInteger(registration.runtimeOpenedTabId)
  ) {
    const lastResumeAttemptAt = Number(registration.lastResumeAttemptAt || 0);
    if (Date.now() - lastResumeAttemptAt >= RETRY_AFTER_MS) {
      const targetId = target.main_target_id || null;
      if (targetId) {
        const resumeKey = `main-resume:${targetId}`;
        inFlightWakeKey = resumeKey;
        try {
          const sent = await sendMainResumeMessage(
            registration.appIdentity,
            targetId,
            Number(target.revision),
          );
          if (sent) {
            await sendExtensionMessage({
              type: "wake:markMainResumeAttempt",
              conversationUrl,
              mainTargetId: targetId,
            });
          }
        } finally {
          if (inFlightWakeKey === resumeKey) {
            inFlightWakeKey = null;
          }
        }
      }
    }
    return;
  }
  if (target.status !== "waking") {
    return;
  }
  const wakeRevision = Number(target.wake_revision);
  if (!Number.isSafeInteger(wakeRevision) || wakeRevision <= 0) {
    return;
  }
  const recentlyAttempted =
    registration.lastAttemptRevision === wakeRevision &&
    Date.now() - Number(registration.lastAttemptAt || 0) < RETRY_AFTER_MS;
  if (recentlyAttempted) {
    return;
  }

  const targetId = runtimeTargetId({ kind, target });
  if (!targetId) {
    return;
  }
  const wakeKey = `${kind}:${targetId}:${wakeRevision}`;
  inFlightWakeKey = wakeKey;
  try {
    const sent = await sendWakeMessage({
      kind,
      targetId,
      wakeRevision,
      appIdentity: registration.appIdentity,
    });
    if (!sent) {
      return;
    }
    await sendExtensionMessage({
      type: "wake:markAttempt",
      conversationUrl,
      targetId,
      wakeRevision,
    });
  } finally {
    if (inFlightWakeKey === wakeKey) {
      inFlightWakeKey = null;
    }
  }
}

function runtimeTargetId({ kind, target }) {
  if (kind === "main") {
    return target.main_target_id || null;
  }
  return target.wake_target_id || null;
}

async function sendWakeMessage({ kind, targetId, wakeRevision, appIdentity }) {
  const command =
    kind === "main"
      ? ` 검수해\nmain_target_id: ${targetId}\nmain_wake_revision: ${wakeRevision}`
      : ` 작업해. 사용자가 Main Chat에서 Worker 위임을 요청했습니다. 이 서버 발급 wake pair는 현재 배정을 조회하기 위한 work 호출만 승인합니다. 파괴적·비가역적·자격증명 필요·외부 프로덕션 작업의 별도 승인을 뜻하지 않습니다.\nwake_target_id: ${targetId}\nwake_revision: ${wakeRevision}`;
  return sendAppCommand({
    appIdentity,
    command,
    draftMarker: targetId,
    delivery: { kind, targetId, targetRevision: wakeRevision, mode: "wake" },
  });
}

async function sendWorkerResumeMessage(appIdentity, targetId, targetRevision) {
  return sendAppCommand({
    appIdentity,
    command: " 작업 계속해",
    draftMarker: "작업 계속해",
    delivery: { kind: "worker", targetId, targetRevision, mode: "resume" },
  });
}

async function sendMainResumeMessage(appIdentity, targetId, targetRevision) {
  return sendAppCommand({
    appIdentity,
    command: " 검수 계속해",
    draftMarker: "검수 계속해",
    delivery: { kind: "main", targetId, targetRevision, mode: "resume" },
  });
}

async function sendAppCommand({
  appIdentity: rawAppIdentity,
  command,
  draftMarker,
  delivery = inferRuntimeDelivery(command, draftMarker),
}) {
  const conversationUrl = normalizeConversationUrl(location.href);
  const appIdentity = normalizeRuntimeAppIdentity(rawAppIdentity);
  if (!appIdentity || !isProvisioningDocumentStillUnclaimed()) {
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "initial_guard_rejected",
    );
  }
  if (isGeneratingResponse()) {
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "initial_generating",
    );
  }
  const preparedAdmission = await sendExtensionMessage({
    type: "wake:prepareFinalSend",
    conversationUrl,
    delivery,
  });
  if (
    !preparedAdmission?.ok ||
    !preparedAdmission.safe ||
    typeof preparedAdmission.admissionToken !== "string" ||
    !preparedAdmission.admissionToken ||
    !isProvisioningDocumentStillUnclaimed()
  ) {
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "prepare_rejected",
    );
  }
  const composer = findComposer();
  if (!composer) {
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "composer_missing",
    );
  }
  const originalRegistrationDraft = readComposerText(composer);
  const staleWakeRevision = exactSerializedWorkerWakeRevision(
    originalRegistrationDraft,
    appIdentity,
    delivery,
    command,
  );
  if (
    exactSerializedCurrentWorkerWakeDraft(
      originalRegistrationDraft,
      appIdentity,
      delivery,
      command,
    )
  ) {
    void sendExtensionMessage({
      type: "wake:recordContentDisposition",
      conversationUrl,
      delivery,
      code: "exact_current_entry",
    });
    const reservation = await sendExtensionMessage({
      type: "runtime:reserveExactCurrentWakeDraft",
      conversationUrl,
      appIdentity,
      draft: originalRegistrationDraft,
      admissionToken: preparedAdmission.admissionToken,
      delivery,
    });
    if (!reservation?.ok || !reservation.safe) {
      return false;
    }
    return sendReservedExactCurrentWakeDraft({
      composer,
      wakeDraft: originalRegistrationDraft,
      appIdentity,
      command,
      conversationUrl,
      delivery,
      admissionToken: preparedAdmission.admissionToken,
    });
  }
  if (staleWakeRevision !== null) {
    const cleanup = await sendExtensionMessage({
      type: "runtime:cleanupStaleWakeDraft",
      conversationUrl,
      appIdentity,
      draft: originalRegistrationDraft,
      admissionToken: preparedAdmission.admissionToken,
      delivery,
    });
    if (
      cleanup?.ok &&
      cleanup.safe &&
      typeof cleanup.draft === "string" &&
      normalizeConversationUrl(location.href) === conversationUrl &&
      findComposer() === composer &&
      readComposerText(composer) === originalRegistrationDraft &&
      composerHasExactInlineSelectionPill(composer, appIdentity) &&
      isProvisioningDocumentStillUnclaimed() &&
      !isGeneratingResponse()
    ) {
      replaceComposerText(composer, cleanup.draft);
    }
    return false;
  }
  const hasExistingDraft = Boolean(originalRegistrationDraft.trim());
  const hasLocalRuntimeOwnedRegistrationDraft =
    hasExistingDraft && consumeRuntimeOwnedRegistrationDraft(composer, appIdentity);
  const durableReservation = !hasLocalRuntimeOwnedRegistrationDraft
    ? await reserveDurableRuntimeOwnedAppDraft(
        composer,
        appIdentity,
        preparedAdmission.admissionToken,
        delivery,
      )
    : { safe: false };
  const hasDurableRuntimeOwnedRegistrationDraft = durableReservation.safe;
  const hasRuntimeOwnedRegistrationDraft =
    hasLocalRuntimeOwnedRegistrationDraft ||
    hasDurableRuntimeOwnedRegistrationDraft;
  if (
    !hasRuntimeOwnedRegistrationDraft &&
    (hasExistingDraft || durableReservation.attempted)
  ) {
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "durable_reserve_rejected",
      durableReservation.rejectionStage,
      durableReservation.rejectionSubstage,
    );
  }

  composer.focus();
  const durableAppNeedsReselection = Boolean(
    hasDurableRuntimeOwnedRegistrationDraft &&
      !composerHasSelectedApp(composer, appIdentity),
  );
  if (!hasRuntimeOwnedRegistrationDraft || durableAppNeedsReselection) {
    if (
      durableAppNeedsReselection &&
      (readComposerText(composer) !== originalRegistrationDraft ||
        !isProvisioningDocumentStillUnclaimed())
    ) {
      return recordWakeSendFailure(
        conversationUrl,
        delivery,
        "reselection_precondition_rejected",
      );
    }
    if (!replaceComposerText(composer, appMentionText(appIdentity))) {
      return recordWakeSendFailure(
        conversationUrl,
        delivery,
        "mention_injection_failed",
      );
    }
    await delay(450);
    // Mention discovery yields to page code. Do not append a runtime command to
    // text a person started composing while the picker was opening.
    if (
      !isProvisioningDocumentStillUnclaimed() ||
      !isRuntimeMentionDraftOwned(composer, appIdentity)
    ) {
      if (!hasExistingDraft) {
        clearBootstrapDraftIfUnchanged(composer, appIdentity);
      }
      return recordWakeSendFailure(
        conversationUrl,
        delivery,
        "mention_ownership_rejected",
      );
    }
    const mentionSelected = await ensureAppSelection(composer, appIdentity);
    if (!mentionSelected) {
      clearBootstrapDraftIfUnchanged(composer, appIdentity);
      return recordWakeSendFailure(
        conversationUrl,
        delivery,
        "app_selection_rejected",
      );
    }
  }
  await delay(200);
  // Selecting a mention can asynchronously rewrite the editable surface too.
  // Revalidate the same composer and its runtime-only draft before injecting.
  if (isGeneratingResponse()) {
    if (!hasExistingDraft) {
      clearBootstrapDraftIfUnchanged(composer, appIdentity);
    }
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "post_200_generating",
    );
  }
  if (!isProvisioningDocumentStillUnclaimed()) {
    if (!hasExistingDraft) {
      clearBootstrapDraftIfUnchanged(composer, appIdentity);
    }
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "post_200_unclaimed",
    );
  }
  if (
    !isRuntimeAppDraftOwned(composer, appIdentity, {
      allowEmptySelectedApp: hasDurableRuntimeOwnedRegistrationDraft,
    })
  ) {
    if (!hasExistingDraft) {
      clearBootstrapDraftIfUnchanged(composer, appIdentity);
    }
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "post_200_ownership_rejected",
    );
  }
  focusComposerEnd(composer);
  insertComposerText(composer, command);
  let wakeDraft = readComposerText(composer);
  if (!wakeDraft.includes(draftMarker)) {
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "marker_insertion_failed",
    );
  }

  // Give the provider's controlled contenteditable one reconciliation turn.
  // The input event above must commit the DOM insertion into provider state;
  // accept only the provider's one exact current-command serialization before
  // treating any other raw mismatch as a page or human replacement.
  await delay(50);
  if (
    exactSerializedCurrentWorkerWakeDraft(
      readComposerText(composer),
      appIdentity,
      delivery,
      command,
    )
  ) {
    wakeDraft = readComposerText(composer);
  }
  if (
    readComposerText(composer) !== wakeDraft ||
    (!isRuntimeWakeDraftOwned(composer, appIdentity, command, {
      allowEmptySelectedAppPrefix: hasDurableRuntimeOwnedRegistrationDraft,
    }) &&
      !exactSerializedCurrentWorkerWakeDraft(
        wakeDraft,
        appIdentity,
        delivery,
        command,
      ))
  ) {
    if (hasDurableRuntimeOwnedRegistrationDraft) {
      restoreRegistrationDraftIfWakeUnchanged(
        composer,
        wakeDraft,
        originalRegistrationDraft,
      );
    } else {
      clearWakeDraftIfUnchanged(composer, wakeDraft);
    }
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      "pre_confirm_ownership_rejected",
    );
  }

  const sendButton = await waitForSendButton(4000);
  if (
    exactSerializedCurrentWorkerWakeDraft(
      readComposerText(composer),
      appIdentity,
      delivery,
      command,
    )
  ) {
    wakeDraft = readComposerText(composer);
  }
  let preConfirmFailureReason = null;
  if (!sendButton) {
    preConfirmFailureReason = "send_button_absent";
  } else if (
    findComposer() !== composer ||
    readComposerText(composer) !== wakeDraft
  ) {
    preConfirmFailureReason = "pre_confirm_ownership_rejected";
  } else if (isGeneratingResponse()) {
    preConfirmFailureReason = "pre_confirm_generating";
  } else if (!isProvisioningDocumentStillUnclaimed()) {
    preConfirmFailureReason = "pre_confirm_unclaimed";
  } else if (
    !isRuntimeWakeDraftOwned(composer, appIdentity, command, {
      allowEmptySelectedAppPrefix: hasDurableRuntimeOwnedRegistrationDraft,
    }) &&
    !exactSerializedCurrentWorkerWakeDraft(
      wakeDraft,
      appIdentity,
      delivery,
      command,
    )
  ) {
    preConfirmFailureReason = "pre_confirm_ownership_rejected";
  }
  if (preConfirmFailureReason) {
    if (hasDurableRuntimeOwnedRegistrationDraft) {
      restoreRegistrationDraftIfWakeUnchanged(
        composer,
        wakeDraft,
        originalRegistrationDraft,
      );
    } else {
      clearWakeDraftIfUnchanged(composer, wakeDraft);
    }
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      preConfirmFailureReason,
    );
  }
  const finalAdmission = await sendExtensionMessage({
    type: "wake:confirmFinalSend",
    conversationUrl,
    admissionToken: preparedAdmission.admissionToken,
    delivery,
  });
  // No async boundary is permitted after this point. The background capability
  // is one-shot; re-read the raw editable surface and every local ownership
  // signal immediately before the click so activation or a human edit wins.
  let finalFailureReason = null;
  if (!finalAdmission?.ok || !finalAdmission.safe) {
    finalFailureReason = "confirm_rejected";
  } else if (normalizeConversationUrl(location.href) !== conversationUrl) {
    finalFailureReason = "final_url_changed";
  } else if (!isProvisioningDocumentStillUnclaimed()) {
    finalFailureReason = "final_unclaimed";
  } else if (findComposer() !== composer) {
    finalFailureReason = "final_composer_changed";
  } else if (readComposerText(composer) !== wakeDraft) {
    finalFailureReason = "final_raw_changed";
  } else if (!composerHasSelectedApp(composer, appIdentity)) {
    finalFailureReason = "final_app_changed";
  } else if (!isCurrentSendButton(sendButton)) {
    finalFailureReason = "final_button_changed";
  } else if (isGeneratingResponse()) {
    finalFailureReason = "final_generating";
  } else if (
    !isRuntimeWakeDraftOwned(composer, appIdentity, command, {
      allowEmptySelectedAppPrefix: hasDurableRuntimeOwnedRegistrationDraft,
    }) &&
    !exactSerializedCurrentWorkerWakeDraft(
      wakeDraft,
      appIdentity,
      delivery,
      command,
    )
  ) {
    finalFailureReason = "final_wake_ownership_rejected";
  }
  if (finalFailureReason) {
    if (hasDurableRuntimeOwnedRegistrationDraft) {
      restoreRegistrationDraftIfWakeUnchanged(
        composer,
        wakeDraft,
        originalRegistrationDraft,
      );
    } else {
      clearWakeDraftIfUnchanged(composer, wakeDraft);
    }
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      finalFailureReason,
    );
  }
  sendButton.click();
  return true;
}

async function sendReservedExactCurrentWakeDraft({
  composer,
  wakeDraft,
  appIdentity,
  command,
  conversationUrl,
  delivery,
  admissionToken,
}) {
  const sendButton = await waitForSendButton(4000);
  let preConfirmFailureReason = null;
  if (!sendButton) {
    preConfirmFailureReason = "send_button_absent";
  } else if (normalizeConversationUrl(location.href) !== conversationUrl) {
    preConfirmFailureReason = "pre_confirm_ownership_rejected";
  } else if (
    findComposer() !== composer ||
    readComposerText(composer) !== wakeDraft
  ) {
    preConfirmFailureReason = "pre_confirm_ownership_rejected";
  } else if (isGeneratingResponse()) {
    preConfirmFailureReason = "pre_confirm_generating";
  } else if (!isProvisioningDocumentStillUnclaimed()) {
    preConfirmFailureReason = "pre_confirm_unclaimed";
  } else if (
    !exactSerializedCurrentWorkerWakeDraft(
      wakeDraft,
      appIdentity,
      delivery,
      command,
    )
  ) {
    preConfirmFailureReason = "pre_confirm_ownership_rejected";
  }
  if (preConfirmFailureReason) {
    return recordWakeSendFailure(
      conversationUrl,
      delivery,
      preConfirmFailureReason,
    );
  }

  const finalAdmission = await sendExtensionMessage({
    type: "wake:confirmFinalSend",
    conversationUrl,
    admissionToken,
    delivery,
  });
  // The recovered provider serialization is immutable: do not normalize,
  // reinsert, restore, or clean it. Re-read every raw local ownership signal
  // without yielding after the one-shot background commit.
  if (
    !finalAdmission?.ok ||
    !finalAdmission.safe ||
    normalizeConversationUrl(location.href) !== conversationUrl ||
    !isProvisioningDocumentStillUnclaimed() ||
    findComposer() !== composer ||
    readComposerText(composer) !== wakeDraft ||
    !composerHasExactInlineSelectionPill(composer, appIdentity) ||
    !isCurrentSendButton(sendButton) ||
    isGeneratingResponse() ||
    !exactSerializedCurrentWorkerWakeDraft(
      wakeDraft,
      appIdentity,
      delivery,
      command,
    )
  ) {
    return false;
  }
  sendButton.click();
  return true;
}

function exactSerializedWorkerWakeRevision(
  rawDraft,
  appIdentity,
  delivery,
  currentCommand,
) {
  if (
    delivery?.kind !== "worker" ||
    delivery?.mode !== "wake" ||
    !Number.isSafeInteger(delivery.targetRevision) ||
    delivery.targetRevision <= 1 ||
    currentCommand !== workerWakeCommand(delivery.targetId, delivery.targetRevision) ||
    !composerHasExactInlineSelectionPill(findComposer(), appIdentity)
  ) {
    return null;
  }
  const draft = typeof rawDraft === "string" ? rawDraft : "";
  const revisionMatch = /wake_revision: ([1-9][0-9]*)$/.exec(draft);
  if (!revisionMatch) {
    return null;
  }
  const revision = Number(revisionMatch[1]);
  if (
    !Number.isSafeInteger(revision) ||
    revision < 1 ||
    revision >= delivery.targetRevision
  ) {
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
  const staleCommand = workerWakeCommand(delivery.targetId, revision);
  const providerFlattenedCommand = staleCommand.replace(/\n/g, " ");
  const liveProviderSerializedDraft = `\uFEFF${iconInitial}${label} ${providerFlattenedCommand}`;
  return prefixes.some(
    (prefix) =>
      draft === `${prefix}${staleCommand}` ||
      draft === `${prefix}${providerFlattenedCommand}`,
  ) || draft === liveProviderSerializedDraft
    ? revision
    : null;
}

function exactSerializedCurrentWorkerWakeDraft(
  rawDraft,
  appIdentity,
  delivery,
  currentCommand,
) {
  if (
    delivery?.kind !== "worker" ||
    delivery?.mode !== "wake" ||
    !Number.isSafeInteger(delivery.targetRevision) ||
    delivery.targetRevision < 1 ||
    currentCommand !== workerWakeCommand(delivery.targetId, delivery.targetRevision) ||
    !composerHasExactInlineSelectionPill(findComposer(), appIdentity)
  ) {
    return false;
  }
  const label = appIdentity.appLabel;
  const iconInitial = Array.from(label)[0] || "";
  return (
    rawDraft ===
    `\uFEFF${iconInitial}${label} ${currentCommand.replace(/\n/g, " ")}`
  );
}

function workerWakeCommand(wakeTargetId, wakeRevision) {
  return ` 작업해. 사용자가 Main Chat에서 Worker 위임을 요청했습니다. 이 서버 발급 wake pair는 현재 배정을 조회하기 위한 work 호출만 승인합니다. 파괴적·비가역적·자격증명 필요·외부 프로덕션 작업의 별도 승인을 뜻하지 않습니다.\nwake_target_id: ${wakeTargetId}\nwake_revision: ${wakeRevision}`;
}

async function recordWakeSendFailure(
  conversationUrl,
  delivery,
  reason,
  rejectionStage,
  rejectionSubstage,
) {
  if (conversationUrl) {
    await sendExtensionMessage({
      type: "wake:recordSendFailure",
      conversationUrl,
      delivery,
      reason,
      ...(rejectionStage === undefined ? {} : { rejectionStage }),
      ...(rejectionSubstage === undefined ? {} : { rejectionSubstage }),
    });
  }
  return false;
}

function inferRuntimeDelivery(command, draftMarker) {
  const mainRevision = /main_wake_revision:\s*(\d+)/.exec(command);
  const wakeRevision = /wake_revision:\s*(\d+)/.exec(command);
  return {
    kind: mainRevision ? "main" : "worker",
    targetId: draftMarker,
    targetRevision: Number((mainRevision || wakeRevision)?.[1] || 1),
    mode: mainRevision || wakeRevision ? "wake" : "resume",
  };
}

async function reserveDurableRuntimeOwnedAppDraft(
  composer,
  appIdentity,
  admissionToken,
  delivery,
) {
  if (!isProvisioningDocumentStillUnclaimed()) {
    return {
      safe: false,
      attempted: false,
      rejectionStage: "pre_candidate",
      rejectionSubstage: "document_claimed",
    };
  }
  const rejectionSubstage = durableRuntimeAppDraftCandidateRejectionSubstage(
    composer,
    appIdentity,
  );
  if (rejectionSubstage !== null) {
    return {
      safe: false,
      attempted: false,
      rejectionStage: "pre_candidate",
      rejectionSubstage,
    };
  }
  const durableDraft = readComposerText(composer);
  const response = await sendExtensionMessage({
    type: "runtime:consumeOwnedRegistrationDraft",
    conversationUrl: normalizeConversationUrl(location.href),
    appIdentity,
    draft: durableDraft,
    admissionToken,
    delivery,
  });
  if (!response?.ok || !response.safe) {
    return {
      safe: false,
      attempted: true,
      rejectionStage:
        typeof response?.rejectionStage === "string"
          ? response.rejectionStage
          : "background_admission",
    };
  }
  if (!isProvisioningDocumentStillUnclaimed()) {
    return { safe: false, attempted: true, rejectionStage: "post_unclaimed" };
  }
  if (findComposer() !== composer) {
    return { safe: false, attempted: true, rejectionStage: "post_composer" };
  }
  if (readComposerText(composer) !== durableDraft) {
    return { safe: false, attempted: true, rejectionStage: "post_raw" };
  }
  if (isGeneratingResponse()) {
    return { safe: false, attempted: true, rejectionStage: "post_generating" };
  }
  if (!isDurableRuntimeAppDraftCandidate(composer, appIdentity)) {
    return { safe: false, attempted: true, rejectionStage: "post_candidate" };
  }
  return { safe: true };
}

function isDurableRuntimeAppDraftCandidate(composer, appIdentity) {
  return durableRuntimeAppDraftCandidateRejectionSubstage(
    composer,
    appIdentity,
  ) === null;
}

function durableRuntimeAppDraftCandidateRejectionSubstage(
  composer,
  appIdentity,
) {
  if (findComposer() !== composer) {
    return "composer_changed";
  }
  if (composerContainsOnlyProviderAppDraft(composer, appIdentity)) {
    return null;
  }
  if (!readComposerText(composer).trim()) {
    return composerHasSelectedApp(composer, appIdentity)
      ? null
      : "empty_without_selected_app";
  }
  return "provider_draft_mismatch";
}

function isRuntimeAppDraftOwned(
  composer,
  appIdentity,
  { allowEmptySelectedApp = false } = {},
) {
  return Boolean(
    findComposer() === composer &&
      (composerContainsOnlyAppMention(composer, appIdentity) ||
        (allowEmptySelectedApp &&
          !readComposerText(composer).trim() &&
          composerHasSelectedApp(composer, appIdentity))),
  );
}

function isRuntimeMentionDraftOwned(composer, appIdentity) {
  return (
    findComposer() === composer && composerContainsOnlyAppMention(composer, appIdentity)
  );
}

function isRuntimeWakeDraftOwned(
  composer,
  appIdentity,
  command,
  { allowEmptySelectedAppPrefix = false } = {},
) {
  if (
    findComposer() !== composer ||
    !composerHasSelectedApp(composer, appIdentity)
  ) {
    return false;
  }
  const rawText = readComposerText(composer);
  if (!rawText.endsWith(command)) {
    return false;
  }
  const rawPrefix = rawText.slice(0, -command.length);
  const text = normalizeComposerAppText(rawPrefix);
  if (!text && allowEmptySelectedAppPrefix) {
    return true;
  }
  const label = normalizeComposerAppText(appIdentity.appLabel);
  const iconInitial = Array.from(label)[0] || "";
  const prefixes = [
    `@${label}`,
    label,
    iconInitial,
    `@${iconInitial}`,
    `${iconInitial}${label}`,
    `@${iconInitial}${label}`,
  ].filter(Boolean);
  return prefixes.some((prefix) => text === prefix);
}

async function bootstrapProvisionedWorker(bootstrapToken, rawAppIdentity) {
  if (typeof bootstrapToken !== "string" || !bootstrapToken) {
    return { ok: false, code: "bootstrap_ownership_unavailable", retryable: false };
  }
  const appIdentity = normalizeRuntimeAppIdentity(rawAppIdentity);
  if (!appIdentity) {
    return { ok: false, code: "app_identity_unavailable", retryable: false };
  }
  if (hasProviderRateLimitSignal()) {
    return { ok: false, code: "provider_rate_limited", rateLimited: true };
  }
  if (isGeneratingResponse()) {
    return { ok: false, code: "generation_in_progress", retryable: false };
  }
  const composer = findComposer();
  if (!composer) {
    return { ok: false, code: "bootstrap_surface_unavailable", retryable: true };
  }
  const hasRuntimeAppDraftOnly =
    isProvisioningDocumentStillUnclaimed() &&
    composerContainsOnlyAppMention(composer, appIdentity);
  if (readComposerText(composer).trim() && !hasRuntimeAppDraftOnly) {
    return { ok: false, code: "composer_not_safe", retryable: false };
  }

  // A new conversation needs a real message to obtain its persistent `/c/`
  // identity.  The message contains only the app mention: no work payload is
  // injected until the normal post-registration wake path later authorizes it.
  composer.focus();
  if (
    !hasRuntimeAppDraftOnly &&
    !replaceComposerText(composer, appMentionText(appIdentity))
  ) {
    return { ok: false, code: "bootstrap_unavailable", retryable: false };
  }
  await delay(450);
  if (!isRuntimeMentionDraftOwned(composer, appIdentity)) {
    return { ok: false, code: "composer_changed", retryable: false };
  }
  if (!(await ensureAppSelection(composer, appIdentity))) {
    clearBootstrapDraftIfUnchanged(composer, appIdentity);
    return {
      ok: false,
      code: appSelectionFailureCode,
      retryable:
        appSelectionFailureCode === "app_menu_unavailable" ||
        appSelectionFailureCode === "app_selection_unconfirmed",
    };
  }
  await delay(200);
  if (!isRuntimeMentionDraftOwned(composer, appIdentity)) {
    return { ok: false, code: "composer_changed", retryable: false };
  }
  const sendButton = await waitForSendButton(4000);
  if (
    !sendButton ||
    !isCurrentSendButton(sendButton) ||
    isGeneratingResponse() ||
    hasProviderRateLimitSignal() ||
    !isRuntimeMentionDraftOwned(composer, appIdentity) ||
    !isProvisioningDocumentStillUnclaimed()
  ) {
    clearBootstrapDraftIfUnchanged(composer, appIdentity);
    return hasProviderRateLimitSignal()
      ? { ok: false, code: "provider_rate_limited", rateLimited: true }
      : { ok: false, code: "bootstrap_unavailable", retryable: false };
  }
  const ownership = await sendExtensionMessage({
    type: "provision:confirmBootstrapSubmit",
    bootstrapToken,
  });
  // The background check yields to Chrome and page code. Re-read every local
  // ownership signal afterwards so a replaced composer, a human draft, or a
  // newly focused provisioning tab can never inherit the old send button.
  if (
    !ownership?.ok ||
    !ownership.safe ||
    !isCurrentSendButton(sendButton) ||
    !isRuntimeMentionDraftOwned(composer, appIdentity) ||
    !isProvisioningDocumentStillUnclaimed() ||
    isGeneratingResponse() ||
    hasProviderRateLimitSignal()
  ) {
    clearBootstrapDraftIfUnchanged(composer, appIdentity);
    return { ok: false, code: "bootstrap_ownership_lost", retryable: false };
  }
  const registrationDraft = readComposerText(composer);
  sendButton.click();
  return { ok: true, registrationDraft };
}

function isCurrentSendButton(sendButton) {
  return (
    document.querySelector('button[data-testid="send-button"]') === sendButton ||
    document.querySelector('button[aria-label="Send prompt"]') === sendButton
  );
}

function isProvisioningDocumentStillUnclaimed() {
  if (provisioningDocumentEverClaimed) {
    return false;
  }
  if (
    typeof document.visibilityState === "string" &&
    document.visibilityState !== "hidden"
  ) {
    return false;
  }
  return typeof document.hasFocus !== "function" || !document.hasFocus();
}

function normalizeRuntimeAppIdentity(rawIdentity) {
  const appRef = String(rawIdentity?.appRef || "").trim();
  const appLabel = String(rawIdentity?.appLabel || "").trim();
  if (
    (appRef && !/^plugin_[A-Za-z0-9_-]+$/.test(appRef)) ||
    !appLabel ||
    appLabel.length > 255
  ) {
    return null;
  }
  return { appRef: appRef || null, appLabel };
}

function appMentionText(appIdentity) {
  return `@${appIdentity.appLabel}`;
}

function composerContainsOnlyAppMention(composer, appIdentity) {
  const text = normalizeComposerAppText(readComposerText(composer));
  const label = normalizeComposerAppText(appIdentity.appLabel);
  if (text === `@${label}` || text === label) {
    return true;
  }
  if (!composerHasSelectedApp(composer, appIdentity)) {
    return false;
  }
  const iconInitial = Array.from(label)[0] || "";
  return Boolean(
    iconInitial &&
      (text === iconInitial ||
        text === `@${iconInitial}` ||
        text === `${iconInitial}${label}` ||
        text === `@${iconInitial}${label}`),
  );
}

function composerContainsOnlyProviderAppDraft(composer, appIdentity) {
  const text = normalizeComposerAppText(readComposerText(composer));
  const label = normalizeComposerAppText(appIdentity.appLabel);
  const iconInitial = Array.from(label)[0] || "";
  return [
    `@${label}`,
    label,
    iconInitial,
    `@${iconInitial}`,
    `${iconInitial}${label}`,
    `@${iconInitial}${label}`,
  ]
    .filter(Boolean)
    .includes(text);
}

function normalizeComposerAppText(value) {
  return String(value || "")
    .replace(/[\u200B-\u200D\u2060\uFEFF]/g, "")
    .replace(/\s+/g, "")
    .toLocaleLowerCase();
}

function clearBootstrapDraftIfUnchanged(composer, appIdentity) {
  if (composerContainsOnlyAppMention(composer, appIdentity)) {
    replaceComposerText(composer, "");
  }
}

function consumeRegistrationDraft(rawAppIdentity) {
  const appIdentity = normalizeRuntimeAppIdentity(rawAppIdentity);
  const composer = findComposer();
  if (!appIdentity || !composer) {
    return { ok: false, cleared: false };
  }
  if (
    !composerHasSelectedApp(composer, appIdentity) ||
    !composerContainsOnlyAppMention(composer, appIdentity)
  ) {
    return { ok: true, owned: false };
  }
  clearRuntimeOwnedRegistrationDraft();
  const marker = {
    appKey: runtimeAppIdentityKey(appIdentity),
    composer,
    conversationUrl: normalizeConversationUrl(location.href),
    draft: readComposerText(composer),
    expiresAt: Date.now() + 15000,
    invalidate: null,
  };
  marker.invalidate = () => clearRuntimeOwnedRegistrationDraft(marker);
  composer.addEventListener?.("beforeinput", marker.invalidate, { once: true });
  composer.addEventListener?.("input", marker.invalidate, { once: true });
  runtimeOwnedRegistrationDraft = marker;
  return { ok: true, owned: true };
}

function consumeRuntimeOwnedRegistrationDraft(composer, appIdentity) {
  const marker = runtimeOwnedRegistrationDraft;
  clearRuntimeOwnedRegistrationDraft(marker);
  const expectedKey = runtimeAppIdentityKey(appIdentity);
  if (
    !marker ||
    marker.appKey !== expectedKey ||
    marker.composer !== composer ||
    marker.conversationUrl !== normalizeConversationUrl(location.href) ||
    marker.draft !== readComposerText(composer) ||
    marker.expiresAt < Date.now() ||
    !composerHasSelectedApp(composer, appIdentity) ||
    !composerContainsOnlyAppMention(composer, appIdentity)
  ) {
    return false;
  }
  return true;
}

function clearRuntimeOwnedRegistrationDraft(expectedMarker = null) {
  const marker = runtimeOwnedRegistrationDraft;
  if (!marker || (expectedMarker && marker !== expectedMarker)) {
    return;
  }
  runtimeOwnedRegistrationDraft = null;
  if (marker.invalidate) {
    marker.composer.removeEventListener?.("beforeinput", marker.invalidate);
    marker.composer.removeEventListener?.("input", marker.invalidate);
  }
}

function runtimeAppIdentityKey(appIdentity) {
  return `${appIdentity.appRef || ""}\u0000${appIdentity.appLabel}`;
}

function findComposer() {
  return (
    document.querySelector('#prompt-textarea[contenteditable="true"]') ||
    document.querySelector('textarea#prompt-textarea') ||
    document.querySelector('[contenteditable="true"][data-virtualkeyboard="true"]')
  );
}

function isGeneratingResponse() {
  return Boolean(
    document.querySelector('button[data-testid="stop-button"]') ||
      document.querySelector('button[aria-label*="Stop"]'),
  );
}

function hasProviderRateLimitSignal() {
  const bodyText = (document.body?.innerText || "").toLowerCase();
  return (
    bodyText.includes("too many requests") ||
    bodyText.includes("rate limit") ||
    bodyText.includes("요청이 너무 많")
  );
}

function selectAppMention(appIdentity, { allowHiddenControls = false } = {}) {
  const candidates = Array.from(
    document.querySelectorAll(
      '[role="option"], [role="menuitem"], [role="menuitemcheckbox"], [data-radix-collection-item], .popover .__menu-item',
    ),
  ).filter((candidate) =>
    isEligibleProviderControl(candidate, { allowHiddenControls }),
  );
  const referenceMatches = [];
  const labelMatches = [];
  for (const candidate of candidates) {
    const reference = providerAppReference(candidate);
    if (appIdentity.appRef && reference === appIdentity.appRef) {
      referenceMatches.push(candidate);
      continue;
    }
    if (!reference && candidateMatchesAppLabel(candidate, appIdentity.appLabel)) {
      labelMatches.push(candidate);
    }
  }
  const matches = referenceMatches.length > 0 ? referenceMatches : labelMatches;
  if (matches.length === 0) {
    return "unavailable";
  }
  if (matches.length > 1) {
    appSelectionFailureCode = "app_menu_ambiguous";
    return "ambiguous";
  }
  activateProviderControl(matches[0]);
  return "selected";
}

function candidateMatchesAppLabel(candidate, rawLabel) {
  const label = normalizeComposerAppText(rawLabel);
  const candidateTexts = [candidate?.textContent];
  for (const labelElement of candidate?.querySelectorAll?.("span") || []) {
    candidateTexts.push(labelElement.textContent);
  }
  return candidateTexts.some((candidateText) =>
    normalizedCandidateMatchesAppLabel(candidateText, label),
  );
}

function normalizedCandidateMatchesAppLabel(rawCandidateText, label) {
  const text = normalizeComposerAppText(rawCandidateText);
  const iconInitial = Array.from(label)[0] || "";
  return Boolean(
    label &&
      (text === label ||
        text === `${iconInitial}${label}` ||
        text === `${label}${label}` ||
        text === `${iconInitial}${label}${label}`)
  );
}

async function ensureAppSelection(composer, appIdentity) {
  appSelectionFailureCode = "app_mention_unavailable";
  const allowHiddenControls = isProvisioningDocumentStillUnclaimed();
  if (
    await waitForAppSelection(composer, appIdentity, 1000, {
      allowHiddenControls,
    })
  ) {
    return true;
  }
  const mentionSelectionUnconfirmed =
    appSelectionFailureCode === "app_selection_unconfirmed";
  if (appSelectionFailureCode === "app_menu_ambiguous") {
    return false;
  }
  if (
    findComposer() !== composer ||
    !composerContainsOnlyAppMention(composer, appIdentity) ||
    isGeneratingResponse()
  ) {
    return false;
  }
  if (!replaceComposerText(composer, "") || readComposerText(composer).trim()) {
    return false;
  }
  const picker = await waitForPluginPickerButton(composer, 4000, {
    allowHiddenControls,
  });
  if (!picker || findComposer() !== composer || isGeneratingResponse()) {
    appSelectionFailureCode = mentionSelectionUnconfirmed
      ? "app_selection_unconfirmed"
      : "app_picker_unavailable";
    return false;
  }
  activateProviderControl(picker.control);
  if (picker.searchInComposer) {
    if (
      findComposer() !== composer ||
      readComposerText(composer).trim() ||
      isGeneratingResponse() ||
      !replaceComposerText(composer, appIdentity.appLabel)
    ) {
      return false;
    }
    await delay(100);
  }
  const deadline = Date.now() + 2000;
  while (Date.now() <= deadline) {
    if (
      findComposer() !== composer ||
      !isRuntimePickerDraftOwned(composer, appIdentity, picker) ||
      isGeneratingResponse()
    ) {
      return false;
    }
    const selection = selectAppMention(appIdentity, {
      allowHiddenControls:
        allowHiddenControls && isProvisioningDocumentStillUnclaimed(),
    });
    if (selection === "ambiguous") {
      return false;
    }
    if (selection === "selected") {
      const selected = await waitForConfirmedAppSelection(
        composer,
        appIdentity,
        4000,
      );
      if (!selected) {
        appSelectionFailureCode = "app_selection_unconfirmed";
      }
      return selected;
    }
    await delay(100);
  }
  appSelectionFailureCode = "app_menu_unavailable";
  return false;
}

async function waitForConfirmedAppSelection(
  composer,
  appIdentity,
  timeoutMilliseconds,
) {
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() <= deadline) {
    if (findComposer() !== composer || isGeneratingResponse()) {
      return false;
    }
    const text = readComposerText(composer).trim();
    if (
      composerHasSelectedApp(composer, appIdentity) &&
      composerContainsOnlyAppMention(composer, appIdentity)
    ) {
      return true;
    }
    if (text && !composerContainsOnlyAppMention(composer, appIdentity)) {
      return false;
    }
    await delay(100);
  }
  return false;
}

async function waitForPluginPickerButton(
  composer,
  timeoutMilliseconds,
  { allowHiddenControls = false } = {},
) {
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() <= deadline) {
    if (
      findComposer() !== composer ||
      readComposerText(composer).trim() ||
      isGeneratingResponse()
    ) {
      return null;
    }
    const picker = findPluginPickerButton({
      allowHiddenControls:
        allowHiddenControls && isProvisioningDocumentStillUnclaimed(),
    });
    if (picker) {
      return picker;
    }
    await delay(100);
  }
  return null;
}

function findPluginPickerButton({ allowHiddenControls = false } = {}) {
  for (const candidate of document.querySelectorAll("button")) {
    const text = (candidate.textContent || "").trim().toLowerCase();
    if (
      (text === "plugins" || text === "플러그인") &&
      isEligibleProviderControl(candidate, { allowHiddenControls })
    ) {
      return { control: candidate, searchInComposer: false };
    }
  }
  const composerPlus = document.querySelector(
    'button[data-testid="composer-plus-btn"]',
  );
  if (
    composerPlus &&
    isEligibleProviderControl(composerPlus, { allowHiddenControls })
  ) {
    return { control: composerPlus, searchInComposer: true };
  }
  return null;
}

function isRuntimePickerDraftOwned(composer, appIdentity, picker) {
  if (picker.searchInComposer) {
    return (
      normalizeComposerAppText(readComposerText(composer)) ===
      normalizeComposerAppText(appIdentity.appLabel)
    );
  }
  return !readComposerText(composer).trim();
}

function activateProviderControl(control) {
  // ChatGPT's current app menu exposes a focused, highlighted menu item whose
  // direct HTMLElement click can be ignored. Prefer the menu's Enter-key path,
  // then retain the direct click for provider controls that do not handle it.
  // Either path is still accepted only after the exact selected-app pill
  // appears in the unchanged composer.
  control.focus?.();
  const enterKey = {
    key: "Enter",
    code: "Enter",
    keyCode: 13,
    which: 13,
    bubbles: true,
    cancelable: true,
    composed: true,
  };
  const keydownHandled =
    control.dispatchEvent?.(new KeyboardEvent("keydown", enterKey)) === false;
  const keyupHandled =
    control.dispatchEvent?.(new KeyboardEvent("keyup", enterKey)) === false;
  if (!keydownHandled && !keyupHandled && control.isConnected !== false) {
    control.click();
  }
}

async function waitForAppSelection(
  composer,
  appIdentity,
  timeoutMilliseconds,
  { allowHiddenControls = false } = {},
) {
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() <= deadline) {
    if (
      findComposer() !== composer ||
      !composerContainsOnlyAppMention(composer, appIdentity) ||
      isGeneratingResponse()
    ) {
      return false;
    }
    if (composerHasSelectedApp(composer, appIdentity)) {
      return true;
    }
    const selection = selectAppMention(appIdentity, {
      allowHiddenControls:
        allowHiddenControls && isProvisioningDocumentStillUnclaimed(),
    });
    if (selection === "ambiguous") {
      return false;
    }
    if (selection === "selected") {
      const selected = await waitForConfirmedAppSelection(
        composer,
        appIdentity,
        4000,
      );
      if (!selected) {
        appSelectionFailureCode = "app_selection_unconfirmed";
      }
      return selected;
    }
    await delay(100);
  }
  return false;
}

function composerHasSelectedApp(composer, appIdentity) {
  let scope = composer;
  for (let depth = 0; depth < 5 && scope; depth += 1, scope = scope.parentElement) {
    const candidates = scope.querySelectorAll?.('a[href*="/plugins/plugin_"]') || [];
    for (const candidate of candidates) {
      const reference = providerAppReference(candidate);
      const label = (candidate.textContent || "").trim().toLocaleLowerCase();
      if (
        (appIdentity.appRef && reference === appIdentity.appRef) ||
        (!appIdentity.appRef && label === appIdentity.appLabel.toLocaleLowerCase())
      ) {
        return true;
      }
    }
  }
  return false;
}

function composerHasExactInlineSelectionPill(composer, appIdentity) {
  if (!composer || findComposer() !== composer || !appIdentity?.appRef) {
    return false;
  }
  const wrappers = Array.from(
    composer.querySelectorAll?.("[data-inline-selection-pill]") || [],
  );
  if (wrappers.length !== 1) {
    return false;
  }
  const wrapper = wrappers[0];
  if (
    !composer.contains(wrapper) ||
    wrapper.getAttribute?.("contenteditable") !== "false" ||
    wrapper.getAttribute?.("data-symbol") !== "ecosystemMention"
  ) {
    return false;
  }
  const anchors = Array.from(
    wrapper.querySelectorAll?.("a") || [],
  );
  if (anchors.length !== 1 || !wrapper.contains?.(anchors[0])) {
    return false;
  }
  const href = anchors[0].getAttribute?.("href") || "";
  let url;
  try {
    url = new URL(href, "https://chatgpt.com");
  } catch (_error) {
    return false;
  }
  return Boolean(
    url.origin === "https://chatgpt.com" &&
      url.pathname === `/plugins/${appIdentity.appRef}` &&
    providerAppReference(anchors[0]) === appIdentity.appRef &&
      url.searchParams.get("plugin_detail_origin") ===
        "inline_selection_pill",
  );
}

function providerAppReference(candidate) {
  const href =
    candidate?.getAttribute?.("href") ||
    candidate?.querySelector?.('a[href*="/plugins/plugin_"]')?.getAttribute?.(
      "href",
    ) ||
    "";
  return href.match(/\/plugins\/(plugin_[A-Za-z0-9_-]+)/)?.[1] || null;
}

function isEligibleProviderControl(control, { allowHiddenControls = false } = {}) {
  if (!control || control.isConnected === false || control.disabled === true) {
    return false;
  }
  if (
    control.getAttribute?.("aria-disabled") === "true" ||
    control.getAttribute?.("aria-hidden") === "true"
  ) {
    return false;
  }
  return isVisible(control) || allowHiddenControls;
}

function replaceComposerText(composer, text) {
  if (composer instanceof HTMLTextAreaElement) {
    const descriptor = Object.getOwnPropertyDescriptor(
      HTMLTextAreaElement.prototype,
      "value",
    );
    descriptor?.set?.call(composer, text);
    composer.dispatchEvent(new InputEvent("input", { bubbles: true, data: text }));
    return composer.value === text;
  }

  const residualReplacement = replaceExactSelectedAppResidual(composer, text);
  if (residualReplacement !== null) {
    return residualReplacement && readComposerText(composer) === text;
  }
  if (
    (composer.querySelectorAll?.("[data-inline-selection-pill]") || []).length > 0
  ) {
    return false;
  }
  const selection = window.getSelection();
  const range = document.createRange();
  range.selectNodeContents(composer);
  selection?.removeAllRanges();
  selection?.addRange(range);
  return executeContentEditableCommand("insertText", text) &&
    readComposerText(composer) === text;
}

function insertComposerText(composer, text) {
  if (composer instanceof HTMLTextAreaElement) {
    composer.setRangeText(text, composer.selectionStart, composer.selectionEnd, "end");
    composer.dispatchEvent(new InputEvent("input", { bubbles: true, data: text }));
    return;
  }

  // ProseMirror-backed composers can render one multiline insertText mutation
  // transiently, then restore their previous editor state.  Express the edit
  // as the same sequence the editor understands: text transactions separated
  // by explicit line-break transactions.
  const lines = text.split("\n");
  for (const [index, line] of lines.entries()) {
    if (line && !executeContentEditableCommand("insertText", line)) {
      return false;
    }
    if (
      index < lines.length - 1 &&
      !executeContentEditableCommand("insertLineBreak", null)
    ) {
      return false;
    }
  }
  return true;
}

function replaceExactSelectedAppResidual(composer, text) {
  const children = Array.from(composer.childNodes || []);
  if (
    children.length !== 3 ||
    children[0]?.nodeType !== 3 ||
    children[0].textContent !== "\uFEFF" ||
    children[2]?.nodeType !== 3
  ) {
    return null;
  }
  const wrapper = children[1];
  if (
    wrapper?.getAttribute?.("data-inline-selection-pill") !== "true" ||
    wrapper.getAttribute?.("contenteditable") !== "false" ||
    wrapper.getAttribute?.("data-symbol") !== "ecosystemMention"
  ) {
    return null;
  }
  const prefix = children[0].textContent + (wrapper.textContent || "");
  if (!readComposerText(composer).startsWith(prefix) || !text.startsWith(prefix)) {
    return null;
  }
  const selection = window.getSelection();
  const range = document.createRange();
  range.selectNodeContents(children[2]);
  selection?.removeAllRanges();
  selection?.addRange(range);
  return executeContentEditableCommand("insertText", text.slice(prefix.length));
}

function executeContentEditableCommand(command, data) {
  try {
    return document.execCommand(command, false, data ?? "") === true;
  } catch (_error) {
    return false;
  }
}

function focusComposerEnd(composer) {
  composer.focus();
  if (composer instanceof HTMLTextAreaElement) {
    const end = composer.value.length;
    composer.setSelectionRange(end, end);
    return;
  }
  const selection = window.getSelection();
  const range = document.createRange();
  range.selectNodeContents(composer);
  range.collapse(false);
  selection?.removeAllRanges();
  selection?.addRange(range);
}

function readComposerText(composer) {
  if (composer instanceof HTMLTextAreaElement) {
    return composer.value;
  }
  return composer.textContent || "";
}

function clearWakeDraftIfUnchanged(composer, wakeDraft) {
  if (findComposer() !== composer || readComposerText(composer) !== wakeDraft) {
    return;
  }
  replaceComposerText(composer, "");
}

function restoreRegistrationDraftIfWakeUnchanged(
  composer,
  wakeDraft,
  registrationDraft,
) {
  if (findComposer() !== composer || readComposerText(composer) !== wakeDraft) {
    return;
  }
  replaceComposerText(composer, registrationDraft);
}

async function waitForSendButton(timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const button =
      document.querySelector('button[data-testid="send-button"]') ||
      document.querySelector('button[aria-label="Send prompt"]');
    if (button && !button.disabled && isVisible(button)) {
      return button;
    }
    await delay(100);
  }
  return null;
}

function isVisible(element) {
  const rect = element.getBoundingClientRect();
  return rect.width > 0 && rect.height > 0;
}

function normalizeConversationUrl(rawUrl) {
  try {
    const url = new URL(rawUrl);
    if (url.origin !== "https://chatgpt.com" || !url.pathname.includes("/c/")) {
      return null;
    }
    return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
  } catch (_error) {
    return null;
  }
}

function sendExtensionMessage(message) {
  if (!extensionContextAvailable) {
    return Promise.resolve(null);
  }
  return new Promise((resolve) => {
    try {
      chrome.runtime.sendMessage(message, (response) => {
        try {
          if (chrome.runtime.lastError) {
            resolve(null);
            return;
          }
          resolve(response);
        } catch (_error) {
          invalidateExtensionContext();
          resolve(null);
        }
      });
    } catch (_error) {
      invalidateExtensionContext();
      resolve(null);
    }
  });
}

function invalidateExtensionContext() {
  extensionContextAvailable = false;
  if (wakePollTimer !== null) {
    clearInterval(wakePollTimer);
    wakePollTimer = null;
  }
  if (inFlightWakeKey !== null) {
    inFlightWakeKey = null;
  }
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

})();
