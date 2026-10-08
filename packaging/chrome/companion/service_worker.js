const NATIVE_HOST = "com.heterarchy.orchestration";
const CHATGPT_ROOT = "https://chatgpt.com/";
const IDLE_POLL_MS = 1000;
const WAKE_ALARM = "heterarchy-poll-wake";
const WAKE_ALARM_PERIOD_MIN = 0.5;
const RECONNECT_MAX_MS = 30000;
const INSPECT_RETRY_LIMIT = 3;
const CONVERSATION_WAIT_MS = 30000;
const CONVERSATION_POLL_MS = 500;
const OWNED_BACKGROUND_WINDOW_KEY = "heterarchy:browser-window:v1";
const OPERATION_JOURNAL_VERSION = 4;
// Versions below 3 predate activation bootstrap evidence; their observed
// conversation id is identity evidence only and never implies a submitted
// bootstrap prompt. Version 4 adds only the optional static failureCode field.
const LEGACY_OPERATION_JOURNAL_VERSIONS = [1, 2, 3];
const BROWSER_SESSION_ID_KEY = "heterarchy:browser-session:v1";
const RUNTIME_SURFACE_PREFIX = "heterarchy:runtime-surface:";
const TAB_SURFACE_PREFIX = "heterarchy:tab-surface:";
const MAX_NATIVE_OUTBOUND_QUEUE = 128;
const BOOTSTRAP_FAILURE_CODES = new Set([
  "surface_open_failed",
  "journal_write_failed",
  "tab_load_timeout",
  "document_observation_failed",
  "document_identity_changed",
  "document_identity_unavailable",
  "script_injection_failed",
  "composer_unavailable",
  "composer_form_unavailable",
  "composer_busy",
  "composer_not_empty",
  "composer_focus_failed",
  "composer_selection_failed",
  "composer_insert_failed",
  "composer_body_mismatch",
  "send_button_unavailable",
  "composer_ready_timeout",
  "click_reply_uncertain",
  "unknown_adapter_failure",
]);
const BOOTSTRAP_RETRYABLE_FAILURE_CODES = new Set([
  "composer_unavailable",
  "composer_form_unavailable",
  "composer_busy",
  "composer_not_empty",
]);

function safeBootstrapFailureCode(value, fallback = "unknown_adapter_failure") {
  return BOOTSTRAP_FAILURE_CODES.has(value) ? value : fallback;
}

function explicitBootstrapFailureCode(value) {
  if (
    typeof value !== "string" ||
    value === "click_reply_uncertain" ||
    !BOOTSTRAP_FAILURE_CODES.has(value)
  ) {
    return null;
  }
  return value;
}

let nativePort = null;
let requestInFlight = false;
let reconnectDelayMs = 1000;
let timerId = null;
let backgroundWindowFlight = null;
let browserSessionIdFlight = null;
let chatTransport = null;
let nextPollKind = "provision";
let nativeOutboundQueue = [];
let requestInFlightEntry = null;

function schedule(callback, delayMs) {
  if (timerId !== null) {
    clearTimeout(timerId);
  }
  timerId = setTimeout(() => {
    timerId = null;
    callback();
  }, delayMs);
}

function connectNative() {
  if (nativePort !== null) {
    return;
  }
  try {
    const port = chrome.runtime.connectNative(NATIVE_HOST);
    nativePort = port;
    port.onMessage.addListener((message) => {
      void handleNativeMessage(message);
    });
    port.onDisconnect.addListener(() => {
      const disconnectMessage = chrome.runtime.lastError?.message;
      if (disconnectMessage !== undefined) {
        console.warn("Heterarchy native companion disconnected", disconnectMessage);
      }
      if (requestInFlightEntry !== null) {
        requeueNativeEntry(requestInFlightEntry);
        requestInFlightEntry = null;
      }
      nativePort = null;
      requestInFlight = false;
      chatTransport?.onNativeDisconnect();
      const delayMs = reconnectDelayMs;
      reconnectDelayMs = Math.min(reconnectDelayMs * 2, RECONNECT_MAX_MS);
      schedule(connectNative, delayMs);
    });
    requestNext();
  } catch (error) {
    console.error("Heterarchy native companion connection failed", error);
    const delayMs = reconnectDelayMs;
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, RECONNECT_MAX_MS);
    schedule(connectNative, delayMs);
  }
}

function nativeMessageKind(message) {
  return message?.type === "next" ||
    (message?.type === "chat" && message.request?.operation === "next")
    ? "poll"
    : "durable";
}

function postNative(message) {
  const kind = nativeMessageKind(message);
  if (kind === "poll" && nativeOutboundQueue.some((entry) => entry.kind === "poll")) {
    return false;
  }
  if (nativeOutboundQueue.length >= MAX_NATIVE_OUTBOUND_QUEUE) {
    return false;
  }
  nativeOutboundQueue.push({ message, kind });
  if (nativePort === null) {
    schedule(connectNative, IDLE_POLL_MS);
  } else {
    flushNativeQueue();
  }
  return true;
}

function flushNativeQueue() {
  if (nativePort === null || requestInFlight || nativeOutboundQueue.length === 0) {
    return;
  }
  const durableIndex = nativeOutboundQueue.findIndex(
    (entry) => entry.kind === "durable",
  );
  const index = durableIndex >= 0 ? durableIndex : 0;
  const entry = nativeOutboundQueue.splice(index, 1)[0];
  if (entry === undefined) return;
  requestInFlight = true;
  requestInFlightEntry = entry;
  try {
    nativePort.postMessage(entry.message);
  } catch (error) {
    requestInFlight = false;
    requestInFlightEntry = null;
    requeueNativeEntry(entry);
    nativePort = null;
    chatTransport?.onNativeDisconnect();
    console.error("Heterarchy native companion send failed", error);
    schedule(connectNative, IDLE_POLL_MS);
  }
}

function requeueNativeEntry(entry) {
  if (entry.kind === "poll" && nativeOutboundQueue.some((queued) => queued.kind === "poll")) {
    return;
  }
  if (nativeOutboundQueue.length >= MAX_NATIVE_OUTBOUND_QUEUE) {
    const pollIndex = nativeOutboundQueue.findIndex((queued) => queued.kind === "poll");
    if (pollIndex >= 0) {
      nativeOutboundQueue.splice(pollIndex, 1);
    }
  }
  nativeOutboundQueue.unshift(entry);
}

function requestNext() {
  const message =
    nextPollKind === "chat"
      ? { type: "chat", request: { operation: "next" } }
      : { type: "next" };
  if (!postNative(message)) {
    schedule(connectNative, IDLE_POLL_MS);
    return;
  }
  nextPollKind = nextPollKind === "chat" ? "provision" : "chat";
}

async function handleNativeMessage(message) {
  requestInFlight = false;
  requestInFlightEntry = null;
  reconnectDelayMs = 1000;
  try {
    if (message === null || typeof message !== "object") {
      schedule(requestNext, 5000);
      return;
    }
    switch (message.type) {
      case "chat":
        if (chatTransport !== null) {
          await chatTransport.handleNativeResponse(message.response);
        } else {
          schedule(requestNext, 5000);
        }
        return;
      case "idle":
        schedule(requestNext, IDLE_POLL_MS);
        return;
      case "dispatch":
        await handleDispatch(message);
        return;
      case "ack":
        await handleAck(message);
        return;
      case "error":
        if (chatTransport !== null && (await chatTransport.handleNativeError(message))) {
          return;
        }
        console.error("Heterarchy native companion rejected a request", message.code);
        schedule(requestNext, message.code === "durable_failure" ? 5000 : IDLE_POLL_MS);
        return;
      default:
        console.error("Heterarchy native companion returned an unknown message", message.type);
        schedule(requestNext, 5000);
    }
  } finally {
    flushNativeQueue();
  }
}

async function handleDispatch(dispatch) {
  if (!validDispatch(dispatch)) {
    schedule(requestNext, 5000);
    return;
  }
  if (dispatch.action === "inspect") {
    await inspectDispatch(dispatch);
    return;
  }
  if (dispatch.action !== "execute") {
    schedule(requestNext, 5000);
    return;
  }
  if ((await operationJournal(dispatch)) !== null) {
    await reportUnknown(dispatch);
    return;
  }
  if (
    dispatch.effect_kind === "wake_existing_runtime" ||
    dispatch.effect_kind === "reuse_ready_runtime"
  ) {
    await executeExistingConversationActivation(dispatch);
    return;
  }
  if (
    dispatch.effect_kind === "wake_new_runtime" ||
    dispatch.effect_kind === "provision_new"
  ) {
    await executeNewConversation(dispatch);
    return;
  }
  await reportFailed(dispatch);
}

function validDispatch(dispatch) {
  return (
    typeof dispatch.operation_id === "string" &&
    Number.isSafeInteger(dispatch.operation_revision) &&
    typeof dispatch.profile_id === "string" &&
    typeof dispatch.runtime_session_id === "string" &&
    dispatch.provider === "chatgpt"
  );
}

async function currentBrowserSessionId() {
  const stored = await chrome.storage.session.get(BROWSER_SESSION_ID_KEY);
  const existing = stored[BROWSER_SESSION_ID_KEY];
  if (typeof existing === "string" && /^[0-9a-f-]{36}$/i.test(existing)) {
    return existing;
  }
  if (browserSessionIdFlight !== null) {
    return browserSessionIdFlight;
  }
  const flight = createBrowserSessionId();
  browserSessionIdFlight = flight;
  try {
    return await flight;
  } finally {
    if (browserSessionIdFlight === flight) {
      browserSessionIdFlight = null;
    }
  }
}

async function createBrowserSessionId() {
  const sessionId = globalThis.crypto.randomUUID();
  await chrome.storage.session.set({ [BROWSER_SESSION_ID_KEY]: sessionId });
  return sessionId;
}

async function journalOwnsCurrentBrowserSession(record) {
  return (
    typeof record?.browserSessionId === "string" &&
    record.browserSessionId === (await currentBrowserSessionId())
  );
}

async function openRuntimeOwnedTab(url) {
  if (backgroundWindowFlight !== null) {
    await backgroundWindowFlight;
    return openRuntimeOwnedTab(url);
  }
  const flight = openRuntimeOwnedTabOnce(url);
  backgroundWindowFlight = flight;
  try {
    return await flight;
  } finally {
    if (backgroundWindowFlight === flight) {
      backgroundWindowFlight = null;
    }
  }
}

async function openRuntimeOwnedTabOnce(url) {
  const ownedWindowId = await currentOwnedBackgroundWindowId();
  if (ownedWindowId !== null) {
    const tab = await chrome.tabs.create({
      windowId: ownedWindowId,
      url,
      active: false,
    });
    if (!Number.isInteger(tab.id)) {
      throw new Error("runtime-owned tab has no identity");
    }
    return { tabId: tab.id, windowId: ownedWindowId };
  }

  const window = await chrome.windows.create({
    url,
    type: "normal",
    focused: false,
    state: "minimized",
  });
  if (!Number.isInteger(window.id)) {
    throw new Error("runtime-owned window has no identity");
  }
  const tabs = Array.isArray(window.tabs)
    ? window.tabs
    : await chrome.tabs.query({ windowId: window.id });
  const tab = tabs.find((candidate) => Number.isInteger(candidate.id));
  if (tab === undefined) {
    throw new Error("runtime-owned window has no tab identity");
  }
  await chrome.storage.session.set({
    [OWNED_BACKGROUND_WINDOW_KEY]: { windowId: window.id },
  });
  return { tabId: tab.id, windowId: window.id };
}

async function currentOwnedBackgroundWindowId() {
  const stored = await chrome.storage.session.get(OWNED_BACKGROUND_WINDOW_KEY);
  const record = stored[OWNED_BACKGROUND_WINDOW_KEY];
  const windowId = record?.windowId;
  if (!Number.isInteger(windowId)) {
    return null;
  }
  try {
    const window = await chrome.windows.get(windowId);
    return Number.isInteger(window.id) ? window.id : null;
  } catch (_ignored) {
    await chrome.storage.session.remove(OWNED_BACKGROUND_WINDOW_KEY);
    return null;
  }
}

async function forgetOwnedBackgroundWindow(windowId) {
  const stored = await chrome.storage.session.get(OWNED_BACKGROUND_WINDOW_KEY);
  if (stored[OWNED_BACKGROUND_WINDOW_KEY]?.windowId === windowId) {
    await chrome.storage.session.remove(OWNED_BACKGROUND_WINDOW_KEY);
  }
}

async function executeExistingConversationActivation(dispatch) {
  if (typeof dispatch.provider_conversation_id !== "string") {
    await reportFailed(dispatch);
    return;
  }
  let bootstrapAttempted = false;
  try {
    const reusable = await reusableRuntimeSurface(
      dispatch.runtime_session_id,
      dispatch.provider_conversation_id,
    );
    const surface =
      reusable ??
      (await openRuntimeOwnedTab(
        `${CHATGPT_ROOT}c/${encodeURIComponent(dispatch.provider_conversation_id)}`,
      ));
    await initializeOperationJournal(
      dispatch,
      surface.tabId,
      surface.windowId,
      dispatch.provider_conversation_id,
      reusable === null ? "tab_opened" : "surface_reused",
    );
    const observed = await waitForConversation(
      dispatch,
      dispatch.provider_conversation_id,
    );
    if (observed === null) {
      await reportUnknown(dispatch);
      return;
    }
    const document = await observeCurrentDocument(dispatch);
    await updateOperationJournal(dispatch, {
      failureCode: null,
      stage: "submitting",
    });
    bootstrapAttempted = true;
    const submission = await injectActivationBootstrap(dispatch, document);
    if (
      submission?.documentId !== document.documentId ||
      submission.result !== true
    ) {
      console.error(
        "Heterarchy activation bootstrap was not submitted",
        dispatch.operation_id,
      );
      await reportUnknown(dispatch, "click_reply_uncertain");
      return;
    }
    await updateOperationJournal(dispatch, {
      activationBootstrapSubmitted: true,
      stage: "prompt_submitted",
    });
    const confirmed = await waitForConversation(
      dispatch,
      dispatch.provider_conversation_id,
    );
    if (confirmed === null) {
      await reportUnknown(dispatch, "click_reply_uncertain");
      return;
    }
    await recordObservedConversation(dispatch, confirmed);
    await reportObserved(dispatch, confirmed);
  } catch (error) {
    console.error(
      "Heterarchy existing-conversation activation browser effect failed",
      error,
    );
    if (bootstrapAttempted) {
      await reportUnknown(dispatch, "click_reply_uncertain");
      return;
    }
    await reportFailed(dispatch);
  }
}

async function executeNewConversation(dispatch) {
  let tabId = null;
  let windowId = null;
  let promptSubmitted = false;
  let clickAttempted = false;
  let preClickFailureCode = "surface_open_failed";
  try {
    const surface = await openRuntimeOwnedTab(CHATGPT_ROOT);
    tabId = surface.tabId;
    windowId = surface.windowId;
    preClickFailureCode = "journal_write_failed";
    await initializeOperationJournal(
      dispatch,
      tabId,
      windowId,
      null,
      "tab_opened",
    );
    preClickFailureCode = "tab_load_timeout";
    await waitForTabComplete(dispatch);
    preClickFailureCode = "document_observation_failed";
    const document = await observeCurrentDocument(dispatch);

    const readiness = await waitForBootstrapReady(dispatch, document);
    if (readiness.ready !== true) {
      await failNewConversation(
        dispatch,
        tabId,
        readiness.failureCode,
      );
      return;
    }

    const prompt = bootstrapPrompt(dispatch);
    preClickFailureCode = "script_injection_failed";
    let preparation;
    try {
      preparation = await prepareActivationBootstrap(document, prompt);
    } catch (_ignored) {
      await failNewConversation(dispatch, tabId, preClickFailureCode);
      return;
    }
    if (preparation?.documentId !== document.documentId) {
      await failNewConversation(
        dispatch,
        tabId,
        preparation === null || preparation === undefined
          ? "script_injection_failed"
          : "document_identity_changed",
      );
      return;
    }
    if (preparation.result?.prepared !== true) {
      await failNewConversation(
        dispatch,
        tabId,
        safeBootstrapFailureCode(
          preparation.result?.reason,
          "script_injection_failed",
        ),
      );
      return;
    }

    await updateOperationJournal(dispatch, {
      failureCode: null,
      stage: "submitting",
    });
    clickAttempted = true;
    let submission;
    try {
      submission = await clickActivationBootstrap(document, prompt);
    } catch (_ignored) {
      await reportUnknown(dispatch, "click_reply_uncertain");
      return;
    }
    if (submission?.documentId !== document.documentId) {
      await reportUnknown(dispatch, "click_reply_uncertain");
      return;
    }
    if (submission.result?.clicked !== true) {
      const clickFailureCode =
        submission.result?.clicked === false
          ? explicitBootstrapFailureCode(submission.result.reason)
          : null;
      if (clickFailureCode === null) {
        await reportUnknown(dispatch, "click_reply_uncertain");
        return;
      }
      await failNewConversation(
        dispatch,
        tabId,
        clickFailureCode,
      );
      return;
    }

    promptSubmitted = true;
    await updateOperationJournal(dispatch, {
      activationBootstrapSubmitted: true,
      failureCode: null,
      stage: "prompt_submitted",
    });
    const conversationId = await waitForConversation(dispatch, null);
    if (conversationId === null) {
      await reportUnknown(dispatch, "click_reply_uncertain");
      return;
    }
    await recordObservedConversation(dispatch, conversationId);
    await reportObserved(dispatch, conversationId);
  } catch (_ignored) {
    if (clickAttempted || promptSubmitted) {
      await reportUnknown(dispatch, "click_reply_uncertain");
      return;
    }
    await failNewConversation(dispatch, tabId, preClickFailureCode);
  }
}

async function failNewConversation(dispatch, tabId, failureCode) {
  // Tab ownership alone does not make unexpected draft content disposable.
  if (
    failureCode !== "composer_not_empty" &&
    failureCode !== "composer_body_mismatch"
  ) {
    await removeOwnedUnchangedTab(dispatch, tabId);
  }
  await reportFailed(dispatch, failureCode);
}

async function removeOwnedUnchangedTab(dispatch, tabId) {
  if (!Number.isInteger(tabId)) {
    return;
  }
  try {
    const record = await operationJournal(dispatch);
    if (
      record === null ||
      record.tabId !== tabId ||
      !Number.isInteger(record.windowId) ||
      typeof record.documentId !== "string" ||
      !(await journalOwnsCurrentBrowserSession(record)) ||
      (await currentOwnedBackgroundWindowId()) !== record.windowId
    ) {
      return;
    }
    const tab = await chrome.tabs.get(tabId);
    if (
      tab.windowId !== record.windowId ||
      tab.pinned === true ||
      tab.status !== "complete" ||
      typeof tab.url !== "string" ||
      !tab.url.startsWith(CHATGPT_ROOT)
    ) {
      return;
    }
    const observation = topLevelInjectionResult(
      await chrome.scripting.executeScript({
        target: { tabId },
        func: currentDocumentUrl,
      }),
    );
    if (
      observation?.documentId !== record.documentId ||
      observation.result !== tab.url ||
      (typeof record.lastUrl === "string" && observation.result !== record.lastUrl)
    ) {
      return;
    }
    const currentTab = await chrome.tabs.get(tabId);
    if (
      currentTab.windowId !== record.windowId ||
      currentTab.pinned === true ||
      currentTab.status !== "complete" ||
      currentTab.url !== observation.result
    ) {
      return;
    }
    await chrome.tabs.remove(tabId);
  } catch (_ignored) {
    // Runtime-owned tab may already be gone or no longer prove the journal fence.
  }
}

async function waitForBootstrapReady(dispatch, documentRecord) {
  const deadline = Date.now() + CONVERSATION_WAIT_MS;
  let lastFailureCode = "composer_unavailable";
  while (Date.now() < deadline) {
    let observation;
    try {
      observation = await inspectActivationBootstrap(documentRecord);
    } catch (_ignored) {
      return { ready: false, failureCode: "script_injection_failed" };
    }
    if (observation === null || observation === undefined) {
      return { ready: false, failureCode: "document_identity_unavailable" };
    }
    if (observation.documentId !== documentRecord.documentId) {
      return { ready: false, failureCode: "document_identity_changed" };
    }
    if (observation.result?.ready === true) {
      return { ready: true, failureCode: null };
    }
    const failureCode = safeBootstrapFailureCode(
      observation.result?.reason,
      "unknown_adapter_failure",
    );
    if (!BOOTSTRAP_RETRYABLE_FAILURE_CODES.has(failureCode)) {
      return { ready: false, failureCode };
    }
    lastFailureCode = failureCode;
    await sleep(CONVERSATION_POLL_MS);
  }
  return { ready: false, failureCode: lastFailureCode };
}

async function inspectActivationBootstrap(documentRecord) {
  const results = await chrome.scripting.executeScript({
    target: {
      tabId: documentRecord.tabId,
      documentIds: [documentRecord.documentId],
    },
    func: inspectBootstrapComposer,
  });
  return topLevelInjectionResult(results);
}

async function prepareActivationBootstrap(documentRecord, prompt) {
  const results = await chrome.scripting.executeScript({
    target: {
      tabId: documentRecord.tabId,
      documentIds: [documentRecord.documentId],
    },
    func: prepareBootstrapPrompt,
    args: [prompt, CONVERSATION_WAIT_MS, CONVERSATION_POLL_MS],
  });
  return topLevelInjectionResult(results);
}

async function clickActivationBootstrap(documentRecord, prompt) {
  const results = await chrome.scripting.executeScript({
    target: {
      tabId: documentRecord.tabId,
      documentIds: [documentRecord.documentId],
    },
    func: clickBootstrapPrompt,
    args: [prompt],
  });
  return topLevelInjectionResult(results);
}

async function injectActivationBootstrap(dispatch, documentRecord) {
  const results = await chrome.scripting.executeScript({
    target: {
      tabId: documentRecord.tabId,
      documentIds: [documentRecord.documentId],
    },
    func: submitBootstrapPrompt,
    args: [bootstrapPrompt(dispatch)],
  });
  return topLevelInjectionResult(results);
}

async function inspectDispatch(dispatch) {
  let record = await operationJournal(dispatch);
  if (record === null) {
    record = await initializeOperationJournal(
      dispatch,
      null,
      null,
      typeof dispatch.provider_conversation_id === "string"
        ? dispatch.provider_conversation_id
        : null,
      "inspect_only",
    );
  }
  if (
    record.schemaVersion === OPERATION_JOURNAL_VERSION &&
    typeof record.observedConversationId !== "string" &&
    record.activationBootstrapSubmitted !== true &&
    record.stage !== "submitting" &&
    record.ackPending !== "unknown" &&
    typeof dispatch.provider_conversation_id === "string" &&
    (record.effectKind === "wake_existing_runtime" ||
      record.effectKind === "reuse_ready_runtime") &&
    Number.isInteger(record.tabId)
  ) {
    if (await reconcileActivationBootstrap(dispatch)) {
      return;
    }
  }
  if (typeof record.observedConversationId === "string") {
    await reportObserved(dispatch, record.observedConversationId);
    return;
  }
  const observed = await inspectCurrentConversation(dispatch, record);
  if (observed !== null) {
    await recordObservedConversation(dispatch, observed);
    await reportObserved(dispatch, observed);
    return;
  }
  const retries = record.inspectRetries ?? 0;
  if (retries >= INSPECT_RETRY_LIMIT) {
    console.error(
      "Heterarchy provision operation requires manual reconciliation",
      dispatch.operation_id,
      {
        stage: record.stage ?? null,
        failureCode: record.failureCode ?? null,
        lastUrl: record.lastUrl ?? null,
        inspectRetries: retries,
      },
    );
    await reportUnknown(dispatch);
    return;
  }
  await updateOperationJournal(dispatch, { inspectRetries: retries + 1 });
  await reportUnknown(dispatch);
}

async function reconcileActivationBootstrap(dispatch) {
  try {
    const record = await requireOperationJournal(dispatch);
    const observed = await inspectCurrentConversation(dispatch, record);
    if (observed === null || observed !== dispatch.provider_conversation_id) {
      return false;
    }
    const document = await observeCurrentDocument(dispatch);
    await updateOperationJournal(dispatch, {
      failureCode: null,
      stage: "submitting",
    });
    const submission = await injectActivationBootstrap(dispatch, document);
    if (
      submission?.documentId !== document.documentId ||
      submission.result !== true
    ) {
      return false;
    }
    await updateOperationJournal(dispatch, {
      activationBootstrapSubmitted: true,
      stage: "prompt_submitted",
    });
    const confirmed = await waitForConversation(
      dispatch,
      dispatch.provider_conversation_id,
    );
    if (confirmed === null) {
      return false;
    }
    await recordObservedConversation(dispatch, confirmed);
    await reportObserved(dispatch, confirmed);
    return true;
  } catch (error) {
    console.error("Heterarchy activation bootstrap reconcile failed", error);
    return false;
  }
}

async function handleAck(message) {
  if (typeof message.operation_id === "string") {
    if (message.state === "confirmed" || message.state === "failed") {
      await chrome.storage.local.remove(operationKey(message.operation_id));
    } else if (message.state === "effect_unknown") {
      const record = await loadOperation(message.operation_id);
      if (
        supportedOperationJournalVersion(record) &&
        record.operationRevision === message.operation_revision
      ) {
        await chrome.storage.local.set({
          [operationKey(message.operation_id)]: { ...record, ackPending: null },
        });
      }
    }
  }
  schedule(requestNext, IDLE_POLL_MS);
}

async function reportObserved(dispatch, providerConversationId) {
  await markReportPending(dispatch, "observed");
  await sendEffectReport({
    type: "effect_observed",
    operation_id: dispatch.operation_id,
    expected_revision: dispatch.operation_revision,
    provider_conversation_id: providerConversationId,
  });
}

async function reportUnknown(dispatch, failureCode = null) {
  await markReportPending(dispatch, "unknown", failureCode);
  await sendEffectReport({
    type: "effect_unknown",
    operation_id: dispatch.operation_id,
    expected_revision: dispatch.operation_revision,
  });
}

async function reportFailed(dispatch, failureCode = null) {
  await markReportPending(dispatch, "failed", failureCode);
  await sendEffectReport({
    type: "effect_failed",
    operation_id: dispatch.operation_id,
    expected_revision: dispatch.operation_revision,
    failure_code: explicitBootstrapFailureCode(failureCode) ?? "unknown_adapter_failure",
  });
}

async function markReportPending(dispatch, reportKind, failureCode = null) {
  const record = await operationJournal(dispatch);
  if (record !== null) {
    const patch = { ackPending: reportKind };
    if (failureCode !== null) {
      patch.failureCode = safeBootstrapFailureCode(failureCode);
    }
    await updateOperationJournal(dispatch, patch);
  }
}

async function sendEffectReport(message) {
  if (!postNative(message)) {
    schedule(connectNative, 1000);
  }
}

function bootstrapPrompt(dispatch) {
  return [
    "Heterarchy runtime bootstrap.",
    `Provision operation: ${dispatch.operation_id}`,
    `Runtime session: ${dispatch.runtime_session_id}`,
    "This conversation is a reusable Heterarchy Agent runtime.",
    "Do not infer Workspace membership, Team role, leadership, source-write permission, Goal/DAG authority, or work assignment from this bootstrap message.",
    `When Heterarchy Orchestration MCP is available, call agent_request_activate with operation_id ${dispatch.operation_id}. That call opens or replays your authenticated logical Session and enters the existing participation lifecycle only after this provider operation is durably confirmed.`,
    "Reuse this exact operation id on retry. Do not create a different Session or request another Agent to recover activation.",
    "After activation reports Ready, use Orchestration durable inbox/assignment/work state for what to do next; do not treat this bootstrap prompt as work authority.",
  ].join("\n");
}

async function waitForTabComplete(dispatch) {
  const deadline = Date.now() + CONVERSATION_WAIT_MS;
  while (Date.now() < deadline) {
    const record = await requireOperationJournal(dispatch);
    if (
      !(await journalOwnsCurrentBrowserSession(record)) ||
      !Number.isInteger(record.tabId)
    ) {
      throw new Error("runtime-owned tab identity is stale or missing");
    }
    const tab = await chrome.tabs.get(record.tabId);
    if (
      tab.status === "complete" &&
      typeof tab.url === "string" &&
      tab.url.startsWith(CHATGPT_ROOT)
    ) {
      return;
    }
    await sleep(CONVERSATION_POLL_MS);
  }
  throw new Error("ChatGPT tab load timed out");
}

async function waitForConversation(dispatch, expectedConversationId) {
  const deadline = Date.now() + CONVERSATION_WAIT_MS;
  while (Date.now() < deadline) {
    const record = await requireOperationJournal(dispatch);
    if (
      !(await journalOwnsCurrentBrowserSession(record)) ||
      !Number.isInteger(record.tabId)
    ) {
      return null;
    }
    const tab = await chrome.tabs.get(record.tabId);
    const candidate = conversationIdFromUrl(tab.url);
    if (
      candidate !== null &&
      (expectedConversationId === null || candidate === expectedConversationId)
    ) {
      try {
        const document = await observeCurrentDocument(dispatch);
        const observed = conversationIdFromUrl(document.lastUrl);
        if (
          observed !== null &&
          (expectedConversationId === null || observed === expectedConversationId)
        ) {
          return observed;
        }
      } catch (_ignored) {
        // Navigation raced with the observation. Retry within the same bounded wait.
      }
    }
    await sleep(CONVERSATION_POLL_MS);
  }
  return null;
}

async function inspectCurrentConversation(dispatch, record) {
  if (
    !(await journalOwnsCurrentBrowserSession(record)) ||
    !Number.isInteger(record.tabId)
  ) {
    return null;
  }
  try {
    const tab = await chrome.tabs.get(record.tabId);
    const candidate = conversationIdFromUrl(tab.url);
    if (candidate === null) {
      return null;
    }
    if (
      typeof record.expectedConversationId === "string" &&
      candidate !== record.expectedConversationId
    ) {
      return null;
    }
    const document = await observeCurrentDocument(dispatch);
    const observed = conversationIdFromUrl(document.lastUrl);
    if (observed !== candidate) {
      return null;
    }
    return observed;
  } catch (_ignored) {
    return null;
  }
}

async function observeCurrentDocument(dispatch) {
  const before = await requireOperationJournal(dispatch);
  if (
    !(await journalOwnsCurrentBrowserSession(before)) ||
    !Number.isInteger(before.tabId)
  ) {
    throw new Error("runtime-owned tab identity is stale or missing");
  }
  const results = await chrome.scripting.executeScript({
    target: { tabId: before.tabId },
    func: currentDocumentUrl,
  });
  const observation = topLevelInjectionResult(results);
  if (
    observation === null ||
    typeof observation.documentId !== "string" ||
    typeof observation.result !== "string"
  ) {
    throw new Error("browser document identity is unavailable");
  }
  const current = await requireOperationJournal(dispatch);
  if (current.tabId !== before.tabId) {
    throw new Error("browser surface changed during observation");
  }
  const nextEpoch =
    current.documentId === observation.documentId
      ? current.navigationEpoch
      : current.navigationEpoch + 1;
  return updateOperationJournal(dispatch, {
    documentId: observation.documentId,
    navigationEpoch: nextEpoch,
    lastUrl: observation.result,
    stage: current.stage === "tab_opened" ? "document_observed" : current.stage,
  });
}

function currentDocumentUrl() {
  return globalThis.location.href;
}

function topLevelInjectionResult(results) {
  if (!Array.isArray(results)) {
    return null;
  }
  return results.find((result) => result.frameId === 0) ?? results[0] ?? null;
}

function conversationIdFromUrl(value) {
  if (typeof value !== "string") {
    return null;
  }
  try {
    const url = new URL(value);
    if (url.origin !== "https://chatgpt.com") {
      return null;
    }
    const match =
      /^\/(?:g\/[^/]+\/)?c\/([0-9a-f-]{8,64})(?:\/|$)/i.exec(url.pathname);
    return match?.[1] ?? null;
  } catch (_ignored) {
    return null;
  }
}

function inspectBootstrapComposer() {
  const composer = document.querySelector(
    '#prompt-textarea, div[contenteditable="true"][role="textbox"][data-composer-markdown], div[contenteditable="true"][role="textbox"]',
  );
  if (
    !(composer instanceof HTMLElement) ||
    !composer.isConnected ||
    composer.getAttribute("aria-disabled") === "true" ||
    composer.getAttribute("contenteditable") === "false"
  ) {
    return { ready: false, reason: "composer_unavailable" };
  }
  const form = composer.closest("form");
  const sendScope = form ?? document;
  const stopButton = sendScope.querySelector(
    'button[data-testid="stop-button"], ' +
      'button[data-testid="composer-stop-button"], ' +
      'button[aria-label="Stop streaming"], ' +
      'button[aria-label="Stop generating"], ' +
      'button[aria-label="Stop answering"], ' +
      'button[aria-label="중지"], ' +
      'button[aria-label*="중지"]',
  );
  if (stopButton instanceof HTMLButtonElement) {
    return { ready: false, reason: "composer_busy" };
  }
  const existing = (composer.innerText ?? composer.textContent ?? "").trim();
  if (existing.length !== 0) {
    return { ready: false, reason: "composer_not_empty" };
  }
  return { ready: true, reason: null };
}

async function prepareBootstrapPrompt(prompt, readinessWaitMs, readinessPollMs) {
  const composerSelector =
    '#prompt-textarea, div[contenteditable="true"][role="textbox"][data-composer-markdown], div[contenteditable="true"][role="textbox"]';
  const composer = document.querySelector(
    composerSelector,
  );
  if (
    !(composer instanceof HTMLElement) ||
    !composer.isConnected ||
    composer.getAttribute("aria-disabled") === "true" ||
    composer.getAttribute("contenteditable") === "false"
  ) {
    return { prepared: false, reason: "composer_unavailable" };
  }
  const form = composer.closest("form");
  const sendScope = form ?? document;
  const stopButton = sendScope.querySelector(
    'button[data-testid="stop-button"], ' +
      'button[data-testid="composer-stop-button"], ' +
      'button[aria-label="Stop streaming"], ' +
      'button[aria-label="Stop generating"], ' +
      'button[aria-label="Stop answering"], ' +
      'button[aria-label="중지"], ' +
      'button[aria-label*="중지"]',
  );
  if (stopButton instanceof HTMLButtonElement) {
    return { prepared: false, reason: "composer_busy" };
  }
  const existing = (composer.innerText ?? composer.textContent ?? "").trim();
  if (existing.length !== 0 || typeof prompt !== "string" || prompt.length === 0) {
    return { prepared: false, reason: "composer_not_empty" };
  }

  const initialDocument = document;
  composer.focus();
  const selection = document.getSelection();
  if (
    selection === null ||
    document.activeElement !== composer ||
    !composer.isConnected
  ) {
    return { prepared: false, reason: "composer_focus_failed" };
  }
  selection.selectAllChildren(composer);
  if (
    !selection.rangeCount ||
    !composer.contains(selection.anchorNode) ||
    !composer.contains(selection.focusNode)
  ) {
    return { prepared: false, reason: "composer_selection_failed" };
  }
  if (!document.execCommand("insertText", false, prompt)) {
    return { prepared: false, reason: "composer_insert_failed" };
  }
  const timeoutMs =
    Number.isFinite(readinessWaitMs) && readinessWaitMs >= 0
      ? readinessWaitMs
      : 30000;
  const pollMs =
    Number.isFinite(readinessPollMs) && readinessPollMs > 0
      ? readinessPollMs
      : 100;
  const deadline = Date.now() + timeoutMs;
  while (true) {
    if (
      document !== initialDocument ||
      !composer.isConnected ||
      document.querySelector(composerSelector) !== composer ||
      composer.closest("form") !== form
    ) {
      return { prepared: false, reason: "composer_body_mismatch" };
    }
    const observed = composer.innerText ?? composer.textContent ?? "";
    if (
      observed.replace(/\s+/g, " ").trim() !== prompt.replace(/\s+/g, " ").trim()
    ) {
      return { prepared: false, reason: "composer_body_mismatch" };
    }
    const sendButton =
      sendScope.querySelector('button[type="submit"][aria-label="보내기"]') ??
      sendScope.querySelector('button[type="submit"][aria-label^="Send" i]') ??
      sendScope.querySelector('button[data-testid="send-button"], button[aria-label="보내기"], button[aria-label^="Send" i]');
    if (
      sendButton instanceof HTMLButtonElement &&
      sendButton.isConnected &&
      !sendButton.disabled &&
      sendButton.getAttribute("aria-disabled") !== "true"
    ) {
      return { prepared: true, reason: null };
    }
    if (Date.now() >= deadline) {
      return { prepared: false, reason: "send_button_unavailable" };
    }
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}

function clickBootstrapPrompt(prompt) {
  const composer = document.querySelector(
    '#prompt-textarea, div[contenteditable="true"][role="textbox"][data-composer-markdown], div[contenteditable="true"][role="textbox"]',
  );
  if (
    !(composer instanceof HTMLElement) ||
    !composer.isConnected ||
    composer.getAttribute("aria-disabled") === "true" ||
    composer.getAttribute("contenteditable") === "false"
  ) {
    return { clicked: false, reason: "composer_unavailable" };
  }
  const form = composer.closest("form");
  const sendScope = form ?? document;
  const stopButton = sendScope.querySelector(
    'button[data-testid="stop-button"], ' +
      'button[data-testid="composer-stop-button"], ' +
      'button[aria-label="Stop streaming"], ' +
      'button[aria-label="Stop generating"], ' +
      'button[aria-label="Stop answering"], ' +
      'button[aria-label="중지"], ' +
      'button[aria-label*="중지"]',
  );
  if (stopButton instanceof HTMLButtonElement) {
    return { clicked: false, reason: "composer_busy" };
  }
  const observed = composer.innerText ?? composer.textContent ?? "";
  if (
    typeof prompt !== "string" ||
    observed.replace(/\s+/g, " ").trim() !== prompt.replace(/\s+/g, " ").trim()
  ) {
    return { clicked: false, reason: "composer_body_mismatch" };
  }
  const sendButton =
    sendScope.querySelector('button[type="submit"][aria-label="보내기"]') ??
    sendScope.querySelector('button[type="submit"][aria-label^="Send" i]') ??
    sendScope.querySelector('button[data-testid="send-button"], button[aria-label="보내기"], button[aria-label^="Send" i]');
  if (
    !(sendButton instanceof HTMLButtonElement) ||
    !sendButton.isConnected ||
    sendButton.disabled ||
    sendButton.getAttribute("aria-disabled") === "true"
  ) {
    return { clicked: false, reason: "send_button_unavailable" };
  }
  sendButton.click();
  return { clicked: true, reason: null };
}

function submitBootstrapPrompt(prompt) {
  const composer = document.querySelector(
    '#prompt-textarea, div[contenteditable="true"][role="textbox"][data-composer-markdown], div[contenteditable="true"][role="textbox"]',
  );
  if (
    !(composer instanceof HTMLElement) ||
    !composer.isConnected ||
    composer.getAttribute("aria-disabled") === "true" ||
    composer.getAttribute("contenteditable") === "false"
  ) {
    return false;
  }
  const form = composer.closest("form");
  const sendScope = form ?? document;
  const stopButton = sendScope.querySelector(
    'button[data-testid="stop-button"], ' +
      'button[data-testid="composer-stop-button"], ' +
      'button[aria-label="Stop streaming"], ' +
      'button[aria-label="Stop generating"], ' +
      'button[aria-label="Stop answering"], ' +
      'button[aria-label="중지"], ' +
      'button[aria-label*="중지"]',
  );
  if (stopButton instanceof HTMLButtonElement) {
    return false;
  }
  const existing = (composer.innerText ?? composer.textContent ?? "").trim();
  if (existing.length !== 0 || prompt.length === 0) {
    return false;
  }

  composer.focus();
  const selection = document.getSelection();
  if (
    selection === null ||
    document.activeElement !== composer ||
    !composer.isConnected
  ) {
    return false;
  }
  selection.selectAllChildren(composer);
  if (
    !selection.rangeCount ||
    !composer.contains(selection.anchorNode) ||
    !composer.contains(selection.focusNode)
  ) {
    return false;
  }
  if (!document.execCommand("insertText", false, prompt)) {
    return false;
  }
  const observed = composer.innerText ?? composer.textContent ?? "";
  if (
    !composer.isConnected ||
    document.querySelector(
      '#prompt-textarea, div[contenteditable="true"][role="textbox"][data-composer-markdown], div[contenteditable="true"][role="textbox"]',
    ) !== composer ||
    observed.replace(/\s+/g, " ").trim() !== prompt.replace(/\s+/g, " ").trim()
  ) {
    return false;
  }
  const sendButton =
    sendScope.querySelector('button[type="submit"][aria-label="보내기"]') ??
    sendScope.querySelector('button[type="submit"][aria-label^="Send" i]') ??
    sendScope.querySelector('button[data-testid="send-button"], button[aria-label="보내기"], button[aria-label^="Send" i]');
  if (
    !(sendButton instanceof HTMLButtonElement) ||
    !sendButton.isConnected ||
    sendButton.disabled ||
    sendButton.getAttribute("aria-disabled") === "true"
  ) {
    return false;
  }
  sendButton.click();
  return true;
}

async function initializeOperationJournal(
  dispatch,
  tabId,
  windowId,
  expectedConversationId,
  stage,
) {
  const browserSessionId = await currentBrowserSessionId();
  const record = {
    schemaVersion: OPERATION_JOURNAL_VERSION,
    browserSessionId,
    operationRevision: dispatch.operation_revision,
    runtimeSessionId: dispatch.runtime_session_id,
    effectKind: dispatch.effect_kind,
    tabId,
    windowId,
    documentId: null,
    navigationEpoch: 0,
    expectedConversationId,
    observedConversationId: null,
    activationBootstrapSubmitted: false,
    stage,
    failureCode: null,
    inspectRetries: 0,
    ackPending: null,
    lastUrl: null,
  };
  await chrome.storage.local.set({ [operationKey(dispatch.operation_id)]: record });
  return record;
}

async function updateOperationJournal(dispatch, patch) {
  const current = await requireOperationJournal(dispatch);
  const next = { ...current, ...patch };
  await chrome.storage.local.set({ [operationKey(dispatch.operation_id)]: next });
  return next;
}

async function recordObservedConversation(dispatch, providerConversationId) {
  const record = await updateOperationJournal(dispatch, {
    observedConversationId: providerConversationId,
    stage: "conversation_observed",
  });
  if (Number.isInteger(record.tabId) && Number.isInteger(record.windowId)) {
    await storeRuntimeSurface(
      dispatch.runtime_session_id,
      record.tabId,
      record.windowId,
      providerConversationId,
      record.documentId,
      record.navigationEpoch,
    );
  }
  return record;
}

async function reusableRuntimeSurface(runtimeSessionId, expectedConversationId) {
  const key = runtimeSurfaceKey(runtimeSessionId);
  const stored = await chrome.storage.session.get(key);
  const record = stored[key];
  if (
    record === null ||
    typeof record !== "object" ||
    !Number.isInteger(record.tabId) ||
    record.conversationId !== expectedConversationId
  ) {
    return null;
  }
  try {
    const [tab, ownedWindowId] = await Promise.all([
      chrome.tabs.get(record.tabId),
      currentOwnedBackgroundWindowId(),
    ]);
    if (
      ownedWindowId === null ||
      tab.windowId !== ownedWindowId ||
      tab.pinned === true ||
      conversationIdFromUrl(tab.url) !== expectedConversationId
    ) {
      await forgetRuntimeSurface(runtimeSessionId, record.tabId);
      return null;
    }
    return { tabId: record.tabId, windowId: ownedWindowId };
  } catch (_ignored) {
    await forgetRuntimeSurface(runtimeSessionId, record.tabId);
    return null;
  }
}

async function storeRuntimeSurface(
  runtimeSessionId,
  tabId,
  windowId,
  conversationId,
  documentId,
  navigationEpoch,
) {
  const key = runtimeSurfaceKey(runtimeSessionId);
  const existing = (await chrome.storage.session.get(key))[key];
  if (Number.isInteger(existing?.tabId) && existing.tabId !== tabId) {
    await chrome.storage.session.remove(tabSurfaceKey(existing.tabId));
  }
  await chrome.storage.session.set({
    [key]: { tabId, windowId, conversationId, documentId, navigationEpoch },
    [tabSurfaceKey(tabId)]: { runtimeSessionId },
  });
}

async function forgetRuntimeSurface(runtimeSessionId, tabId) {
  const key = runtimeSurfaceKey(runtimeSessionId);
  const stored = await chrome.storage.session.get(key);
  if (stored[key]?.tabId === tabId) {
    await chrome.storage.session.remove(key);
  }
  await chrome.storage.session.remove(tabSurfaceKey(tabId));
}

async function forgetRuntimeSurfaceForTab(tabId) {
  const reverseKey = tabSurfaceKey(tabId);
  const stored = await chrome.storage.session.get(reverseKey);
  const runtimeSessionId = stored[reverseKey]?.runtimeSessionId;
  if (typeof runtimeSessionId === "string") {
    await forgetRuntimeSurface(runtimeSessionId, tabId);
  } else {
    await chrome.storage.session.remove(reverseKey);
  }
}

function runtimeSurfaceKey(runtimeSessionId) {
  return `${RUNTIME_SURFACE_PREFIX}${runtimeSessionId}`;
}

function tabSurfaceKey(tabId) {
  return `${TAB_SURFACE_PREFIX}${tabId}`;
}

async function operationJournal(dispatch) {
  const record = await loadOperation(dispatch.operation_id);
  return journalMatchesDispatch(record, dispatch) ? record : null;
}

async function requireOperationJournal(dispatch) {
  const record = await operationJournal(dispatch);
  if (record === null) {
    throw new Error("browser operation journal fence changed");
  }
  return record;
}

function supportedOperationJournalVersion(record) {
  return (
    record?.schemaVersion === OPERATION_JOURNAL_VERSION ||
    LEGACY_OPERATION_JOURNAL_VERSIONS.includes(record?.schemaVersion)
  );
}

function journalMatchesDispatch(record, dispatch) {
  return (
    record !== null &&
    supportedOperationJournalVersion(record) &&
    record.operationRevision === dispatch.operation_revision &&
    record.runtimeSessionId === dispatch.runtime_session_id &&
    record.effectKind === dispatch.effect_kind
  );
}

async function loadOperation(operationId) {
  const key = operationKey(operationId);
  const stored = await chrome.storage.local.get(key);
  const value = stored[key];
  return value !== null && typeof value === "object" ? value : null;
}

function operationKey(operationId) {
  return `provision:${operationId}`;
}

importScripts("chat_transport.js");
chatTransport = createHeterarchyChatTransport({
  postNative,
  schedule,
  requestNext,
  currentBrowserSessionId,
  journalOwnsCurrentBrowserSession,
  openRuntimeOwnedTab,
  reusableRuntimeSurface,
  storeRuntimeSurface,
  topLevelInjectionResult,
  conversationIdFromUrl,
});

function sleep(delayMs) {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

chrome.tabs.onRemoved.addListener((tabId) => {
  void forgetRuntimeSurfaceForTab(tabId);
});
chrome.windows.onRemoved.addListener((windowId) => {
  void forgetOwnedBackgroundWindow(windowId);
});
chrome.runtime.onStartup.addListener(connectNative);
chrome.runtime.onInstalled.addListener(connectNative);

// MV3 suspends this service worker when its poll timers die with it. A
// periodic alarm wakes the worker at least every 30 seconds so the native
// host port is re-opened and pending durable dispatches keep flowing.
try {
  if (chrome.alarms && typeof chrome.alarms.create === "function") {
    // One-shot watchdog, re-armed at the end of every alarm pass (CoS
    // background.js pattern): a periodic alarm can be clamped to one minute
    // and piles up behind a stalled pass.
    chrome.alarms.create(WAKE_ALARM, {
      delayInMinutes: WAKE_ALARM_PERIOD_MIN,
    });
  }
} catch {
  // Alarms are unavailable in narrow test harnesses; reconnect timers still
  // drive polling while the worker is alive.
}
if (
  chrome.alarms &&
  chrome.alarms.onAlarm &&
  typeof chrome.alarms.onAlarm.addListener === "function"
) {
  chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name !== WAKE_ALARM) return;
    if (nativePort === null) {
      connectNative();
      return;
    }
    if (requestInFlight && requestInFlightEntry !== null) {
      // The host stopped answering while a request was in flight: recycle
      // the port so onDisconnect re-opens it and polling resumes.
      try {
        nativePort.disconnect();
      } catch {
        // already dead
      }
      return;
    }
    if (timerId === null) {
      schedule(requestNext, 0);
    }
    try {
      if (chrome.alarms && typeof chrome.alarms.create === "function") {
        chrome.alarms.create(WAKE_ALARM, {
          delayInMinutes: WAKE_ALARM_PERIOD_MIN,
        });
      }
    } catch {
      // re-arming is best-effort; the next poll re-arms again
    }
    return;
  });
}
if (globalThis.__HETERARCHY_CHAT_TEST__ === true) {
  globalThis.__HETERARCHY_SERVICE_WORKER_TEST__ = {
    postNative,
    requestNext,
    flushNativeQueue,
    handleDispatch,
    inspectDispatch,
    inspectBootstrapComposer,
    prepareBootstrapPrompt,
    clickBootstrapPrompt,
    queue: () => nativeOutboundQueue.map((entry) => entry.message),
  };
}
connectNative();
