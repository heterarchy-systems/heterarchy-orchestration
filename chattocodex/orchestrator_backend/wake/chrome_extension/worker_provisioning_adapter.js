// This file owns only the provider-facing provisioning mechanics.  Capacity,
// admission, and all logical work remain behind the loopback control boundary.
// A request must already be admitted by that boundary before this adapter can
// create a ChatGPT tab.

const WORKER_PROVISIONING_ENDPOINTS = Object.freeze({
  next: "/api/worker-provisioning/next",
  registered: (requestId) =>
    `/api/worker-provisioning/${encodeURIComponent(requestId)}/registered`,
  failed: (requestId) =>
    `/api/worker-provisioning/${encodeURIComponent(requestId)}/failed`,
});

const WORKER_PROVISIONING_STORAGE_KEY = "workerProvisioningState";
const WORKER_PROVISIONING_UI_ATTEMPTS = 10;
const WORKER_PROVISIONING_UI_RETRY_MS = 500;
const WORKER_PROVISIONING_CONVERSATION_WAIT_MS = 15_000;
const WORKER_PROVISIONING_CONVERSATION_POLL_MS = 500;
const MIN_PROVIDER_RATE_LIMIT_BACKOFF_MS = 30_000;
const MAX_PROVIDER_RATE_LIMIT_BACKOFF_MS = 120_000;
// Browser storage is only replay protection for the local adapter; the
// loopback service remains authoritative for admissions. Keep enough recent
// completed admissions to reject delayed browser events, but never let an
// indefinitely-running extension accumulate one record per provisioning run.
const MAX_WORKER_PROVISIONING_ADMISSION_HISTORY = 128;
const MAX_WORKER_PROVISIONING_IN_FLIGHT_HISTORY = 1;

let workerProvisioningInFlight = false;
const provisioningBootstrapAdmissions = new Map();
const provisioningProjectOpenAdmissions = new Map();
const provisioningRuntimeOwnedTabIds = new Set();
const claimedProvisioningTabIds = new Set();

class WorkerProvisioningAdapterError extends Error {
  constructor(code, { rateLimited = false } = {}) {
    super(code);
    this.code = code;
    this.rateLimited = rateLimited;
  }
}

// Fetch and execute at most one server-admitted request.  In particular, this
// intentionally does not loop after a successful registration: the server
// decides whether a later poll may admit another conversation.
async function processNextAdmittedWorkerProvision() {
  if (workerProvisioningInFlight || (await workerProvisioningBackoffActive())) {
    return { processed: false, reason: "busy_or_backoff" };
  }

  workerProvisioningInFlight = true;
  let reservation = null;
  try {
    await resumePendingWorkerProvisioningFinalizations();
    reservation = await reserveRuntimeOpenAdmission();
    if (!reservation) {
      return { processed: false, reason: "browser_open_cooldown" };
    }
    if (!(await runtimeSideEffectsAllowedWithoutPendingRetirement())) {
      return { processed: false, reason: "worker_retirement_pending" };
    }
    // Keep the POST adjacent to the final retirement barrier: the loopback
    // admission must not be consumed after a durable retirement revokes it.
    const payload = await requestJson(WORKER_PROVISIONING_ENDPOINTS.next, {
      method: "POST",
    });
    const request = normalizeAdmittedWorkerProvision(payload?.request);
    if (!request) {
      return { processed: false, reason: "none" };
    }
    if (!(await beginWorkerProvisioningAdmission(request))) {
      return { processed: false, reason: "duplicate_or_stale" };
    }
    return await executeAdmittedWorkerProvision(request);
  } catch (error) {
    if (Number(error?.status) === 429) {
      await setWorkerProvisioningBackoff();
      return { processed: false, reason: "provider_rate_limited" };
    }
    return { processed: false, reason: "admission_unavailable" };
  } finally {
    if (reservation) {
      releaseRuntimeOpenAdmission();
    }
    workerProvisioningInFlight = false;
  }
}

function normalizeAdmittedWorkerProvision(rawRequest) {
  if (rawRequest == null) {
    return null;
  }
  if (!rawRequest || typeof rawRequest !== "object") {
    throw new WorkerProvisioningAdapterError("invalid_admission");
  }
  const requestId = String(rawRequest.request_id || "");
  const projectId = String(rawRequest.project_id || "");
  const wakeTargetId = String(rawRequest.wake_target_id || "");
  const admissionRevision = Number(rawRequest.admission_revision);
  const status =
    typeof rawRequest.status === "string"
      ? rawRequest.status
      : rawRequest.status?.status;
  if (status && status !== "opening_project") {
    return null;
  }
  if (!/^prq_[0-9a-f]{32}$/.test(requestId)) {
    throw new WorkerProvisioningAdapterError("invalid_admission");
  }
  requireProjectId(projectId);
  if (!/^wtgt_[0-9a-f]{32}$/.test(wakeTargetId)) {
    throw new WorkerProvisioningAdapterError("invalid_admission");
  }
  if (!Number.isSafeInteger(admissionRevision) || admissionRevision <= 0) {
    throw new WorkerProvisioningAdapterError("invalid_admission");
  }
  return {
    requestId,
    projectId,
    wakeTargetId,
    admissionRevision,
    adapterMetadata:
      rawRequest.adapter_metadata && typeof rawRequest.adapter_metadata === "object"
        ? rawRequest.adapter_metadata
        : null,
  };
}

// Do not derive provider paths from a reference.  A future provider UI change
// must result in an explicit unsupported report, rather than an unbound Chat.
function explicitProjectBoundProvisioningPlan(adapterMetadata) {
  if (
    !adapterMetadata ||
    adapterMetadata.provider !== "chatgpt" ||
    typeof adapterMetadata.project_ref !== "string" ||
    !adapterMetadata.project_ref.trim() ||
    typeof adapterMetadata.project_path !== "string" ||
    !adapterMetadata.project_path.trim() ||
    typeof adapterMetadata.app_label !== "string" ||
    !adapterMetadata.app_label.trim() ||
    adapterMetadata.app_label.trim().length > 255
  ) {
    return null;
  }
  try {
    const url = new URL(adapterMetadata.project_path, "https://chatgpt.com");
    if (
      url.origin !== "https://chatgpt.com" ||
      !url.pathname.startsWith("/g/") ||
      url.pathname.replace(/\/+$/, "") !==
        `/g/${adapterMetadata.project_ref.trim()}` ||
      url.pathname.includes("/c/") ||
      url.search ||
      url.hash
    ) {
      return null;
    }
    return {
      projectUrl: `${url.origin}${url.pathname.replace(/\/+$/, "")}`,
      projectPath: url.pathname.replace(/\/+$/, ""),
      projectRef: adapterMetadata.project_ref.trim(),
      appIdentity: {
        appRef:
          typeof adapterMetadata.app_ref === "string" &&
          /^plugin_[A-Za-z0-9_-]+$/.test(adapterMetadata.app_ref.trim())
            ? adapterMetadata.app_ref.trim()
            : null,
        appLabel: adapterMetadata.app_label.trim(),
      },
    };
  } catch (_error) {
    return null;
  }
}

async function executeAdmittedWorkerProvision(request) {
  const plan = explicitProjectBoundProvisioningPlan(request.adapterMetadata);
  if (!plan) {
    await failWorkerProvisioningAdmission(
      request,
      "unsupported_project_identity",
    );
    return { processed: true, provisioned: false, reason: "unsupported_project_identity" };
  }

  let provisioningTab = null;
  let registrationEstablished = false;
  try {
    if (!(await runtimeSideEffectsAllowedWithoutPendingRetirement())) {
      throw new WorkerProvisioningAdapterError("worker_retirement_pending");
    }
    // No await between this global barrier and Chrome's create side effect.
    const created = await chrome.tabs.create({
      url: plan.projectUrl,
      active: false,
    });
    if (!Number.isInteger(created?.id)) {
      throw new WorkerProvisioningAdapterError("browser_open_failed");
    }
    provisioningTab = {
      id: created.id,
      windowId: Number.isInteger(created.windowId) ? created.windowId : null,
      projectPath: plan.projectPath,
      conversationId: null,
      openedAt: Date.now(),
    };
    provisioningRuntimeOwnedTabIds.add(created.id);
    await recordRuntimeOpen(provisioningTab.openedAt);

    await requestProjectBoundNewChat(created.id, plan);
    // Let the provider apply its project-scoped new-conversation navigation
    // before looking for a composer.  This is bounded UI readiness waiting,
    // not a retry of the provider-facing creation action.
    await provisioningDelay(WORKER_PROVISIONING_UI_RETRY_MS);
    const bootstrap = await bootstrapProvisionedChat(created.id, plan.appIdentity);
    const conversationUrl = await waitForProvisionedConversationUrl(created.id);
    provisioningTab.conversationId = chatConversationId(conversationUrl);

    await requireSafeProvisioningTab(created.id);
    if (!(await canStoreProvisionedWorker(conversationUrl, request))) {
      throw new WorkerProvisioningAdapterError("local_registration_conflict");
    }
    await requireSafeProvisioningTab(created.id);
    const admittedBinding = await readExactProvisioningConnectorBinding(
      request,
      plan,
    );
    await requireSafeProvisioningTab(created.id);

    const registration = provisionedWorkerRegistration(
      request,
      plan.appIdentity,
      admittedBinding,
      {
        tabId: provisioningTab.id,
        conversationId: provisioningTab.conversationId,
        draft: bootstrap.registrationDraft,
      },
    );
    await markWorkerProvisioningAdmission(request, "registration_pending", {
      conversationUrl,
      wakeTargetId: request.wakeTargetId,
      projectId: request.projectId,
      connectorBinding: admittedBinding,
    });
    const storedRegistration = await writeProvisionedWorkerRegistration(
      conversationUrl,
      registration,
    );
    registrationEstablished = true;
    const runtimeRegistration = await adoptProvisionedWorkerRuntimeTab(
      conversationUrl,
      storedRegistration,
      provisioningTab,
    );
    let finalizedRegistration = runtimeRegistration;
    try {
      await submitWorkerProvisioningRegistration(request, conversationUrl);
      const finalizedBinding = await readExactProvisioningConnectorBinding(
        request,
        plan,
        admittedBinding,
      );
      finalizedRegistration = await persistProvisionedConnectorProof(
        conversationUrl,
        runtimeRegistration,
        request,
        finalizedBinding,
      );
      if (!finalizedRegistration) {
        throw new WorkerProvisioningAdapterError("connector_binding_changed");
      }
      await markWorkerProvisioningAdmission(request, "registered");
    } catch (error) {
      if (Number(error?.status) === 409) {
        await discardConflictedWorkerProvisioningRegistration(
          request,
          conversationUrl,
        );
        await removeFailedProvisioningTabWhenSafe(provisioningTab);
        return {
          processed: true,
          provisioned: false,
          reason: "registration_conflict",
        };
      }
      if (Number(error?.status) === 429) {
        await setWorkerProvisioningBackoff();
        return {
          processed: true,
          provisioned: false,
          reason: "provider_rate_limited",
        };
      }
      if (error instanceof WorkerProvisioningAdapterError) {
        await retireProvisionedWorkerOrClearOwnership(
          runtimeRegistration,
          error.code,
        );
        await markWorkerProvisioningAdmission(request, "failed");
        return {
          processed: true,
          provisioned: false,
          reason: error.code,
        };
      }
      return {
        processed: true,
        provisioned: false,
        reason: "registration_finalize_pending",
      };
    }

    // This is only a presence refresh.  The established content-script polling
    // path delivers a later admitted wake; provisioning never sends work text.
    try {
      await pollRuntimeTarget(finalizedRegistration);
    } catch (_error) {
      // Registration and callback are durable. A later normal heartbeat
      // can recover a transient loopback read failure without reopening a tab.
    }
    return { processed: true, provisioned: true, conversationUrl };
  } catch (error) {
    const failure = workerProvisioningFailureFrom(error);
    if (provisioningTab && !registrationEstablished) {
      await removeFailedProvisioningTabWhenSafe(provisioningTab);
    }
    await failWorkerProvisioningAdmission(request, failure.code, {
      rateLimited: failure.rateLimited,
    });
    return { processed: true, provisioned: false, reason: failure.code };
  } finally {
    if (provisioningTab) {
      claimedProvisioningTabIds.delete(provisioningTab.id);
      provisioningRuntimeOwnedTabIds.delete(provisioningTab.id);
      provisioningBootstrapAdmissions.delete(provisioningTab.id);
      provisioningProjectOpenAdmissions.delete(provisioningTab.id);
    }
  }
}

async function readExactProvisioningConnectorBinding(
  request,
  plan,
  expectedBinding = null,
) {
  const payload = await requestJson(
    `/api/project-bindings/${encodeURIComponent(request.projectId)}/connector`,
  );
  const binding = payload?.connector;
  const bindingRevision = Number(binding?.revision);
  const connectorEpoch = Number(binding?.connector_epoch);
  if (
    binding?.project_id !== request.projectId ||
    binding?.provider !== "chatgpt" ||
    binding?.project_ref !== plan.projectRef ||
    binding?.project_path !== plan.projectPath ||
    binding?.app_identity?.app_ref !== plan.appIdentity.appRef ||
    binding?.app_identity?.app_label !== plan.appIdentity.appLabel ||
    !Number.isSafeInteger(bindingRevision) ||
    bindingRevision < 0 ||
    !Number.isSafeInteger(connectorEpoch) ||
    connectorEpoch < 0
  ) {
    throw new WorkerProvisioningAdapterError("connector_binding_changed");
  }
  const compact = {
    projectId: request.projectId,
    provider: "chatgpt",
    projectRef: plan.projectRef,
    projectPath: plan.projectPath,
    appRef: plan.appIdentity.appRef,
    appLabel: plan.appIdentity.appLabel,
    bindingRevision,
    connectorEpoch,
  };
  if (
    expectedBinding &&
    Object.keys(compact).some((key) => compact[key] !== expectedBinding[key])
  ) {
    throw new WorkerProvisioningAdapterError("connector_binding_changed");
  }
  return compact;
}

async function persistProvisionedConnectorProof(
  conversationUrl,
  expectedRegistration,
  request,
  binding,
) {
  return updateRegistrationIfBindingMatches(conversationUrl, expectedRegistration, {
    bindingRevision: binding.bindingRevision,
    connectorEpoch: binding.connectorEpoch,
    provisionedConnectorProof: {
      requestId: request.requestId,
      wakeTargetId: request.wakeTargetId,
      conversationId: chatConversationId(conversationUrl),
      admissionRevision: request.admissionRevision,
      ...binding,
      finalizedAt: Date.now(),
    },
  });
}

async function adoptProvisionedWorkerRuntimeTab(
  conversationUrl,
  registration,
  provisioningTab,
) {
  try {
    await requireSafeProvisioningTab(provisioningTab.id);
  } catch (_error) {
    return registration;
  }
  const updated = await updateRegistrationIfBindingMatches(
    conversationUrl,
    registration,
    {
      runtimeOpenedTabId: provisioningTab.id,
      runtimeOpenedAt: provisioningTab.openedAt,
      idleSince: 0,
    },
  );
  if (!updated) {
    return registration;
  }
  try {
    await requireSafeProvisioningTab(provisioningTab.id);
    return updated;
  } catch (_error) {
    await clearRuntimeOwnership(conversationUrl);
    return normalizeRegistration(await readRegistration(conversationUrl));
  }
}

// A provisioning tab is runtime-owned only until its local conversation
// registration succeeds. A failure must not leave an unused background Chat
// behind, but a person may claim that tab while the asynchronous provider flow
// is running. Re-read immediately before close and fail closed on any change.
async function removeFailedProvisioningTabWhenSafe(provisioningTab) {
  try {
    const current = await chrome.tabs.get(provisioningTab.id);
    if (!isStillSafeFailedProvisioningTab(current, provisioningTab)) {
      return false;
    }
    await chrome.tabs.remove(provisioningTab.id);
    return true;
  } catch (_error) {
    // A disappeared, claimed, or otherwise changed tab is intentionally left
    // alone. The failure callback remains sufficient to release server demand.
    return false;
  }
}

function isStillSafeFailedProvisioningTab(current, provisioningTab) {
  if (
    claimedProvisioningTabIds.has(provisioningTab?.id) ||
    !Number.isInteger(provisioningTab?.id) ||
    !Number.isInteger(current?.id) ||
    current.id !== provisioningTab.id ||
    current.active !== false ||
    current.pinned !== false
  ) {
    return false;
  }
  if (
    provisioningTab.windowId !== null &&
    (!Number.isInteger(current.windowId) ||
      current.windowId !== provisioningTab.windowId)
  ) {
    return false;
  }
  return provisioningTabMatchesExpectedContext(current, provisioningTab);
}

function provisioningTabMatchesExpectedContext(current, provisioningTab) {
  const rawUrl = current?.url || current?.pendingUrl || "";
  try {
    const url = new URL(rawUrl);
    if (url.origin !== "https://chatgpt.com") {
      return false;
    }
    if (provisioningTab.conversationId) {
      return chatConversationId(url.href) === provisioningTab.conversationId;
    }
    return chatGPTProjectLandingMatches({
      rawUrl: url.href,
      projectPath: provisioningTab.projectPath,
      projectRef: provisioningTab.projectPath.slice("/g/".length),
    });
  } catch (_error) {
    return false;
  }
}

function workerProvisioningFailureFrom(error) {
  if (error instanceof WorkerProvisioningAdapterError) {
    return { code: error.code, rateLimited: error.rateLimited };
  }
  if (Number(error?.status) === 429) {
    return { code: "provider_rate_limited", rateLimited: true };
  }
  return { code: "browser_provision_failed", rateLimited: false };
}

async function requestProjectBoundNewChat(tabId, plan) {
  await requireSafeProvisioningTab(tabId);
  const ownershipToken = crypto.randomUUID();
  provisioningProjectOpenAdmissions.set(tabId, ownershipToken);
  let response;
  try {
    response = await retryProvisioningTabMessage(tabId, {
      type: "provision:openProjectNewChat",
      projectPath: plan.projectPath,
      projectRef: plan.projectRef,
      ownershipToken,
    });
  } finally {
    if (provisioningProjectOpenAdmissions.get(tabId) === ownershipToken) {
      provisioningProjectOpenAdmissions.delete(tabId);
    }
  }
  if (response?.rateLimited) {
    throw new WorkerProvisioningAdapterError("provider_rate_limited", {
      rateLimited: true,
    });
  }
  if (!response?.ok) {
    throw new WorkerProvisioningAdapterError(
      response?.code || "unsupported_project_ui",
    );
  }
}

async function bootstrapProvisionedChat(tabId, appIdentity) {
  await requireSafeProvisioningTab(tabId);
  const bootstrapToken = crypto.randomUUID();
  provisioningBootstrapAdmissions.set(tabId, bootstrapToken);
  let response;
  try {
    response = await retryProvisioningTabMessage(tabId, {
      type: "provision:bootstrap",
      bootstrapToken,
      appIdentity,
    });
  } finally {
    if (provisioningBootstrapAdmissions.get(tabId) === bootstrapToken) {
      provisioningBootstrapAdmissions.delete(tabId);
    }
  }
  if (response?.rateLimited) {
    throw new WorkerProvisioningAdapterError("provider_rate_limited", {
      rateLimited: true,
    });
  }
  if (!response?.ok) {
    throw new WorkerProvisioningAdapterError(
      response?.code || "bootstrap_unavailable",
    );
  }
  return response;
}

async function confirmProvisioningBootstrapSubmit({ tabId, bootstrapToken }) {
  if (
    !Number.isInteger(tabId) ||
    typeof bootstrapToken !== "string" ||
    provisioningBootstrapAdmissions.get(tabId) !== bootstrapToken
  ) {
    return { safe: false };
  }
  // Consume before yielding so a content script cannot reuse a once-approved
  // capability. The tab state is then read as close as possible to submit.
  provisioningBootstrapAdmissions.delete(tabId);
  try {
    await requireSafeProvisioningTab(tabId);
    return { safe: true };
  } catch (_error) {
    return { safe: false };
  }
}

async function confirmProvisioningProjectOpen({ tabId, ownershipToken }) {
  if (
    !Number.isInteger(tabId) ||
    typeof ownershipToken !== "string" ||
    provisioningProjectOpenAdmissions.get(tabId) !== ownershipToken
  ) {
    return { safe: false };
  }
  provisioningProjectOpenAdmissions.delete(tabId);
  try {
    await requireSafeProvisioningTab(tabId);
    return { safe: true };
  } catch (_error) {
    return { safe: false };
  }
}

function noteProvisioningTabActivation(tabId) {
  if (provisioningRuntimeOwnedTabIds.has(tabId)) {
    claimedProvisioningTabIds.add(tabId);
  }
}

async function retryProvisioningTabMessage(tabId, message) {
  let lastResponse = null;
  for (let attempt = 0; attempt < WORKER_PROVISIONING_UI_ATTEMPTS; attempt += 1) {
    lastResponse = await sendProvisioningTabMessage(tabId, message);
    if (
      lastResponse?.ok ||
      lastResponse?.rateLimited ||
      lastResponse?.retryable === false
    ) {
      return lastResponse;
    }
    if (attempt + 1 < WORKER_PROVISIONING_UI_ATTEMPTS) {
      await provisioningDelay(WORKER_PROVISIONING_UI_RETRY_MS);
    }
  }
  return lastResponse;
}

async function waitForProvisionedConversationUrl(tabId) {
  const deadline = Date.now() + WORKER_PROVISIONING_CONVERSATION_WAIT_MS;
  while (Date.now() < deadline) {
    const providerState = await sendProvisioningTabMessage(tabId, {
      type: "provision:providerState",
    });
    if (providerState?.rateLimited) {
      throw new WorkerProvisioningAdapterError("provider_rate_limited", {
        rateLimited: true,
      });
    }
    try {
      const tab = await chrome.tabs.get(tabId);
      if (claimedProvisioningTabIds.has(tabId) || tab.active || tab.pinned) {
        throw new WorkerProvisioningAdapterError("provisioning_tab_claimed");
      }
      const conversationUrl = normalizedChatConversationUrl(
        tab?.url || tab?.pendingUrl || "",
      );
      if (conversationUrl) {
        return conversationUrl;
      }
    } catch (error) {
      if (error instanceof WorkerProvisioningAdapterError) {
        throw error;
      }
      throw new WorkerProvisioningAdapterError("provisioned_tab_unavailable");
    }
    await provisioningDelay(WORKER_PROVISIONING_CONVERSATION_POLL_MS);
  }
  throw new WorkerProvisioningAdapterError("persistent_conversation_unavailable");
}

async function requireSafeProvisioningTab(tabId) {
  if (claimedProvisioningTabIds.has(tabId)) {
    throw new WorkerProvisioningAdapterError("provisioning_tab_claimed");
  }
  try {
    const tab = await chrome.tabs.get(tabId);
    if (tab.active || tab.pinned) {
      throw new WorkerProvisioningAdapterError("provisioning_tab_claimed");
    }
  } catch (error) {
    if (error instanceof WorkerProvisioningAdapterError) {
      throw error;
    }
    throw new WorkerProvisioningAdapterError("provisioned_tab_unavailable");
  }
}

function provisioningDelay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function sendProvisioningTabMessage(tabId, message) {
  try {
    return await chrome.tabs.sendMessage(tabId, message);
  } catch (_error) {
    return null;
  }
}

function provisionedWorkerRegistration(
  request,
  appIdentity,
  connectorBinding,
  { tabId, conversationId, draft },
) {
  return {
    kind: "worker",
    wakeTargetId: request.wakeTargetId,
    projectId: request.projectId,
    projectName: request.projectId,
    appIdentity,
    bindingRevision: connectorBinding.bindingRevision,
    connectorEpoch: connectorBinding.connectorEpoch,
    runtimeOwnedRegistrationDraft:
      Number.isInteger(tabId) &&
      typeof conversationId === "string" &&
      conversationId &&
      typeof draft === "string" &&
      draft
        ? {
            tabId,
            conversationId,
            draft,
            appRef: appIdentity.appRef,
            appLabel: appIdentity.appLabel,
          }
        : null,
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

async function canStoreProvisionedWorker(conversationUrl, request) {
  const existingAtConversation = normalizeRegistration(
    await readRegistration(conversationUrl),
  );
  if (existingAtConversation) {
    return (
      existingAtConversation.kind === "worker" &&
      existingAtConversation.projectId === request.projectId &&
      existingAtConversation.wakeTargetId === request.wakeTargetId
    );
  }
  const registrations = await readAllRegistrations();
  return !Object.values(registrations)
    .map(normalizeRegistration)
    .some(
      (registration) =>
        registration?.kind === "worker" &&
        registration.wakeTargetId === request.wakeTargetId,
    );
}

async function writeProvisionedWorkerRegistration(conversationUrl, registration) {
  const existing = normalizeRegistration(await readRegistration(conversationUrl));
  if (existing) {
    if (
      existing.kind === registration.kind &&
      existing.projectId === registration.projectId &&
      existing.wakeTargetId === registration.wakeTargetId
    ) {
      return existing;
    }
    throw new WorkerProvisioningAdapterError("local_registration_conflict");
  }
  await writeRegistration(conversationUrl, registration);
  return normalizeRegistration(await readRegistration(conversationUrl));
}

async function discardConflictedWorkerProvisioningRegistration(
  request,
  conversationUrl,
) {
  const conversationId = chatConversationId(conversationUrl);
  if (conversationId) {
    await mutateRegistrationMap(async () => {
      const { registrations } = await readCanonicalRegistrationMap();
      const storageKey = `conversation:${conversationId}`;
      const existing = normalizeRegistration(registrations[storageKey]);
      if (
        existing?.kind === "worker" &&
        existing.projectId === request.projectId &&
        existing.wakeTargetId === request.wakeTargetId
      ) {
        delete registrations[storageKey];
        await chrome.storage.local.set({ [STORAGE_KEY]: registrations });
      }
    });
  }
  await markWorkerProvisioningAdmission(request, "registration_conflict");
}

async function beginWorkerProvisioningAdmission(request) {
  const state = await readWorkerProvisioningState();
  const previous = state.admissions[request.requestId];
  if (
    previous &&
    Number(previous.admissionRevision || 0) >= request.admissionRevision
  ) {
    return false;
  }
  state.admissions[request.requestId] = {
    admissionRevision: request.admissionRevision,
    status: "in_flight",
    updatedAt: Date.now(),
  };
  await writeWorkerProvisioningState(state);
  return true;
}

async function markWorkerProvisioningAdmission(request, status, patch = {}) {
  const state = await readWorkerProvisioningState();
  const current = state.admissions[request.requestId];
  if (Number(current?.admissionRevision || 0) !== request.admissionRevision) {
    return false;
  }
  state.admissions[request.requestId] = {
    ...current,
    ...patch,
    status,
    updatedAt: Date.now(),
  };
  await writeWorkerProvisioningState(state);
  return true;
}

async function submitWorkerProvisioningRegistration(request, conversationUrl) {
  await requestJson(WORKER_PROVISIONING_ENDPOINTS.registered(request.requestId), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      conversation_id: chatConversationId(conversationUrl),
      wake_target_id: request.wakeTargetId,
      admission_revision: request.admissionRevision,
    }),
  });
}

async function resumePendingWorkerProvisioningFinalizations() {
  const state = await readWorkerProvisioningState();
  for (const [requestId, admission] of Object.entries(state.admissions)) {
    if (
      admission.status !== "registration_pending" ||
      typeof admission.conversationUrl !== "string" ||
      typeof admission.wakeTargetId !== "string" ||
      typeof admission.projectId !== "string"
    ) {
      continue;
    }
    const registration = normalizeRegistration(
      await readRegistration(admission.conversationUrl),
    );
    if (
      registration?.wakeTargetId !== admission.wakeTargetId ||
      registration?.projectId !== admission.projectId
    ) {
      continue;
    }
    const request = {
      requestId,
      projectId: admission.projectId,
      wakeTargetId: admission.wakeTargetId,
      admissionRevision: admission.admissionRevision,
    };
    try {
      const binding = admission.connectorBinding;
      if (!binding) {
        continue;
      }
      const plan = {
        projectRef: binding.projectRef,
        projectPath: binding.projectPath,
        appIdentity: { appRef: binding.appRef, appLabel: binding.appLabel },
      };
      await submitWorkerProvisioningRegistration(
        request,
        admission.conversationUrl,
      );
      const finalizedBinding = await readExactProvisioningConnectorBinding(
        request,
        plan,
        binding,
      );
      const finalized = await persistProvisionedConnectorProof(
        admission.conversationUrl,
        registration,
        request,
        finalizedBinding,
      );
      if (!finalized) {
        throw new WorkerProvisioningAdapterError("connector_binding_changed");
      }
      await markWorkerProvisioningAdmission(request, "registered");
    } catch (error) {
      if (Number(error?.status) !== 409) {
        if (error instanceof WorkerProvisioningAdapterError) {
          await retireProvisionedWorkerOrClearOwnership(
            registration,
            error.code,
          );
          await markWorkerProvisioningAdmission(request, "failed");
          continue;
        }
        throw error;
      }
      await discardConflictedWorkerProvisioningRegistration(
        request,
        admission.conversationUrl,
      );
    }
  }
}

async function failWorkerProvisioningAdmission(
  request,
  reason,
  { rateLimited = false } = {},
) {
  await markWorkerProvisioningAdmission(request, "failed");
  let response = null;
  try {
    response = await requestJson(WORKER_PROVISIONING_ENDPOINTS.failed(request.requestId), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        admission_revision: request.admissionRevision,
        reason,
        rate_limited: rateLimited,
      }),
    });
  } catch (_error) {
    // A failed report must not trigger a browser-side retry loop.
  }
  if (rateLimited) {
    await setWorkerProvisioningBackoff(response?.retry_after_ms);
  }
}

async function workerProvisioningBackoffActive() {
  const state = await readWorkerProvisioningState();
  return Number(state.backoffUntil || 0) > Date.now();
}

async function setWorkerProvisioningBackoff(retryAfterMilliseconds) {
  const requested = Number(retryAfterMilliseconds);
  const bounded = Math.min(
    MAX_PROVIDER_RATE_LIMIT_BACKOFF_MS,
    Math.max(
      MIN_PROVIDER_RATE_LIMIT_BACKOFF_MS,
      Number.isFinite(requested) ? requested : MIN_PROVIDER_RATE_LIMIT_BACKOFF_MS,
    ),
  );
  const state = await readWorkerProvisioningState();
  state.backoffUntil = Math.max(Number(state.backoffUntil || 0), Date.now() + bounded);
  await writeWorkerProvisioningState(state);
}

async function readWorkerProvisioningState() {
  const stored = await chrome.storage.local.get(WORKER_PROVISIONING_STORAGE_KEY);
  const raw = stored[WORKER_PROVISIONING_STORAGE_KEY];
  const state = {
    admissions: compactWorkerProvisioningAdmissions(raw?.admissions),
    backoffUntil: normalizedWorkerProvisioningBackoffUntil(raw?.backoffUntil),
  };
  // Compact legacy/unbounded records on the next normal adapter read rather
  // than waiting for a future admission write that may never arrive.
  if (raw && !workerProvisioningStateMatches(raw, state)) {
    await chrome.storage.local.set({
      [WORKER_PROVISIONING_STORAGE_KEY]: state,
    });
  }
  return state;
}

async function writeWorkerProvisioningState(state) {
  const normalized = {
    admissions: compactWorkerProvisioningAdmissions(state?.admissions),
    backoffUntil: normalizedWorkerProvisioningBackoffUntil(state?.backoffUntil),
  };
  await chrome.storage.local.set({
    [WORKER_PROVISIONING_STORAGE_KEY]: normalized,
  });
}

function compactWorkerProvisioningAdmissions(rawAdmissions) {
  if (!rawAdmissions || typeof rawAdmissions !== "object") {
    return {};
  }
  const entries = Object.entries(rawAdmissions)
    .map(([requestId, rawAdmission]) => {
      const admissionRevision = Number(rawAdmission?.admissionRevision);
      const updatedAt = Number(rawAdmission?.updatedAt);
      if (
        !requestId ||
        !Number.isSafeInteger(admissionRevision) ||
        admissionRevision <= 0 ||
        !Number.isFinite(updatedAt) ||
        updatedAt < 0 ||
        typeof rawAdmission?.status !== "string"
      ) {
        return null;
      }
      if (
        rawAdmission.status === "registration_pending" &&
        (!normalizedChatConversationUrl(rawAdmission.conversationUrl || "") ||
          !/^wtgt_[0-9a-f]{32}$/.test(rawAdmission.wakeTargetId || "") ||
          typeof rawAdmission.projectId !== "string" ||
          !rawAdmission.projectId)
      ) {
        return null;
      }
      const connectorBinding = normalizeStoredProvisioningConnectorBinding(
        rawAdmission.connectorBinding,
      );
      return [
        requestId,
        {
          admissionRevision,
          status: rawAdmission.status,
          updatedAt,
          ...(typeof rawAdmission.conversationUrl === "string"
            ? { conversationUrl: rawAdmission.conversationUrl }
            : {}),
          ...(typeof rawAdmission.wakeTargetId === "string"
            ? { wakeTargetId: rawAdmission.wakeTargetId }
            : {}),
          ...(typeof rawAdmission.projectId === "string"
            ? { projectId: rawAdmission.projectId }
            : {}),
          ...(connectorBinding
            ? { connectorBinding }
            : {}),
        },
      ];
    })
    .filter(Boolean);
  const newestFirst = (left, right) =>
    right[1].updatedAt - left[1].updatedAt ||
    right[1].admissionRevision - left[1].admissionRevision ||
    left[0].localeCompare(right[0]);
  const retainedInFlight = entries
    .filter(([, admission]) =>
      ["in_flight", "registration_pending"].includes(admission.status),
    )
    .sort(newestFirst)
    .slice(0, MAX_WORKER_PROVISIONING_IN_FLIGHT_HISTORY);
  const retainedRecent = entries
    .filter(
      ([, admission]) =>
        !["in_flight", "registration_pending"].includes(admission.status),
    )
    .sort(newestFirst)
    .slice(
      0,
      Math.max(0, MAX_WORKER_PROVISIONING_ADMISSION_HISTORY - retainedInFlight.length),
    );
  return Object.fromEntries([...retainedInFlight, ...retainedRecent]);
}

function normalizeStoredProvisioningConnectorBinding(rawBinding) {
  if (!rawBinding || typeof rawBinding !== "object") {
    return null;
  }
  const bindingRevision = Number(rawBinding.bindingRevision);
  const connectorEpoch = Number(rawBinding.connectorEpoch);
  const candidate = {
    projectId: String(rawBinding.projectId || ""),
    provider: String(rawBinding.provider || ""),
    projectRef: String(rawBinding.projectRef || ""),
    projectPath: String(rawBinding.projectPath || ""),
    appRef: String(rawBinding.appRef || ""),
    appLabel: String(rawBinding.appLabel || ""),
    bindingRevision,
    connectorEpoch,
  };
  if (
    !/^prj_[a-z0-9-]{1,64}_[0-9a-f]{8}$/.test(candidate.projectId) ||
    candidate.provider !== "chatgpt" ||
    !candidate.projectRef ||
    candidate.projectRef.length > 255 ||
    candidate.projectPath !== `/g/${candidate.projectRef}` ||
    !/^plugin_[A-Za-z0-9_-]+$/.test(candidate.appRef) ||
    !candidate.appLabel ||
    candidate.appLabel.length > 255 ||
    !Number.isSafeInteger(bindingRevision) ||
    bindingRevision < 0 ||
    !Number.isSafeInteger(connectorEpoch) ||
    connectorEpoch < 0
  ) {
    return null;
  }
  return candidate;
}

function normalizedWorkerProvisioningBackoffUntil(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function workerProvisioningStateMatches(raw, state) {
  const rawAdmissions =
    raw && typeof raw.admissions === "object" && raw.admissions
      ? raw.admissions
      : {};
  const rawKeys = Object.keys(rawAdmissions).sort();
  const compactedKeys = Object.keys(state.admissions).sort();
  if (
    rawKeys.length !== compactedKeys.length ||
    rawKeys.some((key, index) => key !== compactedKeys[index]) ||
    normalizedWorkerProvisioningBackoffUntil(raw?.backoffUntil) !== state.backoffUntil
  ) {
    return false;
  }
  return rawKeys.every((key) => {
    const rawAdmission = rawAdmissions[key];
    const compactedAdmission = state.admissions[key];
    return (
      Number(rawAdmission?.admissionRevision) ===
        compactedAdmission.admissionRevision &&
      rawAdmission?.status === compactedAdmission.status &&
      Number(rawAdmission?.updatedAt) === compactedAdmission.updatedAt
      && rawAdmission?.conversationUrl === compactedAdmission.conversationUrl
      && rawAdmission?.wakeTargetId === compactedAdmission.wakeTargetId
      && rawAdmission?.projectId === compactedAdmission.projectId
      && JSON.stringify(rawAdmission?.connectorBinding) ===
        JSON.stringify(compactedAdmission.connectorBinding)
    );
  });
}
