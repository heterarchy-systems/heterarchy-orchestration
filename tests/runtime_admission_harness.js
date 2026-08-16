"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const repositoryRoot = path.resolve(__dirname, "..");
const extensionRoot = path.join(
  repositoryRoot,
  "chattocodex",
  "wake",
  "chrome_extension",
);
const APP_IDENTITY = Object.freeze({
  appRef: "plugin_asdk_app_custom",
  appLabel: "my-local-mcp",
});

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function workerRegistration(index, patch = {}) {
  const conversationId = "worker-" + index;
  const wakeTargetId = "wtgt_" + String(index).padStart(32, "0");
  const projectId = "prj_runtime_12345678";
  return {
    storageKey: "conversation:" + conversationId,
    value: {
      kind: "worker",
      wakeTargetId,
      projectId,
      conversationId,
      conversationUrl: "https://chatgpt.com/c/" + conversationId,
      registeredAt: 10,
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
      ...patch,
    },
  };
}

function mainRegistration(index, patch = {}) {
  const conversationId = "main-" + index;
  const mainTargetId = "mtgt_" + String(index).padStart(32, "0");
  const projectId = "prj_runtime_12345678";
  return {
    storageKey: "conversation:" + conversationId,
    value: {
      kind: "main",
      mainTargetId,
      projectId,
      conversationId,
      conversationUrl: "https://chatgpt.com/c/" + conversationId,
      registeredAt: 10,
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
      ...patch,
    },
  };
}

function targetForRegistration(registration, status, wakeRevision) {
  const value = registration.value;
  return {
    ...(value.kind === "main"
      ? { main_target_id: value.mainTargetId }
      : { wake_target_id: value.wakeTargetId }),
    project_id: value.projectId,
    ...(value.kind === "worker"
      ? { registered_connector_epoch: Number(value.connectorEpoch ?? 2) }
      : {}),
    status,
    wake_revision: wakeRevision,
    revision: wakeRevision,
  };
}

function senderTabForRegistration(registration, fallbackTabId = 1) {
  return {
    id: Number.isInteger(registration.value.runtimeOpenedTabId)
      ? registration.value.runtimeOpenedTabId
      : fallbackTabId,
    url: registration.value.conversationUrl,
  };
}

function makeRuntime({
  registrations,
  targets,
  projectBindings = {},
  storage = null,
  now = 1000000,
  delayCreates = false,
  initialTabs = [],
  onProjectBindingRead = null,
  onTargetRead = null,
  onTabQuery = null,
  onTabGet = null,
  onCanPark = null,
  onRuntimeOpenAdmissionRead = null,
  onRegistrationMapRead = null,
  onStorageSet = null,
  canParkResponse = { ok: true, canPark: true },
  onRetireRequest = null,
  onRetireBatchRequest = null,
  wakeAdapterDiagnosis = {
    version: "0.1.14",
    extensionContextAvailable: true,
    pollTimerActive: true,
    everClaimed: false,
    composerPresent: true,
    generating: false,
  },
  onExecuteScript = null,
}) {
  const sharedStorage =
    storage ||
    {
      wakeTargets: Object.fromEntries(
        registrations.map((registration) => [
          registration.storageKey,
          clone(registration.value),
        ]),
      ),
    };
  const targetById = new Map(targets);
  const tabs = new Map(initialTabs.map((tab) => [tab.id, clone(tab)]));
  const createCalls = [];
  const removeCalls = [];
  const fetchPaths = [];
  const fetchRequests = [];
  const tabMessageCalls = [];
  const scriptExecutionCalls = [];
  const storageSetCalls = [];
  const pendingCreateResolvers = [];
  let nextTabId = Math.max(0, ...tabs.keys()) + 1;
  let createInFlight = 0;
  let maxCreateInFlight = 0;
  let clock = now;
  let tabActivatedListener = null;
  let tabUpdatedListener = null;

  const chrome = {
    runtime: {
      getManifest() {
        return { version: "0.1.14" };
      },
      onMessage: { addListener() {} },
      onInstalled: { addListener() {} },
      onStartup: { addListener() {} },
    },
    alarms: {
      async get() {
        return { name: "existing-runtime-alarm" };
      },
      async create() {},
      onAlarm: { addListener() {} },
    },
    tabs: {
      onActivated: {
        addListener(listener) {
          tabActivatedListener = listener;
        },
      },
      onRemoved: { addListener() {} },
      onUpdated: {
        addListener(listener) {
          tabUpdatedListener = listener;
        },
      },
      async query() {
        const result = [...tabs.values()].map(clone);
        if (onTabQuery) {
          await onTabQuery({ storage: sharedStorage, tabs });
        }
        return result;
      },
      async get(tabId) {
        const tab = tabs.get(tabId);
        if (!tab) {
          throw new Error("Unknown tab " + tabId);
        }
        if (onTabGet) {
          await onTabGet({ tabId, storage: sharedStorage, tabs });
        }
        return clone(tab);
      },
      async create({ url, active }) {
        createCalls.push({ url, active, startedAt: clock });
        createInFlight += 1;
        maxCreateInFlight = Math.max(maxCreateInFlight, createInFlight);
        if (delayCreates) {
          await new Promise((resolve) => pendingCreateResolvers.push(resolve));
        }
        const tab = {
          id: nextTabId,
          url,
          active,
          pinned: false,
          discarded: false,
        };
        nextTabId += 1;
        tabs.set(tab.id, tab);
        createInFlight -= 1;
        return clone(tab);
      },
      async reload() {},
      async remove(tabId) {
        removeCalls.push(tabId);
        tabs.delete(tabId);
      },
      async sendMessage(tabId, message) {
        if (message?.type === "runtime:diagnoseWakeAdapter") {
          return clone(wakeAdapterDiagnosis);
        }
        tabMessageCalls.push({ tabId, message: clone(message) });
        if (message?.type === "runtime:canPark" && onCanPark) {
          await onCanPark({ tabId, storage: sharedStorage, tabs });
        }
        return clone(canParkResponse);
      },
    },
    scripting: {
      async executeScript(details) {
        scriptExecutionCalls.push(clone(details));
        if (onExecuteScript) {
          await onExecuteScript({ details, storage: sharedStorage, tabs });
        }
        if (details.files?.includes("content.js")) {
          wakeAdapterDiagnosis = {
            version: "0.1.14",
            extensionContextAvailable: true,
            pollTimerActive: true,
            everClaimed: false,
            composerPresent: true,
            generating: false,
          };
        }
        return [];
      },
    },
    storage: {
      local: {
        async get(keys) {
          if (typeof keys === "string") {
            const snapshot = clone(sharedStorage[keys]);
            if (
              keys === "runtimeOpenAdmission" &&
              onRuntimeOpenAdmissionRead
            ) {
              await onRuntimeOpenAdmissionRead({ storage: sharedStorage });
            }
            if (keys === "wakeTargets" && onRegistrationMapRead) {
              await onRegistrationMapRead({ snapshot, storage: sharedStorage });
            }
            return { [keys]: snapshot };
          }
          if (Array.isArray(keys)) {
            return Object.fromEntries(
              keys.map((key) => [key, clone(sharedStorage[key])]),
            );
          }
          return clone(sharedStorage);
        },
        async set(patch) {
          storageSetCalls.push(clone(patch));
          if (onStorageSet) {
            await onStorageSet({ patch: clone(patch), storage: sharedStorage });
          }
          Object.assign(sharedStorage, clone(patch));
        },
      },
    },
  };

  async function fetchStub(rawUrl, options = {}) {
    const url = new URL(rawUrl);
    const pathname = url.pathname;
    fetchPaths.push(pathname);
    fetchRequests.push({
      pathname,
      method: options.method || "GET",
      body: options.body ? JSON.parse(options.body) : null,
    });
    if (pathname === "/api/worker-provisioning/next") {
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true, request: null };
        },
      };
    }
    if (/^\/api\/project-bindings\/[^/]+\/connector$/.test(pathname)) {
      const projectId = decodeURIComponent(pathname.split("/").at(-2));
      const binding = projectBindings[projectId];
      if (onProjectBindingRead) {
        await onProjectBindingRead({
          projectId,
          binding: clone(binding),
          storage: sharedStorage,
        });
      }
      if (binding) {
        return {
          ok: true,
          status: 200,
          async json() {
            return { ok: true, connector: clone(binding) };
          },
        };
      }
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true };
        },
      };
    }
    const workerMatch = pathname.match(
      /^\/api\/wake\/targets\/([^/]+)\/runtime-heartbeat$/,
    );
    const workerRetireMatch = pathname.match(
      /^\/api\/wake\/targets\/([^/]+)\/retire$/,
    );
    if (pathname === "/api/wake/targets/retire-batch") {
      const body = options.body ? JSON.parse(options.body) : null;
      if (onRetireBatchRequest) {
        return onRetireBatchRequest({ body, storage: sharedStorage, targetById });
      }
      for (const target of body?.targets || []) {
        targetById.delete(target.wake_target_id);
      }
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            ok: true,
            project_id: body.project_id,
            retired_wake_target_ids: body.targets.map(
              (target) => target.wake_target_id,
            ),
          };
        },
      };
    }
    if (workerRetireMatch) {
      if (onRetireRequest) {
        const response = await onRetireRequest({
          targetId: workerRetireMatch[1],
          options,
          storage: sharedStorage,
        });
        if (response) {
          return response;
        }
      }
      targetById.delete(workerRetireMatch[1]);
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true, wake_target_id: workerRetireMatch[1] };
        },
      };
    }
    const mainHeartbeatMatch = pathname.match(
      /^\/api\/main-runtime\/review-targets\/([^/]+)\/chat-heartbeat$/,
    );
    if (mainHeartbeatMatch) {
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true };
        },
      };
    }
    const mainMatch = pathname.match(
      /^\/api\/main-runtime\/review-targets\/([^/]+)$/,
    );
    const workerReadMatch = pathname.match(
      /^\/api\/wake\/targets\/([^/]+)$/,
    );
    const targetId = (workerMatch && workerMatch[1]) ||
      (workerReadMatch && workerReadMatch[1]) ||
      (mainMatch && mainMatch[1]) ||
      null;
    const target = targetId ? targetById.get(targetId) : null;
    if (onTargetRead) {
      await onTargetRead({
        pathname,
        targetId,
        storage: sharedStorage,
        targetById,
      });
    }
    if (!target) {
      return {
        ok: false,
        status: 404,
        async json() {
          return {
            ok: false,
            error_code: "not_found",
            message: "Unknown runtime target.",
          };
        },
      };
    }
    return {
      ok: true,
      status: 200,
      async json() {
        return { ok: true, target: clone(target) };
      },
    };
  }

  const identitySource = fs.readFileSync(
    path.join(extensionRoot, "runtime_conversation_identity.js"),
    "utf8",
  );
  const policySource = fs.readFileSync(
    path.join(extensionRoot, "runtime_tab_reopen_policy.js"),
    "utf8",
  );
  const persistentWorkerSource = fs.readFileSync(
    path.join(extensionRoot, "persistent_worker_runtime_policy.js"),
    "utf8",
  );
  const provisioningSource = fs.readFileSync(
    path.join(extensionRoot, "worker_provisioning_adapter.js"),
    "utf8",
  );
  const workerSource = fs
    .readFileSync(path.join(extensionRoot, "service_worker.js"), "utf8")
    .replace(/importScripts\([^\n]+\);\n/g, "")
    .replace("\nvoid bootstrapRuntimeSweep();\n", "\n");
  const source = identitySource + "\n" + policySource + "\n" + persistentWorkerSource + "\n" + provisioningSource + "\n" + workerSource + "\n" +
    "globalThis.__gateA = {" +
    "sweepRuntimeTargets," +
    "deliveryAdmissionForRuntimeTarget," +
    "deliverBackendAuthorizedRuntimeAdmission," +
    "handleMessage," +
    "ensureCurrentWakeAdapter," +
    "bindProjectBinding," +
    "pollTarget," +
    "consumeRuntimeOwnedRegistrationDraft," +
    "reserveExactCurrentWakeDraft: typeof reserveExactCurrentWakeDraft === 'function' ? reserveExactCurrentWakeDraft : null," +
    "cleanupStaleRuntimeWakeDraft: typeof cleanupStaleRuntimeWakeDraft === 'function' ? cleanupStaleRuntimeWakeDraft : null," +
    "prepareFinalSendAdmission," +
    "confirmFinalSendAdmission," +
    "recordWakeSendFailure," +
    "recordWakeContentDisposition: typeof recordWakeContentDisposition === 'function' ? recordWakeContentDisposition : null," +
    "markAttempt," +
    "markResumeAttempt," +
    "markMainResumeAttempt," +
    "updateRegistration," +
    "parkRuntimeOwnedTabWhenSafe," +
    "transferRuntimeOwnership," +
    "clearRemovedRuntimeTab," +
    "resumePendingWorkerRetirements," +
    "noteRuntimeCreatedTabActivation," +
    "removeRuntimeCreatedTabWhenStillOwned," +
    "runtimeCreatedTabIds," +
    "MIN_RUNTIME_OPEN_INTERVAL_MS," +
    "openInFlightCount: () => runtimeOpenInFlightCount," +
    "};";
  const context = vm.createContext({
    chrome,
    fetch: fetchStub,
    URL,
    crypto: { randomUUID: () => "00000000-0000-0000-0000-000000000000" },
    Date: { now: () => clock },
    console,
  });
  vm.runInContext(source, context, { filename: "service_worker.js" });

  return {
    api: context.__gateA,
    storage: sharedStorage,
    createCalls,
    removeCalls,
    fetchPaths,
    fetchRequests,
    tabMessageCalls,
    scriptExecutionCalls,
    storageSetCalls,
    pendingCreateResolvers,
    get createInFlight() {
      return createInFlight;
    },
    get maxCreateInFlight() {
      return maxCreateInFlight;
    },
    patchTab(tabId, patch) {
      Object.assign(tabs.get(tabId), patch);
    },
    dispatchTabActivated(tabId) {
      if (tabActivatedListener) {
        tabActivatedListener({ tabId });
      }
    },
    dispatchTabUpdated(tabId, changeInfo, patch = {}) {
      Object.assign(tabs.get(tabId), patch);
      if (tabUpdatedListener) {
        tabUpdatedListener(tabId, clone(changeInfo), clone(tabs.get(tabId)));
      }
    },
    advance(milliseconds) {
      clock += milliseconds;
    },
    releaseCreates() {
      for (const resolve of pendingCreateResolvers.splice(0)) {
        resolve();
      }
    },
    removeTab(tabId) {
      tabs.delete(tabId);
    },
    getTab(tabId) {
      const tab = tabs.get(tabId);
      return tab ? clone(tab) : null;
    },
  };
}

function candidate(api, registration, target) {
  const admission = api.deliveryAdmissionForRuntimeTarget(
    registration.value,
    target,
  );
  assert.ok(admission, "candidate must have backend authorization");
  return {
    conversationUrl: registration.value.conversationUrl,
    registration: registration.value,
    target,
    admission,
  };
}

function projectBinding(projectId, {
  projectRef = "project-safe",
  appRef = "plugin_replacement",
  appLabel = "Replacement Connector",
  connectorEpoch = 2,
  revision = 2,
} = {}) {
  return {
    project_id: projectId,
    provider: "chatgpt",
    project_ref: projectRef,
    project_path: `/g/${projectRef}`,
    app_identity: {
      app_ref: appRef,
      app_label: appLabel,
    },
    connector_epoch: connectorEpoch,
    revision,
  };
}

async function flushMicrotasks() {
  for (let index = 0; index < 12; index += 1) {
    await Promise.resolve();
  }
}

async function waitFor(condition) {
  for (let index = 0; index < 50; index += 1) {
    if (condition()) {
      return;
    }
    await new Promise((resolve) => setImmediate(resolve));
  }
  throw new Error("Timed out waiting for the runtime-admission harness.");
}

async function staleNoReadyDoesNotOpenTabs() {
  const registrations = Array.from({ length: 20 }, (_, index) =>
    workerRegistration(index + 1),
  );
  const targets = registrations
    .filter((_registration, index) => index >= 10 && index < 15)
    .map((registration) => [
      registration.value.wakeTargetId,
      targetForRegistration(registration, "idle", 0),
    ])
    .concat(
      registrations.slice(15).map((registration) => [
        registration.value.wakeTargetId,
        targetForRegistration(registration, "terminal", 9),
      ]),
    );
  const runtime = makeRuntime({ registrations, targets });

  await runtime.api.sweepRuntimeTargets();
  await runtime.api.sweepRuntimeTargets();
  await runtime.api.sweepRuntimeTargets();

  assert.equal(runtime.createCalls.length, 0);
  assert.equal(
    runtime.fetchPaths.some((pathname) => pathname.endsWith("/register")),
    false,
    "unknown local registration must not be resurrected",
  );
}

async function concurrentRegistrationUpdatesPreserveBothConversations() {
  const first = workerRegistration(81);
  const second = workerRegistration(82);
  const runtime = makeRuntime({ registrations: [first, second] });

  await Promise.all([
    runtime.api.updateRegistration(first.value.conversationUrl, {
      lastAttemptRevision: 11,
      lastAttemptAt: 111,
    }),
    runtime.api.updateRegistration(second.value.conversationUrl, {
      lastAttemptRevision: 22,
      lastAttemptAt: 222,
    }),
  ]);

  assert.equal(
    runtime.storage.wakeTargets[first.storageKey].lastAttemptRevision,
    11,
    "a concurrent update must not erase the first conversation",
  );
  assert.equal(
    runtime.storage.wakeTargets[second.storageKey].lastAttemptRevision,
    22,
    "a concurrent update must not erase the second conversation",
  );
}

async function userClaimDuringParkingPreventsClose() {
  for (const claim of ["active", "pinned"]) {
    const registration = workerRegistration(91, {
      runtimeOpenedTabId: 91,
      runtimeOpenedAt: 100,
      idleSince: 900000,
    });
    const runtime = makeRuntime({
      registrations: [registration],
      initialTabs: [
        {
          id: 91,
          url: registration.value.conversationUrl,
          active: false,
          pinned: false,
          discarded: false,
        },
      ],
      onCanPark: async ({ tabs }) => {
        // This models a user claim while the content-script safety request is
        // in flight, after the first tab read but before chrome.tabs.remove.
        tabs.get(91)[claim] = true;
      },
    });

    await runtime.api.parkRuntimeOwnedTabWhenSafe(
      registration.value.conversationUrl,
      registration.value,
    );

    assert.equal(
      runtime.removeCalls.length,
      0,
      `a user-${claim} tab must not be closed after an async park check`,
    );
    assert.ok(runtime.getTab(91), "the user-owned tab must remain open");
    assert.equal(
      runtime.storage.wakeTargets[registration.storageKey].runtimeOpenedTabId,
      null,
      "a user claim must release runtime ownership rather than closing the tab",
    );
  }
}

function makeContentRuntime({
  appIdentity = APP_IDENTITY,
  mutateDuringDelay = null,
  sendInitiallyVisible = true,
  sendRequiresNonAppText = false,
  bootstrapSafety = { ok: true, safe: true },
  mutateDuringOwnershipCheck = null,
  inlineSelectedApp = false,
  selectedAppAncestorDepth = 0,
  selectedAppOrigin = "inline_selection_pill",
  selectedAppWrapperPresent = true,
  selectedAppWrapperEditable = false,
  selectedAppDuplicateCount = 1,
  initialComposerText = "",
  mentionCandidates = true,
  pluginPickerFallback = false,
  unifiedComposerPickerFallback = false,
  unifiedComposerCandidateCount = 1,
  pluginMenuCandidates = null,
  pluginMenuDescription = "",
  menuAvailableAfterQueries = 0,
  pluginPickerSelectedText = APP_IDENTITY.appLabel,
  mentionSelectionNoop = false,
  runtimeSendThrows = false,
  syntheticPointerSequenceInvalidatesControl = false,
  menuSelectionRequiresEnter = false,
  menuSelectionConfirmationDelayMs = 0,
  providerControlsVisible = true,
  visibilityState = "hidden",
  documentHasFocus = false,
  generationInProgress = false,
  runtimeOwnedDraftSafety = { ok: false, safe: false },
  finalSendPrepareSafety = {
    ok: true,
    safe: true,
    admissionToken: "final-send-admission",
  },
  finalSendConfirmSafety = { ok: true, safe: true },
  mutateDuringFinalSendConfirmation = null,
  mutateDuringDurableDraftReservation = null,
  mutateDuringStaleWakeCleanup = null,
  runtimeMessageHandler = null,
  conversationUrl = "https://chatgpt.com/c/runtime-harness",
  contentEditableComposer = false,
  controlledEditorDom = false,
  controlledEditorResidualText = null,
  splitControlledEditorResidual = false,
  execCommandEmitsInput = false,
  cancelBeforeInput = false,
  forceExecCommandFalse = false,
  revertExecCommandWithoutInput = false,
  rejectMultilineInsertTextTransaction = false,
  providerShadowText = null,
  providerShadowDelayMs = null,
} = {}) {
  const contentLocation = { href: conversationUrl };
  let currentProviderShadowText = providerShadowText;
  let currentVisibilityState = visibilityState;
  let currentDocumentHasFocus = documentHasFocus;
  let selectedApp = inlineSelectedApp;
  let selectedAppLabel = appIdentity.appLabel;
  let selectedAppRef = appIdentity.appRef;
  let pendingSelectionAt = null;
  let currentGenerationInProgress = generationInProgress;
  let controlledPillWrapper = null;
  let controlledPillAnchor = null;
  let controlledSentinelNode = null;
  let controlledResidualNode = null;
  let lastBeforeInputCanceled = false;
  let execCommandCalls = 0;
  function selectedAppCandidates(selector = 'a[href*="/plugins/plugin_"]') {
    if (!selectedApp) {
      return [];
    }
    if (controlledEditorDom && controlledPillWrapper && controlledPillAnchor) {
      if (selector === "[data-inline-selection-pill]") {
        return controlledPillWrapper.isConnected ? [controlledPillWrapper] : [];
      }
      return controlledPillAnchor.isConnected ? [controlledPillAnchor] : [];
    }
    const anchors = [];
    const wrappers = selectedAppWrapperPresent
      ? Array.from({ length: selectedAppDuplicateCount }, () => ({
          __inComposer: selectedAppAncestorDepth === 0,
          isContentEditable: selectedAppWrapperEditable,
          getAttribute(name) {
            if (name === "data-inline-selection-pill") {
              return "true";
            }
            if (name === "contenteditable") {
              return selectedAppWrapperEditable ? "true" : "false";
            }
            if (name === "data-symbol") {
              return "ecosystemMention";
            }
            return null;
          },
          contains(node) {
            return node?.parentElement === this;
          },
          querySelectorAll(query) {
            return query === "a"
              ? anchors.filter((anchor) => anchor.parentElement === this)
              : [];
          },
        }))
      : [];
    for (let index = 0; index < selectedAppDuplicateCount; index += 1) {
      const wrapper = wrappers[index] || null;
      anchors.push({
        textContent: selectedAppLabel,
        getAttribute(name) {
          return name === "href"
            ? selectedAppRef
              ? `/plugins/${selectedAppRef}${
                  selectedAppOrigin === null
                    ? ""
                    : `?plugin_detail_origin=${selectedAppOrigin}`
                }`
              : null
            : null;
        },
        closest(query) {
          return query === "[data-inline-selection-pill]" ? wrapper : null;
        },
        parentElement: wrapper,
      });
    }
    return selector === "[data-inline-selection-pill]" ? wrappers : anchors;
  }
  class FakeTextArea {
    constructor() {
      this._value = "";
      this.selectionStart = 0;
      this.selectionEnd = 0;
      this.isConnected = true;
      this.listeners = new Map();
    }

    focus() {}

    contains(node) {
      return Boolean(node?.__inComposer);
    }

    get value() {
      return this._value;
    }

    set value(value) {
      this._value = String(value);
      for (const listener of this.listeners.get("input") || []) {
        listener({ type: "input" });
      }
    }

    addEventListener(type, listener) {
      const listeners = this.listeners.get(type) || new Set();
      listeners.add(listener);
      this.listeners.set(type, listeners);
    }

    removeEventListener(type, listener) {
      this.listeners.get(type)?.delete(listener);
    }

    dispatchEvent() {
      return true;
    }

    setSelectionRange(start, end) {
      this.selectionStart = start;
      this.selectionEnd = end;
    }

    setRangeText(text, start, end, selectionMode) {
      this.value = this.value.slice(0, start) + text + this.value.slice(end);
      const cursor = start + text.length;
      if (selectionMode === "end") {
        this.selectionStart = cursor;
        this.selectionEnd = cursor;
      }
    }

    querySelectorAll(selector) {
      if (pendingSelectionAt !== null && contentClock >= pendingSelectionAt) {
        selectedApp = true;
        pendingSelectionAt = null;
      }
      return selectedApp && selectedAppAncestorDepth === 0
        ? selectedAppCandidates(selector)
        : [];
    }
  }

  class FakeContentEditable {
    constructor() {
      this._textContent = "";
      this.isConnected = true;
      this.listeners = new Map();
      this.pendingReactText = null;
      this.pendingReactReason = null;
      this.inputEvents = [];
      this.childNodes = [];
      if (controlledEditorDom) {
        controlledSentinelNode = {
          nodeType: 3,
          data: "\uFEFF",
          get textContent() { return this.data; },
          set textContent(value) { this.data = String(value); },
          parentElement: this,
          isConnected: true,
        };
        controlledPillAnchor = {
          __inComposer: true,
          textContent: appIdentity.appLabel,
          parentElement: null,
          isConnected: true,
          getAttribute(name) {
            return name === "href"
              ? `/plugins/${appIdentity.appRef}?plugin_detail_origin=${selectedAppOrigin}`
              : null;
          },
          closest(query) {
            return query === "[data-inline-selection-pill]"
              ? controlledPillWrapper
              : null;
          },
        };
        controlledPillWrapper = {
          __inComposer: true,
          isContentEditable: false,
          isConnected: true,
          parentElement: this,
          textContent: `${Array.from(appIdentity.appLabel)[0]}${appIdentity.appLabel} `,
          getAttribute(name) {
            if (name === "data-inline-selection-pill") return "true";
            if (name === "contenteditable") return "false";
            if (name === "data-symbol") return "ecosystemMention";
            return null;
          },
          contains(node) { return node === controlledPillAnchor; },
          querySelectorAll(query) {
            return query === "a" ? [controlledPillAnchor] : [];
          },
        };
        controlledPillAnchor.parentElement = controlledPillWrapper;
        controlledResidualNode = {
          nodeType: 3,
          data: String(controlledEditorResidualText ?? initialComposerText),
          get textContent() { return this.data; },
          set textContent(value) { this.data = String(value); },
          parentElement: this,
          isConnected: true,
        };
        this.childNodes = [
          controlledSentinelNode,
          controlledPillWrapper,
          controlledResidualNode,
        ];
        if (splitControlledEditorResidual) {
          const splitAt = Math.max(1, Math.floor(controlledResidualNode.data.length / 2));
          const trailingResidualNode = {
            nodeType: 3,
            data: controlledResidualNode.data.slice(splitAt),
            get textContent() { return this.data; },
            set textContent(value) { this.data = String(value); },
            parentElement: this,
            isConnected: true,
          };
          controlledResidualNode.data = controlledResidualNode.data.slice(0, splitAt);
          this.childNodes.push(trailingResidualNode);
        }
      }
    }

    get textContent() {
      return controlledEditorDom
        ? this.childNodes.map((node) => node.textContent || "").join("")
        : this._textContent;
    }

    set textContent(value) {
      if (!controlledEditorDom) {
        this._textContent = String(value);
        return;
      }
      for (const node of this.childNodes) {
        node.isConnected = false;
        if (node === controlledPillWrapper) {
          controlledPillAnchor.isConnected = false;
        }
      }
      const replacement = {
        nodeType: 3,
        data: String(value),
        get textContent() { return this.data; },
        set textContent(next) { this.data = String(next); },
        parentElement: this,
        isConnected: true,
      };
      this.childNodes = [replacement];
      this._textContent = String(value);
    }

    appendControlledText(value) {
      if (controlledEditorDom && controlledResidualNode?.isConnected) {
        controlledResidualNode.data += String(value);
      } else {
        this.textContent += String(value);
      }
    }

    focus() {}

    contains(node) {
      return Boolean(node?.__inComposer);
    }

    addEventListener(type, listener) {
      const listeners = this.listeners.get(type) || new Set();
      listeners.add(listener);
      this.listeners.set(type, listeners);
    }

    removeEventListener(type, listener) {
      this.listeners.get(type)?.delete(listener);
    }

    dispatchEvent(event) {
      if (event?.type === "beforeinput") {
        this.providerTransactionPending = true;
        lastBeforeInputCanceled = Boolean(cancelBeforeInput);
      }
      if (event?.type === "input") {
        this.inputEvents.push(event);
        if (
          currentProviderShadowText !== null &&
          providerShadowDelayMs === null
        ) {
          currentProviderShadowText = this.textContent;
        }
        this.providerTransactionPending = false;
        if (this.pendingReactReason !== "multiline-transaction") {
          this.pendingReactText = null;
          this.pendingReactReason = null;
        }
      }
      for (const listener of this.listeners.get(event?.type) || []) {
        listener(event);
      }
      return !(event?.type === "beforeinput" && lastBeforeInputCanceled);
    }

    querySelectorAll(selector) {
      return selectedApp && selectedAppAncestorDepth === 0
        ? selectedAppCandidates(selector)
        : [];
    }
  }

  const composer = contentEditableComposer
    ? new FakeContentEditable()
    : new FakeTextArea();
  let scope = composer;
  for (let depth = 1; depth <= selectedAppAncestorDepth; depth += 1) {
    const scopeDepth = depth;
    const parentScope = {
      parentElement: null,
      querySelectorAll() {
        return selectedApp && scopeDepth === selectedAppAncestorDepth
          ? selectedAppCandidates()
          : [];
      },
    };
    scope.parentElement = parentScope;
    scope = parentScope;
  }
  if (contentEditableComposer) {
    if (!controlledEditorDom) {
      composer.textContent = initialComposerText;
    }
  } else {
    composer.value = initialComposerText;
  }
  let currentComposer = composer;
  function currentComposerText() {
    return contentEditableComposer
      ? String(currentComposer.textContent || "")
      : String(currentComposer.value || "");
  }
  function currentComposerContainsOnlySelectedApp() {
    if (!selectedApp) {
      return false;
    }
    const normalize = (value) =>
      String(value || "")
        .replace(/[\u200B-\u200D\u2060\uFEFF]/g, "")
        .replace(/\s+/g, "")
        .toLocaleLowerCase();
    const text = normalize(currentComposerText());
    const label = normalize(selectedAppLabel);
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
  const sendButton = {
    get disabled() {
      return (
        sendRequiresNonAppText && currentComposerContainsOnlySelectedApp()
      );
    },
    clicks: 0,
    getBoundingClientRect() {
      return { width: 1, height: 1 };
    },
    click() {
      this.clicks += 1;
    },
  };
  function syntheticPointerEventInvalidatesControl(event) {
    return (
      syntheticPointerSequenceInvalidatesControl &&
      /^(?:mouse|pointer)/.test(String(event?.type || ""))
    );
  }
  const mention = {
    textContent: appIdentity.appLabel,
    getBoundingClientRect() {
      return providerControlsVisible
        ? { width: 1, height: 1 }
        : { width: 0, height: 0 };
    },
    dispatchEvent(event) {
      if (syntheticPointerEventInvalidatesControl(event)) {
        this.invalidated = true;
      }
      return true;
    },
    click() {
      if (!mentionSelectionNoop && !this.invalidated) {
        selectedApp = true;
      }
    },
  };
  let pluginMenuOpen = false;
  const pluginPicker = {
    textContent: "Plugins",
    getAttribute() {
      return null;
    },
    getBoundingClientRect() {
      return providerControlsVisible
        ? { width: 1, height: 1 }
        : { width: 0, height: 0 };
    },
    dispatchEvent(event) {
      if (syntheticPointerEventInvalidatesControl(event)) {
        this.invalidated = true;
      }
      return true;
    },
    click() {
      if (!this.invalidated) {
        pluginMenuOpen = true;
      }
    },
  };
  const unifiedComposerPicker = {
    textContent: "",
    getAttribute(name) {
      return name === "data-testid" ? "composer-plus-btn" : null;
    },
    getBoundingClientRect() {
      return providerControlsVisible
        ? { width: 1, height: 1 }
        : { width: 0, height: 0 };
    },
    dispatchEvent(event) {
      if (syntheticPointerEventInvalidatesControl(event)) {
        this.invalidated = true;
      }
      return true;
    },
    click() {
      if (!this.invalidated) {
        pluginMenuOpen = true;
      }
    },
  };
  const configuredMenuCandidates = pluginMenuCandidates || [
    {
      appLabel: unifiedComposerPickerFallback
        ? `${appIdentity.appLabel[0]}${appIdentity.appLabel}${appIdentity.appLabel}`
        : appIdentity.appLabel,
      appRef: appIdentity.appRef,
    },
  ];
  function commitPluginMenuSelection(candidate) {
    selectedAppLabel = candidate.appLabel;
    selectedAppRef = candidate.appRef;
    currentComposer.value = pluginPickerSelectedText;
    pluginMenuOpen = false;
    if (menuSelectionConfirmationDelayMs > 0) {
      pendingSelectionAt = contentClock + menuSelectionConfirmationDelayMs;
    } else {
      selectedApp = true;
    }
  }
  const pluginMenuItems = configuredMenuCandidates.map((candidate) => ({
    textContent: `${candidate.appLabel}${pluginMenuDescription}`,
    getAttribute(name) {
      return name === "href" && candidate.appRef
        ? `/plugins/${candidate.appRef}`
        : null;
    },
    querySelector() {
      return null;
    },
    querySelectorAll(selector) {
      return selector === "span"
        ? [{ textContent: candidate.appLabel }]
        : [];
    },
    getBoundingClientRect() {
      return providerControlsVisible
        ? { width: 1, height: 1 }
        : { width: 0, height: 0 };
    },
    focus() {
      this.focused = true;
    },
    dispatchEvent(event) {
      if (syntheticPointerEventInvalidatesControl(event)) {
        this.invalidated = true;
      }
      if (
        menuSelectionRequiresEnter &&
        this.focused &&
        event?.type === "keydown" &&
        event.key === "Enter"
      ) {
        commitPluginMenuSelection(candidate);
        return false;
      }
      return true;
    },
    click() {
      if (this.invalidated || menuSelectionRequiresEnter) {
        return;
      }
      commitPluginMenuSelection(candidate);
    },
  }));
  let sendQueryCount = 0;
  let menuQueryCount = 0;
  let composerContentsSelected = false;
  let controlledResidualSelected = false;
  const stopButton = { disabled: false };
  const document = {
    body: { innerText: "" },
    get visibilityState() {
      return currentVisibilityState;
    },
    hasFocus() {
      return currentDocumentHasFocus;
    },
    addEventListener() {},
    createRange() {
      return {
        selectNodeContents(node) {
          this.selectedNode = node;
          this.collapsed = false;
        },
        collapse() {
          this.collapsed = true;
        },
      };
    },
    execCommand(command, _showUi, text = "") {
      execCommandCalls += 1;
      if (command !== "insertText" && command !== "insertLineBreak") {
        return false;
      }
      if (forceExecCommandFalse || cancelBeforeInput || lastBeforeInputCanceled) {
        lastBeforeInputCanceled = false;
        return false;
      }
      if (contentEditableComposer) {
        const priorText = currentComposer.textContent;
        if (controlledResidualSelected && command === "insertText") {
          controlledResidualNode.data = text;
          controlledResidualSelected = false;
        } else if (composerContentsSelected && command === "insertText") {
          currentComposer.textContent = text;
          composerContentsSelected = false;
        } else {
          currentComposer.appendControlledText(
            command === "insertLineBreak" ? "\n" : text,
          );
        }
        if (revertExecCommandWithoutInput) {
          currentComposer.pendingReactText = priorText;
          currentComposer.pendingReactReason = "missing-input";
        }
        if (
          rejectMultilineInsertTextTransaction &&
          command === "insertText" &&
          text.includes("\n")
        ) {
          currentComposer.pendingReactText = priorText;
          currentComposer.pendingReactReason = "multiline-transaction";
        }
        if (execCommandEmitsInput) {
          currentComposer.dispatchEvent(
            {
              type: "input",
              bubbles: true,
              composed: true,
              inputType: command,
              data: command === "insertLineBreak" ? null : text,
            },
          );
        }
      }
      return true;
    },
    querySelector(selector) {
      if (
        currentGenerationInProgress &&
        (selector === 'button[data-testid="stop-button"]' ||
          selector === 'button[aria-label*="Stop"]')
      ) {
        return stopButton;
      }
      if (
        selector === '#prompt-textarea[contenteditable="true"]' ||
        selector === "textarea#prompt-textarea"
      ) {
        return currentComposer;
      }
      if (selector === 'button[data-testid="composer-plus-btn"]') {
        return unifiedComposerPickerFallback ? unifiedComposerPicker : null;
      }
      if (
        selector === 'button[data-testid="send-button"]' ||
        selector === 'button[aria-label="Send prompt"]'
      ) {
        sendQueryCount += 1;
        return sendInitiallyVisible || sendQueryCount > 2 ? sendButton : null;
      }
      return null;
    },
    querySelectorAll(selector) {
      if (selector === "button") {
        return pluginPickerFallback ? [pluginPicker] : [];
      }
      if (
        (pluginPickerFallback || unifiedComposerPickerFallback) &&
        pluginMenuOpen
      ) {
        menuQueryCount += 1;
        if (menuQueryCount <= menuAvailableAfterQueries) {
          return [];
        }
        return Array.from(
          { length: unifiedComposerCandidateCount },
          (_unused, index) =>
            pluginMenuItems[index % pluginMenuItems.length],
        ).filter(Boolean);
      }
      return mentionCandidates ? [mention] : [];
    },
  };
  let runtimeMessageListener = null;
  const chrome = {
    runtime: {
      getManifest() {
        return { version: "0.1.14" };
      },
      onMessage: {
        addListener(listener) {
          runtimeMessageListener = listener;
        },
      },
      sendMessage(message, callback) {
        if (runtimeSendThrows) {
          throw new Error("Extension context invalidated.");
        }
        if (
          message?.type === "provision:confirmBootstrapSubmit" &&
          mutateDuringOwnershipCheck
        ) {
          mutateDuringOwnershipCheck({
            composer,
            replaceComposer(nextComposer) {
              currentComposer = nextComposer;
            },
            newComposer(text = "") {
              const next = new FakeTextArea();
              next.value = text;
              return next;
            },
          });
        }
        if (
          message?.type === "wake:confirmFinalSend" &&
          mutateDuringFinalSendConfirmation
        ) {
          mutateDuringFinalSendConfirmation({
            composer,
            claimDocument() {
              currentVisibilityState = "visible";
              currentDocumentHasFocus = true;
            },
            navigate(nextUrl) {
              contentLocation.href = nextUrl;
            },
            replaceComposer(nextComposer) {
              currentComposer = nextComposer;
            },
            newComposer(text = "") {
              const next = new FakeTextArea();
              next.value = text;
              return next;
            },
          });
        }
        if (
          message?.type === "runtime:consumeOwnedRegistrationDraft" &&
          mutateDuringDurableDraftReservation
        ) {
          mutateDuringDurableDraftReservation({
            composer,
            claimDocument() {
              currentVisibilityState = "visible";
              currentDocumentHasFocus = true;
            },
            replaceComposer(nextComposer) {
              currentComposer = nextComposer;
            },
            newComposer(text = "") {
              const next = new FakeTextArea();
              next.value = text;
              return next;
            },
            setGenerating(value) {
              currentGenerationInProgress = Boolean(value);
            },
            clearSelectedApp() {
              selectedApp = false;
            },
          });
        }
        if (
          message?.type === "runtime:cleanupStaleWakeDraft" &&
          mutateDuringStaleWakeCleanup
        ) {
          mutateDuringStaleWakeCleanup({
            composer,
            claimDocument() {
              currentVisibilityState = "visible";
              currentDocumentHasFocus = true;
            },
            replaceComposer(nextComposer) {
              currentComposer = nextComposer;
            },
            newComposer(text = "") {
              const next = new FakeTextArea();
              next.value = text;
              return next;
            },
            setGenerating(value) {
              currentGenerationInProgress = Boolean(value);
            },
          });
        }
        if (runtimeMessageHandler) {
          void Promise.resolve(runtimeMessageHandler(message)).then(callback);
          return;
        }
        callback(
          message?.type === "provision:confirmBootstrapSubmit"
            ? bootstrapSafety
            : message?.type === "wake:prepareFinalSend"
              ? finalSendPrepareSafety
            : message?.type === "wake:confirmFinalSend"
              ? finalSendConfirmSafety
            : message?.type === "runtime:consumeOwnedRegistrationDraft"
              ? runtimeOwnedDraftSafety
            : { ok: false },
        );
      },
      lastError: null,
    },
  };
  const contentSource = fs
    .readFileSync(path.join(extensionRoot, "content.js"), "utf8")
    .replace(
      /wakePollTimer = setInterval\(\(\) => \{\n  void pollWake\(\);\n\}, POLL_INTERVAL_MS\);\nvoid pollWake\(\);/,
      "",
    )
    .replace(
      /\n\}\)\(\);\s*$/,
      "\nglobalThis.__contentHarness = { sendAppCommand, bootstrapProvisionedWorker, consumeRegistrationDraft, reserveDurableRuntimeOwnedAppDraft, pollWake };\n})();\n",
    );
  let contentClock = 1000000;
  const context = vm.createContext({
    chrome,
    document,
    location: contentLocation,
    URL,
    window: {
      getSelection() {
        return {
          removeAllRanges() {
            composerContentsSelected = false;
            controlledResidualSelected = false;
          },
          addRange(range) {
            composerContentsSelected =
              range?.selectedNode === currentComposer && !range.collapsed;
            controlledResidualSelected =
              range?.selectedNode === controlledResidualNode && !range.collapsed;
          },
        };
      },
      addEventListener() {},
      removeEventListener() {},
    },
    HTMLTextAreaElement: FakeTextArea,
    InputEvent: class InputEvent {
      constructor(type, init) {
        this.type = type;
        Object.assign(this, init);
      }
    },
    MouseEvent: class MouseEvent {
      constructor(type, init) {
        this.type = type;
        Object.assign(this, init);
      }
    },
    KeyboardEvent: class KeyboardEvent {
      constructor(type, init) {
        this.type = type;
        Object.assign(this, init);
      }
    },
    setTimeout(callback, milliseconds) {
      contentClock += milliseconds;
      if (
        contentEditableComposer &&
        currentComposer.pendingReactText !== null
      ) {
        currentComposer.textContent = currentComposer.pendingReactText;
        currentComposer.pendingReactText = null;
        currentComposer.pendingReactReason = null;
      }
      if (
        contentEditableComposer &&
        currentProviderShadowText !== null &&
        milliseconds === providerShadowDelayMs
      ) {
        currentComposer.textContent = currentProviderShadowText;
      }
      if (mutateDuringDelay) {
        mutateDuringDelay({
          composer,
          milliseconds,
          sendQueryCount,
          claimDocument() {
            currentVisibilityState = "visible";
            currentDocumentHasFocus = true;
          },
          replaceComposer(nextComposer) {
            currentComposer = nextComposer;
          },
          newComposer(text = "") {
            const next = new FakeTextArea();
            next.value = text;
            return next;
          },
        });
      }
      callback();
      return 1;
    },
    clearTimeout() {},
    clearInterval() {},
    Date: { now: () => contentClock },
  });
  vm.runInContext(contentSource, context, { filename: "content.js" });
  return {
    api: context.__contentHarness,
    composer,
    sendButton,
    controlledEditor: {
      get sentinel() { return controlledSentinelNode; },
      get pillWrapper() { return controlledPillWrapper; },
      get pillAnchor() { return controlledPillAnchor; },
      get residual() { return controlledResidualNode; },
      get execCommandCalls() { return execCommandCalls; },
    },
    get currentComposer() {
      return currentComposer;
    },
    replaceCurrentComposer(text = "") {
      const next = new FakeTextArea();
      next.value = text;
      currentComposer = next;
      return next;
    },
    advanceProviderTurn() {
      if (contentEditableComposer && currentProviderShadowText !== null) {
        currentComposer.textContent = currentProviderShadowText;
      }
    },
    dispatchRuntimeMessage(message) {
      let response;
      runtimeMessageListener(message, {}, (value) => {
        response = value;
      });
      return response;
    },
  };
}

async function runtimeOwnedProvisioningDraftCanBeConsumedSafely() {
  const activeFromStart = makeContentRuntime({
    visibilityState: "visible",
    documentHasFocus: true,
  });
  const activeFromStartResult = await activeFromStart.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_active\nwake_revision: 1",
    draftMarker: "wtgt_active",
  });
  assert.equal(activeFromStartResult, false);
  assert.equal(activeFromStart.composer.value, "");
  assert.equal(activeFromStart.sendButton.clicks, 0);

  const activatedWhileComposing = makeContentRuntime({
    mutateDuringDelay({ milliseconds, claimDocument }) {
      if (milliseconds === 200) {
        claimDocument();
      }
    },
  });
  const activatedWhileComposingResult =
    await activatedWhileComposing.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_compose_claim\nwake_revision: 1",
      draftMarker: "wtgt_compose_claim",
    });
  assert.equal(activatedWhileComposingResult, false);
  assert.equal(activatedWhileComposing.composer.value, "");
  assert.equal(activatedWhileComposing.sendButton.clicks, 0);

  const activatedAtFinalConfirmation = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
    mutateDuringFinalSendConfirmation({ claimDocument }) {
      claimDocument();
    },
  });
  const activatedAtFinalResult =
    await activatedAtFinalConfirmation.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_final_claim\nwake_revision: 1",
      draftMarker: "wtgt_final_claim",
    });
  assert.equal(activatedAtFinalResult, false);
  assert.equal(activatedAtFinalConfirmation.composer.value, APP_IDENTITY.appLabel);
  assert.equal(activatedAtFinalConfirmation.sendButton.clicks, 0);

  const owned = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const sent = await owned.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_owned\nwake_revision: 1",
    draftMarker: "wtgt_owned",
  });
  assert.equal(sent, true);
  assert.equal(owned.sendButton.clicks, 1);

  const userOwned = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: false, safe: false },
  });
  const rejected = await userOwned.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_user\nwake_revision: 1",
    draftMarker: "wtgt_user",
  });
  assert.equal(rejected, false);
  assert.equal(userOwned.composer.value, APP_IDENTITY.appLabel);
  assert.equal(userOwned.sendButton.clicks, 0);
}

async function finalSendAdmissionIsOneShotAndRevocable() {
  const registration = workerRegistration(601, { runtimeOpenedTabId: 601 });
  const target = targetForRegistration(registration, "waking", 9);
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    initialTabs: [
      {
        id: 601,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const request = {
    tabId: 601,
    conversationUrl: registration.value.conversationUrl,
    delivery: {
      kind: "worker",
      mode: "wake",
      targetId: registration.value.wakeTargetId,
      targetRevision: 9,
    },
  };
  const prepared = await runtime.api.prepareFinalSendAdmission(request);
  assert.equal(prepared.safe, true);
  runtime.patchTab(601, { active: true });
  const revoked = await runtime.api.confirmFinalSendAdmission({
    ...request,
    admissionToken: prepared.admissionToken,
  });
  assert.equal(revoked.safe, false);
  runtime.patchTab(601, { active: false });
  const replay = await runtime.api.confirmFinalSendAdmission({
    ...request,
    admissionToken: prepared.admissionToken,
  });
  assert.equal(replay.safe, false, "final-send admission must be one-shot");

  const retirementPrepared = await runtime.api.prepareFinalSendAdmission(request);
  assert.equal(retirementPrepared.safe, true);
  runtime.storage.wakeTargets[registration.storageKey].retirementPending = {
    wakeTargetId: registration.value.wakeTargetId,
    conversationId: registration.value.conversationId,
  };
  const retired = await runtime.api.confirmFinalSendAdmission({
    ...request,
    admissionToken: retirementPrepared.admissionToken,
  });
  assert.equal(retired.safe, false, "pending retirement must revoke final send");
  delete runtime.storage.wakeTargets[registration.storageKey].retirementPending;

  const ownershipPrepared = await runtime.api.prepareFinalSendAdmission(request);
  assert.equal(ownershipPrepared.safe, true);
  runtime.storage.wakeTargets[registration.storageKey].runtimeOpenedTabId = null;
  const ownershipLost = await runtime.api.confirmFinalSendAdmission({
    ...request,
    admissionToken: ownershipPrepared.admissionToken,
  });
  assert.equal(ownershipLost.safe, false, "ownership loss must revoke final send");
  runtime.storage.wakeTargets[registration.storageKey].runtimeOpenedTabId = 601;

  const hiddenPrepared = await runtime.api.prepareFinalSendAdmission(request);
  assert.equal(hiddenPrepared.safe, true);
  const hiddenConfirmed = await runtime.api.confirmFinalSendAdmission({
    ...request,
    admissionToken: hiddenPrepared.admissionToken,
  });
  assert.equal(hiddenConfirmed.safe, true);
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].lastAttemptRevision,
    9,
    "a safe final-send response must follow the durable attempt commit",
  );

  // Deterministic post-click barrier: content has synchronously clicked, but
  // its legacy wake:markAttempt message has not run yet. A user activation in
  // this exact interval must observe the committed attempt and clear ownership,
  // never start first-wake retirement.
  runtime.patchTab(601, { active: true });
  await runtime.api.transferRuntimeOwnership(601);
  const claimed = runtime.storage.wakeTargets[registration.storageKey];
  assert.ok(claimed, "post-click activation must preserve the attempted target");
  assert.equal(claimed.runtimeOpenedTabId, null);
  assert.equal(claimed.retirementPending, undefined);
  assert.equal(
    runtime.fetchRequests.some((request) => request.pathname.endsWith("/retire")),
    false,
  );
  await runtime.api.markAttempt({
    conversationUrl: registration.value.conversationUrl,
    targetId: registration.value.wakeTargetId,
    wakeRevision: 9,
  });
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].lastAttemptRevision,
    9,
    "the post-click markAttempt remains replay-safe after ownership transfer",
  );

  // If activation wins content's final synchronous recheck and prevents the
  // click, the conservative committed revision must not poison retirement or a
  // later backend wake revision.
  runtime.storage.wakeTargets[registration.storageKey] = {
    ...runtime.storage.wakeTargets[registration.storageKey],
    runtimeOpenedTabId: 602,
    runtimeOpenedAt: 1000001,
  };
  const retryTarget = targetForRegistration(registration, "waking", 10);
  const retryRuntime = makeRuntime({
    registrations: [],
    storage: runtime.storage,
    targets: [[registration.value.wakeTargetId, retryTarget]],
    initialTabs: [
      {
        id: 602,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const retryRequest = {
    ...request,
    tabId: 602,
    delivery: { ...request.delivery, targetRevision: 10 },
  };
  const retryPrepared = await retryRuntime.api.prepareFinalSendAdmission(retryRequest);
  assert.equal(retryPrepared.safe, true);
  const retryConfirmed = await retryRuntime.api.confirmFinalSendAdmission({
    ...retryRequest,
    admissionToken: retryPrepared.admissionToken,
  });
  assert.equal(retryConfirmed.safe, true);
  assert.equal(
    retryRuntime.storage.wakeTargets[registration.storageKey].lastAttemptRevision,
    10,
  );
  await retryRuntime.api.markAttempt({
    conversationUrl: registration.value.conversationUrl,
    targetId: registration.value.wakeTargetId,
    wakeRevision: 9,
  });
  assert.equal(
    retryRuntime.storage.wakeTargets[registration.storageKey].lastAttemptRevision,
    10,
    "a delayed post-click marker must not regress a newer confirmed revision",
  );
  assert.equal(
    retryRuntime.storage.wakeTargets[registration.storageKey].retirementPending,
    undefined,
  );

  const navigatedContent = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: "",
    mentionCandidates: false,
    mutateDuringFinalSendConfirmation({ navigate }) {
      navigate("https://chatgpt.com/c/different-conversation");
    },
  });
  const navigatedSend = await navigatedContent.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_url_changed\nwake_revision: 1",
    draftMarker: "wtgt_url_changed",
  });
  assert.equal(
    navigatedSend,
    false,
    "a SPA URL change after the last background read must block the click",
  );
  assert.equal(navigatedContent.sendButton.clicks, 0);

  const urlEventRegistration = workerRegistration(603, {
    runtimeOpenedTabId: 603,
  });
  const urlEventTarget = targetForRegistration(
    urlEventRegistration,
    "waking",
    3,
  );
  const urlEventRuntime = makeRuntime({
    registrations: [urlEventRegistration],
    targets: [[urlEventRegistration.value.wakeTargetId, urlEventTarget]],
    initialTabs: [
      {
        id: 603,
        url: urlEventRegistration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const urlEventRequest = {
    tabId: 603,
    conversationUrl: urlEventRegistration.value.conversationUrl,
    delivery: {
      kind: "worker",
      mode: "wake",
      targetId: urlEventRegistration.value.wakeTargetId,
      targetRevision: 3,
    },
  };
  const urlEventPrepared =
    await urlEventRuntime.api.prepareFinalSendAdmission(urlEventRequest);
  urlEventRuntime.dispatchTabUpdated(
    603,
    { url: "https://chatgpt.com/c/different-conversation", status: "loading" },
    { url: "https://chatgpt.com/c/different-conversation" },
  );
  const urlEventConfirmed = await urlEventRuntime.api.confirmFinalSendAdmission({
    ...urlEventRequest,
    admissionToken: urlEventPrepared.admissionToken,
  });
  assert.equal(urlEventConfirmed.safe, false);

  for (const [index, name, changeInfo, tabPatch] of [
    [611, "active", { active: true }, { active: true }],
    [612, "pinned", { pinned: true }, { pinned: true }],
    [
      613,
      "url",
      { url: "https://chatgpt.com/c/navigated", status: "loading" },
      { url: "https://chatgpt.com/c/navigated" },
    ],
  ]) {
    const racedRegistration = workerRegistration(index, {
      runtimeOpenedTabId: index,
    });
    const racedTarget = targetForRegistration(racedRegistration, "waking", 4);
    let targetReadCount = 0;
    let racedRuntime;
    racedRuntime = makeRuntime({
      registrations: [racedRegistration],
      targets: [[racedRegistration.value.wakeTargetId, racedTarget]],
      initialTabs: [
        {
          id: index,
          url: racedRegistration.value.conversationUrl,
          active: false,
          pinned: false,
        },
      ],
      async onTargetRead() {
        targetReadCount += 1;
        if (targetReadCount === 2) {
          racedRuntime.dispatchTabUpdated(index, changeInfo, tabPatch);
          await Promise.resolve();
        }
      },
    });
    const racedRequest = {
      tabId: index,
      conversationUrl: racedRegistration.value.conversationUrl,
      delivery: {
        kind: "worker",
        mode: "wake",
        targetId: racedRegistration.value.wakeTargetId,
        targetRevision: 4,
      },
    };
    const racedPrepared =
      await racedRuntime.api.prepareFinalSendAdmission(racedRequest);
    const racedConfirmed = await racedRuntime.api.confirmFinalSendAdmission({
      ...racedRequest,
      admissionToken: racedPrepared.admissionToken,
    });
    assert.equal(
      racedConfirmed.safe,
      false,
      `${name} during a delayed external read must revoke confirmation`,
    );
    assert.equal(
      racedRuntime.storage.wakeTargets[racedRegistration.storageKey]
        .lastAttemptRevision,
      0,
      name,
    );
  }
}

async function staleWakeAdapterIsReinjectedOnlyIntoOwnedBackgroundTab() {
  const registration = workerRegistration(620, {
    runtimeOpenedTabId: 620,
    runtimeOpenedAt: 100,
  });
  const target = targetForRegistration(registration, "waking", 5);
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    initialTabs: [
      {
        id: 620,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
    wakeAdapterDiagnosis: {
      version: "0.1.13",
      extensionContextAvailable: false,
      pollTimerActive: false,
      everClaimed: false,
      composerPresent: true,
      generating: false,
    },
  });
  const admissionCandidate = candidate(runtime.api, registration, target);
  const located = {
    tab: runtime.getTab(620),
    runtimeOwned: true,
  };
  const repaired =
    await runtime.api.deliverBackendAuthorizedRuntimeAdmission(admissionCandidate);
  assert.equal(repaired.admitted, true);
  assert.equal(runtime.scriptExecutionCalls.length, 2);
  assert.deepEqual(runtime.scriptExecutionCalls[1].files, ["content.js"]);
  const alreadyCurrent = await runtime.api.ensureCurrentWakeAdapter(
    registration.value.conversationUrl,
    registration.value,
    admissionCandidate,
    located,
  );
  assert.equal(alreadyCurrent, true);
  assert.equal(
    runtime.scriptExecutionCalls.length,
    2,
    "a current adapter with one active poll timer must not be reinjected",
  );

  for (const [name, runtimeOwned, tabPatch] of [
    ["active", true, { active: true }],
    ["pinned", true, { pinned: true }],
    ["user-owned", false, {}],
    [
      "mismatched conversation",
      true,
      { url: "https://chatgpt.com/c/different-conversation" },
    ],
  ]) {
    const before = runtime.scriptExecutionCalls.length;
    const safe = await runtime.api.ensureCurrentWakeAdapter(
      registration.value.conversationUrl,
      registration.value,
      admissionCandidate,
      {
        tab: { ...runtime.getTab(620), active: false, pinned: false, ...tabPatch },
        runtimeOwned,
      },
    );
    assert.equal(safe, false, name);
    assert.equal(runtime.scriptExecutionCalls.length, before, name);
  }

  const claimedDiagnosisRuntime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    initialTabs: [located.tab],
    wakeAdapterDiagnosis: {
      version: "0.1.14",
      extensionContextAvailable: true,
      pollTimerActive: true,
      everClaimed: true,
      composerPresent: true,
      generating: false,
    },
  });
  const claimedSafe = await claimedDiagnosisRuntime.api.ensureCurrentWakeAdapter(
    registration.value.conversationUrl,
    registration.value,
    candidate(claimedDiagnosisRuntime.api, registration, target),
    located,
  );
  assert.equal(claimedSafe, false);
  assert.equal(claimedDiagnosisRuntime.scriptExecutionCalls.length, 0);

  for (const mutation of ["active", "rebind", "throw"]) {
    let racedRuntime;
    racedRuntime = makeRuntime({
      registrations: [registration],
      targets: [[registration.value.wakeTargetId, target]],
      initialTabs: [located.tab],
      wakeAdapterDiagnosis: null,
      async onExecuteScript({ details, storage, tabs }) {
        if (mutation === "throw") {
          throw new Error("script injection failed");
        }
        if (!details.files?.includes("content.js")) {
          return;
        }
        if (mutation === "active") {
          tabs.get(620).active = true;
        } else {
          storage.wakeTargets[registration.storageKey] = {
            ...storage.wakeTargets[registration.storageKey],
            wakeTargetId: "wtgt_rebound_during_injection",
          };
        }
      },
    });
    const racedOutcome =
      await racedRuntime.api.deliverBackendAuthorizedRuntimeAdmission(
        candidate(racedRuntime.api, registration, target),
      );
    assert.notEqual(racedOutcome.admitted, true, mutation);
    assert.equal(
      racedRuntime.storage.wakeTargets[registration.storageKey]
        .lastAdmittedWakeRevision,
      0,
      mutation,
    );
  }

  let retryAttempts = 0;
  let retryRuntime;
  retryRuntime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    initialTabs: [located.tab],
    wakeAdapterDiagnosis: null,
    async onExecuteScript() {
      retryAttempts += 1;
      if (retryAttempts === 1) {
        throw new Error("first injection failed");
      }
    },
  });
  const retryCandidate = candidate(retryRuntime.api, registration, target);
  const firstRetryOutcome =
    await retryRuntime.api.deliverBackendAuthorizedRuntimeAdmission(
      retryCandidate,
    );
  assert.notEqual(firstRetryOutcome.admitted, true);
  const secondRetryOutcome =
    await retryRuntime.api.deliverBackendAuthorizedRuntimeAdmission(
      retryCandidate,
    );
  assert.equal(secondRetryOutcome.admitted, true);
  assert.equal(
    retryRuntime.scriptExecutionCalls.filter((call) =>
      call.files?.includes("content.js"),
    ).length,
    1,
    "retry may install only one current content adapter poll loop",
  );
}

function contentAdapterReplacementSurvivesThrowingOldCleanup() {
  const source = fs
    .readFileSync(path.join(extensionRoot, "content.js"), "utf8")
    .replace(
      /wakePollTimer = setInterval\(\(\) => \{\n  void pollWake\(\);\n\}, POLL_INTERVAL_MS\);/,
      "wakePollTimer = setInterval(() => {}, POLL_INTERVAL_MS);",
    )
    .replace("\nvoid pollWake();", "");
  const runtimeListeners = new Set();
  const intervals = new Set();
  let nextIntervalId = 1;
  const context = vm.createContext({
    chrome: {
      runtime: {
        getManifest() {
          return { version: "0.1.14" };
        },
        onMessage: {
          addListener(listener) {
            runtimeListeners.add(listener);
          },
          removeListener(listener) {
            if (listener?.oldInvalidated) {
              throw new Error("invalidated context");
            }
            runtimeListeners.delete(listener);
          },
        },
      },
    },
    document: {
      visibilityState: "hidden",
      hasFocus() {
        return false;
      },
      addEventListener() {},
      removeEventListener() {},
      querySelector() {
        return null;
      },
      querySelectorAll() {
        return [];
      },
      body: { innerText: "" },
    },
    window: {
      addEventListener() {},
      removeEventListener() {},
      getSelection() {},
    },
    location: { href: "https://chatgpt.com/c/adapter-lifecycle" },
    URL,
    setInterval(callback) {
      const id = nextIntervalId;
      nextIntervalId += 1;
      intervals.add(id);
      return id;
    },
    clearInterval(id) {
      intervals.delete(id);
    },
    setTimeout(callback) {
      callback();
      return 1;
    },
    clearTimeout() {},
  });
  const oldListener = () => {};
  oldListener.oldInvalidated = true;
  runtimeListeners.add(oldListener);
  context.__chatToCodexWakeAdapter = {
    version: "0.1.13",
    disposed: false,
    dispose() {
      context.chrome.runtime.onMessage.removeListener(oldListener);
    },
  };

  vm.runInContext(source, context, { filename: "content.js:first-reinject" });
  assert.equal(context.__chatToCodexWakeAdapter.version, "0.1.14");
  assert.equal(context.__chatToCodexWakeAdapter.disposed, false);
  assert.equal(runtimeListeners.size, 2);
  assert.equal(intervals.size, 1);
  const currentAdapter = context.__chatToCodexWakeAdapter;

  vm.runInContext(source, context, { filename: "content.js:same-version" });
  assert.equal(context.__chatToCodexWakeAdapter, currentAdapter);
  assert.equal(runtimeListeners.size, 2);
  assert.equal(intervals.size, 1);
}

async function providerNormalizedRegistrationDraftRemainsRuntimeOwned() {
  const liveAppIdentity = {
    appRef: "plugin_chat_orchestration_live",
    appLabel: "ChatOrchestration",
  };
  const providerDraftWithoutComposerPill = makeContentRuntime({
    appIdentity: liveAppIdentity,
    inlineSelectedApp: false,
    initialComposerText: "\uFEFFCChatOrchestration ",
    mentionCandidates: true,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const firstWakeSent =
    await providerDraftWithoutComposerPill.api.sendAppCommand({
      appIdentity: liveAppIdentity,
      command:
        " 작업해\nwake_target_id: wtgt_provider_no_pill\nwake_revision: 1",
      draftMarker: "wtgt_provider_no_pill",
    });
  assert.equal(
    firstWakeSent,
    true,
    "a durable provider app-only draft must be reselected when bootstrap leaves no composer pill",
  );
  assert.equal(providerDraftWithoutComposerPill.sendButton.clicks, 1);

  const arbitraryDraftWithoutPill = makeContentRuntime({
    appIdentity: liveAppIdentity,
    inlineSelectedApp: false,
    initialComposerText: "human draft",
    mentionCandidates: true,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const arbitrarySent = await arbitraryDraftWithoutPill.api.sendAppCommand({
    appIdentity: liveAppIdentity,
    command: " 작업해\nwake_target_id: wtgt_human\nwake_revision: 1",
    draftMarker: "wtgt_human",
  });
  assert.equal(arbitrarySent, false);
  assert.equal(arbitraryDraftWithoutPill.composer.value, "human draft");
  assert.equal(arbitraryDraftWithoutPill.sendButton.clicks, 0);

  const claimedProviderDraftWithoutPill = makeContentRuntime({
    appIdentity: liveAppIdentity,
    inlineSelectedApp: false,
    initialComposerText: "\uFEFFCChatOrchestration ",
    mentionCandidates: true,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
    visibilityState: "visible",
    documentHasFocus: true,
  });
  const claimedSent =
    await claimedProviderDraftWithoutPill.api.sendAppCommand({
      appIdentity: liveAppIdentity,
      command: " 작업해\nwake_target_id: wtgt_claimed\nwake_revision: 1",
      draftMarker: "wtgt_claimed",
    });
  assert.equal(claimedSent, false);
  assert.equal(
    claimedProviderDraftWithoutPill.composer.value,
    "\uFEFFCChatOrchestration ",
  );
  assert.equal(claimedProviderDraftWithoutPill.sendButton.clicks, 0);

  const providerClearedComposer = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: "",
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const emptyDraftSent = await providerClearedComposer.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_empty\nwake_revision: 1",
    draftMarker: "wtgt_empty",
  });
  assert.equal(
    emptyDraftSent,
    true,
    "a provider-cleared composer with the exact selected app must consume the durable capability",
  );
  assert.equal(providerClearedComposer.sendButton.clicks, 1);

  const registration = workerRegistration(508, {
    appIdentity: {
      appRef: APP_IDENTITY.appRef,
      appLabel: "ChatOrchestrationLive",
    },
    runtimeOpenedTabId: 508,
    runtimeOpenedAt: 100,
    runtimeOwnedRegistrationDraft: {
      tabId: 508,
      conversationId: "worker-508",
      draft: "CChatOrchestrationLive",
      appRef: APP_IDENTITY.appRef,
      appLabel: "ChatOrchestrationLive",
    },
  });
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [],
    initialTabs: [
      {
        id: 508,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
        discarded: false,
      },
    ],
  });

  const providerNormalized =
    await runtime.api.consumeRuntimeOwnedRegistrationDraft({
      tabId: 508,
      conversationUrl: registration.value.conversationUrl,
      appIdentity: registration.value.appIdentity,
      draft: "\uFEFFCChatOrchestrationLive ",
    });

  assert.equal(
    providerNormalized.safe,
    false,
    "provider normalization alone must not bypass final-send token and delivery binding",
  );

  const humanDraftRegistration = workerRegistration(509, {
    appIdentity: clone(registration.value.appIdentity),
    runtimeOpenedTabId: 509,
    runtimeOpenedAt: 100,
    runtimeOwnedRegistrationDraft: {
      ...clone(registration.value.runtimeOwnedRegistrationDraft),
      tabId: 509,
      conversationId: "worker-509",
    },
  });
  const humanDraftRuntime = makeRuntime({
    registrations: [humanDraftRegistration],
    targets: [],
    initialTabs: [
      {
        id: 509,
        url: humanDraftRegistration.value.conversationUrl,
        active: false,
        pinned: false,
        discarded: false,
      },
    ],
  });
  const humanDraft =
    await humanDraftRuntime.api.consumeRuntimeOwnedRegistrationDraft({
      tabId: 509,
      conversationUrl: humanDraftRegistration.value.conversationUrl,
      appIdentity: humanDraftRegistration.value.appIdentity,
      draft: "\uFEFFCChatOrchestrationLive human text",
    });
  assert.equal(
    humanDraft.safe,
    false,
    "semantic provider normalization must not authorize appended human text",
  );
}

async function contentEditableInsertionCommitsProviderState() {
  const command =
    " 작업해\nwake_target_id: wtgt_react_reconcile\nwake_revision: 18";
  const reconciled = makeContentRuntime({
    contentEditableComposer: true,
    rejectMultilineInsertTextTransaction: true,
    execCommandEmitsInput: true,
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });

  const sent = await reconciled.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command,
    draftMarker: "wtgt_react_reconcile",
  });

  assert.equal(sent, true);
  assert.equal(reconciled.sendButton.clicks, 1);
  assert.equal(reconciled.composer.textContent, APP_IDENTITY.appLabel + command);
  assert.equal(
    reconciled.composer.inputEvents.length,
    command.split("\n").length * 2 - 1,
    "native execCommand input events must not be duplicated",
  );
  assert.ok(
    reconciled.composer.inputEvents.every(
      (event) => event.bubbles === true && event.composed === true,
    ),
  );
  assert.equal(
    reconciled.composer.inputEvents.some(
      (event) => event.inputType === "insertText" && event.data.includes("\n"),
    ),
    false,
  );
  assert.equal(
    reconciled.composer.inputEvents.filter(
      (event) => event.inputType === "insertLineBreak" && event.data === null,
    ).length,
    command.split("\n").length - 1,
  );

  const humanEdited = makeContentRuntime({
    contentEditableComposer: true,
    rejectMultilineInsertTextTransaction: true,
    execCommandEmitsInput: true,
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
    mutateDuringDelay({ composer, milliseconds }) {
      if (milliseconds === 50) {
        composer.textContent += " human edit";
      }
    },
  });
  const humanEditedSent = await humanEdited.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command,
    draftMarker: "wtgt_react_reconcile",
  });
  assert.equal(humanEditedSent, false);
  assert.equal(humanEdited.sendButton.clicks, 0);
  assert.ok(humanEdited.composer.textContent.endsWith(" human edit"));

  const missingNativeInput = makeContentRuntime({
    contentEditableComposer: true,
    revertExecCommandWithoutInput: true,
    execCommandEmitsInput: false,
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const missingNativeInputSent = await missingNativeInput.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command,
    draftMarker: "wtgt_react_reconcile",
  });
  assert.equal(
    missingNativeInputSent,
    true,
    "contenteditable insertion must synthesize input when execCommand mutates the DOM without emitting it",
  );
  assert.equal(missingNativeInput.sendButton.clicks, 1);
  assert.equal(
    missingNativeInput.composer.textContent,
    APP_IDENTITY.appLabel + command,
  );
  assert.equal(
    missingNativeInput.composer.inputEvents.length,
    command.split("\n").length * 2 - 1,
  );
}

async function providerShadowReconciliationCannotStrandCurrentWakeAcrossPolls() {
  const fixture = staleWakeCleanupFixture(699, { targetRevision: 1 });
  const registrationDraft = "\uFEFF\nC\nChatOrchestration\n ";
  fixture.runtime.storage.wakeTargets[
    fixture.registration.storageKey
  ].runtimeOwnedRegistrationDraft.draft = registrationDraft;
  const command = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const providerSerializedWake = `\uFEFF${fixture.appIdentity.appLabel[0]}${fixture.appIdentity.appLabel} ${command.replace(/\n/g, " ")}`;
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: registrationDraft,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    providerShadowText: providerSerializedWake,
    providerShadowDelayMs: 50,
    runtimeMessageHandler: fixture.routeRuntimeMessage,
  });

  const firstSent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command,
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });
  const firstFailure = clone(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .lastWakeSendFailure,
  );
  const secondSent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command,
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });
  const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
  const secondFailure = clone(stored.lastWakeSendFailure);

  assert.deepEqual(
    {
      firstSent,
      secondSent,
      clicks: content.sendButton.clicks,
      lastAttemptRevision: stored.lastAttemptRevision,
      firstFailure: {
        reason: firstFailure?.reason,
        rejectionStage: firstFailure?.rejectionStage,
        rejectionSubstage: firstFailure?.rejectionSubstage,
      },
      secondFailure: {
        reason: secondFailure?.reason,
        rejectionStage: secondFailure?.rejectionStage,
        rejectionSubstage: secondFailure?.rejectionSubstage,
      },
    },
    {
      firstSent: true,
      secondSent: false,
      clicks: 1,
      lastAttemptRevision: 1,
      firstFailure: {
        reason: undefined,
        rejectionStage: undefined,
        rejectionSubstage: undefined,
      },
      secondFailure: {
        reason: undefined,
        rejectionStage: undefined,
        rejectionSubstage: undefined,
      },
    },
    "provider reconciliation must not produce pre_confirm_ownership_rejected followed by pre_candidate/provider_draft_mismatch with lastAttemptRevision=0",
  );
}

async function durableDraftReserveCommitSurvivesLocalFailureAndRestart() {
  // This live-shaped serialization is coverage for the two-phase protocol, not
  // evidence that whitespace normalization caused the observed browser abort.
  const multilineProviderDraft = "\uFEFF\nC\nChatOrchestration\n ";
  const registration = runtimeOwnedWorker(540, {
    appIdentity: {
      appRef: APP_IDENTITY.appRef,
      appLabel: "ChatOrchestration",
    },
    bindingRevision: 2,
    connectorEpoch: 2,
  });
  registration.value.conversationId = "6a7d9f1e-b6ec-83e8-9c90-fde23014cce9";
  registration.value.conversationUrl =
    "https://chatgpt.com/c/6a7d9f1e-b6ec-83e8-9c90-fde23014cce9";
  registration.storageKey = `conversation:${registration.value.conversationId}`;
  registration.value.runtimeOwnedRegistrationDraft.conversationId =
    registration.value.conversationId;
  registration.value.runtimeOwnedRegistrationDraft.draft = multilineProviderDraft;
  registration.value.runtimeOwnedRegistrationDraft.appLabel = "ChatOrchestration";
  registration.value.provisionedConnectorProof =
    provisionedConnectorProofFor(registration, { appLabel: "ChatOrchestration" });
  const target = targetForRegistration(registration, "waking", 1);
  const binding = projectBinding(registration.value.projectId, {
    appRef: APP_IDENTITY.appRef,
    appLabel: "ChatOrchestration",
  });
  const initialRuntime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    projectBindings: { [registration.value.projectId]: binding },
    initialTabs: [
      {
        id: registration.value.runtimeOpenedTabId,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const delivery = {
    kind: "worker",
    mode: "wake",
    targetId: registration.value.wakeTargetId,
    targetRevision: 1,
  };
  async function routeRuntimeMessage(runtime, message) {
    if (message.type === "wake:prepareFinalSend") {
      const prepared = await runtime.api.prepareFinalSendAdmission({
        tabId: registration.value.runtimeOpenedTabId,
        conversationUrl: message.conversationUrl,
        delivery: message.delivery,
      });
      return { ok: true, ...prepared };
    }
    if (message.type === "runtime:consumeOwnedRegistrationDraft") {
      const reserved = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
        tabId: registration.value.runtimeOpenedTabId,
        conversationUrl: message.conversationUrl,
        appIdentity: message.appIdentity,
        draft: message.draft,
        admissionToken: message.admissionToken,
        delivery: message.delivery,
      });
      return { ok: true, ...reserved };
    }
    if (message.type === "wake:confirmFinalSend") {
      const confirmed = await runtime.api.confirmFinalSendAdmission({
        tabId: registration.value.runtimeOpenedTabId,
        conversationUrl: message.conversationUrl,
        admissionToken: message.admissionToken,
        delivery: message.delivery,
      });
      return { ok: true, ...confirmed };
    }
    if (message.type === "wake:recordSendFailure") {
      const expectedKeys = [
        "conversationUrl",
        "delivery",
        "reason",
        "type",
      ];
      if (message.rejectionStage !== undefined) {
        expectedKeys.push("rejectionStage");
      }
      if (message.rejectionSubstage !== undefined) {
        expectedKeys.push("rejectionSubstage");
      }
      assert.deepEqual(Object.keys(message).sort(), expectedKeys.sort());
      const recorded = await runtime.api.recordWakeSendFailure({
        tabId: registration.value.runtimeOpenedTabId,
        conversationUrl: message.conversationUrl,
        delivery: message.delivery,
        reason: message.reason,
        rejectionStage: message.rejectionStage,
        rejectionSubstage: message.rejectionSubstage,
      });
      return { ok: true, ...recorded };
    }
    return { ok: false };
  }
  const failedContent = makeContentRuntime({
    appIdentity: registration.value.appIdentity,
    inlineSelectedApp: true,
    initialComposerText: multilineProviderDraft,
    mentionCandidates: false,
    mutateDuringDelay({ milliseconds, composer }) {
      if (milliseconds === 200) {
        composer.value += " human edit";
      }
    },
    conversationUrl: registration.value.conversationUrl,
    runtimeMessageHandler: (message) => routeRuntimeMessage(initialRuntime, message),
  });
  const failed = await failedContent.api.sendAppCommand({
    appIdentity: registration.value.appIdentity,
    command: ` 작업해\nwake_target_id: ${registration.value.wakeTargetId}\nwake_revision: 1`,
    draftMarker: registration.value.wakeTargetId,
    delivery,
  });
  assert.equal(failed, false);
  assert.ok(
    initialRuntime.storage.wakeTargets[registration.storageKey]
      .runtimeOwnedRegistrationDraft,
    "a local failure after reserve must retain the durable capability",
  );
  assert.equal(
    initialRuntime.storage.wakeTargets[registration.storageKey]
      .runtimeOwnedRegistrationDraftConsumedAt,
    undefined,
  );
  assert.equal(
    initialRuntime.storage.wakeTargets[registration.storageKey]
      .lastAttemptRevision,
    0,
  );
  assert.equal(
    initialRuntime.storage.wakeTargets[registration.storageKey]
      .lastWakeSendFailure.reason,
    "post_200_ownership_rejected",
    "the exact local phase must be recorded without composer text",
  );
  assert.deepEqual(
    Object.keys(
      initialRuntime.storage.wakeTargets[registration.storageKey]
        .lastWakeSendFailure,
    ).sort(),
    [
      "conversationUrl",
      "kind",
      "mode",
      "reason",
      "recordedAt",
      "targetId",
      "targetRevision",
    ],
  );
  const overwritten = await initialRuntime.api.recordWakeSendFailure({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery,
    reason: "composer_missing",
  });
  assert.equal(overwritten.safe, true);
  assert.equal(
    initialRuntime.storage.wakeTargets[registration.storageKey]
      .lastWakeSendFailure.reason,
    "composer_missing",
    "only the newest bounded failure may remain",
  );
  const forged = await initialRuntime.api.recordWakeSendFailure({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery: { ...delivery, targetId: "wtgt_forged" },
    reason: "prepare_rejected",
  });
  assert.equal(forged.safe, false);
  const unrelated = await initialRuntime.api.recordWakeSendFailure({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: "https://chatgpt.com/c/unrelated-conversation",
    delivery,
    reason: "prepare_rejected",
  });
  assert.equal(unrelated.safe, false);
  const unknownReason = await initialRuntime.api.recordWakeSendFailure({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery,
    reason: "draft_contents_forbidden",
  });
  assert.equal(unknownReason.safe, false);

  const restartedRuntime = makeRuntime({
    registrations: [],
    storage: initialRuntime.storage,
    targets: [[registration.value.wakeTargetId, target]],
    projectBindings: { [registration.value.projectId]: binding },
    initialTabs: [
      {
        id: registration.value.runtimeOpenedTabId,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const successfulContent = makeContentRuntime({
    appIdentity: registration.value.appIdentity,
    inlineSelectedApp: true,
    // ChatGPT may preserve the exact selected app pill while normalizing the
    // provider-owned composer serialization between delivery attempts.
    initialComposerText: registration.value.appIdentity.appLabel[0],
    mentionCandidates: false,
    conversationUrl: registration.value.conversationUrl,
    runtimeMessageHandler: (message) =>
      routeRuntimeMessage(restartedRuntime, message),
  });
  const succeeded = await successfulContent.api.sendAppCommand({
    appIdentity: registration.value.appIdentity,
    command: ` 작업해\nwake_target_id: ${registration.value.wakeTargetId}\nwake_revision: 1`,
    draftMarker: registration.value.wakeTargetId,
    delivery,
  });
  assert.equal(succeeded, true);
  const committed = restartedRuntime.storage.wakeTargets[registration.storageKey];
  assert.equal(committed.runtimeOwnedRegistrationDraft, null);
  assert.ok(committed.runtimeOwnedRegistrationDraftConsumedAt > 0);
  assert.equal(committed.lastAttemptRevision, 1);
  assert.equal(committed.lastWakeSendFailure, null);
  assert.equal(successfulContent.sendButton.clicks, 1);

  const stranded = runtimeOwnedWorker(541, {
    appIdentity: clone(APP_IDENTITY),
    bindingRevision: 2,
    connectorEpoch: 2,
    runtimeOwnedRegistrationDraft: null,
    runtimeOwnedRegistrationDraftConsumedAt: 999,
    lastAttemptRevision: 0,
  });
  stranded.value.runtimeOwnedRegistrationDraft = null;
  stranded.value.provisionedConnectorProof = provisionedConnectorProofFor(stranded);
  const strandedTarget = targetForRegistration(stranded, "waking", 2);
  const strandedBinding = projectBinding(stranded.value.projectId, {
    appRef: APP_IDENTITY.appRef,
    appLabel: APP_IDENTITY.appLabel,
  });
  const strandedRuntime = makeRuntime({
    registrations: [stranded],
    targets: [[stranded.value.wakeTargetId, strandedTarget]],
    projectBindings: { [stranded.value.projectId]: strandedBinding },
    initialTabs: [
      {
        id: stranded.value.runtimeOpenedTabId,
        url: stranded.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const strandedDelivery = {
    kind: "worker",
    mode: "wake",
    targetId: stranded.value.wakeTargetId,
    targetRevision: 2,
  };
  const strandedPrepared = await strandedRuntime.api.prepareFinalSendAdmission({
    tabId: stranded.value.runtimeOpenedTabId,
    conversationUrl: stranded.value.conversationUrl,
    delivery: strandedDelivery,
  });
  const strandedReserveRequest = {
    tabId: stranded.value.runtimeOpenedTabId,
    conversationUrl: stranded.value.conversationUrl,
    appIdentity: APP_IDENTITY,
    // A legacy first-wake failure may leave only the provider icon text while
    // the exact app pill remains selected in the hidden runtime-owned tab.
    draft: APP_IDENTITY.appLabel[0],
    admissionToken: strandedPrepared.admissionToken,
    delivery: strandedDelivery,
  };
  const strandedReserves = await Promise.all([
    strandedRuntime.api.consumeRuntimeOwnedRegistrationDraft(
      strandedReserveRequest,
    ),
    strandedRuntime.api.consumeRuntimeOwnedRegistrationDraft(
      strandedReserveRequest,
    ),
  ]);
  assert.equal(strandedReserves.filter((result) => result.safe).length, 1);
  const confirmations = await Promise.all([
    strandedRuntime.api.confirmFinalSendAdmission({
      tabId: stranded.value.runtimeOpenedTabId,
      conversationUrl: stranded.value.conversationUrl,
      admissionToken: strandedPrepared.admissionToken,
      delivery: strandedDelivery,
    }),
    strandedRuntime.api.confirmFinalSendAdmission({
      tabId: stranded.value.runtimeOpenedTabId,
      conversationUrl: stranded.value.conversationUrl,
      admissionToken: strandedPrepared.admissionToken,
      delivery: strandedDelivery,
    }),
  ]);
  assert.equal(confirmations.filter((result) => result.safe).length, 1);
  assert.equal(
    strandedRuntime.storage.wakeTargets[stranded.storageKey].lastAttemptRevision,
    2,
  );
}

async function durableDraftReservationFailsClosed() {
  for (const fixture of [
    {
      name: "human edit",
      draft: "human draft",
      expectedStage: "background_draft_capability",
    },
    {
      name: "corrupted stored capability",
      storedDraft: "human draft",
      expectedStage: "background_draft_capability",
    },
    {
      name: "active tab",
      tabPatchAfterPrepare: { active: true },
      expectedStage: "background_admission",
    },
    {
      name: "pinned tab",
      tabPatchAfterPrepare: { pinned: true },
      expectedStage: "background_admission",
    },
    {
      name: "binding mismatch",
      bindingPatch: { revision: 3 },
      expectedStage: "background_proof",
    },
    {
      name: "previous attempt",
      registrationPatch: { lastAttemptRevision: 1 },
      expectedStage: "background_registration",
    },
    {
      name: "missing proof",
      removeProof: true,
      expectedStage: "background_proof",
    },
  ]) {
    const registration = runtimeOwnedWorker(550, {
      appIdentity: clone(APP_IDENTITY),
      bindingRevision: 2,
      connectorEpoch: 2,
      ...(fixture.registrationPatch || {}),
    });
    registration.value.runtimeOwnedRegistrationDraft.draft =
      fixture.storedDraft || APP_IDENTITY.appLabel;
    registration.value.provisionedConnectorProof =
      provisionedConnectorProofFor(registration);
    if (fixture.removeProof) {
      delete registration.value.provisionedConnectorProof;
    }
    const target = targetForRegistration(registration, "waking", 2);
    const delivery = {
      kind: "worker",
      mode: "wake",
      targetId: registration.value.wakeTargetId,
      targetRevision: 2,
    };
    const runtime = makeRuntime({
      registrations: [registration],
      targets: [[registration.value.wakeTargetId, target]],
      projectBindings: {
        [registration.value.projectId]: projectBinding(
          registration.value.projectId,
          {
            appRef: APP_IDENTITY.appRef,
            appLabel: APP_IDENTITY.appLabel,
            ...(fixture.bindingPatch || {}),
          },
        ),
      },
      initialTabs: [
        {
          id: registration.value.runtimeOpenedTabId,
          url: registration.value.conversationUrl,
          active: false,
          pinned: false,
        },
      ],
    });
    const prepared = await runtime.api.prepareFinalSendAdmission({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery,
    });
    if (fixture.tabPatchAfterPrepare) {
      runtime.patchTab(
        registration.value.runtimeOpenedTabId,
        fixture.tabPatchAfterPrepare,
      );
    }
    const reserved = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      appIdentity: APP_IDENTITY,
      draft: fixture.draft || APP_IDENTITY.appLabel,
      admissionToken: prepared.admissionToken,
      delivery,
    });
    assert.equal(reserved.safe, false, fixture.name);
    assert.equal(
      reserved.rejectionStage,
      fixture.expectedStage,
      fixture.name,
    );
    assert.ok(
      runtime.storage.wakeTargets[registration.storageKey]
        .runtimeOwnedRegistrationDraft,
      fixture.name,
    );
  }
}

async function durableReserveRejectionStageTelemetryIsBounded() {
  const delivery = {
    kind: "worker",
    mode: "wake",
    targetId: "wtgt_stage",
    targetRevision: 1,
  };

  async function contentFailureStage({
    initialComposerText = APP_IDENTITY.appLabel,
    runtimeOwnedDraftSafety = { ok: true, safe: true },
    mutateDuringDurableDraftReservation = null,
  } = {}) {
    const failures = [];
    const runtime = makeContentRuntime({
      inlineSelectedApp: true,
      initialComposerText,
      mentionCandidates: false,
      runtimeOwnedDraftSafety,
      mutateDuringDurableDraftReservation,
      runtimeMessageHandler(message) {
        if (message.type === "wake:prepareFinalSend") {
          return {
            ok: true,
            safe: true,
            admissionToken: "stage-admission",
          };
        }
        if (message.type === "runtime:consumeOwnedRegistrationDraft") {
          return runtimeOwnedDraftSafety;
        }
        if (message.type === "wake:recordSendFailure") {
          failures.push(clone(message));
          return { ok: true, safe: true };
        }
        return { ok: false };
      },
    });
    const sent = await runtime.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_stage\nwake_revision: 1",
      draftMarker: "wtgt_stage",
      delivery,
    });
    assert.equal(sent, false);
    assert.equal(failures.length, 1);
    const failure = failures[0];
    assert.equal(failure.reason, "durable_reserve_rejected");
    const expectedFailureKeys = [
      "conversationUrl",
      "delivery",
      "reason",
      "rejectionStage",
      "type",
    ];
    if (failure.rejectionSubstage !== undefined) {
      expectedFailureKeys.push("rejectionSubstage");
    }
    assert.deepEqual(Object.keys(failure).sort(), expectedFailureKeys.sort());
    if (initialComposerText) {
      assert.equal(JSON.stringify(failure).includes(initialComposerText), false);
    }
    return failure.rejectionStage;
  }

  assert.equal(
    await contentFailureStage({ initialComposerText: "human draft" }),
    "pre_candidate",
  );
  for (const stage of [
    "background_admission",
    "background_identity",
    "background_registration",
    "background_draft_capability",
    "background_tab",
    "background_proof",
    "background_snapshot",
  ]) {
    assert.equal(
      await contentFailureStage({
        runtimeOwnedDraftSafety: {
          ok: true,
          safe: false,
          rejectionStage: stage,
        },
      }),
      stage,
    );
  }
  for (const [stage, mutate] of [
    ["post_unclaimed", ({ claimDocument }) => claimDocument()],
    [
      "post_composer",
      ({ replaceComposer, newComposer }) =>
        replaceComposer(newComposer(APP_IDENTITY.appLabel)),
    ],
    ["post_raw", ({ composer }) => { composer.value += " changed"; }],
    ["post_generating", ({ setGenerating }) => setGenerating(true)],
    ["post_candidate", ({ clearSelectedApp }) => clearSelectedApp()],
  ]) {
    assert.equal(
      await contentFailureStage({
        initialComposerText: stage === "post_candidate" ? "" : APP_IDENTITY.appLabel,
        mutateDuringDurableDraftReservation: mutate,
      }),
      stage,
    );
  }

  const registration = runtimeOwnedWorker(579, {
    appIdentity: clone(APP_IDENTITY),
    bindingRevision: 2,
    connectorEpoch: 2,
  });
  registration.value.provisionedConnectorProof =
    provisionedConnectorProofFor(registration);
  const target = targetForRegistration(registration, "waking", 1);
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    projectBindings: {
      [registration.value.projectId]: projectBinding(
        registration.value.projectId,
        APP_IDENTITY,
      ),
    },
    initialTabs: [
      {
        id: registration.value.runtimeOpenedTabId,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const storedDelivery = {
    ...delivery,
    targetId: registration.value.wakeTargetId,
  };
  const prepared = await runtime.api.prepareFinalSendAdmission({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery: storedDelivery,
  });
  const invalidAdmission = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    appIdentity: APP_IDENTITY,
    draft: APP_IDENTITY.appLabel,
    admissionToken: "forged-admission",
    delivery: storedDelivery,
  });
  assert.equal(invalidAdmission.rejectionStage, "background_admission");
  const invalidIdentity = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    appIdentity: APP_IDENTITY,
    draft: APP_IDENTITY.appLabel,
    admissionToken: prepared.admissionToken,
    delivery: { ...storedDelivery, targetId: "wtgt_forged" },
  });
  assert.equal(invalidIdentity.rejectionStage, "background_identity");
  const accepted = await runtime.api.recordWakeSendFailure({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery: storedDelivery,
    reason: "durable_reserve_rejected",
    rejectionStage: "background_proof",
  });
  assert.equal(accepted.safe, true);
  const stored =
    runtime.storage.wakeTargets[registration.storageKey].lastWakeSendFailure;
  assert.equal(stored.rejectionStage, "background_proof");
  assert.equal(Object.hasOwn(stored, "draft"), false);

  const acceptedPreCandidate = await runtime.api.recordWakeSendFailure({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery: storedDelivery,
    reason: "durable_reserve_rejected",
    rejectionStage: "pre_candidate",
    rejectionSubstage: "composer_changed",
  });
  assert.equal(acceptedPreCandidate.safe, true);
  const storedPreCandidate =
    runtime.storage.wakeTargets[registration.storageKey].lastWakeSendFailure;
  assert.equal(storedPreCandidate.rejectionStage, "pre_candidate");
  assert.equal(storedPreCandidate.rejectionSubstage, "composer_changed");
  for (const forbiddenKey of ["draft", "text", "length", "hash", "prefix"]) {
    assert.equal(Object.hasOwn(storedPreCandidate, forbiddenKey), false);
  }

  for (const rejected of [
    runtime.api.recordWakeSendFailure({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery: storedDelivery,
      reason: "durable_reserve_rejected",
      rejectionStage: "pre_candidate",
    }),
    runtime.api.recordWakeSendFailure({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery: storedDelivery,
      reason: "durable_reserve_rejected",
      rejectionStage: "background_unknown",
    }),
    runtime.api.recordWakeSendFailure({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery: storedDelivery,
      reason: "composer_missing",
      rejectionStage: "background_proof",
    }),
    runtime.api.recordWakeSendFailure({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery: storedDelivery,
      reason: "durable_reserve_rejected",
      rejectionStage: "pre_candidate",
      rejectionSubstage: "pre_candidate_unknown",
    }),
    runtime.api.recordWakeSendFailure({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery: storedDelivery,
      reason: "durable_reserve_rejected",
      rejectionStage: "background_proof",
      rejectionSubstage: "composer_changed",
    }),
    runtime.api.recordWakeSendFailure({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery: storedDelivery,
      reason: "composer_missing",
      rejectionStage: "pre_candidate",
      rejectionSubstage: "composer_changed",
    }),
    runtime.api.recordWakeSendFailure({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery: { ...storedDelivery, targetId: "wtgt_forged" },
      reason: "durable_reserve_rejected",
      rejectionStage: "background_proof",
    }),
  ]) {
    assert.equal((await rejected).safe, false);
  }
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].lastWakeSendFailure
      .rejectionStage,
    "pre_candidate",
  );
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].lastWakeSendFailure
      .rejectionSubstage,
    "composer_changed",
  );
}

async function preCandidateProviderDraftMismatchIsBoundedAndTextFree() {
  const failures = [];
  const rawProviderDraft = "Codex app\nConnected";
  const runtime = makeContentRuntime({
    inlineSelectedApp: true,
    selectedAppAncestorDepth: 4,
    initialComposerText: rawProviderDraft,
    mentionCandidates: false,
    runtimeMessageHandler(message) {
      if (message.type === "wake:prepareFinalSend") {
        return { ok: true, safe: true, admissionToken: "pre-candidate-token" };
      }
      if (message.type === "wake:recordSendFailure") {
        failures.push(clone(message));
        return { ok: true, safe: true };
      }
      return { ok: false };
    },
  });

  const sent = await runtime.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_pre_candidate\nwake_revision: 1",
    draftMarker: "wtgt_pre_candidate",
    delivery: {
      kind: "worker",
      mode: "wake",
      targetId: "wtgt_pre_candidate",
      targetRevision: 1,
    },
  });

  assert.equal(sent, false);
  assert.equal(failures.length, 1);
  assert.equal(failures[0].reason, "durable_reserve_rejected");
  assert.equal(failures[0].rejectionStage, "pre_candidate");
  assert.equal(failures[0].rejectionSubstage, "provider_draft_mismatch");
  const serializedFailure = JSON.stringify(failures[0]);
  assert.equal(serializedFailure.includes(rawProviderDraft), false);
  for (const forbiddenKey of ["draft", "text", "length", "hash", "prefix"]) {
    assert.equal(Object.hasOwn(failures[0], forbiddenKey), false, forbiddenKey);
  }
}

async function preCandidateComposerChangeHasBoundedSubstage() {
  const runtime = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
  });
  runtime.replaceCurrentComposer(APP_IDENTITY.appLabel);

  const result = await runtime.api.reserveDurableRuntimeOwnedAppDraft(
    runtime.composer,
    APP_IDENTITY,
    "pre-candidate-token",
    { kind: "worker", mode: "wake", targetId: "wtgt_changed", targetRevision: 1 },
  );

  assert.deepEqual(clone(result), {
    safe: false,
    attempted: false,
    rejectionStage: "pre_candidate",
    rejectionSubstage: "composer_changed",
  });
}

async function preCandidateDocumentClaimHasBoundedSubstage() {
  const runtime = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    visibilityState: "visible",
  });

  const result = await runtime.api.reserveDurableRuntimeOwnedAppDraft(
    runtime.composer,
    APP_IDENTITY,
    "pre-candidate-token",
    { kind: "worker", mode: "wake", targetId: "wtgt_claimed", targetRevision: 1 },
  );

  assert.deepEqual(clone(result), {
    safe: false,
    attempted: false,
    rejectionStage: "pre_candidate",
    rejectionSubstage: "document_claimed",
  });
}

async function preCandidateEmptyWithoutSelectedAppHasBoundedSubstage() {
  const runtime = makeContentRuntime({
    inlineSelectedApp: false,
    initialComposerText: "",
  });

  const result = await runtime.api.reserveDurableRuntimeOwnedAppDraft(
    runtime.composer,
    APP_IDENTITY,
    "pre-candidate-token",
    { kind: "worker", mode: "wake", targetId: "wtgt_empty", targetRevision: 1 },
  );

  assert.deepEqual(clone(result), {
    safe: false,
    attempted: false,
    rejectionStage: "pre_candidate",
    rejectionSubstage: "empty_without_selected_app",
  });
}

async function selectedAppPillScopeStopsAfterFiveComposerScopes() {
  const selectedWithinScope = makeContentRuntime({
    inlineSelectedApp: true,
    selectedAppAncestorDepth: 4,
    initialComposerText: "",
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const withinResult = await selectedWithinScope.api.reserveDurableRuntimeOwnedAppDraft(
    selectedWithinScope.composer,
    APP_IDENTITY,
    "pre-candidate-token",
    { kind: "worker", mode: "wake", targetId: "wtgt_scope_4", targetRevision: 1 },
  );
  assert.equal(withinResult.safe, true);

  const selectedOutsideScope = makeContentRuntime({
    inlineSelectedApp: true,
    selectedAppAncestorDepth: 5,
    initialComposerText: "",
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const outsideResult = await selectedOutsideScope.api.reserveDurableRuntimeOwnedAppDraft(
    selectedOutsideScope.composer,
    APP_IDENTITY,
    "pre-candidate-token",
    { kind: "worker", mode: "wake", targetId: "wtgt_scope_5", targetRevision: 1 },
  );
  assert.deepEqual(clone(outsideResult), {
    safe: false,
    attempted: false,
    rejectionStage: "pre_candidate",
    rejectionSubstage: "empty_without_selected_app",
  });
}

async function durableDraftReservationUsesSafetyCriticalCas() {
  async function reserveWithConcurrentMutation(index, mutateRegistration) {
    const registration = runtimeOwnedWorker(index, {
      appIdentity: clone(APP_IDENTITY),
      bindingRevision: 2,
      connectorEpoch: 2,
    });
    registration.value.provisionedConnectorProof =
      provisionedConnectorProofFor(registration);
    const delivery = {
      kind: "worker",
      mode: "wake",
      targetId: registration.value.wakeTargetId,
      targetRevision: 2,
    };
    const runtime = makeRuntime({
      registrations: [registration],
      targets: [
        [
          registration.value.wakeTargetId,
          targetForRegistration(registration, "waking", 2),
        ],
      ],
      projectBindings: {
        [registration.value.projectId]: projectBinding(
          registration.value.projectId,
          APP_IDENTITY,
        ),
      },
      initialTabs: [
        {
          id: registration.value.runtimeOpenedTabId,
          url: registration.value.conversationUrl,
          active: false,
          pinned: false,
        },
      ],
      onProjectBindingRead({ storage }) {
        mutateRegistration(
          storage.wakeTargets[registration.storageKey],
          storage,
          registration,
        );
      },
    });
    const prepared = await runtime.api.prepareFinalSendAdmission({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery,
    });
    const reserved = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      appIdentity: APP_IDENTITY,
      draft: APP_IDENTITY.appLabel,
      admissionToken: prepared.admissionToken,
      delivery,
    });
    return { registration, runtime, reserved };
  }

  const telemetryOnly = await reserveWithConcurrentMutation(580, (
    current,
    storage,
    registration,
  ) => {
    current.lastWakeSendFailure = {
      reason: "concurrent_diagnostic",
      recordedAt: 1000001,
    };
    storage.wakeContentDisposition = {
      [registration.storageKey]: {
        conversationUrl: current.conversationUrl,
        kind: "worker",
        mode: "wake",
        targetId: current.wakeTargetId,
        targetRevision: 2,
        code: CONTENT_DISPOSITION_CODES.POLL_RECEIVED,
        observedAt: 1000001,
      },
    };
    current.idleSince = 1000001;
  });
  assert.equal(
    telemetryOnly.reserved.safe,
    true,
    "diagnostic and idle telemetry must not invalidate a durable reservation",
  );
  assert.equal(
    telemetryOnly.runtime.storage.wakeTargets[telemetryOnly.registration.storageKey]
      .lastWakeSendFailure.reason,
    "concurrent_diagnostic",
  );
  assert.equal(
    telemetryOnly.runtime.storage.wakeTargets[telemetryOnly.registration.storageKey]
      .idleSince,
    1000001,
  );
  assert.equal(
    telemetryOnly.runtime.storage.wakeContentDisposition[
      telemetryOnly.registration.storageKey
    ].code,
    CONTENT_DISPOSITION_CODES.POLL_RECEIVED,
    "content disposition telemetry must be outside semantic send CAS",
  );
  assert.equal(
    Object.prototype.hasOwnProperty.call(
      telemetryOnly.runtime.storage.wakeTargets[telemetryOnly.registration.storageKey],
      "lastWakeContentDisposition",
    ),
    false,
  );

  const criticalMutations = [
    ["app identity", (current) => { current.appIdentity.appRef = "plugin_other"; }],
    ["target", (current) => { current.wakeTargetId = "wtgt_concurrent_other"; }],
    ["runtime tab", (current) => { current.runtimeOpenedTabId += 1; }],
    ["proof", (current) => { current.provisionedConnectorProof.connectorEpoch += 1; }],
    ["capability", (current) => { current.runtimeOwnedRegistrationDraft.appLabel = "Other"; }],
    ["attempt", (current) => { current.lastAttemptRevision = 1; }],
    ["retirement", (current) => { current.retirementPending = true; }],
    ["binding", (current) => { current.bindingRevision += 1; }],
    ["unknown field", (current) => { current.concurrentUnknownField = true; }],
  ];
  for (const [offset, [name, mutateRegistration]] of criticalMutations.entries()) {
    const result = await reserveWithConcurrentMutation(
      581 + offset,
      mutateRegistration,
    );
    assert.equal(result.reserved.safe, false, name);
    assert.equal(result.reserved.rejectionStage, "background_snapshot", name);
  }

  const retiring = runtimeOwnedWorker(599, {
    appIdentity: clone(APP_IDENTITY),
    bindingRevision: 2,
    connectorEpoch: 2,
  });
  retiring.value.retirementPending = {
    wakeTargetId: retiring.value.wakeTargetId,
    projectId: retiring.value.projectId,
    conversationId: retiring.value.conversationId,
    runtimeOpenedTabId: retiring.value.runtimeOpenedTabId,
    markedAt: 1000000,
  };
  retiring.value.provisionedConnectorProof =
    provisionedConnectorProofFor(retiring);
  const retiringDelivery = {
    kind: "worker",
    mode: "wake",
    targetId: retiring.value.wakeTargetId,
    targetRevision: 2,
  };
  const retiringRuntime = makeRuntime({
    registrations: [retiring],
    targets: [
      [retiring.value.wakeTargetId, targetForRegistration(retiring, "waking", 2)],
    ],
    projectBindings: {
      [retiring.value.projectId]: projectBinding(
        retiring.value.projectId,
        APP_IDENTITY,
      ),
    },
    initialTabs: [
      {
        id: retiring.value.runtimeOpenedTabId,
        url: retiring.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const retiringPrepared = await retiringRuntime.api.prepareFinalSendAdmission({
    tabId: retiring.value.runtimeOpenedTabId,
    conversationUrl: retiring.value.conversationUrl,
    delivery: retiringDelivery,
  });
  assert.equal(retiringPrepared.safe, false);
  assert.equal(
    retiringRuntime.fetchPaths.some((path) => path.includes("project-bindings")),
    false,
  );
}

async function durableDraftConfirmationUsesSafetyCriticalCas() {
  async function confirmWithConcurrentMutation(index, mutateRegistration) {
    const registration = runtimeOwnedWorker(index, {
      appIdentity: clone(APP_IDENTITY),
      bindingRevision: 2,
      connectorEpoch: 2,
    });
    registration.value.provisionedConnectorProof =
      provisionedConnectorProofFor(registration);
    const delivery = {
      kind: "worker",
      mode: "wake",
      targetId: registration.value.wakeTargetId,
      targetRevision: 2,
    };
    let confirmPhase = false;
    let confirmReadCount = 0;
    let mutated = false;
    const runtime = makeRuntime({
      registrations: [registration],
      targets: [
        [registration.value.wakeTargetId, targetForRegistration(registration, "waking", 2)],
      ],
      projectBindings: {
        [registration.value.projectId]: projectBinding(
          registration.value.projectId,
          APP_IDENTITY,
        ),
      },
      initialTabs: [
        {
          id: registration.value.runtimeOpenedTabId,
          url: registration.value.conversationUrl,
          active: false,
          pinned: false,
        },
      ],
      onRegistrationMapRead({ storage }) {
        if (!confirmPhase) return;
        confirmReadCount += 1;
        if (confirmReadCount === 4) {
          mutateRegistration(storage.wakeTargets[registration.storageKey]);
          mutated = true;
        }
      },
    });
    const prepared = await runtime.api.prepareFinalSendAdmission({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery,
    });
    const reserved = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      appIdentity: APP_IDENTITY,
      draft: APP_IDENTITY.appLabel,
      admissionToken: prepared.admissionToken,
      delivery,
    });
    assert.equal(reserved.safe, true);
    confirmPhase = true;
    const confirmed = await runtime.api.confirmFinalSendAdmission({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      admissionToken: prepared.admissionToken,
      delivery,
    });
    assert.equal(mutated, true);
    return { confirmed, confirmReadCount, registration, runtime };
  }

  const benign = await confirmWithConcurrentMutation(600, (current) => {
    current.idleSince = 2000000;
    current.lastWakeSendFailure = { reason: "concurrent_confirm" };
  });
  assert.equal(benign.confirmed.safe, true);
  assert.equal(
    benign.runtime.storage.wakeTargets[benign.registration.storageKey].idleSince,
    2000000,
  );
  assert.equal(
    benign.runtime.storage.wakeTargets[benign.registration.storageKey]
      .lastAttemptRevision,
    2,
  );

  const authority = await confirmWithConcurrentMutation(601, (current) => {
    current.bindingRevision += 1;
  });
  assert.equal(
    authority.confirmed.safe,
    false,
    `confirm registration reads: ${authority.confirmReadCount}`,
  );
  assert.equal(
    authority.runtime.storage.wakeTargets[authority.registration.storageKey]
      .lastAttemptRevision,
    0,
  );
}

async function resumeAttemptDoesNotOverwriteQueuedRegistrationUpdate() {
  for (const [index, kind] of [
    [590, "worker"],
    [591, "main"],
  ]) {
    const registration =
      kind === "worker" ? workerRegistration(index) : mainRegistration(index);
    let releaseFirstRead;
    const firstReadGate = new Promise((resolve) => {
      releaseFirstRead = resolve;
    });
    let firstReadStarted = false;
    let readCount = 0;
    const runtime = makeRuntime({
      registrations: [registration],
      targets: [],
      async onRegistrationMapRead() {
        readCount += 1;
        if (readCount === 1) {
          firstReadStarted = true;
          await firstReadGate;
        }
      },
    });
    const markPromise =
      kind === "worker"
        ? runtime.api.markResumeAttempt({
            conversationUrl: registration.value.conversationUrl,
            wakeTargetId: registration.value.wakeTargetId,
          })
        : runtime.api.markMainResumeAttempt({
            conversationUrl: registration.value.conversationUrl,
            mainTargetId: registration.value.mainTargetId,
          });
    await waitFor(() => firstReadStarted);
    const updatePromise = runtime.api.updateRegistration(
      registration.value.conversationUrl,
      { idleSince: 777000 + index, lastWakeSendFailure: { reason: "concurrent" } },
    );
    await flushMicrotasks();
    releaseFirstRead();
    await Promise.all([markPromise, updatePromise]);
    const current = runtime.storage.wakeTargets[registration.storageKey];
    assert.equal(current.idleSince, 777000 + index, kind);
    assert.equal(current.lastWakeSendFailure.reason, "concurrent", kind);
    assert.equal(current.lastResumeAttemptAt, 1000000, kind);
  }
}

async function corruptedStoredCapabilityFieldsFailClosed() {
  const corruptions = [
    ["tab id", { tabId: 999999 }],
    ["conversation id", { conversationId: "different-conversation" }],
    ["app ref", { appRef: "plugin_different" }],
    ["app label", { appLabel: "Different Connector" }],
  ];

  for (const [name, capabilityPatch] of corruptions) {
    const registration = runtimeOwnedWorker(560 + corruptions.findIndex(([item]) => item === name), {
      appIdentity: clone(APP_IDENTITY),
      bindingRevision: 2,
      connectorEpoch: 2,
    });
    registration.value.runtimeOwnedRegistrationDraft = {
      ...registration.value.runtimeOwnedRegistrationDraft,
      ...capabilityPatch,
    };
    registration.value.provisionedConnectorProof =
      provisionedConnectorProofFor(registration);
    const target = targetForRegistration(registration, "waking", 2);
    const delivery = {
      kind: "worker",
      mode: "wake",
      targetId: registration.value.wakeTargetId,
      targetRevision: 2,
    };
    const runtime = makeRuntime({
      registrations: [registration],
      targets: [[registration.value.wakeTargetId, target]],
      projectBindings: {
        [registration.value.projectId]: projectBinding(
          registration.value.projectId,
          {
            appRef: APP_IDENTITY.appRef,
            appLabel: APP_IDENTITY.appLabel,
          },
        ),
      },
      initialTabs: [
        {
          id: registration.value.runtimeOpenedTabId,
          url: registration.value.conversationUrl,
          active: false,
          pinned: false,
        },
      ],
    });
    const prepared = await runtime.api.prepareFinalSendAdmission({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      delivery,
    });
    const before = clone(
      runtime.storage.wakeTargets[registration.storageKey]
        .runtimeOwnedRegistrationDraft,
    );

    const reserved = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: registration.value.conversationUrl,
      appIdentity: APP_IDENTITY,
      draft: APP_IDENTITY.appLabel,
      admissionToken: prepared.admissionToken,
      delivery,
    });

    assert.equal(reserved.safe, false, name);
    assert.deepEqual(
      runtime.storage.wakeTargets[registration.storageKey]
        .runtimeOwnedRegistrationDraft,
      before,
      name,
    );
    assert.equal(
      runtime.storage.wakeTargets[registration.storageKey].lastAttemptRevision,
      0,
      name,
    );
  }
}

async function strandedHumanTextCannotConsumeFirstWakeCapability() {
  const registration = runtimeOwnedWorker(570, {
    appIdentity: clone(APP_IDENTITY),
    bindingRevision: 2,
    connectorEpoch: 2,
    runtimeOwnedRegistrationDraftConsumedAt: 999,
    lastAttemptRevision: 0,
  });
  registration.value.runtimeOwnedRegistrationDraft = null;
  registration.value.provisionedConnectorProof =
    provisionedConnectorProofFor(registration);
  const target = targetForRegistration(registration, "waking", 2);
  const delivery = {
    kind: "worker",
    mode: "wake",
    targetId: registration.value.wakeTargetId,
    targetRevision: 2,
  };
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    projectBindings: {
      [registration.value.projectId]: projectBinding(registration.value.projectId, {
        appRef: APP_IDENTITY.appRef,
        appLabel: APP_IDENTITY.appLabel,
      }),
    },
    initialTabs: [
      {
        id: registration.value.runtimeOpenedTabId,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const prepared = await runtime.api.prepareFinalSendAdmission({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery,
  });
  const strandedHumanDraft = `${APP_IDENTITY.appLabel[0]} human text`;

  const reserved = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    appIdentity: APP_IDENTITY,
    draft: strandedHumanDraft,
    admissionToken: prepared.admissionToken,
    delivery,
  });

  assert.equal(reserved.safe, false);
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey]
      .runtimeOwnedRegistrationDraftConsumedAt,
    999,
  );
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].lastAttemptRevision,
    0,
  );
}

async function generationInProgressPreventsRuntimeActions() {
  const generating = makeContentRuntime({ generationInProgress: true });

  const wakeResult = await generating.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_generating\nwake_revision: 1",
    draftMarker: "wtgt_generating",
  });
  assert.equal(wakeResult, false);
  assert.equal(
    generating.composer.value,
    "",
    "a normal wake must not inject a draft while ChatGPT is generating",
  );
  assert.equal(
    generating.sendButton.clicks,
    0,
    "a normal wake must not submit while ChatGPT is generating",
  );

  const bootstrapResult = await generating.api.bootstrapProvisionedWorker(
    "token-generating",
    APP_IDENTITY,
  );
  assert.deepEqual(JSON.parse(JSON.stringify(bootstrapResult)), {
    ok: false,
    code: "generation_in_progress",
    retryable: false,
  });
  assert.equal(
    generating.composer.value,
    "",
    "provisioning bootstrap must fail closed before injecting a draft",
  );
  assert.equal(
    generating.sendButton.clicks,
    0,
    "provisioning bootstrap must not submit while ChatGPT is generating",
  );

  const parkSafety = generating.dispatchRuntimeMessage({
    type: "runtime:canPark",
  });
  assert.deepEqual(JSON.parse(JSON.stringify(parkSafety)), {
    ok: true,
    canPark: false,
  });

  const registration = workerRegistration(93, {
    runtimeOpenedTabId: 93,
    runtimeOpenedAt: 100,
    idleSince: 900000,
  });
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [],
    initialTabs: [
      {
        id: 93,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
        discarded: false,
      },
    ],
    canParkResponse: parkSafety,
  });

  await runtime.api.parkRuntimeOwnedTabWhenSafe(
    registration.value.conversationUrl,
    registration.value,
  );

  assert.equal(
    runtime.removeCalls.length,
    0,
    "a generating runtime-owned tab must not be parked or closed",
  );
  assert.ok(runtime.getTab(93), "the generating runtime-owned tab must remain open");
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].runtimeOpenedTabId,
    93,
    "failed parking must retain runtime ownership for a later safe retry",
  );
}

async function bootstrapComposerAndOwnershipRacesNeverSubmit() {
  const inlineSelection = makeContentRuntime({
    inlineSelectedApp: true,
    mentionCandidates: false,
  });
  const inlineSelectionResult =
    await inlineSelection.api.bootstrapProvisionedWorker(
      "token-inline",
      APP_IDENTITY,
    );
  assert.equal(
    inlineSelectionResult.ok,
    true,
    JSON.stringify(inlineSelectionResult),
  );
  assert.equal(inlineSelection.sendButton.clicks, 1);

  const bootstrapBodyRequired = makeContentRuntime({
    inlineSelectedApp: true,
    mentionCandidates: false,
    sendRequiresNonAppText: true,
  });
  const bootstrapBodyRequiredResult =
    await bootstrapBodyRequired.api.bootstrapProvisionedWorker(
      "token-bootstrap-body-required",
      APP_IDENTITY,
    );
  assert.equal(
    bootstrapBodyRequiredResult.ok,
    true,
    "bootstrap must append a fixed non-work body when the provider disables app-only sends",
  );
  assert.equal(bootstrapBodyRequired.sendButton.clicks, 1);
  assert.equal(
    bootstrapBodyRequiredResult.registrationDraft,
    `@${APP_IDENTITY.appLabel}`,
    "the durable registration capability must preserve the original app-only draft",
  );
  assert.match(
    bootstrapBodyRequired.composer.value,
    /registration handshake/i,
  );

  const noOpMentionSelection = makeContentRuntime({
    mentionSelectionNoop: true,
  });
  const noOpMentionSelectionResult =
    await noOpMentionSelection.api.bootstrapProvisionedWorker(
      "token-no-op-mention",
      APP_IDENTITY,
    );
  assert.equal(noOpMentionSelectionResult.ok, false);
  assert.equal(
    noOpMentionSelectionResult.code,
    "app_selection_unconfirmed",
  );
  assert.equal(noOpMentionSelectionResult.retryable, true);
  assert.equal(
    noOpMentionSelection.sendButton.clicks,
    0,
    "a dispatched no-op candidate click must not count as selected",
  );

  const noOpMentionRecoveredByPicker = makeContentRuntime({
    mentionSelectionNoop: true,
    pluginPickerFallback: true,
  });
  const noOpMentionRecoveredByPickerResult =
    await noOpMentionRecoveredByPicker.api.bootstrapProvisionedWorker(
      "token-no-op-mention-picker-recovery",
      APP_IDENTITY,
    );
  assert.equal(noOpMentionRecoveredByPickerResult.ok, true);
  assert.equal(noOpMentionRecoveredByPicker.sendButton.clicks, 1);

  const decoratedInlineSelection = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: `\uFEFF${APP_IDENTITY.appLabel[0]}${APP_IDENTITY.appLabel} `,
    mentionCandidates: false,
  });
  const decoratedInlineSelectionResult =
    await decoratedInlineSelection.api.bootstrapProvisionedWorker(
      "token-inline-decorated",
      APP_IDENTITY,
    );
  assert.equal(decoratedInlineSelectionResult.ok, true);
  assert.equal(decoratedInlineSelection.sendButton.clicks, 1);

  const atDecoratedInlineSelection = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: `@${APP_IDENTITY.appLabel[0]}${APP_IDENTITY.appLabel}`,
    mentionCandidates: false,
  });
  const atDecoratedInlineSelectionResult =
    await atDecoratedInlineSelection.api.bootstrapProvisionedWorker(
      "token-inline-at-decorated",
      APP_IDENTITY,
    );
  assert.equal(atDecoratedInlineSelectionResult.ok, true);
  assert.equal(atDecoratedInlineSelection.sendButton.clicks, 1);

  const iconOnlyInlineSelection = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel[0],
    mentionCandidates: false,
  });
  const iconOnlyInlineSelectionResult =
    await iconOnlyInlineSelection.api.bootstrapProvisionedWorker(
      "token-inline-icon-only",
      APP_IDENTITY,
    );
  assert.equal(iconOnlyInlineSelectionResult.ok, true);
  assert.equal(iconOnlyInlineSelection.sendButton.clicks, 1);

  const pickerSelection = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
  });
  const pickerSelectionResult =
    await pickerSelection.api.bootstrapProvisionedWorker(
      "token-picker",
      APP_IDENTITY,
    );
  assert.equal(pickerSelectionResult.ok, true);
  assert.equal(pickerSelection.sendButton.clicks, 1);

  const decoratedPickerSelection = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    pluginPickerSelectedText: `\uFEFF${APP_IDENTITY.appLabel[0]}${APP_IDENTITY.appLabel} `,
  });
  const decoratedPickerSelectionResult =
    await decoratedPickerSelection.api.bootstrapProvisionedWorker(
      "token-picker-decorated",
      APP_IDENTITY,
    );
  assert.equal(decoratedPickerSelectionResult.ok, true);
  assert.equal(decoratedPickerSelection.sendButton.clicks, 1);

  const unifiedComposerPickerSelection = makeContentRuntime({
    mentionCandidates: false,
    unifiedComposerPickerFallback: true,
    pluginPickerSelectedText: `\uFEFF${APP_IDENTITY.appLabel[0]}${APP_IDENTITY.appLabel} `,
  });
  const unifiedComposerPickerSelectionResult =
    await unifiedComposerPickerSelection.api.bootstrapProvisionedWorker(
      "token-unified-composer-picker",
      APP_IDENTITY,
    );
  assert.equal(unifiedComposerPickerSelectionResult.ok, true);
  assert.equal(unifiedComposerPickerSelection.sendButton.clicks, 1);

  const describedPickerSelection = makeContentRuntime({
    mentionCandidates: false,
    unifiedComposerPickerFallback: true,
    pluginMenuCandidates: [
      { appLabel: APP_IDENTITY.appLabel, appRef: null },
    ],
    pluginMenuDescription: "ChatToCodex local orchestration runtime",
  });
  const describedPickerSelectionResult =
    await describedPickerSelection.api.bootstrapProvisionedWorker(
      "token-described-picker",
      { appRef: null, appLabel: APP_IDENTITY.appLabel },
    );
  assert.equal(describedPickerSelectionResult.ok, true);
  assert.equal(describedPickerSelection.sendButton.clicks, 1);

  const clickOnlyPickerSelection = makeContentRuntime({
    mentionCandidates: false,
    unifiedComposerPickerFallback: true,
    syntheticPointerSequenceInvalidatesControl: true,
  });
  const clickOnlyPickerSelectionResult =
    await clickOnlyPickerSelection.api.bootstrapProvisionedWorker(
      "token-click-only-picker",
      APP_IDENTITY,
    );
  assert.equal(clickOnlyPickerSelectionResult.ok, true);
  assert.equal(clickOnlyPickerSelection.sendButton.clicks, 1);

  const keyboardFirstPickerSelection = makeContentRuntime({
    mentionCandidates: false,
    unifiedComposerPickerFallback: true,
    menuSelectionRequiresEnter: true,
  });
  const keyboardFirstPickerSelectionResult =
    await keyboardFirstPickerSelection.api.bootstrapProvisionedWorker(
      "token-keyboard-first-picker",
      APP_IDENTITY,
    );
  assert.equal(keyboardFirstPickerSelectionResult.ok, true);
  assert.equal(keyboardFirstPickerSelection.sendButton.clicks, 1);

  const delayedSelectionConfirmation = makeContentRuntime({
    mentionCandidates: false,
    unifiedComposerPickerFallback: true,
    menuSelectionConfirmationDelayMs: 2500,
  });
  const delayedSelectionConfirmationResult =
    await delayedSelectionConfirmation.api.bootstrapProvisionedWorker(
      "token-delayed-selection-confirmation",
      APP_IDENTITY,
    );
  assert.equal(delayedSelectionConfirmationResult.ok, true);
  assert.equal(delayedSelectionConfirmation.sendButton.clicks, 1);

  const ambiguousUnifiedComposerPicker = makeContentRuntime({
    mentionCandidates: false,
    unifiedComposerPickerFallback: true,
    unifiedComposerCandidateCount: 2,
  });
  const ambiguousUnifiedComposerPickerResult =
    await ambiguousUnifiedComposerPicker.api.bootstrapProvisionedWorker(
      "token-ambiguous-unified-composer-picker",
      APP_IDENTITY,
    );
  assert.equal(ambiguousUnifiedComposerPickerResult.ok, false);
  assert.equal(
    ambiguousUnifiedComposerPickerResult.code,
    "app_menu_ambiguous",
  );
  assert.equal(ambiguousUnifiedComposerPickerResult.retryable, false);
  assert.equal(ambiguousUnifiedComposerPicker.sendButton.clicks, 0);

  const unavailableMenu = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    unifiedComposerCandidateCount: 0,
  });
  const unavailableMenuResult =
    await unavailableMenu.api.bootstrapProvisionedWorker(
      "token-menu-unavailable",
      APP_IDENTITY,
    );
  assert.equal(unavailableMenuResult.ok, false);
  assert.equal(unavailableMenuResult.code, "app_menu_unavailable");
  assert.equal(unavailableMenuResult.retryable, true);
  assert.equal(unavailableMenu.sendButton.clicks, 0);

  const retryOwnedAppDraft = makeContentRuntime({
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    pluginPickerFallback: true,
  });
  const retryOwnedAppDraftResult =
    await retryOwnedAppDraft.api.bootstrapProvisionedWorker(
      "token-retry-owned-app-draft",
      APP_IDENTITY,
    );
  assert.equal(retryOwnedAppDraftResult.ok, true);
  assert.equal(retryOwnedAppDraft.sendButton.clicks, 1);

  const hiddenProviderControls = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    providerControlsVisible: false,
  });
  const hiddenProviderControlsResult =
    await hiddenProviderControls.api.bootstrapProvisionedWorker(
      "token-hidden-provider-controls",
      APP_IDENTITY,
    );
  assert.equal(hiddenProviderControlsResult.ok, true);
  assert.equal(hiddenProviderControls.sendButton.clicks, 1);

  const claimedHiddenProviderControls = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    providerControlsVisible: false,
    visibilityState: "visible",
    documentHasFocus: true,
  });
  const claimedHiddenProviderControlsResult =
    await claimedHiddenProviderControls.api.bootstrapProvisionedWorker(
      "token-claimed-hidden-provider-controls",
      APP_IDENTITY,
    );
  assert.equal(claimedHiddenProviderControlsResult.ok, false);
  assert.equal(
    claimedHiddenProviderControlsResult.code,
    "app_picker_unavailable",
    "a claimed document must not use hidden provider controls",
  );
  assert.equal(claimedHiddenProviderControlsResult.retryable, false);
  assert.equal(claimedHiddenProviderControls.sendButton.clicks, 0);

  const claimedInlineSelection = makeContentRuntime({
    inlineSelectedApp: true,
    mentionCandidates: false,
    visibilityState: "visible",
    documentHasFocus: true,
  });
  const claimedInlineSelectionResult =
    await claimedInlineSelection.api.bootstrapProvisionedWorker(
      "token-claimed-inline-selection",
      APP_IDENTITY,
    );
  assert.equal(claimedInlineSelectionResult.ok, false);
  assert.equal(
    claimedInlineSelectionResult.code,
    "provisioning_tab_claimed",
    "a visible or focused provisioning document must report the exact closed failure boundary",
  );
  assert.equal(claimedInlineSelectionResult.retryable, false);
  assert.equal(claimedInlineSelection.sendButton.clicks, 0);
  assert.equal(
    claimedInlineSelection.composer.value,
    `@${APP_IDENTITY.appLabel}`,
    "a rejected bootstrap must restore the exact app-only registration draft",
  );

  const delayedMenu = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    menuAvailableAfterQueries: 2,
  });
  const delayedMenuResult = await delayedMenu.api.bootstrapProvisionedWorker(
    "token-delayed-menu",
    APP_IDENTITY,
  );
  assert.equal(delayedMenuResult.ok, true);
  assert.equal(delayedMenu.sendButton.clicks, 1);

  const duplicateReferenceMenu = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    unifiedComposerCandidateCount: 2,
    pluginMenuCandidates: [
      { appLabel: "first app", appRef: APP_IDENTITY.appRef },
      { appLabel: "second app", appRef: APP_IDENTITY.appRef },
    ],
  });
  const duplicateReferenceResult =
    await duplicateReferenceMenu.api.bootstrapProvisionedWorker(
      "token-duplicate-reference",
      APP_IDENTITY,
    );
  assert.equal(duplicateReferenceResult.ok, false);
  assert.equal(duplicateReferenceResult.code, "app_menu_ambiguous");
  assert.equal(duplicateReferenceResult.retryable, false);
  assert.equal(duplicateReferenceMenu.sendButton.clicks, 0);

  const duplicateExactLabelMenu = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    unifiedComposerCandidateCount: 2,
    pluginMenuCandidates: [
      { appLabel: "ChatOrchestration", appRef: null },
      { appLabel: "ChatOrchestration", appRef: null },
    ],
    pluginPickerSelectedText: "ChatOrchestration",
  });
  const duplicateExactLabelResult =
    await duplicateExactLabelMenu.api.bootstrapProvisionedWorker(
      "token-duplicate-exact-label",
      { appRef: null, appLabel: "ChatOrchestration" },
    );
  assert.equal(duplicateExactLabelResult.ok, false);
  assert.equal(duplicateExactLabelResult.code, "app_menu_ambiguous");
  assert.equal(duplicateExactLabelResult.retryable, false);
  assert.equal(duplicateExactLabelMenu.sendButton.clicks, 0);

  const exactLabelMenu = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    unifiedComposerCandidateCount: 2,
    pluginMenuCandidates: [
      { appLabel: "ChatOrchestration2", appRef: null },
      { appLabel: "ChatOrchestration", appRef: null },
    ],
    pluginPickerSelectedText: "ChatOrchestration",
  });
  const exactLabelResult =
    await exactLabelMenu.api.bootstrapProvisionedWorker(
      "token-exact-label",
      { appRef: null, appLabel: "ChatOrchestration" },
    );
  assert.equal(exactLabelResult.ok, true);
  assert.equal(exactLabelMenu.sendButton.clicks, 1);

  const replaced = makeContentRuntime({
    mutateDuringDelay({ milliseconds, replaceComposer, newComposer }) {
      if (milliseconds === 450) {
        replaceComposer(newComposer("human draft in replacement composer"));
      }
    },
  });
  const replacedResult = await replaced.api.bootstrapProvisionedWorker(
    "token-1",
    APP_IDENTITY,
  );
  assert.equal(replacedResult.ok, false);
  assert.equal(replaced.sendButton.clicks, 0);

  const delayedHumanDraft = makeContentRuntime({
    mutateDuringDelay({ composer, milliseconds }) {
      if (milliseconds === 200) {
        composer.value = "human draft during async mention selection";
      }
    },
  });
  const humanResult = await delayedHumanDraft.api.bootstrapProvisionedWorker(
    "token-2",
    APP_IDENTITY,
  );
  assert.equal(humanResult.ok, false);
  assert.equal(
    delayedHumanDraft.composer.value,
    "human draft during async mention selection",
  );
  assert.equal(delayedHumanDraft.sendButton.clicks, 0);

  const claimed = makeContentRuntime({
    bootstrapSafety: { ok: true, safe: false },
  });
  const claimedResult = await claimed.api.bootstrapProvisionedWorker(
    "token-3",
    APP_IDENTITY,
  );
  assert.equal(claimedResult.ok, false);
  assert.equal(claimedResult.code, "bootstrap_ownership_lost");
  assert.equal(claimed.sendButton.clicks, 0);
  assert.equal(
    claimed.composer.value,
    `@${APP_IDENTITY.appLabel}`,
    "ownership loss must remove only the runtime-owned handshake body",
  );

  const replacedDuringOwnership = makeContentRuntime({
    mutateDuringOwnershipCheck({ replaceComposer, newComposer }) {
      replaceComposer(newComposer("human draft after background ownership check"));
    },
  });
  const lateReplacementResult =
    await replacedDuringOwnership.api.bootstrapProvisionedWorker(
      "token-4",
      APP_IDENTITY,
    );
  assert.equal(lateReplacementResult.ok, false);
  assert.equal(lateReplacementResult.code, "composer_changed");
  assert.equal(replacedDuringOwnership.sendButton.clicks, 0);
  assert.equal(
    replacedDuringOwnership.currentComposer.value,
    "human draft after background ownership check",
    "a replacement composer owned by the user must never be cleared",
  );

  const invalidated = makeContentRuntime({ runtimeSendThrows: true });
  const invalidatedResult = await invalidated.api.bootstrapProvisionedWorker(
    "token-invalidated",
    APP_IDENTITY,
  );
  assert.equal(invalidatedResult.ok, false);
  assert.equal(invalidatedResult.code, "bootstrap_ownership_unavailable");
  assert.equal(invalidated.sendButton.clicks, 0);
  assert.equal(
    invalidated.composer.value,
    `@${APP_IDENTITY.appLabel}`,
    "an unavailable background boundary must restore the app-only draft",
  );
}

function makeProjectProvisioningContentRuntime({
  visibilityState = "hidden",
  replaceControlDuringOwnershipCheck = false,
  ownershipSafe = true,
  claimThenHideDuringOwnershipCheck = false,
  projectComposerOnly = false,
  projectChatMode = false,
  providerControlsVisible = true,
} = {}) {
  const originalControl = {
    disabled: false,
    clicks: 0,
    getBoundingClientRect() {
      return providerControlsVisible
        ? { width: 1, height: 1 }
        : { width: 0, height: 0 };
    },
    click() {
      this.clicks += 1;
    },
  };
  const replacementControl = { ...originalControl, clicks: 0 };
  let currentControl = originalControl;
  const workModeControl = {
    textContent: "Work",
    clicks: 0,
    getAttribute(name) {
      return name === "aria-checked" ? "false" : null;
    },
    getBoundingClientRect() {
      return providerControlsVisible
        ? { width: 1, height: 1 }
        : { width: 0, height: 0 };
    },
    click() {
      this.clicks += 1;
    },
  };
  const documentListeners = new Map();
  const document = {
    visibilityState,
    hasFocus() {
      return visibilityState === "visible";
    },
    body: { innerText: "" },
    querySelector(selector) {
      if (projectComposerOnly && !selector.includes("prompt-textarea")) {
        return null;
      }
      return currentControl;
    },
    querySelectorAll(selector) {
      return projectChatMode && selector === '[role="radio"]'
        ? [workModeControl]
        : [];
    },
    addEventListener(type, listener) {
      documentListeners.set(type, listener);
    },
  };
  const chrome = {
    runtime: {
      onMessage: { addListener() {} },
      sendMessage(_message, callback) {
        if (replaceControlDuringOwnershipCheck) {
          currentControl = replacementControl;
        }
        if (claimThenHideDuringOwnershipCheck) {
          document.visibilityState = "visible";
          documentListeners.get("visibilitychange")?.();
          document.visibilityState = "hidden";
          documentListeners.get("visibilitychange")?.();
        }
        callback({ ok: true, safe: ownershipSafe });
      },
      lastError: null,
    },
  };
  const source = fs
    .readFileSync(
      path.join(extensionRoot, "project_bound_provisioning_content.js"),
      "utf8",
    )
    .replace(
      /\n\}\)\(\);\s*$/,
      "\nglobalThis.__projectContentHarness = { openProjectBoundNewChat };\n})();\n",
    );
  const context = vm.createContext({
    chrome,
    document,
    window: {},
    location: { href: "https://chatgpt.com/g/project-safe" },
    URL,
  });
  vm.runInContext(source, context, {
    filename: "project_bound_provisioning_content.js",
  });
  return {
    api: context.__projectContentHarness,
    originalControl,
    replacementControl,
    workModeControl,
  };
}

async function projectNewChatClickRechecksPageAndUserOwnership() {
  const unhydratedProject = makeProjectProvisioningContentRuntime({
    projectComposerOnly: true,
    visibilityState: "visible",
  });
  unhydratedProject.originalControl.getBoundingClientRect = () => ({
    width: 0,
    height: 0,
  });
  const unhydratedResult = await unhydratedProject.api.openProjectBoundNewChat({
    projectPath: "/g/project-safe",
    projectRef: "project-safe",
    ownershipToken: "project-token",
  });
  assert.deepEqual(JSON.parse(JSON.stringify(unhydratedResult)), {
    ok: false,
    code: "unsupported_project_ui",
    retryable: true,
  });

  for (const runtime of [
    makeProjectProvisioningContentRuntime({ visibilityState: "visible" }),
    makeProjectProvisioningContentRuntime({
      replaceControlDuringOwnershipCheck: true,
    }),
    makeProjectProvisioningContentRuntime({ ownershipSafe: false }),
    makeProjectProvisioningContentRuntime({
      claimThenHideDuringOwnershipCheck: true,
    }),
  ]) {
    const result = await runtime.api.openProjectBoundNewChat({
      projectPath: "/g/project-safe",
      projectRef: "project-safe",
      ownershipToken: "project-token",
    });
    assert.equal(result.ok, false);
    assert.equal(runtime.originalControl.clicks, 0);
    assert.equal(runtime.replacementControl.clicks, 0);
  }

  const currentProjectComposer = makeProjectProvisioningContentRuntime({
    projectComposerOnly: true,
    projectChatMode: true,
  });
  const composerResult = await currentProjectComposer.api.openProjectBoundNewChat({
    projectPath: "/g/project-safe",
    projectRef: "project-safe",
    ownershipToken: "project-token",
  });
  assert.equal(composerResult.ok, true);
  assert.equal(currentProjectComposer.originalControl.clicks, 0);
  assert.equal(currentProjectComposer.workModeControl.clicks, 1);

  const hiddenCurrentProjectComposer = makeProjectProvisioningContentRuntime({
    projectComposerOnly: true,
    projectChatMode: true,
    providerControlsVisible: false,
  });
  const hiddenComposerResult =
    await hiddenCurrentProjectComposer.api.openProjectBoundNewChat({
      projectPath: "/g/project-safe",
      projectRef: "project-safe",
      ownershipToken: "project-token",
    });
  assert.equal(hiddenComposerResult.ok, true);
  assert.equal(hiddenCurrentProjectComposer.originalControl.clicks, 0);
}

async function humanComposerRacesNeverInjectOrClearText() {
  const registrationDraft = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: `@${APP_IDENTITY.appLabel}`,
  });
  const consumedRegistrationDraft =
    registrationDraft.api.consumeRegistrationDraft(APP_IDENTITY);
  assert.equal(consumedRegistrationDraft.ok, true);
  assert.equal(consumedRegistrationDraft.owned, true);
  assert.equal(registrationDraft.composer.value, `@${APP_IDENTITY.appLabel}`);
  const registrationWakeResult = await registrationDraft.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 검수해\nmain_target_id: mtgt_registered\nmain_wake_revision: 1",
    draftMarker: "mtgt_registered",
  });
  assert.equal(registrationWakeResult, true);
  assert.equal(registrationDraft.sendButton.clicks, 1);

  const claimedAfterDurableConsume = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
    mutateDuringDelay({ milliseconds, claimDocument }) {
      if (milliseconds === 200) {
        claimDocument();
      }
    },
  });
  const claimedAfterConsumeResult =
    await claimedAfterDurableConsume.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_claim_after_consume\nwake_revision: 1",
      draftMarker: "wtgt_claim_after_consume",
    });
  assert.equal(claimedAfterConsumeResult, false);
  assert.equal(claimedAfterDurableConsume.composer.value, APP_IDENTITY.appLabel);
  assert.equal(claimedAfterDurableConsume.sendButton.clicks, 0);

  const claimedWhileWaitingForDurableSend = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: APP_IDENTITY.appLabel,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
    sendInitiallyVisible: false,
    mutateDuringDelay({ milliseconds, claimDocument }) {
      if (milliseconds === 100) {
        claimDocument();
      }
    },
  });
  const claimedWhileWaitingResult =
    await claimedWhileWaitingForDurableSend.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_claim_before_send\nwake_revision: 1",
      draftMarker: "wtgt_claim_before_send",
    });
  assert.equal(claimedWhileWaitingResult, false);
  assert.equal(
    claimedWhileWaitingForDurableSend.composer.value,
    APP_IDENTITY.appLabel,
  );
  assert.equal(claimedWhileWaitingForDurableSend.sendButton.clicks, 0);

  const staleRegistrationDraft = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: `@${APP_IDENTITY.appLabel}`,
  });
  const markedStaleRegistrationDraft =
    staleRegistrationDraft.api.consumeRegistrationDraft(APP_IDENTITY);
  assert.equal(markedStaleRegistrationDraft.owned, true);
  staleRegistrationDraft.composer.value = "human draft";
  staleRegistrationDraft.composer.value = `@${APP_IDENTITY.appLabel}`;
  const staleRegistrationWakeResult =
    await staleRegistrationDraft.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 검수해\nmain_target_id: mtgt_stale\nmain_wake_revision: 1",
      draftMarker: "mtgt_stale",
    });
  assert.equal(staleRegistrationWakeResult, false);
  assert.equal(
    staleRegistrationDraft.composer.value,
    `@${APP_IDENTITY.appLabel}`,
  );
  assert.equal(staleRegistrationDraft.sendButton.clicks, 0);

  const decoratedRegistrationDraft = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: `@${APP_IDENTITY.appLabel[0]}${APP_IDENTITY.appLabel}`,
  });
  const consumedDecoratedRegistrationDraft =
    decoratedRegistrationDraft.api.consumeRegistrationDraft(APP_IDENTITY);
  assert.equal(consumedDecoratedRegistrationDraft.ok, true);
  assert.equal(consumedDecoratedRegistrationDraft.owned, true);
  assert.equal(
    decoratedRegistrationDraft.composer.value,
    `@${APP_IDENTITY.appLabel[0]}${APP_IDENTITY.appLabel}`,
  );

  const unselectedExactDraft = makeContentRuntime({
    initialComposerText: `@${APP_IDENTITY.appLabel}`,
  });
  const preservedUnselectedExactDraft =
    unselectedExactDraft.api.consumeRegistrationDraft(APP_IDENTITY);
  assert.equal(preservedUnselectedExactDraft.ok, true);
  assert.equal(preservedUnselectedExactDraft.owned, false);
  assert.equal(
    unselectedExactDraft.composer.value,
    `@${APP_IDENTITY.appLabel}`,
  );

  const registrationHumanDraft = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: `@${APP_IDENTITY.appLabel} human draft`,
  });
  const preservedRegistrationHumanDraft =
    registrationHumanDraft.api.consumeRegistrationDraft(APP_IDENTITY);
  assert.equal(preservedRegistrationHumanDraft.ok, true);
  assert.equal(preservedRegistrationHumanDraft.owned, false);
  assert.equal(
    registrationHumanDraft.composer.value,
    `@${APP_IDENTITY.appLabel} human draft`,
  );

  const unclaimedHiddenRuntime = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    providerControlsVisible: false,
  });
  const unclaimedHiddenRuntimeResult =
    await unclaimedHiddenRuntime.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_hidden\nwake_revision: 1",
      draftMarker: "wtgt_hidden",
    });
  assert.equal(unclaimedHiddenRuntimeResult, true);
  assert.equal(unclaimedHiddenRuntime.sendButton.clicks, 1);

  const claimedHiddenRuntime = makeContentRuntime({
    mentionCandidates: false,
    pluginPickerFallback: true,
    providerControlsVisible: false,
    visibilityState: "visible",
    documentHasFocus: true,
  });
  const claimedHiddenRuntimeResult = await claimedHiddenRuntime.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_claimed\nwake_revision: 1",
    draftMarker: "wtgt_claimed",
  });
  assert.equal(claimedHiddenRuntimeResult, false);
  assert.equal(claimedHiddenRuntime.composer.value, "");
  assert.equal(claimedHiddenRuntime.sendButton.clicks, 0);

  const providerRetainedMention = makeContentRuntime({
    initialComposerText: `@${APP_IDENTITY.appLabel}`,
    mentionCandidates: false,
    pluginPickerFallback: true,
  });
  const retainedMentionResult =
    await providerRetainedMention.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_retained\nwake_revision: 1",
      draftMarker: "wtgt_retained",
    });
  assert.equal(retainedMentionResult, false);
  assert.equal(
    providerRetainedMention.composer.value,
    `@${APP_IDENTITY.appLabel}`,
  );
  assert.equal(providerRetainedMention.sendButton.clicks, 0);

  const afterMentionSelection = makeContentRuntime({
    mutateDuringDelay({ composer, milliseconds }) {
      if (milliseconds === 200) {
        composer.value = "human text after mention selection";
      }
    },
  });
  const mentionResult = await afterMentionSelection.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_race\nwake_revision: 1",
    draftMarker: "wtgt_race",
  });
  assert.equal(mentionResult, false);
  assert.equal(
    afterMentionSelection.composer.value,
    "human text after mention selection",
    "the delayed mention-selection check must preserve human text",
  );
  assert.equal(afterMentionSelection.sendButton.clicks, 0);

  for (const humanEdit of [
    " 작업해\nwake_target_ id: wtgt_edit\nwake_revision: 1",
    " 작업해\nwake_target_id:\u200B wtgt_edit\nwake_revision: 1",
    " 작업해\nWake_target_id: wtgt_edit\nwake_revision: 1",
  ]) {
    const editedRuntimeCommand = makeContentRuntime({
      sendInitiallyVisible: false,
      mutateDuringDelay({ composer, milliseconds }) {
        if (milliseconds === 100) {
          composer.value = `${APP_IDENTITY.appLabel[0]}${APP_IDENTITY.appLabel}${humanEdit}`;
        }
      },
    });
    const editedRuntimeResult = await editedRuntimeCommand.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command: " 작업해\nwake_target_id: wtgt_edit\nwake_revision: 1",
      draftMarker: "wtgt_edit",
    });
    assert.equal(editedRuntimeResult, false);
    assert.equal(editedRuntimeCommand.composer.value.endsWith(humanEdit), true);
    assert.equal(editedRuntimeCommand.sendButton.clicks, 0);
  }

  let changed = false;
  const whileWaitingForSend = makeContentRuntime({
    sendInitiallyVisible: false,
    mutateDuringDelay({ composer, milliseconds }) {
      if (milliseconds === 100 && !changed) {
        changed = true;
        composer.value = "human text while runtime waits for send";
      }
    },
  });
  const sendResult = await whileWaitingForSend.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command: " 작업해\nwake_target_id: wtgt_race\nwake_revision: 2",
    draftMarker: "wtgt_race",
  });
  assert.equal(sendResult, false);
  assert.equal(
    whileWaitingForSend.composer.value,
    "human text while runtime waits for send",
    "a failed runtime wake must never clear human text introduced meanwhile",
  );
  assert.equal(whileWaitingForSend.sendButton.clicks, 0);
}

async function oneExplicitWakeOnlyOpensItsConversationOnce() {
  const registration = workerRegistration(101);
  const target = targetForRegistration(registration, "waking", 7);
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
  });

  await runtime.api.sweepRuntimeTargets();
  await runtime.api.sweepRuntimeTargets();

  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.createCalls[0].url, registration.value.conversationUrl);
  assert.equal(runtime.maxCreateInFlight, 1);
  assert.equal(runtime.api.openInFlightCount(), 0);
}

async function pendingRetirementInFinalOpenGapPreventsRuntimeTabCreation() {
  const admitted = workerRegistration(102);
  const retiring = runtimeOwnedWorker(103);
  const target = targetForRegistration(admitted, "waking", 8);
  let inserted = false;
  const runtime = makeRuntime({
    registrations: [admitted],
    targets: [[admitted.value.wakeTargetId, target]],
    async onRuntimeOpenAdmissionRead({ storage }) {
      if (inserted) {
        return;
      }
      inserted = true;
      storage.wakeTargets[retiring.storageKey] = clone(retiring.value);
      storage.wakeTargets[retiring.storageKey].retirementPending = {
        wakeTargetId: retiring.value.wakeTargetId,
        projectId: retiring.value.projectId,
        conversationId: retiring.value.conversationId,
        runtimeOpenedTabId: retiring.value.runtimeOpenedTabId,
        markedAt: 102,
        reason: "runtime_tab_missing",
      };
    },
  });

  await runtime.api.sweepRuntimeTargets();

  assert.equal(inserted, true);
  assert.equal(runtime.createCalls.length, 0);
  assert.equal(
    runtime.fetchRequests.some(
      (request) => request.pathname === "/api/worker-provisioning/next",
    ),
    false,
  );
}

async function lowerWakeRevisionCannotReopen() {
  const registration = workerRegistration(151);
  registration.value.lastAdmittedDeliveryKey =
    "worker:" + registration.value.wakeTargetId + ":7";
  registration.value.lastAdmittedWakeRevision = 7;
  const staleTarget = targetForRegistration(registration, "waking", 6);
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, staleTarget]],
  });

  await runtime.api.sweepRuntimeTargets();

  assert.equal(
    runtime.createCalls.length,
    0,
    "a lower wake revision must never reopen an already-admitted target",
  );
}

function reboundWorkerRegistration(registration) {
  return {
    ...clone(registration.value),
    wakeTargetId: "wtgt_ffffffffffffffffffffffffffffffff",
    projectId: "prj_rebound_87654321",
    registeredAt: 99,
    lastAdmittedDeliveryKey: "replacement:admission",
    lastAdmittedWakeRevision: 99,
    lastAdmittedAt: 99,
  };
}

async function reboundRegistrationCannotContaminateDelivery() {
  const registration = workerRegistration(161);
  const target = targetForRegistration(registration, "waking", 1);
  const replacement = reboundWorkerRegistration(registration);

  const replacedBeforeDelivery = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
  });
  const oldCandidate = candidate(replacedBeforeDelivery.api, registration, target);
  replacedBeforeDelivery.storage.wakeTargets[registration.storageKey] = clone(
    replacement,
  );
  await replacedBeforeDelivery.api.deliverBackendAuthorizedRuntimeAdmission(
    oldCandidate,
  );
  assert.equal(replacedBeforeDelivery.createCalls.length, 0);
  assert.equal(
    replacedBeforeDelivery.fetchPaths.length,
    0,
    "a rebound registration must be rejected before its old target is read",
  );

  const replacementDuringDelivery = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    initialTabs: [
      {
        id: 61,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
        discarded: false,
      },
    ],
    onTabQuery: async ({ storage }) => {
      storage.wakeTargets[registration.storageKey] = clone(replacement);
    },
  });
  const candidateDuringDelivery = candidate(
    replacementDuringDelivery.api,
    registration,
    target,
  );
  await replacementDuringDelivery.api.deliverBackendAuthorizedRuntimeAdmission(
    candidateDuringDelivery,
  );
  assert.equal(replacementDuringDelivery.createCalls.length, 0);
  assert.deepEqual(
    replacementDuringDelivery.storage.wakeTargets[registration.storageKey],
    replacement,
    "a pre-record rebind must not receive the old delivery admission",
  );

  let emptyTabLookupCount = 0;
  const replacementDuringFinalEmptyLookup = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    onTabQuery: async ({ storage }) => {
      emptyTabLookupCount += 1;
      if (emptyTabLookupCount === 2) {
        storage.wakeTargets[registration.storageKey] = clone(replacement);
      }
    },
  });
  const emptyLookupCandidate = candidate(
    replacementDuringFinalEmptyLookup.api,
    registration,
    target,
  );
  await replacementDuringFinalEmptyLookup.api.deliverBackendAuthorizedRuntimeAdmission(
    emptyLookupCandidate,
  );
  assert.equal(
    emptyTabLookupCount,
    2,
    "the rebind must happen during the final no-tab lookup before create",
  );
  assert.equal(
    replacementDuringFinalEmptyLookup.createCalls.length,
    0,
    "a rebind during the final empty tab lookup must block chrome.tabs.create",
  );
  assert.deepEqual(
    replacementDuringFinalEmptyLookup.storage.wakeTargets[registration.storageKey],
    replacement,
  );

  let targetReadCount = 0;
  const replacementDuringFinalTargetRead = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    onTargetRead: async ({ storage }) => {
      targetReadCount += 1;
      if (targetReadCount === 4) {
        storage.wakeTargets[registration.storageKey] = clone(replacement);
      }
    },
  });
  const targetReadCandidate = candidate(
    replacementDuringFinalTargetRead.api,
    registration,
    target,
  );
  await replacementDuringFinalTargetRead.api.deliverBackendAuthorizedRuntimeAdmission(
    targetReadCandidate,
  );
  assert.equal(
    targetReadCount,
    4,
    "the rebind must happen during the final backend admission read before create",
  );
  assert.equal(
    replacementDuringFinalTargetRead.createCalls.length,
    0,
    "a rebind during the final backend admission read must block create",
  );
  assert.deepEqual(
    replacementDuringFinalTargetRead.storage.wakeTargets[registration.storageKey],
    replacement,
  );
}

async function claimedRuntimeCreatedTabSurvivesStaleCleanup() {
  const registration = workerRegistration(162);
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [],
    initialTabs: [
      {
        id: 71,
        url: registration.value.conversationUrl,
        active: true,
        pinned: false,
        discarded: false,
      },
    ],
  });
  runtime.api.runtimeCreatedTabIds.add(71);
  runtime.api.noteRuntimeCreatedTabActivation(71);
  runtime.patchTab(71, { active: false });
  const removed = await runtime.api.removeRuntimeCreatedTabWhenStillOwned(
    { id: 71 },
    registration.value.conversationUrl,
  );
  assert.equal(removed, false);
  assert.equal(runtime.removeCalls.length, 0);
  assert.ok(runtime.getTab(71));
}

async function concurrentCandidatesNeverExceedOneGlobalOpen() {
  const registrations = [
    workerRegistration(201),
    workerRegistration(202),
    workerRegistration(203),
  ];
  const targets = registrations.map((registration, index) => [
    registration.value.wakeTargetId,
    targetForRegistration(registration, "waking", index + 1),
  ]);
  const runtime = makeRuntime({
    registrations,
    targets,
    delayCreates: true,
  });

  const deliveries = registrations.map((registration, index) =>
    runtime.api.deliverBackendAuthorizedRuntimeAdmission(
      candidate(runtime.api, registration, targets[index][1]),
    ),
  );
  await flushMicrotasks();
  await waitFor(() => runtime.createCalls.length === 1);

  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.createInFlight, 1);
  assert.equal(runtime.maxCreateInFlight, 1);
  assert.equal(runtime.api.openInFlightCount(), 1);

  runtime.releaseCreates();
  const outcomes = await Promise.all(deliveries);
  assert.equal(outcomes.filter((outcome) => outcome.openedNewTab).length, 1);
  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.api.openInFlightCount(), 0);
}

async function recoveriesAndPersistedCooldownRemainSafe() {
  const worker = workerRegistration(301);
  const workerWake = targetForRegistration(worker, "waking", 4);
  const workerUserTab = {
    id: 41,
    url: worker.value.conversationUrl,
    active: false,
    pinned: false,
    discarded: false,
  };
  const workerRuntime = makeRuntime({
    registrations: [worker],
    targets: [[worker.value.wakeTargetId, workerWake]],
    initialTabs: [workerUserTab],
  });
  await workerRuntime.api.sweepRuntimeTargets();
  assert.equal(
    workerRuntime.createCalls.length,
    0,
    "an explicit wake delivered into a user tab must not duplicate it",
  );
  workerRuntime.removeTab(workerUserTab.id);
  const workerBusy = targetForRegistration(worker, "busy", 4);
  // The same backend delivery has moved to BUSY after the user tab disappeared.
  // It is eligible for one recovery reopen despite never being runtime-owned.
  const workerRecoveryRuntime = makeRuntime({
    registrations: [],
    targets: [[worker.value.wakeTargetId, workerBusy]],
    storage: workerRuntime.storage,
    now: 1000000,
  });
  await workerRecoveryRuntime.api.sweepRuntimeTargets();
  assert.equal(workerRecoveryRuntime.createCalls.length, 1);
  assert.equal(workerRecoveryRuntime.createCalls[0].url, worker.value.conversationUrl);

  const main = mainRegistration(302, {
    conversationUrl: "https://chatgpt.com/g/project-recovery/c/main-302",
  });
  const mainWake = targetForRegistration(main, "waking", 5);
  const mainUserTab = {
    id: 42,
    url: main.value.conversationUrl,
    active: false,
    pinned: false,
    discarded: false,
  };
  const mainRuntime = makeRuntime({
    registrations: [main],
    targets: [[main.value.mainTargetId, mainWake]],
    initialTabs: [mainUserTab],
    projectBindings: {
      [main.value.projectId]: projectBinding(main.value.projectId, {
        projectRef: "project-recovery",
      }),
    },
  });
  await mainRuntime.api.sweepRuntimeTargets();
  assert.equal(mainRuntime.createCalls.length, 0);
  mainRuntime.removeTab(mainUserTab.id);
  const mainActive = targetForRegistration(main, "active", 5);
  const mainRecoveryRuntime = makeRuntime({
    registrations: [],
    targets: [[main.value.mainTargetId, mainActive]],
    storage: mainRuntime.storage,
    now: 1000000,
    projectBindings: {
      [main.value.projectId]: projectBinding(main.value.projectId, {
        projectRef: "project-recovery",
      }),
    },
  });
  await mainRecoveryRuntime.api.sweepRuntimeTargets();
  assert.equal(mainRecoveryRuntime.createCalls.length, 1);
  assert.equal(mainRecoveryRuntime.createCalls[0].url, main.value.conversationUrl);

  const first = workerRegistration(401);
  const firstTarget = targetForRegistration(first, "waking", 1);
  const initial = makeRuntime({
    registrations: [first],
    targets: [[first.value.wakeTargetId, firstTarget]],
  });
  await initial.api.sweepRuntimeTargets();
  assert.equal(initial.createCalls.length, 1);

  const second = workerRegistration(402);
  const secondTarget = targetForRegistration(second, "waking", 1);
  initial.storage.wakeTargets[second.storageKey] = clone(second.value);
  const restarted = makeRuntime({
    registrations: [],
    targets: [
      [first.value.wakeTargetId, firstTarget],
      [second.value.wakeTargetId, secondTarget],
    ],
    storage: initial.storage,
    now: 1000000 + initial.api.MIN_RUNTIME_OPEN_INTERVAL_MS - 1,
  });

  await restarted.api.sweepRuntimeTargets();
  assert.equal(
    restarted.createCalls.length,
    0,
    "persisted cooldown must survive an extension service-worker restart",
  );
  restarted.advance(1);
  await restarted.api.sweepRuntimeTargets();
  assert.equal(restarted.createCalls.length, 1);
  assert.equal(restarted.createCalls[0].url, second.value.conversationUrl);
}

function mainRefreshFixture({
  conversationProjectRef = "project-safe",
  bindingProjectRef = "project-safe",
  bindingAppRef = "plugin_replacement",
  bindingAppLabel = "Replacement Connector",
} = {}) {
  const main = mainRegistration(501, {
    conversationUrl:
      `https://chatgpt.com/g/${conversationProjectRef}/c/main-501`,
    appIdentity: {
      appRef: "plugin_original",
      appLabel: "Original Connector",
    },
  });
  const unrelated = workerRegistration(502, {
    appIdentity: {
      appRef: "plugin_worker",
      appLabel: "Worker Connector",
    },
  });
  const mainTarget = targetForRegistration(main, "idle", 0);
  const unrelatedTarget = targetForRegistration(unrelated, "idle", 0);
  const claimedMainTab = {
    id: 501,
    url: main.value.conversationUrl,
    active: true,
    pinned: false,
    discarded: false,
  };
  const runtime = makeRuntime({
    registrations: [main, unrelated],
    targets: [
      [main.value.mainTargetId, mainTarget],
      [unrelated.value.wakeTargetId, unrelatedTarget],
    ],
    projectBindings: {
      [main.value.projectId]: projectBinding(main.value.projectId, {
        projectRef: bindingProjectRef,
        appRef: bindingAppRef,
        appLabel: bindingAppLabel,
      }),
    },
    initialTabs: [claimedMainTab],
  });
  return { runtime, main, unrelated };
}

async function existingMainRefreshesFromCanonicalProjectBinding() {
  const { runtime, main } = mainRefreshFixture();

  await runtime.api.sweepRuntimeTargets();

  assert.deepEqual(
    runtime.storage.wakeTargets[main.storageKey].appIdentity,
    {
      appRef: "plugin_replacement",
      appLabel: "Replacement Connector",
    },
    "an existing Main must adopt the connector selected by its canonical localhost binding",
  );
}

async function existingMainRefreshReadsItsCanonicalProjectBinding() {
  const { runtime, main } = mainRefreshFixture();

  await runtime.api.sweepRuntimeTargets();

  assert.ok(
    runtime.fetchPaths.includes(
      `/api/project-bindings/${main.value.projectId}/connector`,
    ),
    "automatic Main refresh must read the deliberate localhost binding update",
  );
}

async function mainBindingRefreshChangesAppIdentityOnly() {
  const { runtime, main } = mainRefreshFixture();
  const before = clone(runtime.storage.wakeTargets[main.storageKey]);

  await runtime.api.sweepRuntimeTargets();

  assert.deepEqual(runtime.storage.wakeTargets[main.storageKey], {
    ...before,
    appIdentity: {
      appRef: "plugin_replacement",
      appLabel: "Replacement Connector",
    },
    bindingRevision: 2,
    connectorEpoch: 2,
  });
  assert.equal(runtime.createCalls.length, 0);
  assert.equal(runtime.tabMessageCalls.length, 0);
}

async function bindingRefreshPreservesUnrelatedRegistration() {
  const { runtime, unrelated } = mainRefreshFixture();
  const before = clone(runtime.storage.wakeTargets[unrelated.storageKey]);

  await runtime.api.sweepRuntimeTargets();

  assert.deepEqual(runtime.storage.wakeTargets[unrelated.storageKey], before);
  assert.equal(runtime.createCalls.length, 0);
  assert.equal(runtime.tabMessageCalls.length, 0);
  assert.equal(
    runtime.fetchPaths.some((pathname) => pathname.endsWith("/register")),
    false,
  );
}

async function bindingRefreshNeverCreatesUnregisteredMain() {
  const worker = workerRegistration(503);
  const runtime = makeRuntime({
    registrations: [worker],
    targets: [[worker.value.wakeTargetId, targetForRegistration(worker, "idle", 0)]],
    projectBindings: {
      [worker.value.projectId]: projectBinding(worker.value.projectId),
    },
  });

  await runtime.api.sweepRuntimeTargets();

  assert.equal(
    Object.values(runtime.storage.wakeTargets).filter(
      (registration) => registration.kind === "main",
    ).length,
    0,
  );
  assert.equal(runtime.createCalls.length, 0);
  assert.equal(
    runtime.fetchPaths.some((pathname) => pathname.endsWith("/register")),
    false,
  );
}

async function bindingRefreshFailsClosedOnProjectMismatchOrMissingApp() {
  for (const fixture of [
    mainRefreshFixture({ bindingProjectRef: "different-project" }),
    mainRefreshFixture({ bindingAppRef: "", bindingAppLabel: "" }),
  ]) {
    const { runtime, main } = fixture;
    const before = clone(runtime.storage.wakeTargets[main.storageKey]);

    await runtime.api.sweepRuntimeTargets();

    assert.deepEqual(runtime.storage.wakeTargets[main.storageKey], before);
    assert.equal(runtime.createCalls.length, 0);
    assert.equal(runtime.tabMessageCalls.length, 0);
  }
}

async function pollTargetRefreshesMainBeforeReturningWakingTarget() {
  const main = mainRegistration(504, {
    conversationUrl: "https://chatgpt.com/g/project-safe/c/main-504",
    appIdentity: {
      appRef: "plugin_original",
      appLabel: "Original Connector",
    },
    bindingRevision: 1,
    connectorEpoch: 1,
    runtimeOpenedTabId: 504,
  });
  const target = targetForRegistration(main, "waking", 3);
  let observedStoredIdentityBeforeTargetRead = null;
  const runtime = makeRuntime({
    registrations: [main],
    targets: [[main.value.mainTargetId, target]],
    projectBindings: {
      [main.value.projectId]: projectBinding(main.value.projectId),
    },
    onTargetRead({ pathname, storage }) {
      if (pathname.endsWith(`/${main.value.mainTargetId}`)) {
        observedStoredIdentityBeforeTargetRead = clone(
          storage.wakeTargets[main.storageKey].appIdentity,
        );
      }
    },
  });

  const result = await runtime.api.pollTarget(
    main.value.conversationUrl,
    senderTabForRegistration(main, 504),
  );

  const replacementIdentity = {
    appRef: "plugin_replacement",
    appLabel: "Replacement Connector",
  };
  assert.deepEqual(clone(result.registration.appIdentity), replacementIdentity);
  assert.deepEqual(
    observedStoredIdentityBeforeTargetRead,
    replacementIdentity,
    "the content poll must persist binding B before exposing its WAKING target",
  );
  assert.deepEqual(clone(result.target), target);
}

async function delayedOlderBindingCannotOverwriteNewerStoredBinding() {
  const main = mainRegistration(505, {
    conversationUrl: "https://chatgpt.com/g/project-safe/c/main-505",
    appIdentity: {
      appRef: "plugin_original",
      appLabel: "Original Connector",
    },
    bindingRevision: 0,
    connectorEpoch: 0,
    runtimeOpenedTabId: 505,
  });
  const target = targetForRegistration(main, "waking", 4);
  let bindingReadStarted = false;
  let releaseBindingRead;
  const bindingReadGate = new Promise((resolve) => {
    releaseBindingRead = resolve;
  });
  const runtime = makeRuntime({
    registrations: [main],
    targets: [[main.value.mainTargetId, target]],
    projectBindings: {
      [main.value.projectId]: projectBinding(main.value.projectId, {
        appRef: "plugin_epoch_1",
        appLabel: "Epoch One Connector",
        connectorEpoch: 1,
        revision: 1,
      }),
    },
    async onProjectBindingRead() {
      bindingReadStarted = true;
      await bindingReadGate;
    },
  });

  const delayedPoll = runtime.api.pollTarget(
    main.value.conversationUrl,
    senderTabForRegistration(main, 505),
  );
  await waitFor(() => bindingReadStarted);
  const epochTwoRegistration = {
    ...clone(runtime.storage.wakeTargets[main.storageKey]),
    appIdentity: {
      appRef: "plugin_epoch_2",
      appLabel: "Epoch Two Connector",
    },
    bindingRevision: 2,
    connectorEpoch: 2,
  };
  runtime.storage.wakeTargets[main.storageKey] = clone(epochTwoRegistration);
  releaseBindingRead();
  await delayedPoll;

  assert.deepEqual(
    runtime.storage.wakeTargets[main.storageKey],
    epochTwoRegistration,
    "a delayed epoch-1 response must not roll back an already stored epoch-2 registration",
  );
}

async function workerSameRefLabelRefreshesBeforeWakeDelivery() {
  const worker = workerRegistration(506, {
    appIdentity: {
      appRef: "plugin_stable_worker",
      appLabel: "Old Connector",
    },
    bindingRevision: 1,
    connectorEpoch: 2,
    runtimeOpenedTabId: 506,
  });
  const target = targetForRegistration(worker, "waking", 3);
  const runtime = makeRuntime({
    registrations: [worker],
    targets: [[worker.value.wakeTargetId, target]],
    projectBindings: {
      [worker.value.projectId]: projectBinding(worker.value.projectId, {
        appRef: "plugin_stable_worker",
        appLabel: "New Connector",
        connectorEpoch: 2,
        revision: 2,
      }),
    },
  });

  const result = await runtime.api.pollTarget(
    worker.value.conversationUrl,
    senderTabForRegistration(worker, 506),
  );

  assert.deepEqual(clone(result.registration.appIdentity), {
    appRef: "plugin_stable_worker",
    appLabel: "New Connector",
  });
  assert.equal(result.registration.bindingRevision, 2);
  assert.equal(result.registration.connectorEpoch, 2);
  assert.deepEqual(
    clone(runtime.storage.wakeTargets[worker.storageKey]),
    {
      ...worker.value,
      appIdentity: {
        appRef: "plugin_stable_worker",
        appLabel: "New Connector",
      },
      bindingRevision: 2,
      connectorEpoch: 2,
    },
    "label refresh must not change target, admission, or runtime ownership fields",
  );
  assert.equal(runtime.createCalls.length, 0, "refresh must not open another Chat");
  assert.equal(
    runtime.fetchPaths.some((pathname) => pathname.endsWith("/register")),
    false,
    "refresh must not register another backend target",
  );

  const content = makeContentRuntime({
    appIdentity: result.registration.appIdentity,
    inlineSelectedApp: true,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });
  const sent = await content.api.sendAppCommand({
    appIdentity: result.registration.appIdentity,
    command: " 작업해\nwake_target_id: " + worker.value.wakeTargetId + "\nwake_revision: 3",
    draftMarker: worker.value.wakeTargetId,
  });
  assert.equal(sent, true);
  assert.equal(content.sendButton.clicks, 1, "the actual wake send must use the new label");
}

async function workerBindingMismatchFailsClosed() {
  const fixtures = [
    {
      name: "app ref",
      binding: { appRef: "plugin_other_worker", connectorEpoch: 2 },
      targetPatch: {},
    },
    {
      name: "connector epoch",
      binding: { appRef: "plugin_stable_worker", connectorEpoch: 3 },
      targetPatch: {},
    },
    {
      name: "project",
      binding: { appRef: "plugin_stable_worker", connectorEpoch: 2 },
      targetPatch: { project_id: "prj_other_deadbeef" },
    },
  ];
  for (const [index, fixture] of fixtures.entries()) {
    const worker = workerRegistration(510 + index, {
      appIdentity: {
        appRef: "plugin_stable_worker",
        appLabel: "Old Connector",
      },
      bindingRevision: 1,
      connectorEpoch: 2,
      runtimeOpenedTabId: 510 + index,
    });
    const target = {
      ...targetForRegistration(worker, "waking", 4),
      ...fixture.targetPatch,
    };
    const runtime = makeRuntime({
      registrations: [worker],
      targets: [[worker.value.wakeTargetId, target]],
      projectBindings: {
        [worker.value.projectId]: projectBinding(
          worker.value.projectId,
          fixture.binding,
        ),
      },
    });

    const result = await runtime.api.pollTarget(
      worker.value.conversationUrl,
      senderTabForRegistration(worker, 510 + index),
    );

    assert.deepEqual(clone(result), { registration: null, target: null }, fixture.name);
    assert.equal(runtime.createCalls.length, 0, fixture.name);
    assert.equal(runtime.tabMessageCalls.length, 0, fixture.name);
  }
}

async function delayedOlderWorkerBindingCannotOverwriteNewerState() {
  const worker = workerRegistration(520, {
    appIdentity: {
      appRef: "plugin_stable_worker",
      appLabel: "Old Connector",
    },
    bindingRevision: 1,
    connectorEpoch: 2,
    runtimeOpenedTabId: 520,
  });
  const target = targetForRegistration(worker, "waking", 5);
  let bindingReadStarted = false;
  let releaseBindingRead;
  const bindingReadGate = new Promise((resolve) => {
    releaseBindingRead = resolve;
  });
  const runtime = makeRuntime({
    registrations: [worker],
    targets: [[worker.value.wakeTargetId, target]],
    projectBindings: {
      [worker.value.projectId]: projectBinding(worker.value.projectId, {
        appRef: "plugin_stable_worker",
        appLabel: "Delayed Connector",
        connectorEpoch: 2,
        revision: 2,
      }),
    },
    async onProjectBindingRead() {
      bindingReadStarted = true;
      await bindingReadGate;
    },
  });

  const delayedPoll = runtime.api.pollTarget(
    worker.value.conversationUrl,
    senderTabForRegistration(worker, 520),
  );
  await waitFor(() => bindingReadStarted);
  const newerRegistration = {
    ...clone(runtime.storage.wakeTargets[worker.storageKey]),
    appIdentity: {
      appRef: "plugin_stable_worker",
      appLabel: "Newest Connector",
    },
    bindingRevision: 3,
    connectorEpoch: 2,
  };
  runtime.storage.wakeTargets[worker.storageKey] = clone(newerRegistration);
  releaseBindingRead();
  const result = await delayedPoll;

  assert.deepEqual(runtime.storage.wakeTargets[worker.storageKey], newerRegistration);
  assert.deepEqual(clone(result.registration), newerRegistration);
}

function runtimeOwnedWorker(index, patch = {}) {
  const registration = workerRegistration(index, {
    runtimeOpenedTabId: 7000 + index,
    runtimeOpenedAt: 100,
    lastAttemptRevision: 0,
    ...patch,
  });
  registration.value.runtimeOwnedRegistrationDraft = {
    tabId: registration.value.runtimeOpenedTabId,
    conversationId: registration.value.conversationId,
    draft: `@${APP_IDENTITY.appLabel}`,
    appRef: APP_IDENTITY.appRef,
    appLabel: APP_IDENTITY.appLabel,
  };
  return registration;
}

function provisionedConnectorProofFor(registration, patch = {}) {
  return {
    requestId: "prq_" + String(9000).padStart(32, "0"),
    wakeTargetId: registration.value.wakeTargetId,
    conversationId: registration.value.conversationId,
    admissionRevision: 1,
    projectId: registration.value.projectId,
    provider: "chatgpt",
    projectRef: "project-safe",
    projectPath: "/g/project-safe",
    appRef: APP_IDENTITY.appRef,
    appLabel: APP_IDENTITY.appLabel,
    bindingRevision: 2,
    connectorEpoch: 2,
    finalizedAt: 10,
    ...patch,
  };
}

async function missingEpochRequiresExactProvisionedProof() {
  const proven = runtimeOwnedWorker(530, {
    appIdentity: APP_IDENTITY,
    bindingRevision: 2,
    connectorEpoch: 2,
    lastAttemptRevision: 7,
    lastAttemptAt: 20,
  });
  proven.value.provisionedConnectorProof = provisionedConnectorProofFor(proven);
  const provenTarget = targetForRegistration(proven, "waking", 1);
  delete provenTarget.registered_connector_epoch;
  const binding = projectBinding(proven.value.projectId, {
    appRef: APP_IDENTITY.appRef,
    appLabel: APP_IDENTITY.appLabel,
  });
  const accepted = makeRuntime({
    registrations: [proven],
    targets: [[proven.value.wakeTargetId, provenTarget]],
    projectBindings: { [proven.value.projectId]: binding },
    now: 99_000_000,
  });
  const acceptedResult = await accepted.api.pollTarget(
    proven.value.conversationUrl,
    senderTabForRegistration(proven, 530),
  );
  assert.equal(acceptedResult.registration.wakeTargetId, proven.value.wakeTargetId);
  assert.equal(accepted.storage.wakeTargets[proven.storageKey].retirementPending, undefined);
  accepted.storage.wakeTargets[proven.storageKey].runtimeOpenedTabId = null;
  accepted.storage.wakeTargets[proven.storageKey].runtimeOpenedAt = 0;
  accepted.storage.wakeTargets[proven.storageKey].runtimeOwnedRegistrationDraft = null;
  accepted.storage.wakeTargets[proven.storageKey].runtimeOpenedTabId =
    proven.value.runtimeOpenedTabId;
  accepted.storage.wakeTargets[proven.storageKey].runtimeOpenedAt = 99_000_001;
  const reusedResult = await accepted.api.pollTarget(
    proven.value.conversationUrl,
    senderTabForRegistration(proven, 530),
  );
  assert.equal(
    reusedResult.registration.wakeTargetId,
    proven.value.wakeTargetId,
    "immutable provisioned proof must remain valid after first attempt and parking",
  );

  for (const [name, epochValue] of [["missing", undefined], ["null", null], ["unequal", 3]]) {
    const rejected = runtimeOwnedWorker(531 + ["missing", "null", "unequal"].indexOf(name), {
      appIdentity: APP_IDENTITY,
      bindingRevision: 2,
      connectorEpoch: 2,
    });
    if (name !== "missing") {
      rejected.value.provisionedConnectorProof = provisionedConnectorProofFor(rejected);
    }
    const target = targetForRegistration(rejected, "waking", 1);
    if (name === "missing") {
      delete target.registered_connector_epoch;
    } else {
      target.registered_connector_epoch = epochValue;
    }
    const runtime = makeRuntime({
      registrations: [rejected],
      targets: [[rejected.value.wakeTargetId, target]],
      projectBindings: {
        [rejected.value.projectId]: projectBinding(rejected.value.projectId, {
          appRef: APP_IDENTITY.appRef,
          appLabel: APP_IDENTITY.appLabel,
        }),
      },
      onRetireBatchRequest() {
        return errorResponse(400, "wake_target_conflict");
      },
    });
    const result = await runtime.api.pollTarget(
      rejected.value.conversationUrl,
      senderTabForRegistration(rejected, 531 + ["missing", "null", "unequal"].indexOf(name)),
    );
    assert.deepEqual(clone(result), { registration: null, target: null }, name);
    assert.equal(
      runtime.storage.wakeTargets[rejected.storageKey].retirementPending.reason,
      name === "missing"
        ? "missing_registered_connector_epoch"
        : "connector_binding_mismatch",
      name,
    );
  }

  const changed = runtimeOwnedWorker(535, {
    appIdentity: APP_IDENTITY,
    bindingRevision: 2,
    connectorEpoch: 2,
    lastAttemptRevision: 7,
    runtimeOpenedTabId: null,
    runtimeOpenedAt: 0,
  });
  changed.value.runtimeOwnedRegistrationDraft = null;
  changed.value.provisionedConnectorProof = provisionedConnectorProofFor(changed);
  const changedTarget = targetForRegistration(changed, "waking", 8);
  delete changedTarget.registered_connector_epoch;
  const changedRuntime = makeRuntime({
    registrations: [changed],
    targets: [[changed.value.wakeTargetId, changedTarget]],
    projectBindings: {
      [changed.value.projectId]: projectBinding(changed.value.projectId, {
        appRef: APP_IDENTITY.appRef,
        appLabel: "Renamed Connector",
      }),
    },
    onRetireBatchRequest() {
      return errorResponse(400, "wake_target_conflict");
    },
  });
  const changedResult = await changedRuntime.api.pollTarget(
    changed.value.conversationUrl,
    senderTabForRegistration(changed, 535),
  );
  assert.deepEqual(clone(changedResult), { registration: null, target: null });
  assert.equal(
    changedRuntime.storage.wakeTargets[changed.storageKey].retirementPending.reason,
    "missing_registered_connector_epoch",
  );
  assert.equal(
    changedRuntime.fetchRequests.some((request) =>
      request.pathname === "/api/wake/targets/retire-batch" &&
        request.body?.targets?.some(
          (target) => target.wake_target_id === changed.value.wakeTargetId,
        ),
    ),
    false,
    "a WAKING mismatch must remain durably marked until it becomes IDLE",
  );
}

function errorResponse(status, errorCode, message = errorCode) {
  return {
    ok: false,
    status,
    async json() {
      return { ok: false, error_code: errorCode, message };
    },
  };
}

async function durableWorkerRetirementSurvivesRestart() {
  const registration = runtimeOwnedWorker(601);
  const target = targetForRegistration(registration, "idle", 7);
  let markerObservedBeforeNetwork = false;
  const first = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    initialTabs: [{
      id: registration.value.runtimeOpenedTabId,
      url: registration.value.conversationUrl,
      active: true,
      pinned: false,
    }],
    async onRetireBatchRequest({ storage }) {
      markerObservedBeforeNetwork = Boolean(
        storage.wakeTargets[registration.storageKey]?.retirementPending,
      );
      return errorResponse(400, "wake_target_conflict");
    },
  });

  await first.api.transferRuntimeOwnership(registration.value.runtimeOpenedTabId);
  assert.equal(markerObservedBeforeNetwork, true);
  assert.equal(first.removeCalls.length, 0, "retirement must never close the human tab");
  assert.ok(first.storage.wakeTargets[registration.storageKey].retirementPending);

  const restarted = makeRuntime({
    registrations: [],
    storage: first.storage,
    targets: [[registration.value.wakeTargetId, target]],
  });
  await restarted.api.sweepRuntimeTargets();
  assert.equal(restarted.storage.wakeTargets[registration.storageKey], undefined);
  assert.equal(
    restarted.fetchRequests.some(
      (request) =>
        request.pathname.endsWith("/runtime-heartbeat") &&
        request.pathname.includes(registration.value.wakeTargetId),
    ),
    false,
    "a marked registration must not emit a runtime heartbeat",
  );
  const retireRequest = restarted.fetchRequests.find((request) =>
    request.pathname === "/api/wake/targets/retire-batch",
  );
  assert.deepEqual(retireRequest.body, {
    project_id: registration.value.projectId,
    targets: [{
      wake_target_id: registration.value.wakeTargetId,
      expected_revision: 7,
    }],
  });

  const alreadyMissing = runtimeOwnedWorker(608);
  const missingRuntime = makeRuntime({ registrations: [alreadyMissing], targets: [] });
  await missingRuntime.api.clearRemovedRuntimeTab(
    alreadyMissing.value.runtimeOpenedTabId,
  );
  assert.ok(
    missingRuntime.storage.wakeTargets[alreadyMissing.storageKey].retirementPending,
    "missing target without a durable revision must remain fail-closed",
  );
  assert.equal(
    missingRuntime.fetchRequests.some((request) => request.pathname.endsWith("/retire")),
    false,
    "a not-found target without a saved revision must not guess a retirement",
  );
}

async function concurrentRetirementDeletesOnlyExactRegistration() {
  const registration = runtimeOwnedWorker(602);
  const target = targetForRegistration(registration, "idle", 3);
  let retireCalls = 0;
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    async onRetireBatchRequest({ body, storage }) {
      retireCalls += 1;
      storage.wakeTargets[registration.storageKey] = {
        ...clone(storage.wakeTargets[registration.storageKey]),
        wakeTargetId: "wtgt_rebound_000000000000000000000000000",
        registeredAt: 999,
        retirementPending: null,
      };
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            ok: true,
            project_id: body.project_id,
            retired_wake_target_ids: body.targets.map(
              (target) => target.wake_target_id,
            ),
          };
        },
      };
    },
  });

  await Promise.all([
    runtime.api.transferRuntimeOwnership(registration.value.runtimeOpenedTabId),
    runtime.api.transferRuntimeOwnership(registration.value.runtimeOpenedTabId),
  ]);
  assert.equal(retireCalls, 1, "concurrent ownership loss must share one retirement");
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].wakeTargetId,
    "wtgt_rebound_000000000000000000000000000",
    "retirement completion must not delete a rebound registration",
  );

  const pollingRegistration = runtimeOwnedWorker(609);
  const pollingTarget = targetForRegistration(pollingRegistration, "idle", 4);
  let releasePollRead;
  const pollReadGate = new Promise((resolve) => { releasePollRead = resolve; });
  let releaseRetire;
  const retireGate = new Promise((resolve) => { releaseRetire = resolve; });
  let pollReadStarted = false;
  let firstTargetRead = true;
  const pollingRuntime = makeRuntime({
    registrations: [pollingRegistration],
    targets: [[pollingRegistration.value.wakeTargetId, pollingTarget]],
    async onTargetRead() {
      if (!firstTargetRead) {
        return;
      }
      firstTargetRead = false;
      pollReadStarted = true;
      await pollReadGate;
    },
    async onRetireBatchRequest({ body }) {
      await retireGate;
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            ok: true,
            project_id: body.project_id,
            retired_wake_target_ids: body.targets.map(
              (target) => target.wake_target_id,
            ),
          };
        },
      };
    },
  });
  const delayedPoll = pollingRuntime.api.pollTarget(
    pollingRegistration.value.conversationUrl,
    senderTabForRegistration(pollingRegistration, 609),
  );
  await waitFor(() => pollReadStarted);
  const concurrentRetirement = pollingRuntime.api.transferRuntimeOwnership(
    pollingRegistration.value.runtimeOpenedTabId,
  );
  await waitFor(() =>
    Boolean(
      pollingRuntime.storage.wakeTargets[pollingRegistration.storageKey]
        ?.retirementPending,
    ),
  );
  releasePollRead();
  releaseRetire();
  const [pollResult] = await Promise.all([delayedPoll, concurrentRetirement]);
  assert.deepEqual(
    clone(pollResult),
    { registration: null, target: null },
    "a poll started before ownership loss must fail closed after the durable marker",
  );
}

async function batchRetirementDrainBlocksProvisioningUntilExactSuccess() {
  const first = runtimeOwnedWorker(611);
  const second = runtimeOwnedWorker(612);
  const unrelated = workerRegistration(613);
  for (const [registration, markedAt] of [[first, 101], [second, 102]]) {
    registration.value.retirementPending = {
      wakeTargetId: registration.value.wakeTargetId,
      projectId: registration.value.projectId,
      conversationId: registration.value.conversationId,
      runtimeOpenedTabId: registration.value.runtimeOpenedTabId,
      markedAt,
      reason: "runtime_tab_missing",
    };
  }
  const targets = [first, second].map((registration, index) => [
    registration.value.wakeTargetId,
    targetForRegistration(registration, "idle", index + 7),
  ]);
  let releaseBatch;
  const batchGate = new Promise((resolve) => { releaseBatch = resolve; });
  let batchBody = null;
  let batchSucceeded = false;
  const runtime = makeRuntime({
    registrations: [first, second, unrelated],
    targets,
    async onRetireBatchRequest({ body }) {
      batchBody = clone(body);
      await batchGate;
      batchSucceeded = true;
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            ok: true,
            project_id: body.project_id,
            retired_wake_target_ids: body.targets.map(
              (target) => target.wake_target_id,
            ),
          };
        },
      };
    },
  });

  const sweep = runtime.api.sweepRuntimeTargets();
  await waitFor(() => batchBody !== null);

  assert.equal(batchSucceeded, false);
  assert.equal(
    runtime.fetchRequests.filter(
      (request) => request.pathname === "/api/wake/targets/retire-batch",
    ).length,
    1,
    "one project drain must issue exactly one batch request",
  );
  assert.equal(
    runtime.fetchRequests.some(
      (request) => request.pathname === "/api/worker-provisioning/next",
    ),
    false,
    "pending batch retirement must block provisioning admission",
  );
  assert.equal(runtime.createCalls.length, 0);
  assert.ok(runtime.storage.wakeTargets[first.storageKey]);
  assert.ok(runtime.storage.wakeTargets[second.storageKey]);
  assert.deepEqual(batchBody, {
    project_id: first.value.projectId,
    targets: [first, second]
      .sort((left, right) =>
        left.value.wakeTargetId.localeCompare(right.value.wakeTargetId),
      )
      .map((registration, index) => ({
        wake_target_id: registration.value.wakeTargetId,
        expected_revision: index + 7,
      })),
  });

  releaseBatch();
  await sweep;

  assert.equal(runtime.storage.wakeTargets[first.storageKey], undefined);
  assert.equal(runtime.storage.wakeTargets[second.storageKey], undefined);
  assert.deepEqual(
    runtime.storage.wakeTargets[unrelated.storageKey],
    unrelated.value,
    "batch success must delete only exact marked registrations",
  );

  const replay = runtimeOwnedWorker(614);
  replay.value.retirementPending = {
    wakeTargetId: replay.value.wakeTargetId,
    projectId: replay.value.projectId,
    conversationId: replay.value.conversationId,
    runtimeOpenedTabId: replay.value.runtimeOpenedTabId,
    markedAt: 103,
    expectedRevision: 11,
    reason: "runtime_tab_missing",
  };
  let replayBody = null;
  const restarted = makeRuntime({
    registrations: [replay],
    targets: [],
    async onRetireBatchRequest({ body }) {
      replayBody = clone(body);
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            ok: true,
            project_id: body.project_id,
            retired_wake_target_ids: [replay.value.wakeTargetId],
          };
        },
      };
    },
  });

  await restarted.api.sweepRuntimeTargets();

  assert.deepEqual(replayBody, {
    project_id: replay.value.projectId,
    targets: [{
      wake_target_id: replay.value.wakeTargetId,
      expected_revision: 11,
    }],
  });
  assert.equal(restarted.storage.wakeTargets[replay.storageKey], undefined);
  assert.equal(restarted.createCalls.length, 0);
}

async function batchRetirementRefreshesRevisionOnlyForExactIdleMarker() {
  const registration = runtimeOwnedWorker(615);
  registration.value.retirementPending = {
    wakeTargetId: registration.value.wakeTargetId,
    projectId: registration.value.projectId,
    conversationId: registration.value.conversationId,
    runtimeOpenedTabId: registration.value.runtimeOpenedTabId,
    markedAt: 104,
    reason: "runtime_tab_missing",
  };
  const target = targetForRegistration(registration, "waking", 7);
  let batchCalls = 0;
  let batchBody = null;
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    async onRetireBatchRequest({ body }) {
      batchCalls += 1;
      if (target.status !== "idle") {
        return errorResponse(400, "wake_target_conflict");
      }
      batchBody = clone(body);
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            ok: true,
            project_id: body.project_id,
            retired_wake_target_ids: [registration.value.wakeTargetId],
          };
        },
      };
    },
  });

  assert.equal(await runtime.api.resumePendingWorkerRetirements(), false);
  assert.equal(batchCalls, 0, "WAKING targets must not reach batch retirement");
  assert.equal(batchBody, null, "WAKING targets must not reach batch retirement");
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey].retirementPending
      .expectedRevision,
    undefined,
    "a transient WAKING revision must not become the durable retirement CAS",
  );

  target.status = "idle";
  target.revision = 8;
  target.wake_revision = 8;
  assert.equal(await runtime.api.resumePendingWorkerRetirements(), true);
  assert.deepEqual(batchBody, {
    project_id: registration.value.projectId,
    targets: [{
      wake_target_id: registration.value.wakeTargetId,
      expected_revision: 8,
    }],
  });
  assert.equal(runtime.storage.wakeTargets[registration.storageKey], undefined);

  const stale = runtimeOwnedWorker(616);
  stale.value.retirementPending = {
    wakeTargetId: stale.value.wakeTargetId,
    projectId: stale.value.projectId,
    conversationId: stale.value.conversationId,
    runtimeOpenedTabId: stale.value.runtimeOpenedTabId,
    markedAt: 105,
    expectedRevision: 11,
    reason: "runtime_tab_missing",
  };
  let refreshedBody = null;
  const refreshed = makeRuntime({
    registrations: [stale],
    targets: [[
      stale.value.wakeTargetId,
      targetForRegistration(stale, "idle", 12),
    ]],
    async onRetireBatchRequest({ body }) {
      refreshedBody = clone(body);
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            ok: true,
            project_id: body.project_id,
            retired_wake_target_ids: [stale.value.wakeTargetId],
          };
        },
      };
    },
  });
  assert.equal(await refreshed.api.resumePendingWorkerRetirements(), true);
  assert.equal(refreshedBody.targets[0].expected_revision, 12);
  assert.equal(refreshed.storage.wakeTargets[stale.storageKey], undefined);

  const changed = runtimeOwnedWorker(617);
  changed.value.retirementPending = {
    wakeTargetId: changed.value.wakeTargetId,
    projectId: changed.value.projectId,
    conversationId: changed.value.conversationId,
    runtimeOpenedTabId: changed.value.runtimeOpenedTabId,
    markedAt: 106,
    expectedRevision: 13,
    reason: "runtime_tab_missing",
  };
  let changedBatchCalls = 0;
  const changedRuntime = makeRuntime({
    registrations: [changed],
    targets: [[
      changed.value.wakeTargetId,
      targetForRegistration(changed, "idle", 14),
    ]],
    onTargetRead({ storage }) {
      storage.wakeTargets[changed.storageKey].retirementPending.markedAt = 999;
    },
    async onRetireBatchRequest() {
      changedBatchCalls += 1;
      throw new Error("a changed marker must not reach batch retirement");
    },
  });
  assert.equal(await changedRuntime.api.resumePendingWorkerRetirements(), false);
  assert.equal(changedBatchCalls, 0);
  assert.equal(
    changedRuntime.storage.wakeTargets[changed.storageKey].retirementPending
      .expectedRevision,
    13,
    "a concurrent marker change must reject the stale revision refresh",
  );
}

async function retirementPreservesMainAndUserManagedWorkers() {
  const active = runtimeOwnedWorker(603);
  const pinned = runtimeOwnedWorker(604);
  const human = workerRegistration(605, {
    runtimeOpenedTabId: 7605,
    runtimeOpenedAt: 100,
    lastAttemptRevision: 0,
  });
  const main = mainRegistration(606, {
    runtimeOpenedTabId: 7606,
    runtimeOpenedAt: 100,
  });
  const registrations = [active, pinned, human, main];
  const runtime = makeRuntime({
    registrations,
    targets: registrations
      .filter((item) => item.value.kind === "worker")
      .map((item) => [item.value.wakeTargetId, targetForRegistration(item, "idle", 0)]),
    initialTabs: [
      { id: active.value.runtimeOpenedTabId, url: active.value.conversationUrl, active: true, pinned: false },
      { id: pinned.value.runtimeOpenedTabId, url: pinned.value.conversationUrl, active: false, pinned: true },
      { id: human.value.runtimeOpenedTabId, url: human.value.conversationUrl, active: true, pinned: false },
      { id: main.value.runtimeOpenedTabId, url: main.value.conversationUrl, active: true, pinned: false },
    ],
  });

  for (const item of registrations) {
    await runtime.api.transferRuntimeOwnership(item.value.runtimeOpenedTabId);
  }
  assert.deepEqual(runtime.removeCalls, []);
  assert.deepEqual(runtime.tabMessageCalls, [], "retirement must not touch composers");
  assert.equal(runtime.storage.wakeTargets[active.storageKey], undefined);
  assert.equal(runtime.storage.wakeTargets[pinned.storageKey], undefined);
  assert.ok(runtime.storage.wakeTargets[human.storageKey]);
  assert.equal(runtime.storage.wakeTargets[human.storageKey].retirementPending, undefined);
  assert.ok(runtime.storage.wakeTargets[main.storageKey]);
  assert.equal(runtime.storage.wakeTargets[main.storageKey].retirementPending, undefined);
}

async function pendingRetirementSuppressesPollingAndHeartbeat() {
  const registration = runtimeOwnedWorker(607, {
    retirementPending: {
      wakeTargetId: "wtgt_00000000000000000000000000000607",
      projectId: "prj_runtime_12345678",
      conversationId: "worker-607",
      runtimeOpenedTabId: 7607,
      markedAt: 99,
    },
  });
  const target = targetForRegistration(registration, "idle", 2);
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    onRetireBatchRequest() {
      return errorResponse(400, "wake_target_conflict");
    },
  });

  const polled = await runtime.api.pollTarget(
    registration.value.conversationUrl,
    senderTabForRegistration(registration, 607),
  );
  await runtime.api.sweepRuntimeTargets();
  assert.deepEqual(clone(polled), { registration: null, target: null });
  assert.equal(
    runtime.fetchRequests.some((request) => request.pathname.endsWith("/runtime-heartbeat")),
    false,
  );
  assert.ok(runtime.storage.wakeTargets[registration.storageKey].retirementPending);
}

const CONTENT_DISPOSITION_CODES = Object.freeze({
  POLL_RECEIVED: "poll_received",
  EXACT_CURRENT_ENTRY: "exact_current_entry",
});

function storedContentDisposition(runtime, registration) {
  return runtime.storage.wakeContentDisposition?.[registration.storageKey];
}

function assertPrivacySafeContentDispositionMessage(message, expected) {
  assert.deepEqual(Object.keys(message).sort(), [
    "code",
    "conversationUrl",
    "delivery",
    "type",
  ]);
  assert.deepEqual(clone(message), {
    type: "wake:recordContentDisposition",
    conversationUrl: expected.conversationUrl,
    delivery: expected.delivery,
    code: expected.code,
  });
  for (const forbidden of [
    "draft",
    "text",
    "composer",
    "token",
    "path",
    "hash",
    "length",
  ]) {
    assert.equal(
      JSON.stringify(message).toLowerCase().includes(`\"${forbidden}`),
      false,
      `content disposition telemetry must not contain ${forbidden}`,
    );
  }
}

async function contentDispositionMessagesAreExactAndPreReserve() {
  const fixture = staleWakeCleanupFixture(621, { targetRevision: 34 });
  const conversationUrl = fixture.registration.value.conversationUrl;
  const pollMessages = [];
  const polled = makeContentRuntime({
    conversationUrl,
    runtimeMessageHandler(message) {
      pollMessages.push(clone(message));
      if (message.type === "wake:poll") {
        return {
          ok: true,
          registration: {
            ...clone(fixture.registration.value),
            lastAttemptRevision: fixture.targetRevision,
            lastAttemptAt: 1000000,
          },
          target: targetForRegistration(
            fixture.registration,
            "waking",
            fixture.targetRevision,
          ),
        };
      }
      return { ok: false };
    },
  });
  await polled.api.pollWake();
  assert.deepEqual(
    pollMessages.map((message) => message.type),
    ["wake:poll"],
    "poll completion telemetry belongs to the service worker; content must not add a 2-second telemetry round-trip",
  );

  const command = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const exactMessages = [];
  const exact = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: exactCurrentProviderWakeDraft(
      fixture.appIdentity,
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    mentionCandidates: false,
    conversationUrl,
    runtimeMessageHandler(message) {
      exactMessages.push(clone(message));
      if (message.type === "wake:prepareFinalSend") {
        return { ok: true, safe: true, admissionToken: "disposition-admission" };
      }
      if (message.type === "wake:recordContentDisposition") {
        return { ok: true, safe: true };
      }
      if (message.type === "runtime:reserveExactCurrentWakeDraft") {
        return { ok: true, safe: false };
      }
      return { ok: false };
    },
  });
  assert.equal(
    await exact.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command,
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    }),
    false,
  );
  assert.deepEqual(
    exactMessages.map((message) => message.type),
    [
      "wake:prepareFinalSend",
      "wake:recordContentDisposition",
      "runtime:reserveExactCurrentWakeDraft",
    ],
    "exact-current detection must be durable before reserve is attempted",
  );
  assertPrivacySafeContentDispositionMessage(exactMessages[1], {
    conversationUrl,
    delivery: fixture.delivery,
    code: CONTENT_DISPOSITION_CODES.EXACT_CURRENT_ENTRY,
  });
}

async function durableContentDispositionIsBoundedAndExact() {
  const registration = runtimeOwnedWorker(622, {
    lastAttemptRevision: 0,
  });
  const targetRevision = 34;
  const target = targetForRegistration(registration, "waking", targetRevision);
  const delivery = {
    kind: "worker",
    mode: "wake",
    targetId: registration.value.wakeTargetId,
    targetRevision,
  };
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    initialTabs: [{
      id: registration.value.runtimeOpenedTabId,
      url: registration.value.conversationUrl,
      active: false,
      pinned: false,
    }],
  });
  const admissionOutcome =
    await runtime.api.deliverBackendAuthorizedRuntimeAdmission(
      candidate(runtime.api, registration, target),
    );
  assert.equal(admissionOutcome.admitted, true);
  assert.equal(
    runtime.storage.wakeTargets[registration.storageKey]
      .lastAdmittedWakeRevision,
    targetRevision,
  );
  assert.equal(
    storedContentDisposition(runtime, registration),
    undefined,
    "an admitted delivery with no disposition record means no content poll",
  );
  assert.equal(
    Object.prototype.hasOwnProperty.call(
      runtime.storage.wakeTargets[registration.storageKey],
      "lastWakeContentDisposition",
    ),
    false,
    "diagnostic disposition state must never live inside a registration record",
  );

  await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: registration.value.conversationUrl },
    { tab: {
      id: registration.value.runtimeOpenedTabId + 1,
      url: registration.value.conversationUrl,
    } },
  );
  assert.equal(
    storedContentDisposition(runtime, registration),
    undefined,
    "a poll from a different sender tab must not create telemetry",
  );

  await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: "https://chatgpt.com/c/worker-forged" },
    { tab: senderTabForRegistration(registration) },
  );
  assert.equal(
    storedContentDisposition(runtime, registration),
    undefined,
    "a poll for a different conversation must not create telemetry",
  );

  runtime.storage.wakeTargets[registration.storageKey]
    .lastAdmittedWakeRevision = targetRevision - 1;
  await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: registration.value.conversationUrl },
    { tab: senderTabForRegistration(registration) },
  );
  assert.equal(
    storedContentDisposition(runtime, registration),
    undefined,
    "a fresh target without its exact admitted revision must not create telemetry",
  );
  runtime.storage.wakeTargets[registration.storageKey]
    .lastAdmittedWakeRevision = targetRevision;

  const polled = await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: registration.value.conversationUrl },
    { tab: senderTabForRegistration(registration) },
  );
  assert.equal(polled.registration.wakeTargetId, registration.value.wakeTargetId);
  assert.equal(polled.target.wake_revision, targetRevision);
  await waitFor(() => Boolean(storedContentDisposition(runtime, registration)));
  const expectedPoll = {
    conversationUrl: registration.value.conversationUrl,
    kind: "worker",
    mode: "wake",
    targetId: registration.value.wakeTargetId,
    targetRevision,
    code: CONTENT_DISPOSITION_CODES.POLL_RECEIVED,
    observedAt: 1000000,
  };
  assert.deepEqual(
    storedContentDisposition(runtime, registration),
    expectedPoll,
    "the service worker must persist poll completion before resolving the exact fresh response",
  );

  const writesAfterFirstPoll = runtime.storageSetCalls.length;
  runtime.advance(1);
  await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: registration.value.conversationUrl },
    { tab: senderTabForRegistration(registration) },
  );
  await Promise.resolve();
  assert.deepEqual(
    storedContentDisposition(runtime, registration),
    expectedPoll,
    "repeating the same poll disposition must preserve its timestamp",
  );
  assert.equal(
    runtime.storageSetCalls.length,
    writesAfterFirstPoll,
    "repeating the same poll disposition must not issue a duplicate storage write",
  );

  assert.equal(
    typeof runtime.api.recordWakeContentDisposition,
    "function",
    "service worker must expose durable content disposition recording",
  );

  const common = {
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery,
  };
  async function recordExactDisposition(request) {
    const { tabId, conversationUrl, delivery: requestDelivery, code } = request;
    return runtime.api.handleMessage(
      {
        type: "wake:recordContentDisposition",
        conversationUrl,
        delivery: requestDelivery,
        code,
      },
      { tab: { id: tabId, url: conversationUrl } },
    );
  }
  const dispositionAdmission = await runtime.api.prepareFinalSendAdmission(common);
  assert.equal(dispositionAdmission.safe, true);
  runtime.advance(1);
  assert.deepEqual(
    clone(await recordExactDisposition({
      ...common,
      code: CONTENT_DISPOSITION_CODES.EXACT_CURRENT_ENTRY,
    })),
    { safe: true },
  );
  await waitFor(() =>
    storedContentDisposition(runtime, registration)?.code ===
      CONTENT_DISPOSITION_CODES.EXACT_CURRENT_ENTRY,
  );
  const expectedExact = {
    ...expectedPoll,
    code: CONTENT_DISPOSITION_CODES.EXACT_CURRENT_ENTRY,
    observedAt: 1000002,
  };
  assert.deepEqual(
    storedContentDisposition(runtime, registration),
    expectedExact,
    "one bounded record must advance from poll to exact-current detection",
  );

  const writesAfterExact = runtime.storageSetCalls.length;
  runtime.advance(1);
  await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: registration.value.conversationUrl },
    { tab: senderTabForRegistration(registration) },
  );
  assert.deepEqual(
    storedContentDisposition(runtime, registration),
    expectedExact,
    "a later poll must not downgrade exact-current disposition",
  );
  assert.equal(
    runtime.storageSetCalls.length,
    writesAfterExact,
    "a later poll must not write when exact-current already supersedes it",
  );

  const rejected = [
    { ...common, tabId: common.tabId + 1 },
    { ...common, conversationUrl: "https://chatgpt.com/c/worker-forged" },
    { ...common, delivery: { ...delivery, targetId: "wtgt_forged" } },
    { ...common, delivery: { ...delivery, targetRevision: targetRevision - 1 } },
    { ...common, delivery: { ...delivery, targetRevision: targetRevision + 1 } },
  ];
  for (const forged of rejected) {
    assert.deepEqual(
      clone(await recordExactDisposition({
        ...forged,
        code: CONTENT_DISPOSITION_CODES.EXACT_CURRENT_ENTRY,
      })),
      { safe: false },
    );
    assert.deepEqual(
      storedContentDisposition(runtime, registration),
      expectedExact,
      "forged or stale delivery telemetry must not mutate durable state",
    );
  }
  assert.deepEqual(
    clone(await recordExactDisposition({
      ...common,
      code: "composer_contains_current_command",
    })),
    { safe: false },
    "unbounded or content-revealing disposition codes must be rejected",
  );
  assert.deepEqual(
    storedContentDisposition(runtime, registration),
    expectedExact,
  );
}

async function exactCurrentDispositionIsNonAuthoritativeLiveness() {
  const fixture = staleWakeCleanupFixture(623, { targetRevision: 34 });
  const registration = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
  registration.lastAdmittedDeliveryKey =
    fixture.runtime.api.deliveryAdmissionForRuntimeTarget(
      registration,
      targetForRegistration(fixture.registration, "waking", 34),
    ).identity.key;
  registration.lastAdmittedWakeRevision = 34;
  registration.lastAdmittedAt = 999999;
  fixture.runtime.storage.wakeContentDisposition = {
    [fixture.registration.storageKey]: {
      conversationUrl: registration.conversationUrl,
      kind: "worker",
      mode: "wake",
      targetId: registration.wakeTargetId,
      targetRevision: 34,
      code: CONTENT_DISPOSITION_CODES.POLL_RECEIVED,
      observedAt: 999999,
    },
  };
  const command = fullWorkerWakeCommand(registration.wakeTargetId, 34);
  const messages = [];
  const route = realServiceWorkerWakeRouter(fixture.runtime, fixture.registration);
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: exactCurrentProviderWakeDraft(
      fixture.appIdentity,
      registration.wakeTargetId,
      34,
    ),
    mentionCandidates: false,
    conversationUrl: registration.conversationUrl,
    runtimeMessageHandler(message) {
      messages.push(message.type);
      if (message.type === "wake:recordContentDisposition") {
        return new Promise(() => {});
      }
      return route(message);
    },
  });
  const sent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command,
    draftMarker: registration.wakeTargetId,
    delivery: fixture.delivery,
  });
  assert.equal(sent, true, "telemetry callback liveness must not authorize sending");
  assert.equal(content.sendButton.clicks, 1);
  assert.equal(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .lastAttemptRevision,
    34,
  );
  assert.deepEqual(messages, [
    "wake:prepareFinalSend",
    "wake:recordContentDisposition",
    "runtime:reserveExactCurrentWakeDraft",
    "wake:confirmFinalSend",
  ]);
}

async function exactDispositionAvoidsExternalReadsAndPollCommitIsBestEffort() {
  const registration = runtimeOwnedWorker(624, {
    appIdentity: clone(APP_IDENTITY),
    bindingRevision: 2,
    connectorEpoch: 2,
  });
  registration.value.provisionedConnectorProof = provisionedConnectorProofFor(registration);
  const revision = 34;
  const target = targetForRegistration(registration, "waking", revision);
  const delivery = {
    kind: "worker",
    mode: "wake",
    targetId: registration.value.wakeTargetId,
    targetRevision: revision,
  };
  let targetReads = 0;
  let tabGets = 0;
  let registrationReads = 0;
  let raceNextRegistrationRead = false;
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    projectBindings: {
      [registration.value.projectId]: projectBinding(
        registration.value.projectId,
        APP_IDENTITY,
      ),
    },
    initialTabs: [{
      id: registration.value.runtimeOpenedTabId,
      url: registration.value.conversationUrl,
      active: false,
      pinned: false,
    }],
    onTargetRead() { targetReads += 1; },
    onTabGet() { tabGets += 1; },
    onRegistrationMapRead({ storage }) {
      registrationReads += 1;
      if (raceNextRegistrationRead) {
        raceNextRegistrationRead = false;
        storage.wakeTargets[registration.storageKey].bindingRevision += 1;
      }
    },
  });
  await runtime.api.deliverBackendAuthorizedRuntimeAdmission(
    candidate(runtime.api, registration, target),
  );
  const senderTab = senderTabForRegistration(registration);
  const firstPoll = await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: registration.value.conversationUrl },
    { tab: senderTab },
  );
  assert.equal(firstPoll.target.wake_revision, revision);
  await waitFor(() => Boolean(storedContentDisposition(runtime, registration)));

  const prepared = await runtime.api.prepareFinalSendAdmission({
    tabId: registration.value.runtimeOpenedTabId,
    conversationUrl: registration.value.conversationUrl,
    delivery,
  });
  assert.equal(prepared.safe, true);

  const readsBeforeExact = { targetReads, tabGets, registrationReads };
  const exact = await runtime.api.handleMessage(
    {
      type: "wake:recordContentDisposition",
      conversationUrl: registration.value.conversationUrl,
      delivery,
      code: CONTENT_DISPOSITION_CODES.EXACT_CURRENT_ENTRY,
    },
    { tab: senderTab },
  );
  assert.deepEqual(clone(exact), { safe: true });
  assert.equal(targetReads, readsBeforeExact.targetReads, "exact telemetry must not read backend target");
  assert.equal(tabGets, readsBeforeExact.tabGets, "sender metadata must replace Chrome tab reads");
  assert.ok(
    registrationReads > readsBeforeExact.registrationReads,
    "exact telemetry must validate current registration state",
  );

  runtime.storage.wakeContentDisposition = {};
  raceNextRegistrationRead = true;
  const racedPoll = await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: registration.value.conversationUrl },
    { tab: senderTab },
  );
  assert.equal(
    racedPoll.target.wake_revision,
    revision,
    "telemetry commit failure must not suppress a valid authoritative poll response",
  );
  assert.equal(racedPoll.registration.wakeTargetId, registration.value.wakeTargetId);
}

async function dispositionStorageQueueIsIsolatedBoundedAndObservedAtValidation() {
  const first = runtimeOwnedWorker(625, {
    appIdentity: clone(APP_IDENTITY), bindingRevision: 2, connectorEpoch: 2,
  });
  const second = runtimeOwnedWorker(626, {
    appIdentity: clone(APP_IDENTITY), bindingRevision: 2, connectorEpoch: 2,
  });
  for (const registration of [first, second]) {
    registration.value.provisionedConnectorProof = provisionedConnectorProofFor(registration);
  }
  let releaseDiagnosticWrite;
  const diagnosticGate = new Promise((resolve) => { releaseDiagnosticWrite = resolve; });
  let heldFirstDiagnosticWrite = false;
  const runtime = makeRuntime({
    registrations: [first, second],
    targets: [first, second].map((registration) => [
      registration.value.wakeTargetId,
      targetForRegistration(registration, "waking", 34),
    ]),
    projectBindings: {
      [first.value.projectId]: projectBinding(first.value.projectId, APP_IDENTITY),
    },
    initialTabs: [first, second].map((registration) => ({
      id: registration.value.runtimeOpenedTabId,
      url: registration.value.conversationUrl,
      active: false,
      pinned: false,
    })),
    storage: {
      wakeTargets: Object.fromEntries([first, second].map((registration) => [
        registration.storageKey,
        clone(registration.value),
      ])),
      wakeContentDisposition: {
        "conversation:stale-deleted-worker": {
          conversationUrl: "https://chatgpt.com/c/stale-deleted-worker",
          kind: "worker",
          mode: "wake",
          targetId: "wtgt_stale_deleted_worker",
          targetRevision: 1,
          code: CONTENT_DISPOSITION_CODES.POLL_RECEIVED,
          observedAt: 1,
        },
      },
    },
    async onStorageSet({ patch }) {
      if (
        patch.wakeContentDisposition &&
        !heldFirstDiagnosticWrite
      ) {
        heldFirstDiagnosticWrite = true;
        await diagnosticGate;
      }
    },
  });
  for (const registration of [first, second]) {
    await runtime.api.deliverBackendAuthorizedRuntimeAdmission(
      candidate(
        runtime.api,
        registration,
        targetForRegistration(registration, "waking", 34),
      ),
    );
  }

  const firstPoll = await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: first.value.conversationUrl },
    { tab: senderTabForRegistration(first) },
  );
  assert.equal(firstPoll.target.wake_revision, 34);
  await waitFor(() => runtime.storageSetCalls.some((patch) => patch.wakeContentDisposition));

  const delivery = {
    kind: "worker", mode: "wake", targetId: first.value.wakeTargetId, targetRevision: 34,
  };
  const prepared = await runtime.api.prepareFinalSendAdmission({
    tabId: first.value.runtimeOpenedTabId,
    conversationUrl: first.value.conversationUrl,
    delivery,
  });
  const reserved = await runtime.api.consumeRuntimeOwnedRegistrationDraft({
    tabId: first.value.runtimeOpenedTabId,
    conversationUrl: first.value.conversationUrl,
    appIdentity: APP_IDENTITY,
    draft: APP_IDENTITY.appLabel,
    admissionToken: prepared.admissionToken,
    delivery,
  });
  assert.equal(reserved.safe, true, "held diagnostic storage must not block reserve");
  const confirmed = await runtime.api.confirmFinalSendAdmission({
    tabId: first.value.runtimeOpenedTabId,
    conversationUrl: first.value.conversationUrl,
    admissionToken: prepared.admissionToken,
    delivery,
  });
  assert.equal(confirmed.safe, true, "held diagnostic queue must not block attempt commit");
  assert.equal(runtime.storage.wakeTargets[first.storageKey].lastAttemptRevision, 34);

  const secondObservedAt = 1000000;
  const secondPoll = await runtime.api.handleMessage(
    { type: "wake:poll", conversationUrl: second.value.conversationUrl },
    { tab: senderTabForRegistration(second) },
  );
  assert.equal(secondPoll.target.wake_revision, 34);
  runtime.advance(5000);
  releaseDiagnosticWrite();
  await waitFor(() => Boolean(storedContentDisposition(runtime, second)));

  const dispositions = runtime.storage.wakeContentDisposition;
  assert.equal(dispositions["conversation:stale-deleted-worker"], undefined);
  assert.deepEqual(Object.keys(dispositions).sort(), [first.storageKey, second.storageKey].sort());
  assert.equal(
    dispositions[second.storageKey].observedAt,
    secondObservedAt,
    "observedAt must reflect validated observation time, not delayed queue drain time",
  );
  for (const registration of [first, second]) {
    assert.equal(
      Object.prototype.hasOwnProperty.call(
        runtime.storage.wakeTargets[registration.storageKey],
        "lastWakeContentDisposition",
      ),
      false,
    );
  }
}

function fullWorkerWakeCommand(targetId, wakeRevision) {
  return ` 작업해. 사용자가 Main Chat에서 Worker 위임을 요청했습니다. 이 서버 발급 wake pair는 현재 배정을 조회하기 위한 work 호출만 승인합니다. 파괴적·비가역적·자격증명 필요·외부 프로덕션 작업의 별도 승인을 뜻하지 않습니다.\nwake_target_id: ${targetId}\nwake_revision: ${wakeRevision}`;
}

function exactCurrentProviderWakeDraft(appIdentity, targetId, targetRevision) {
  const command = fullWorkerWakeCommand(targetId, targetRevision);
  return `\uFEFF${appIdentity.appLabel[0]}${appIdentity.appLabel} ${command.replace(/\n/g, " ")}`;
}

function durableWakeSafetySnapshot(registration) {
  return clone({
    wakeTargetId: registration.wakeTargetId,
    projectId: registration.projectId,
    conversationId: registration.conversationId,
    conversationUrl: registration.conversationUrl,
    runtimeOpenedTabId: registration.runtimeOpenedTabId,
    lastAttemptRevision: registration.lastAttemptRevision,
    runtimeOwnedRegistrationDraft: registration.runtimeOwnedRegistrationDraft,
    runtimeOwnedRegistrationDraftConsumedAt:
      registration.runtimeOwnedRegistrationDraftConsumedAt,
    appIdentity: registration.appIdentity,
    bindingRevision: registration.bindingRevision,
    connectorEpoch: registration.connectorEpoch,
    provisionedConnectorProof: registration.provisionedConnectorProof,
  });
}

function realServiceWorkerWakeRouter(runtime, registration) {
  return async function routeRuntimeMessage(message) {
    const common = {
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: message.conversationUrl,
      delivery: message.delivery,
    };
    if (message.type === "wake:prepareFinalSend") {
      return {
        ok: true,
        ...(await runtime.api.prepareFinalSendAdmission(common)),
      };
    }
    if (message.type === "runtime:reserveExactCurrentWakeDraft") {
      return {
        ok: true,
        ...(await runtime.api.reserveExactCurrentWakeDraft({
          ...common,
          appIdentity: message.appIdentity,
          draft: message.draft,
          admissionToken: message.admissionToken,
        })),
      };
    }
    if (message.type === "runtime:consumeOwnedRegistrationDraft") {
      return {
        ok: true,
        ...(await runtime.api.consumeRuntimeOwnedRegistrationDraft({
          ...common,
          appIdentity: message.appIdentity,
          draft: message.draft,
          admissionToken: message.admissionToken,
        })),
      };
    }
    if (message.type === "wake:confirmFinalSend") {
      return {
        ok: true,
        ...(await runtime.api.confirmFinalSendAdmission({
          ...common,
          admissionToken: message.admissionToken,
        })),
      };
    }
    if (message.type === "wake:recordSendFailure") {
      return {
        ok: true,
        ...(await runtime.api.recordWakeSendFailure({
          ...common,
          reason: message.reason,
          rejectionStage: message.rejectionStage,
          rejectionSubstage: message.rejectionSubstage,
        })),
      };
    }
    if (message.type === "wake:recordContentDisposition") {
      if (!runtime.api.recordWakeContentDisposition) {
        return { ok: false, safe: false };
      }
      return {
        ok: true,
        ...(await runtime.api.recordWakeContentDisposition({
          ...common,
          code: message.code,
        })),
      };
    }
    return { ok: false };
  };
}

function staleWakeCleanupFixture(index, options = {}) {
  const appIdentity = options.appIdentity || {
    appRef: "plugin_chat_orchestration_live",
    appLabel: "ChatOrchestration",
  };
  const registration = runtimeOwnedWorker(index, {
    appIdentity: clone(appIdentity),
    bindingRevision: 2,
    connectorEpoch: 2,
    lastAttemptRevision: options.lastAttemptRevision || 0,
  });
  const storedDraft = `@${appIdentity.appLabel}`;
  registration.value.runtimeOwnedRegistrationDraft.draft = storedDraft;
  registration.value.runtimeOwnedRegistrationDraft.appRef = appIdentity.appRef;
  registration.value.runtimeOwnedRegistrationDraft.appLabel = appIdentity.appLabel;
  registration.value.provisionedConnectorProof = provisionedConnectorProofFor(
    registration,
    { appRef: appIdentity.appRef, appLabel: appIdentity.appLabel },
  );
  if (options.missingCapability) {
    registration.value.runtimeOwnedRegistrationDraft = null;
  }
  if (options.missingProof) {
    registration.value.provisionedConnectorProof = null;
  }
  const targetRevision = options.targetRevision || 14;
  const target = targetForRegistration(registration, "waking", targetRevision);
  const binding = projectBinding(registration.value.projectId, {
    appRef: appIdentity.appRef,
    appLabel: appIdentity.appLabel,
    connectorEpoch: 2,
    revision: 2,
    ...(options.bindingPatch || {}),
  });
  let targetReads = 0;
  const runtime = makeRuntime({
    registrations: [registration],
    targets: [[registration.value.wakeTargetId, target]],
    projectBindings: { [registration.value.projectId]: binding },
    initialTabs: [
      {
        id: registration.value.runtimeOpenedTabId,
        url: registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
    onProjectBindingRead: options.onProjectBindingRead,
    onTargetRead(context) {
      targetReads += 1;
      if (options.onCleanupTargetRead && targetReads > 1) {
        options.onCleanupTargetRead(context);
      }
    },
  });
  const delivery = {
    kind: "worker",
    mode: "wake",
    targetId: registration.value.wakeTargetId,
    targetRevision,
  };
  let cleanupRequest = null;
  async function routeRuntimeMessage(message) {
    const common = {
      tabId: registration.value.runtimeOpenedTabId,
      conversationUrl: message.conversationUrl,
      delivery: message.delivery,
    };
    if (message.type === "wake:prepareFinalSend") {
      return {
        ok: true,
        ...(await runtime.api.prepareFinalSendAdmission(common)),
      };
    }
    if (message.type === "runtime:cleanupStaleWakeDraft") {
      cleanupRequest = clone(message);
      if (!runtime.api.cleanupStaleRuntimeWakeDraft) {
        return { ok: true, safe: false };
      }
      return {
        ok: true,
        ...(await runtime.api.cleanupStaleRuntimeWakeDraft({
          ...common,
          appIdentity: message.appIdentity,
          draft: message.draft,
          admissionToken: message.admissionToken,
        })),
      };
    }
    if (message.type === "runtime:consumeOwnedRegistrationDraft") {
      return {
        ok: true,
        ...(await runtime.api.consumeRuntimeOwnedRegistrationDraft({
          ...common,
          appIdentity: message.appIdentity,
          draft: message.draft,
          admissionToken: message.admissionToken,
        })),
      };
    }
    if (message.type === "wake:confirmFinalSend") {
      return {
        ok: true,
        ...(await runtime.api.confirmFinalSendAdmission({
          ...common,
          admissionToken: message.admissionToken,
        })),
      };
    }
    if (message.type === "wake:recordSendFailure") {
      return {
        ok: true,
        ...(await runtime.api.recordWakeSendFailure({
          ...common,
          reason: message.reason,
          rejectionStage: message.rejectionStage,
          rejectionSubstage: message.rejectionSubstage,
        })),
      };
    }
    return { ok: false };
  }
  return {
    appIdentity,
    binding,
    cleanupRequest: () => cleanupRequest,
    delivery,
    registration,
    routeRuntimeMessage,
    runtime,
    storedDraft,
    targetRevision,
  };
}

async function staleOwnedWakeCommandRestoresDraftThenCurrentWakeSendsOnce() {
  const fixture = staleWakeCleanupFixture(640);
  const staleDraft =
    fixture.appIdentity.appLabel[0] +
    fullWorkerWakeCommand(fixture.registration.value.wakeTargetId, 1);
  const capabilityBefore = clone(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .runtimeOwnedRegistrationDraft,
  );
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    inlineSelectedApp: true,
    initialComposerText: staleDraft,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler: fixture.routeRuntimeMessage,
  });

  const cleaned = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command: fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });

  assert.equal(cleaned, false, "cleanup must not click the stale command");
  assert.equal(
    content.composer.value,
    fixture.storedDraft,
    "an exact stale own-target wake must restore the exact stored registration draft",
  );
  assert.equal(content.sendButton.clicks, 0);
  let stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
  assert.equal(stored.lastAttemptRevision, 0);
  assert.deepEqual(stored.runtimeOwnedRegistrationDraft, capabilityBefore);
  assert.ok(fixture.cleanupRequest(), "content must ask the service worker to authorize cleanup");

  const replay = await fixture.runtime.api.cleanupStaleRuntimeWakeDraft({
    tabId: fixture.registration.value.runtimeOpenedTabId,
    ...fixture.cleanupRequest(),
  });
  assert.equal(replay.safe, false, "stale cleanup admission must be one-shot");

  const sent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command: fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });
  assert.equal(sent, true, "the current revision may send after exact restoration");
  assert.equal(content.sendButton.clicks, 1);
  stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
  assert.equal(stored.lastAttemptRevision, fixture.targetRevision);
  assert.equal(stored.runtimeOwnedRegistrationDraft, null);
}

async function singleLeadingFeffProviderWakeRestoresDraftThenCurrentWakeSendsOnce() {
  const fixture = staleWakeCleanupFixture(641);
  const staleCommand = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    1,
  ).replace(/\n/g, " ");
  const registrationDraft = controlledEditorPrefix(fixture.appIdentity);
  fixture.runtime.storage.wakeTargets[
    fixture.registration.storageKey
  ].runtimeOwnedRegistrationDraft.draft = registrationDraft;
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    controlledEditorDom: true,
    controlledEditorResidualText: staleCommand,
    inlineSelectedApp: true,
    selectedAppAncestorDepth: 0,
    selectedAppOrigin: "inline_selection_pill",
    selectedAppWrapperPresent: true,
    selectedAppWrapperEditable: false,
    selectedAppDuplicateCount: 1,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler: fixture.routeRuntimeMessage,
  });

  const cleaned = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command: fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });

  assert.equal(cleaned, false, "cleanup must not click the stale command");
  assert.equal(
    content.composer.textContent,
    registrationDraft,
    "one leading provider FEFF must be ignored before restoring the stored draft",
  );
  assert.equal(content.sendButton.clicks, 0);

  const sent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command: fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });
  assert.equal(sent, true, "the current revision may send after exact restoration");
  assert.equal(content.sendButton.clicks, 1);
}

async function staleCleanupRestorationSurvivesProviderTurn() {
  const fixture = staleWakeCleanupFixture(698);
  const staleCommand = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    1,
  ).replace(/\n/g, " ");
  const registrationDraft = controlledEditorPrefix(fixture.appIdentity);
  const staleDraft = `${registrationDraft}${staleCommand}`;
  fixture.runtime.storage.wakeTargets[
    fixture.registration.storageKey
  ].runtimeOwnedRegistrationDraft.draft = registrationDraft;
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    controlledEditorDom: true,
    controlledEditorResidualText: staleCommand,
    inlineSelectedApp: true,
    selectedAppAncestorDepth: 0,
    selectedAppOrigin: "inline_selection_pill",
    selectedAppWrapperPresent: true,
    selectedAppWrapperEditable: false,
    selectedAppDuplicateCount: 1,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    providerShadowText: staleDraft,
    execCommandEmitsInput: true,
    runtimeMessageHandler: fixture.routeRuntimeMessage,
  });

  const cleaned = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command: fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });

  assert.equal(cleaned, false, "cleanup must not click the stale command");
  assert.equal(
    content.composer.textContent,
    registrationDraft,
    "authorized cleanup must initially restore the registration draft",
  );
  content.advanceProviderTurn();
  assert.equal(
    content.composer.textContent,
    registrationDraft,
    "authorized cleanup restoration must remain committed after one provider reconciliation turn",
  );
  assert.equal(content.sendButton.clicks, 0);
  assert.equal(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .lastAttemptRevision,
    0,
  );
}

async function staleWakeCleanupRejectsNoncanonicalZeroWidthText() {
  const cases = [
    {
      name: "leading FEFF with zero extra provider spacers",
      alter: (draft) => `\uFEFF${draft.replace("  작업해", " 작업해")}`,
    },
    {
      name: "leading FEFF with two extra provider spacers",
      alter: (draft) => `\uFEFF${draft.replace("  작업해", "   작업해")}`,
    },
    {
      name: "provider spacer before icon initial",
      alter: (draft) => `\uFEFF ${draft}`,
    },
    {
      name: "provider spacer between icon initial and label",
      alter: (draft) => `\uFEFF${draft[0]} ${draft.slice(1)}`,
    },
    {
      name: "two leading FEFF sentinels",
      alter: (draft) => `\uFEFF\uFEFF${draft}`,
    },
    {
      name: "FEFF in the middle",
      alter: (draft) => `${draft.slice(0, 2)}\uFEFF${draft.slice(2)}`,
    },
    {
      name: "FEFF at the end",
      alter: (draft) => `${draft}\uFEFF`,
    },
    {
      name: "arbitrary zero-width character",
      alter: (draft) => `\u200B${draft}`,
    },
    {
      name: "leading FEFF plus human text",
      alter: (draft) => `\uFEFF${draft} human text`,
    },
  ];

  for (const [offset, testCase] of cases.entries()) {
    const fixture = staleWakeCleanupFixture(642 + offset);
    const iconInitial = Array.from(fixture.appIdentity.appLabel)[0];
    const flattenedCommand = fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      1,
    ).replace(/\n/g, " ");
    const canonicalDraft = `${iconInitial}${fixture.appIdentity.appLabel} ${flattenedCommand}`;
    const rawDraft = testCase.alter(canonicalDraft);
    const capabilityBefore = clone(
      fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
        .runtimeOwnedRegistrationDraft,
    );
    const content = makeContentRuntime({
      appIdentity: fixture.appIdentity,
      contentEditableComposer: true,
      inlineSelectedApp: true,
      selectedAppAncestorDepth: 0,
      selectedAppOrigin: "inline_selection_pill",
      selectedAppWrapperPresent: true,
      selectedAppWrapperEditable: false,
      selectedAppDuplicateCount: 1,
      initialComposerText: rawDraft,
      mentionCandidates: false,
      conversationUrl: fixture.registration.value.conversationUrl,
      runtimeMessageHandler: fixture.routeRuntimeMessage,
    });

    const sent = await content.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command: fullWorkerWakeCommand(
        fixture.registration.value.wakeTargetId,
        fixture.targetRevision,
      ),
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    });

    assert.equal(sent, false, testCase.name);
    assert.equal(content.composer.textContent, rawDraft, testCase.name);
    assert.equal(content.sendButton.clicks, 0, testCase.name);
    const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
    assert.equal(stored.lastAttemptRevision, 0, testCase.name);
    assert.deepEqual(
      stored.runtimeOwnedRegistrationDraft,
      capabilityBefore,
      `${testCase.name} must not consume the durable capability`,
    );
  }
}

async function staleOwnedWakeCleanupFailsClosedForNearMissesAndRaces() {
  const cases = [
    { name: "altered character", staleRevision: 1, alter: (draft) => draft.replace("요청했습니다", "요청했습니X") },
    { name: "appended human text", staleRevision: 1, alter: (draft) => `${draft}\nkeep this human text` },
    { name: "wrong target", staleRevision: 1, targetId: "wtgt_ffffffffffffffffffffffffffffffff" },
    { name: "equal revision", staleRevision: 14 },
    { name: "newer revision", staleRevision: 15 },
    { name: "wrong app ref", staleRevision: 1, wrongAppRef: true },
    { name: "missing capability", staleRevision: 1, missingCapability: true },
    { name: "missing proof", staleRevision: 1, missingProof: true },
    { name: "prior attempt", staleRevision: 1, lastAttemptRevision: 1 },
    { name: "activation during cleanup", staleRevision: 1, cleanupMutation: "active" },
    { name: "pin during cleanup", staleRevision: 1, cleanupMutation: "pinned" },
    { name: "focus during cleanup", staleRevision: 1, cleanupMutation: "focus" },
    { name: "generation during cleanup", staleRevision: 1, cleanupMutation: "generation" },
    { name: "composer mutation during cleanup", staleRevision: 1, cleanupMutation: "composer-mutation", expectedComposer: "human edit during cleanup" },
    { name: "composer replacement during cleanup", staleRevision: 1, cleanupMutation: "composer-replacement", expectedComposer: "replacement human draft" },
    { name: "binding revision race", staleRevision: 1, bindingRace: true },
    { name: "target revision race", staleRevision: 1, targetRace: true },
  ];

  for (const [offset, testCase] of cases.entries()) {
    let fixture;
    fixture = staleWakeCleanupFixture(650 + offset, {
      missingCapability: testCase.missingCapability,
      missingProof: testCase.missingProof,
      lastAttemptRevision: testCase.lastAttemptRevision,
      onProjectBindingRead: testCase.bindingRace
        ? ({ storage }) => {
            storage.wakeTargets[fixture.registration.storageKey].bindingRevision += 1;
          }
        : null,
      onCleanupTargetRead: testCase.targetRace
        ? ({ targetId, targetById }) => {
            targetById.get(targetId).wake_revision += 1;
            targetById.get(targetId).revision += 1;
          }
        : null,
    });
    const suppliedIdentity = testCase.wrongAppRef
      ? { ...fixture.appIdentity, appRef: "plugin_wrong" }
      : fixture.appIdentity;
    const staleTargetId = testCase.targetId || fixture.registration.value.wakeTargetId;
    let staleDraft =
      suppliedIdentity.appLabel[0] +
      fullWorkerWakeCommand(staleTargetId, testCase.staleRevision);
    staleDraft = testCase.alter ? testCase.alter(staleDraft) : staleDraft;
    const capabilityBefore = clone(
      fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
        .runtimeOwnedRegistrationDraft,
    );
    const content = makeContentRuntime({
      appIdentity: suppliedIdentity,
      inlineSelectedApp: true,
      initialComposerText: staleDraft,
      mentionCandidates: false,
      conversationUrl: fixture.registration.value.conversationUrl,
      runtimeMessageHandler: fixture.routeRuntimeMessage,
      mutateDuringStaleWakeCleanup({
        composer,
        claimDocument,
        replaceComposer,
        newComposer,
        setGenerating,
      }) {
        if (testCase.cleanupMutation === "active") {
          fixture.runtime.patchTab(fixture.registration.value.runtimeOpenedTabId, { active: true });
        } else if (testCase.cleanupMutation === "pinned") {
          fixture.runtime.patchTab(fixture.registration.value.runtimeOpenedTabId, { pinned: true });
        } else if (testCase.cleanupMutation === "focus") {
          claimDocument();
        } else if (testCase.cleanupMutation === "generation") {
          setGenerating(true);
        } else if (testCase.cleanupMutation === "composer-mutation") {
          composer.value = "human edit during cleanup";
        } else if (testCase.cleanupMutation === "composer-replacement") {
          replaceComposer(newComposer("replacement human draft"));
        }
      },
    });
    const sent = await content.api.sendAppCommand({
      appIdentity: suppliedIdentity,
      command: fullWorkerWakeCommand(
        fixture.registration.value.wakeTargetId,
        fixture.targetRevision,
      ),
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    });
    assert.equal(sent, false, testCase.name);
    assert.equal(content.sendButton.clicks, 0, testCase.name);
    assert.equal(
      content.currentComposer.value,
      testCase.expectedComposer || staleDraft,
      `${testCase.name} must preserve the human/provider-owned composer state`,
    );
    const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
    assert.equal(
      stored.lastAttemptRevision,
      testCase.lastAttemptRevision || 0,
      testCase.name,
    );
    assert.deepEqual(
      stored.runtimeOwnedRegistrationDraft,
      capabilityBefore,
      `${testCase.name} must not consume the durable capability`,
    );
  }
}

async function staleWakeCleanupRequiresExactInlineSelectionPill() {
  const malformedPills = [
    { name: "missing plugin detail origin", selectedAppOrigin: null },
    { name: "wrong plugin detail origin", selectedAppOrigin: "details_modal" },
    {
      name: "matching sibling-only anchor",
      selectedAppAncestorDepth: 1,
    },
    {
      name: "matching ancestor-only anchor",
      selectedAppAncestorDepth: 2,
    },
    { name: "missing inline pill wrapper", selectedAppWrapperPresent: false },
    { name: "editable inline pill wrapper", selectedAppWrapperEditable: true },
    { name: "duplicate exact inline pills", selectedAppDuplicateCount: 2 },
  ];
  for (const [offset, malformed] of malformedPills.entries()) {
    const fixture = staleWakeCleanupFixture(680 + offset);
    const staleDraft =
      fixture.appIdentity.appLabel[0] +
      fullWorkerWakeCommand(fixture.registration.value.wakeTargetId, 1);
    const content = makeContentRuntime({
      appIdentity: fixture.appIdentity,
      inlineSelectedApp: true,
      initialComposerText: staleDraft,
      mentionCandidates: false,
      conversationUrl: fixture.registration.value.conversationUrl,
      runtimeMessageHandler: fixture.routeRuntimeMessage,
      ...malformed,
    });
    const capabilityBefore = clone(
      fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
        .runtimeOwnedRegistrationDraft,
    );
    const sent = await content.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command: fullWorkerWakeCommand(
        fixture.registration.value.wakeTargetId,
        fixture.targetRevision,
      ),
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    });
    assert.equal(sent, false, malformed.name);
    assert.equal(content.currentComposer.value, staleDraft, malformed.name);
    assert.equal(content.sendButton.clicks, 0, malformed.name);
    const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
    assert.equal(stored.lastAttemptRevision, 0, malformed.name);
    assert.deepEqual(
      stored.runtimeOwnedRegistrationDraft,
      capabilityBefore,
      `${malformed.name} must not consume the durable capability`,
    );
  }
}

async function staleWakeCleanupRejectsConsumedCapabilityStillPresent() {
  const fixture = staleWakeCleanupFixture(690);
  fixture.runtime.storage.wakeTargets[
    fixture.registration.storageKey
  ].runtimeOwnedRegistrationDraftConsumedAt = 999;
  const staleDraft =
    fixture.appIdentity.appLabel[0] +
    fullWorkerWakeCommand(fixture.registration.value.wakeTargetId, 1);
  const capabilityBefore = clone(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .runtimeOwnedRegistrationDraft,
  );
  assert.equal(
    typeof fixture.runtime.api.cleanupStaleRuntimeWakeDraft,
    "function",
    "the service worker must expose stale cleanup admission",
  );
  const prepared = await fixture.runtime.api.prepareFinalSendAdmission({
    tabId: fixture.registration.value.runtimeOpenedTabId,
    conversationUrl: fixture.registration.value.conversationUrl,
    delivery: fixture.delivery,
  });
  assert.equal(prepared.safe, true);
  const cleaned = await fixture.runtime.api.cleanupStaleRuntimeWakeDraft({
    tabId: fixture.registration.value.runtimeOpenedTabId,
    conversationUrl: fixture.registration.value.conversationUrl,
    appIdentity: fixture.appIdentity,
    draft: staleDraft,
    admissionToken: prepared.admissionToken,
    delivery: fixture.delivery,
  });
  assert.equal(
    cleaned.safe,
    false,
    "a consumed timestamp and a live capability object are inconsistent provenance",
  );
  const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
  assert.deepEqual(stored.runtimeOwnedRegistrationDraft, capabilityBefore);
  assert.equal(stored.runtimeOwnedRegistrationDraftConsumedAt, 999);
  assert.equal(stored.lastAttemptRevision, 0);
}

function controlledEditorPrefix(appIdentity) {
  return `\uFEFF${Array.from(appIdentity.appLabel)[0]}${appIdentity.appLabel} `;
}

async function staleCleanupPreservesControlledEditorPillIdentity() {
  const fixture = staleWakeCleanupFixture(691);
  const staleResidual = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    1,
  ).replace(/\n/g, " ");
  fixture.runtime.storage.wakeTargets[
    fixture.registration.storageKey
  ].runtimeOwnedRegistrationDraft.draft = controlledEditorPrefix(
    fixture.appIdentity,
  );
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    controlledEditorDom: true,
    controlledEditorResidualText: staleResidual,
    inlineSelectedApp: true,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler: fixture.routeRuntimeMessage,
  });
  const wrapper = content.controlledEditor.pillWrapper;
  const anchor = content.controlledEditor.pillAnchor;
  const sentinel = content.controlledEditor.sentinel;

  const sent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command: fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });

  assert.equal(sent, false);
  assert.equal(content.sendButton.clicks, 0);
  assert.equal(content.controlledEditor.residual.data, "");
  assert.equal(content.composer.childNodes[0], sentinel);
  assert.equal(content.composer.childNodes[1], wrapper);
  assert.equal(wrapper.querySelectorAll("a")[0], anchor);
  assert.equal(wrapper.isConnected, true);
  assert.equal(anchor.isConnected, true);
}

async function splitResidualTopologyNeverFallsBackToWholeComposerReplacement() {
  const fixture = staleWakeCleanupFixture(692);
  const staleResidual = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    1,
  ).replace(/\n/g, " ");
  fixture.runtime.storage.wakeTargets[
    fixture.registration.storageKey
  ].runtimeOwnedRegistrationDraft.draft = controlledEditorPrefix(
    fixture.appIdentity,
  );
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    controlledEditorDom: true,
    controlledEditorResidualText: staleResidual,
    splitControlledEditorResidual: true,
    inlineSelectedApp: true,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler: fixture.routeRuntimeMessage,
  });
  const children = [...content.composer.childNodes];
  const rawDraft = content.composer.textContent;

  const sent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command: fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    ),
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });

  assert.equal(sent, false);
  assert.equal(content.composer.textContent, rawDraft);
  assert.deepEqual(content.composer.childNodes, children);
  assert.equal(content.controlledEditor.execCommandCalls, 0);
  assert.equal(content.sendButton.clicks, 0);
}

async function failedSendRollbackPreservesControlledEditorPillIdentity() {
  const registrationDraft = controlledEditorPrefix(APP_IDENTITY);
  const command =
    " 작업해\nwake_target_id: wtgt_controlled_rollback\nwake_revision: 1";
  const content = makeContentRuntime({
    contentEditableComposer: true,
    controlledEditorDom: true,
    controlledEditorResidualText: "",
    inlineSelectedApp: true,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
    finalSendConfirmSafety: { ok: true, safe: false },
  });
  const wrapper = content.controlledEditor.pillWrapper;
  const anchor = content.controlledEditor.pillAnchor;
  const sentinel = content.controlledEditor.sentinel;
  assert.equal(content.composer.textContent, registrationDraft);

  const sent = await content.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command,
    draftMarker: "wtgt_controlled_rollback",
  });

  assert.equal(sent, false);
  assert.equal(content.sendButton.clicks, 0);
  assert.equal(content.controlledEditor.residual.data, "");
  assert.equal(content.composer.childNodes[0], sentinel);
  assert.equal(content.composer.childNodes[1], wrapper);
  assert.equal(wrapper.querySelectorAll("a")[0], anchor);
  assert.equal(wrapper.isConnected, true);
  assert.equal(anchor.isConnected, true);
}

async function nativeExecCommandInputIsNotDuplicated() {
  const command =
    " 작업해\nwake_target_id: wtgt_native_input\nwake_revision: 1";
  const content = makeContentRuntime({
    contentEditableComposer: true,
    controlledEditorDom: true,
    controlledEditorResidualText: "",
    execCommandEmitsInput: true,
    inlineSelectedApp: true,
    mentionCandidates: false,
    runtimeOwnedDraftSafety: { ok: true, safe: true },
  });

  const sent = await content.api.sendAppCommand({
    appIdentity: APP_IDENTITY,
    command,
    draftMarker: "wtgt_native_input",
  });

  assert.equal(sent, true);
  assert.equal(content.sendButton.clicks, 1);
  assert.equal(
    content.composer.inputEvents.length,
    content.controlledEditor.execCommandCalls,
    "a UA-emitted input event must not be followed by a duplicate synthetic input",
  );
}

async function rejectedControlledEditorTransactionsDoNotCommit() {
  for (const transaction of [
    { name: "cancelled beforeinput", cancelBeforeInput: true },
    { name: "execCommand false", forceExecCommandFalse: true },
  ]) {
    const command =
      " 작업해\nwake_target_id: wtgt_rejected_transaction\nwake_revision: 1";
    const content = makeContentRuntime({
      contentEditableComposer: true,
      controlledEditorDom: true,
      controlledEditorResidualText: "",
      inlineSelectedApp: true,
      mentionCandidates: false,
      runtimeOwnedDraftSafety: { ok: true, safe: true },
      ...transaction,
    });
    const originalText = content.composer.textContent;
    const wrapper = content.controlledEditor.pillWrapper;
    const anchor = content.controlledEditor.pillAnchor;

    const sent = await content.api.sendAppCommand({
      appIdentity: APP_IDENTITY,
      command,
      draftMarker: "wtgt_rejected_transaction",
    });

    assert.equal(sent, false, transaction.name);
    assert.equal(content.composer.textContent, originalText, transaction.name);
    assert.equal(content.composer.inputEvents.length, 0, transaction.name);
    assert.equal(content.controlledEditor.pillWrapper, wrapper, transaction.name);
    assert.equal(content.controlledEditor.pillAnchor, anchor, transaction.name);
    assert.equal(content.sendButton.clicks, 0, transaction.name);
  }
}

async function exactCurrentNormalizationNearMissesFailClosed() {
  const textCases = [
    { name: "extra leading FEFF", residualPrefix: "\uFEFF" },
    { name: "extra provider space", residualPrefix: " " },
    { name: "wrong target", targetId: "wtgt_ffffffffffffffffffffffffffffffff" },
    { name: "wrong revision", revision: 13 },
    { name: "appended text", residualSuffix: " human text" },
  ];
  for (const [offset, testCase] of textCases.entries()) {
    const fixture = staleWakeCleanupFixture(700 + offset);
    const capabilityBefore = clone(
      fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
        .runtimeOwnedRegistrationDraft,
    );
    const residual =
      (testCase.residualPrefix || "") +
      fullWorkerWakeCommand(
        testCase.targetId || fixture.registration.value.wakeTargetId,
        testCase.revision || fixture.targetRevision,
      ).replace(/\n/g, " ") +
      (testCase.residualSuffix || "");
    const content = makeContentRuntime({
      appIdentity: fixture.appIdentity,
      contentEditableComposer: true,
      controlledEditorDom: true,
      controlledEditorResidualText: residual,
      inlineSelectedApp: true,
      mentionCandidates: false,
      conversationUrl: fixture.registration.value.conversationUrl,
      runtimeMessageHandler: fixture.routeRuntimeMessage,
    });
    const sent = await content.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command: fullWorkerWakeCommand(
        fixture.registration.value.wakeTargetId,
        fixture.targetRevision,
      ),
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    });
    const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
    assert.equal(sent, false, testCase.name);
    assert.equal(content.sendButton.clicks, 0, testCase.name);
    assert.equal(stored.lastAttemptRevision, 0, testCase.name);
    assert.deepEqual(stored.runtimeOwnedRegistrationDraft, capabilityBefore, testCase.name);
  }

  for (const [offset, testCase] of [
    { name: "missing pill", inlineSelectedApp: false },
    { name: "duplicate pill", selectedAppDuplicateCount: 2 },
    { name: "wrong-origin pill", selectedAppOrigin: "details_modal" },
  ].entries()) {
    const fixture = staleWakeCleanupFixture(710 + offset);
    const capabilityBefore = clone(
      fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
        .runtimeOwnedRegistrationDraft,
    );
    const currentDraft =
      fixture.appIdentity.appLabel[0] +
      fullWorkerWakeCommand(
        fixture.registration.value.wakeTargetId,
        fixture.targetRevision,
      );
    const content = makeContentRuntime({
      appIdentity: fixture.appIdentity,
      initialComposerText: currentDraft,
      mentionCandidates: false,
      conversationUrl: fixture.registration.value.conversationUrl,
      runtimeMessageHandler: fixture.routeRuntimeMessage,
      inlineSelectedApp: true,
      ...testCase,
    });
    const sent = await content.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command: fullWorkerWakeCommand(
        fixture.registration.value.wakeTargetId,
        fixture.targetRevision,
      ),
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    });
    const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
    assert.equal(sent, false, testCase.name);
    assert.equal(content.sendButton.clicks, 0, testCase.name);
    assert.equal(stored.lastAttemptRevision, 0, testCase.name);
    assert.deepEqual(stored.runtimeOwnedRegistrationDraft, capabilityBefore, testCase.name);
  }
}

async function exactCurrentAtEntryAndPollSuppressionAreFailClosed() {
  const fixture = staleWakeCleanupFixture(720);
  const command = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const currentDraft = `\uFEFF${fixture.appIdentity.appLabel[0]}${fixture.appIdentity.appLabel} ${command.replace(/\n/g, " ")}`;
  const capabilityBefore = clone(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .runtimeOwnedRegistrationDraft,
  );
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: currentDraft,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler: fixture.routeRuntimeMessage,
  });
  const inferred = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command,
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });
  assert.equal(inferred, false);
  assert.equal(content.sendButton.clicks, 0);
  assert.equal(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .lastAttemptRevision,
    0,
  );
  assert.deepEqual(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .runtimeOwnedRegistrationDraft,
    capabilityBefore,
  );

  let nonPollMessages = 0;
  const suppressed = makeContentRuntime({
    inlineSelectedApp: true,
    initialComposerText: currentDraft,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler(message) {
      if (message.type === "wake:poll") {
        return {
          ok: true,
          registration: {
            ...clone(fixture.registration.value),
            lastAttemptRevision: fixture.targetRevision,
            lastAttemptAt: 1000000,
          },
          target: targetForRegistration(
            fixture.registration,
            "waking",
            fixture.targetRevision,
          ),
        };
      }
      nonPollMessages += 1;
      return { ok: false };
    },
  });
  await suppressed.api.pollWake();
  assert.equal(nonPollMessages, 0);
  assert.equal(suppressed.sendButton.clicks, 0);
}

async function providerNormalizedDraftChangedDuringWaitNeverConfirms() {
  const fixture = staleWakeCleanupFixture(721, { targetRevision: 1 });
  const command = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const providerCurrent = `\uFEFF${fixture.appIdentity.appLabel[0]}${fixture.appIdentity.appLabel} ${command.replace(/\n/g, " ")}`;
  const capabilityBefore = clone(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey]
      .runtimeOwnedRegistrationDraft,
  );
  let confirmCalls = 0;
  let humanEdited = false;
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: fixture.storedDraft,
    mentionCandidates: false,
    sendInitiallyVisible: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    providerShadowText: providerCurrent,
    providerShadowDelayMs: 50,
    mutateDuringDelay({ composer, milliseconds }) {
      if (milliseconds === 100 && !humanEdited) {
        humanEdited = true;
        composer.textContent += " human edit";
      }
    },
    runtimeMessageHandler(message) {
      if (message.type === "wake:confirmFinalSend") {
        confirmCalls += 1;
      }
      return fixture.routeRuntimeMessage(message);
    },
  });

  const sent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command,
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });
  const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];

  assert.equal(sent, false);
  assert.equal(confirmCalls, 0);
  assert.equal(content.sendButton.clicks, 0);
  assert.ok(content.composer.textContent.endsWith(" human edit"));
  assert.equal(stored.lastAttemptRevision, 0);
  assert.deepEqual(stored.runtimeOwnedRegistrationDraft, capabilityBefore);
}

async function exactCurrentWakeAtEntryCommitsReservedCapability() {
  const fixture = staleWakeCleanupFixture(730);
  const command = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const currentDraft = exactCurrentProviderWakeDraft(
    fixture.appIdentity,
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const before = durableWakeSafetySnapshot(
    fixture.runtime.storage.wakeTargets[fixture.registration.storageKey],
  );
  assert.equal(before.lastAttemptRevision, 0);
  assert.ok(before.runtimeOwnedRegistrationDraft);
  assert.ok(before.provisionedConnectorProof);
  assert.deepEqual(fixture.runtime.getTab(before.runtimeOpenedTabId), {
    id: before.runtimeOpenedTabId,
    url: before.conversationUrl,
    active: false,
    pinned: false,
  });
  const messages = [];
  const route = realServiceWorkerWakeRouter(
    fixture.runtime,
    fixture.registration,
  );
  const content = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: currentDraft,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    async runtimeMessageHandler(message) {
      messages.push(message.type);
      if (message.type === "wake:confirmFinalSend") {
        const beforeConfirmation =
          fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
        assert.equal(beforeConfirmation.lastAttemptRevision, 0);
        assert.deepEqual(
          beforeConfirmation.runtimeOwnedRegistrationDraft,
          before.runtimeOwnedRegistrationDraft,
        );
      }
      const response = await route(message);
      if (message.type === "wake:confirmFinalSend" && response.safe) {
        const afterConfirmation =
          fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
        assert.equal(afterConfirmation.lastAttemptRevision, fixture.targetRevision);
        assert.equal(afterConfirmation.runtimeOwnedRegistrationDraft, null);
        assert.equal(content.sendButton.clicks, 0);
      }
      return response;
    },
  });

  const sent = await content.api.sendAppCommand({
    appIdentity: fixture.appIdentity,
    command,
    draftMarker: fixture.registration.value.wakeTargetId,
    delivery: fixture.delivery,
  });
  const committed = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];

  assert.equal(sent, true, "an exact CURRENT provider serialization must recover");
  assert.deepEqual(messages, [
    "wake:prepareFinalSend",
    "wake:recordContentDisposition",
    "runtime:reserveExactCurrentWakeDraft",
    "wake:confirmFinalSend",
  ]);
  assert.equal(content.sendButton.clicks, 1);
  assert.equal(committed.lastAttemptRevision, fixture.targetRevision);
  assert.equal(committed.runtimeOwnedRegistrationDraft, null);
  assert.ok(committed.runtimeOwnedRegistrationDraftConsumedAt > 0);
  assert.deepEqual(committed.provisionedConnectorProof, before.provisionedConnectorProof);
  assert.deepEqual(committed.appIdentity, before.appIdentity);
  assert.equal(committed.bindingRevision, before.bindingRevision);
  assert.equal(committed.connectorEpoch, before.connectorEpoch);
}

async function exactCurrentWakeRecoverySurvivesRestartAndRejectsReplay() {
  const fixture = staleWakeCleanupFixture(731);
  const command = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const currentDraft = exactCurrentProviderWakeDraft(
    fixture.appIdentity,
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const target = targetForRegistration(
    fixture.registration,
    "waking",
    fixture.targetRevision,
  );
  const restartedRuntime = makeRuntime({
    registrations: [],
    storage: clone(fixture.runtime.storage),
    targets: [[fixture.registration.value.wakeTargetId, target]],
    projectBindings: {
      [fixture.registration.value.projectId]: clone(fixture.binding),
    },
    initialTabs: [
      {
        id: fixture.registration.value.runtimeOpenedTabId,
        url: fixture.registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const recovered = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: currentDraft,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler: realServiceWorkerWakeRouter(
      restartedRuntime,
      fixture.registration,
    ),
  });

  assert.equal(
    await recovered.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command,
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    }),
    true,
  );
  assert.equal(recovered.sendButton.clicks, 1);

  const replayRuntime = makeRuntime({
    registrations: [],
    storage: clone(restartedRuntime.storage),
    targets: [[fixture.registration.value.wakeTargetId, target]],
    projectBindings: {
      [fixture.registration.value.projectId]: clone(fixture.binding),
    },
    initialTabs: [
      {
        id: fixture.registration.value.runtimeOpenedTabId,
        url: fixture.registration.value.conversationUrl,
        active: false,
        pinned: false,
      },
    ],
  });
  const replay = makeContentRuntime({
    appIdentity: fixture.appIdentity,
    contentEditableComposer: true,
    inlineSelectedApp: true,
    initialComposerText: currentDraft,
    mentionCandidates: false,
    conversationUrl: fixture.registration.value.conversationUrl,
    runtimeMessageHandler: realServiceWorkerWakeRouter(
      replayRuntime,
      fixture.registration,
    ),
  });
  const committedBeforeReplay = durableWakeSafetySnapshot(
    replayRuntime.storage.wakeTargets[fixture.registration.storageKey],
  );

  assert.equal(
    await replay.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command,
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    }),
    false,
  );
  assert.equal(replay.sendButton.clicks, 0);
  assert.equal(replay.composer.textContent, currentDraft);
  assert.deepEqual(
    durableWakeSafetySnapshot(
      replayRuntime.storage.wakeTargets[fixture.registration.storageKey],
    ),
    committedBeforeReplay,
  );
}

async function concurrentExactCurrentWakeRecoveryCommitsOnce() {
  const fixture = staleWakeCleanupFixture(732);
  const command = fullWorkerWakeCommand(
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const currentDraft = exactCurrentProviderWakeDraft(
    fixture.appIdentity,
    fixture.registration.value.wakeTargetId,
    fixture.targetRevision,
  );
  const route = realServiceWorkerWakeRouter(
    fixture.runtime,
    fixture.registration,
  );
  const contents = [0, 1].map(() =>
    makeContentRuntime({
      appIdentity: fixture.appIdentity,
      contentEditableComposer: true,
      inlineSelectedApp: true,
      initialComposerText: currentDraft,
      mentionCandidates: false,
      conversationUrl: fixture.registration.value.conversationUrl,
      runtimeMessageHandler: route,
    }),
  );

  const results = await Promise.all(
    contents.map((content) =>
      content.api.sendAppCommand({
        appIdentity: fixture.appIdentity,
        command,
        draftMarker: fixture.registration.value.wakeTargetId,
        delivery: fixture.delivery,
      }),
    ),
  );
  const committed = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];

  assert.equal(results.filter(Boolean).length, 1);
  assert.equal(
    contents.reduce((total, content) => total + content.sendButton.clicks, 0),
    1,
  );
  assert.equal(committed.lastAttemptRevision, fixture.targetRevision);
  assert.equal(committed.runtimeOwnedRegistrationDraft, null);
  assert.ok(committed.runtimeOwnedRegistrationDraftConsumedAt > 0);
}

async function exactCurrentWakeRecoveryNearMissesFailClosedWithoutEditing() {
  const directHandlerCases = [
    {
      name: "direct @label flattened",
      draft(appIdentity, command) {
        return `@${appIdentity.appLabel}${command.replace(/\n/g, " ")}`;
      },
    },
    {
      name: "direct canonical prefix multiline",
      draft(appIdentity, command) {
        return `\uFEFF${appIdentity.appLabel[0]}${appIdentity.appLabel} ${command}`;
      },
    },
    {
      name: "direct label flattened",
      draft(appIdentity, command) {
        return `${appIdentity.appLabel}${command.replace(/\n/g, " ")}`;
      },
    },
    {
      name: "direct extra FEFF",
      draft(appIdentity, command) {
        return `\uFEFF\uFEFF${appIdentity.appLabel[0]}${appIdentity.appLabel} ${command.replace(/\n/g, " ")}`;
      },
    },
    {
      name: "direct extra provider space",
      draft(appIdentity, command) {
        return `\uFEFF${appIdentity.appLabel[0]}${appIdentity.appLabel}  ${command.replace(/\n/g, " ")}`;
      },
    },
    {
      name: "direct zero width prefix",
      draft(appIdentity, command) {
        return `\u200B\uFEFF${appIdentity.appLabel[0]}${appIdentity.appLabel} ${command.replace(/\n/g, " ")}`;
      },
    },
    {
      name: "direct human suffix",
      draft(appIdentity, command) {
        return `\uFEFF${appIdentity.appLabel[0]}${appIdentity.appLabel} ${command.replace(/\n/g, " ")} human text`;
      },
    },
  ];
  for (const [offset, testCase] of directHandlerCases.entries()) {
    const fixture = staleWakeCleanupFixture(760 + offset);
    const command = fullWorkerWakeCommand(
      fixture.registration.value.wakeTargetId,
      fixture.targetRevision,
    );
    const before = durableWakeSafetySnapshot(
      fixture.runtime.storage.wakeTargets[fixture.registration.storageKey],
    );
    const common = {
      tabId: fixture.registration.value.runtimeOpenedTabId,
      conversationUrl: fixture.registration.value.conversationUrl,
      delivery: fixture.delivery,
    };
    const prepared = await fixture.runtime.api.prepareFinalSendAdmission(common);
    assert.equal(prepared.safe, true, testCase.name);
    const reserved = await fixture.runtime.api.reserveExactCurrentWakeDraft({
      ...common,
      appIdentity: fixture.appIdentity,
      draft: testCase.draft(fixture.appIdentity, command),
      admissionToken: prepared.admissionToken,
    });
    assert.equal(reserved.safe, false, testCase.name);
    assert.deepEqual(
      durableWakeSafetySnapshot(
        fixture.runtime.storage.wakeTargets[fixture.registration.storageKey],
      ),
      before,
      testCase.name,
    );
  }

  const cases = [
    { name: "human text", composerSuffix: " human edit" },
    { name: "active tab", tabPatch: { active: true } },
    { name: "pinned tab", tabPatch: { pinned: true } },
    { name: "wrong target", composerTarget: "wtgt_ffffffffffffffffffffffffffffffff" },
    { name: "wrong revision", composerRevisionOffset: 1 },
    { name: "missing pill", contentPatch: { inlineSelectedApp: false } },
    { name: "wrong pill", contentPatch: { selectedAppOrigin: "details_modal" } },
    { name: "missing proof", fixturePatch: { missingProof: true } },
    {
      name: "consumed capability",
      registrationMutation(registration) {
        registration.runtimeOwnedRegistrationDraft = null;
        registration.runtimeOwnedRegistrationDraftConsumedAt = 999;
      },
    },
    {
      name: "previous attempt",
      fixturePatch: { lastAttemptRevision: 14 },
    },
    {
      name: "binding mismatch",
      fixturePatch: { bindingPatch: { revision: 3 } },
    },
  ];
  for (const [offset, testCase] of cases.entries()) {
    const fixture = staleWakeCleanupFixture(740 + offset, {
      targetRevision: 14,
      ...(testCase.fixturePatch || {}),
    });
    const stored = fixture.runtime.storage.wakeTargets[fixture.registration.storageKey];
    if (testCase.registrationMutation) {
      testCase.registrationMutation(stored);
    }
    if (testCase.tabPatch) {
      fixture.runtime.patchTab(
        fixture.registration.value.runtimeOpenedTabId,
        testCase.tabPatch,
      );
    }
    const composerRevision =
      fixture.targetRevision + Number(testCase.composerRevisionOffset || 0);
    const originalText =
      exactCurrentProviderWakeDraft(
        fixture.appIdentity,
        testCase.composerTarget || fixture.registration.value.wakeTargetId,
        composerRevision,
      ) + (testCase.composerSuffix || "");
    const before = durableWakeSafetySnapshot(stored);
    const content = makeContentRuntime({
      appIdentity: fixture.appIdentity,
      contentEditableComposer: true,
      inlineSelectedApp: true,
      initialComposerText: originalText,
      mentionCandidates: false,
      conversationUrl: fixture.registration.value.conversationUrl,
      runtimeMessageHandler: realServiceWorkerWakeRouter(
        fixture.runtime,
        fixture.registration,
      ),
      ...(testCase.contentPatch || {}),
    });

    const sent = await content.api.sendAppCommand({
      appIdentity: fixture.appIdentity,
      command: fullWorkerWakeCommand(
        fixture.registration.value.wakeTargetId,
        fixture.targetRevision,
      ),
      draftMarker: fixture.registration.value.wakeTargetId,
      delivery: fixture.delivery,
    });

    assert.equal(sent, false, testCase.name);
    assert.equal(content.sendButton.clicks, 0, testCase.name);
    assert.equal(content.composer.textContent, originalText, testCase.name);
    assert.deepEqual(
      durableWakeSafetySnapshot(
        fixture.runtime.storage.wakeTargets[fixture.registration.storageKey],
      ),
      before,
      testCase.name,
    );
  }
}

async function runScenario(scenario) {
  switch (scenario) {
    case "stale-no-ready":
      await staleNoReadyDoesNotOpenTabs();
      return;
    case "registration-map-race":
      await concurrentRegistrationUpdatesPreserveBothConversations();
      return;
    case "park-user-claim-race":
      await userClaimDuringParkingPreventsClose();
      return;
    case "composer-human-race":
      await humanComposerRacesNeverInjectOrClearText();
      return;
    case "bootstrap-composer-race":
      await bootstrapComposerAndOwnershipRacesNeverSubmit();
      return;
    case "generation-in-progress":
      await generationInProgressPreventsRuntimeActions();
      return;
    case "project-new-chat-race":
      await projectNewChatClickRechecksPageAndUserOwnership();
      return;
    case "one-explicit-wake":
      await oneExplicitWakeOnlyOpensItsConversationOnce();
      return;
    case "retirement-runtime-open-gap":
      await pendingRetirementInFinalOpenGapPreventsRuntimeTabCreation();
      return;
    case "lower-stale-revision":
      await lowerWakeRevisionCannotReopen();
      return;
    case "registration-rebind":
      await reboundRegistrationCannotContaminateDelivery();
      return;
    case "stale-cleanup-user-claim":
      await claimedRuntimeCreatedTabSurvivesStaleCleanup();
      return;
    case "concurrent-candidates":
      await concurrentCandidatesNeverExceedOneGlobalOpen();
      return;
    case "recovery-and-cooldown":
      await recoveriesAndPersistedCooldownRemainSafe();
      return;
    case "existing-main-binding-refresh":
      await existingMainRefreshesFromCanonicalProjectBinding();
      return;
    case "existing-main-binding-refresh-fetches-localhost":
      await existingMainRefreshReadsItsCanonicalProjectBinding();
      return;
    case "main-binding-refresh-identity-only":
      await mainBindingRefreshChangesAppIdentityOnly();
      return;
    case "main-binding-refresh-preserves-unrelated":
      await bindingRefreshPreservesUnrelatedRegistration();
      return;
    case "main-binding-refresh-no-auto-main":
      await bindingRefreshNeverCreatesUnregisteredMain();
      return;
    case "main-binding-refresh-fail-closed":
      await bindingRefreshFailsClosedOnProjectMismatchOrMissingApp();
      return;
    case "poll-main-refresh-before-waking-target":
      await pollTargetRefreshesMainBeforeReturningWakingTarget();
      return;
    case "poll-main-delayed-old-binding":
      await delayedOlderBindingCannotOverwriteNewerStoredBinding();
      return;
    case "poll-worker-same-ref-label-refresh":
      await workerSameRefLabelRefreshesBeforeWakeDelivery();
      return;
    case "poll-worker-binding-mismatch":
      await workerBindingMismatchFailsClosed();
      return;
    case "poll-worker-delayed-old-binding":
      await delayedOlderWorkerBindingCannotOverwriteNewerState();
      return;
    case "poll-worker-missing-epoch-proof":
      await missingEpochRequiresExactProvisionedProof();
      return;
    case "runtime-owned-provisioning-draft":
      await runtimeOwnedProvisioningDraftCanBeConsumedSafely();
      return;
    case "final-send-admission":
      await finalSendAdmissionIsOneShotAndRevocable();
      return;
    case "wake-adapter-version-handshake":
      await staleWakeAdapterIsReinjectedOnlyIntoOwnedBackgroundTab();
      return;
    case "wake-adapter-content-lifecycle":
      contentAdapterReplacementSurvivesThrowingOldCleanup();
      return;
    case "runtime-owned-draft-confirmation":
      await durableDraftReservationFailsClosed();
      return;
    case "corrupted-stored-draft-capability":
      await corruptedStoredCapabilityFieldsFailClosed();
      return;
    case "stranded-human-draft":
      await strandedHumanTextCannotConsumeFirstWakeCapability();
      return;
    case "durable-draft-reserve-commit":
      await durableDraftReserveCommitSurvivesLocalFailureAndRestart();
      return;
    case "durable-draft-reserve-semantic-cas":
      await durableDraftReservationUsesSafetyCriticalCas();
      return;
    case "durable-draft-confirm-semantic-cas":
      await durableDraftConfirmationUsesSafetyCriticalCas();
      return;
    case "resume-attempt-queued-merge":
      await resumeAttemptDoesNotOverwriteQueuedRegistrationUpdate();
      return;
    case "durable-reserve-rejection-stage":
      await durableReserveRejectionStageTelemetryIsBounded();
      return;
    case "pre-candidate-provider-draft-mismatch":
      await preCandidateProviderDraftMismatchIsBoundedAndTextFree();
      return;
    case "pre-candidate-composer-changed":
      await preCandidateComposerChangeHasBoundedSubstage();
      return;
    case "pre-candidate-document-claimed":
      await preCandidateDocumentClaimHasBoundedSubstage();
      return;
    case "pre-candidate-empty-without-selected-app":
      await preCandidateEmptyWithoutSelectedAppHasBoundedSubstage();
      return;
    case "selected-app-pill-ancestor-scope":
      await selectedAppPillScopeStopsAfterFiveComposerScopes();
      return;
    case "provider-normalized-registration-draft":
      await providerNormalizedRegistrationDraftRemainsRuntimeOwned();
      return;
    case "stale-owned-wake-cleanup-retry":
      await staleOwnedWakeCommandRestoresDraftThenCurrentWakeSendsOnce();
      return;
    case "stale-owned-wake-cleanup-leading-feff":
      await singleLeadingFeffProviderWakeRestoresDraftThenCurrentWakeSendsOnce();
      return;
    case "stale-cleanup-provider-turn-persistence":
      await staleCleanupRestorationSurvivesProviderTurn();
      return;
    case "stale-owned-wake-cleanup-zero-width-rejections":
      await staleWakeCleanupRejectsNoncanonicalZeroWidthText();
      return;
    case "stale-owned-wake-cleanup-fail-closed":
      await staleOwnedWakeCleanupFailsClosedForNearMissesAndRaces();
      return;
    case "stale-wake-cleanup-pill-provenance":
      await staleWakeCleanupRequiresExactInlineSelectionPill();
      return;
    case "stale-wake-cleanup-consumed-capability-conflict":
      await staleWakeCleanupRejectsConsumedCapabilityStillPresent();
      return;
    case "controlled-editor-stale-cleanup":
      await staleCleanupPreservesControlledEditorPillIdentity();
      return;
    case "controlled-editor-split-residual":
      await splitResidualTopologyNeverFallsBackToWholeComposerReplacement();
      return;
    case "controlled-editor-failure-rollback":
      await failedSendRollbackPreservesControlledEditorPillIdentity();
      return;
    case "controlled-editor-native-input":
      await nativeExecCommandInputIsNotDuplicated();
      return;
    case "controlled-editor-rejected-transactions":
      await rejectedControlledEditorTransactionsDoNotCommit();
      return;
    case "exact-current-normalization-near-misses":
      await exactCurrentNormalizationNearMissesFailClosed();
      return;
    case "exact-current-entry-poll-suppression":
      await exactCurrentAtEntryAndPollSuppressionAreFailClosed();
      return;
    case "provider-normalized-wait-human-edit":
      await providerNormalizedDraftChangedDuringWaitNeverConfirms();
      return;
    case "exact-current-wake-recovery-commit":
      await exactCurrentWakeAtEntryCommitsReservedCapability();
      return;
    case "exact-current-wake-recovery-restart-replay":
      await exactCurrentWakeRecoverySurvivesRestartAndRejectsReplay();
      return;
    case "exact-current-wake-recovery-concurrency":
      await concurrentExactCurrentWakeRecoveryCommitsOnce();
      return;
    case "exact-current-wake-recovery-near-misses":
      await exactCurrentWakeRecoveryNearMissesFailClosedWithoutEditing();
      return;
    case "contenteditable-react-reconciliation":
      await contentEditableInsertionCommitsProviderState();
      return;
    case "provider-shadow-two-poll-live-signature":
      await providerShadowReconciliationCannotStrandCurrentWakeAcrossPolls();
      return;
    case "durable-worker-retirement-restart":
      await durableWorkerRetirementSurvivesRestart();
      return;
    case "concurrent-worker-retirement":
      await concurrentRetirementDeletesOnlyExactRegistration();
      return;
    case "batch-worker-retirement-drain":
      await batchRetirementDrainBlocksProvisioningUntilExactSuccess();
      return;
    case "batch-worker-retirement-revision-refresh":
      await batchRetirementRefreshesRevisionOnlyForExactIdleMarker();
      return;
    case "worker-retirement-preserves-human-main":
      await retirementPreservesMainAndUserManagedWorkers();
      return;
    case "worker-retirement-suppresses-heartbeat":
      await pendingRetirementSuppressesPollingAndHeartbeat();
      return;
    case "content-disposition-message-contract":
      await contentDispositionMessagesAreExactAndPreReserve();
      return;
    case "content-disposition-durable-validation":
      await durableContentDispositionIsBoundedAndExact();
      return;
    case "content-disposition-nonauthoritative-liveness":
      await exactCurrentDispositionIsNonAuthoritativeLiveness();
      return;
    case "content-disposition-read-and-race-boundary":
      await exactDispositionAvoidsExternalReadsAndPollCommitIsBestEffort();
      return;
    case "content-disposition-storage-queue-boundary":
      await dispositionStorageQueueIsIsolatedBoundedAndObservedAtValidation();
      return;
    default:
      throw new Error("Unknown Gate A runtime-admission scenario: " + scenario);
  }
}

runScenario(process.argv[2]).catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
