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
const extensionVersion = JSON.parse(
  fs.readFileSync(path.join(extensionRoot, "manifest.json"), "utf8"),
).version;

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function admittedRequest(index, patch = {}) {
  return {
    request_id: "prq_" + String(index).padStart(32, "0"),
    project_id: "prj_runtime_12345678",
    wake_target_id: "wtgt_" + String(index).padStart(32, "0"),
    admission_revision: index,
    status: { status: "opening_project" },
    adapter_metadata: {
      provider: "chatgpt",
      project_ref: "project-" + index,
      project_path: "/g/project-" + index,
      app_ref: "plugin_asdk_app_custom",
      app_label: "my-local-mcp",
    },
    ...patch,
  };
}

function makeProvisioningRuntime({
  requests,
  bootstrapResponse = { ok: true, registrationDraft: "my-local-mcp" },
  projectResponse = { ok: true },
  registeredStatus = 200,
  registeredStatuses = null,
  delayCreates = false,
  claimTabDuringProjectOpen = false,
  claimThenReleaseDuringProjectOpen = false,
  claimTabDuringBootstrapConfirm = null,
  claimTabDuringRegistrationConflict = false,
  projectRedirectUrl = null,
  projectBinding = null,
  projectBindingSequence = null,
  omitHeartbeatEpoch = false,
  initialStorage = {},
  onRuntimeOpenAdmissionRead = null,
  onWorkerProvisioningNext = null,
  now = 1000000,
}) {
  const sharedStorage = clone(initialStorage);
  const tabs = new Map();
  const events = [];
  const createCalls = [];
  const registeredBodies = [];
  const failedBodies = [];
  const bootstrapMessages = [];
  const pendingCreateResolvers = [];
  const requestQueue = [...requests].map(clone);
  const firstRequest = requestQueue[0] || null;
  const pendingConnectorBinding = Object.values(
    initialStorage.workerProvisioningState?.admissions || {},
  ).find((admission) => admission?.connectorBinding)?.connectorBinding;
  const defaultProjectBinding = firstRequest
    ? {
        project_id: firstRequest.project_id,
        provider: "chatgpt",
        project_ref: firstRequest.adapter_metadata?.project_ref,
        project_path: firstRequest.adapter_metadata?.project_path,
        app_identity: {
          app_ref: firstRequest.adapter_metadata?.app_ref,
          app_label: firstRequest.adapter_metadata?.app_label,
        },
        connector_epoch: 0,
        revision: 0,
      }
    : pendingConnectorBinding
      ? {
          project_id: pendingConnectorBinding.projectId,
          provider: pendingConnectorBinding.provider,
          project_ref: pendingConnectorBinding.projectRef,
          project_path: pendingConnectorBinding.projectPath,
          app_identity: {
            app_ref: pendingConnectorBinding.appRef,
            app_label: pendingConnectorBinding.appLabel,
          },
          connector_epoch: pendingConnectorBinding.connectorEpoch,
          revision: pendingConnectorBinding.bindingRevision,
        }
      : null;
  const bindingReads = projectBindingSequence
    ? [...projectBindingSequence].map(clone)
    : [clone(projectBinding || defaultProjectBinding)];
  let lastProjectBinding = bindingReads[0] || null;
  let clock = now;
  let nextTabId = 1;
  let createInFlight = 0;
  let maxCreateInFlight = 0;
  let wakeStatus = "idle";
  let wakeRevision = 0;
  const registeredStatusQueue = registeredStatuses ? [...registeredStatuses] : null;
  let provisioningApi = null;

  const chrome = {
    runtime: {
      getManifest() {
        return { version: extensionVersion };
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
      onActivated: { addListener() {} },
      onRemoved: { addListener() {} },
      async create({ url, active }) {
        events.push("tabs.create");
        createCalls.push({ url, active, startedAt: clock });
        createInFlight += 1;
        maxCreateInFlight = Math.max(maxCreateInFlight, createInFlight);
        if (delayCreates) {
          await new Promise((resolve) => pendingCreateResolvers.push(resolve));
        }
        const tab = {
          id: nextTabId,
          url: projectRedirectUrl || url,
          active,
          pinned: false,
          discarded: false,
        };
        nextTabId += 1;
        tabs.set(tab.id, tab);
        createInFlight -= 1;
        return clone(tab);
      },
      async get(tabId) {
        const tab = tabs.get(tabId);
        if (!tab) {
          throw new Error("Unknown tab " + tabId);
        }
        return clone(tab);
      },
      async query() {
        return [...tabs.values()].map(clone);
      },
      async reload(tabId) {
        events.push("tabs.reload");
        const tab = tabs.get(tabId);
        if (tab) {
          tab.discarded = false;
        }
      },
      async remove(tabId) {
        events.push("tabs.remove");
        tabs.delete(tabId);
      },
      async sendMessage(tabId, message) {
        const tab = tabs.get(tabId);
        if (!tab) {
          throw new Error("Unknown tab " + tabId);
        }
        events.push(message.type);
        if (message.type === "provision:openProjectNewChat") {
          if (claimTabDuringProjectOpen) {
            tab.active = true;
          }
          if (claimThenReleaseDuringProjectOpen) {
            tab.active = true;
            provisioningApi.noteProvisioningTabActivation(tabId);
            tab.active = false;
          }
          if (projectResponse?.ok) {
            const ownership = await provisioningApi.confirmProvisioningProjectOpen({
              tabId,
              ownershipToken: message.ownershipToken,
            });
            if (!ownership.safe) {
              return {
                ok: false,
                code: "provisioning_tab_claimed",
                retryable: false,
              };
            }
          }
          return clone(projectResponse);
        }
        if (message.type === "provision:bootstrap") {
          bootstrapMessages.push(clone(message));
          if (claimTabDuringBootstrapConfirm) {
            tab[claimTabDuringBootstrapConfirm] = true;
            const ownership = await provisioningApi.confirmProvisioningBootstrapSubmit({
              tabId,
              bootstrapToken: message.bootstrapToken,
            });
            assert.equal(ownership.safe, false);
            return {
              ok: false,
              code: "bootstrap_ownership_lost",
              retryable: false,
            };
          }
          const ownership = await provisioningApi.confirmProvisioningBootstrapSubmit({
            tabId,
            bootstrapToken: message.bootstrapToken,
          });
          if (!ownership.safe) {
            return {
              ok: false,
              code: "bootstrap_ownership_lost",
              retryable: false,
            };
          }
          const response = clone(bootstrapResponse);
          if (response?.ok) {
            tab.url = "https://chatgpt.com/c/provisioned-" + tabId;
          }
          return response;
        }
        if (message.type === "provision:providerState") {
          return { ok: true, rateLimited: false };
        }
        if (message.type === "runtime:canPark") {
          return { ok: true, canPark: true };
        }
        if (message.type === "runtime:diagnoseWakeAdapter") {
          return {
            version: extensionVersion,
            extensionContextAvailable: true,
            pollTimerActive: true,
            everClaimed: false,
            composerPresent: true,
            generating: false,
          };
        }
        return null;
      },
    },
    scripting: {
      async executeScript() {
        throw new Error("a current adapter must not require reinjection");
      },
    },
    storage: {
      local: {
        async get(keys) {
          if (typeof keys === "string") {
            if (
              keys === "runtimeOpenAdmission" &&
              onRuntimeOpenAdmissionRead
            ) {
              await onRuntimeOpenAdmissionRead({ storage: sharedStorage });
            }
            return { [keys]: clone(sharedStorage[keys]) };
          }
          return clone(sharedStorage);
        },
        async set(patch) {
          Object.assign(sharedStorage, clone(patch));
        },
      },
    },
  };

  async function fetchStub(rawUrl, options = {}) {
    const url = new URL(rawUrl);
    const pathname = url.pathname;
    if (pathname === "/api/worker-provisioning/next") {
      events.push("next");
      const request = clone(requestQueue.shift() || null);
      if (onWorkerProvisioningNext) {
        await onWorkerProvisioningNext({ request, storage: sharedStorage });
      }
      return jsonResponse({ request });
    }
    const registeredMatch = pathname.match(
      /^\/api\/worker-provisioning\/([^/]+)\/registered$/,
    );
    if (registeredMatch) {
      events.push("registered");
      if (claimTabDuringRegistrationConflict) {
        const tab = [...tabs.values()][0];
        if (tab) {
          tab.active = true;
          provisioningApi.noteProvisioningTabActivation(tab.id);
        }
      }
      registeredBodies.push({
        requestId: decodeURIComponent(registeredMatch[1]),
        body: JSON.parse(options.body),
      });
      return jsonResponse(
        {},
        registeredStatusQueue?.length
          ? registeredStatusQueue.shift()
          : registeredStatus,
      );
    }
    const failedMatch = pathname.match(
      /^\/api\/worker-provisioning\/([^/]+)\/failed$/,
    );
    if (failedMatch) {
      events.push("failed");
      failedBodies.push({
        requestId: decodeURIComponent(failedMatch[1]),
        body: JSON.parse(options.body),
      });
      return jsonResponse({ retry_after_ms: 60000 });
    }
    const heartbeatMatch = pathname.match(
      /^\/api\/wake\/targets\/([^/]+)\/runtime-heartbeat$/,
    );
    if (heartbeatMatch) {
      events.push("runtime-heartbeat");
      return jsonResponse({
        target: {
          wake_target_id: heartbeatMatch[1],
          project_id: "prj_runtime_12345678",
          ...(!omitHeartbeatEpoch
            ? {
                registered_connector_epoch: Number(
                  lastProjectBinding?.connector_epoch ?? 0,
                ),
              }
            : {}),
          status: wakeStatus,
          wake_revision: wakeRevision,
        },
      });
    }
    if (/^\/api\/project-bindings\/[^/]+\/connector$/.test(pathname)) {
      const nextBinding = bindingReads.length > 1
        ? bindingReads.shift()
        : bindingReads[0];
      if (!nextBinding) {
        throw new Error("Unexpected connector binding read " + pathname);
      }
      lastProjectBinding = clone(nextBinding);
      return jsonResponse({ connector: clone(nextBinding) });
    }
    throw new Error("Unexpected loopback path " + pathname);
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
  const source =
    identitySource +
    "\n" +
    policySource +
    "\n" +
    persistentWorkerSource +
    "\n" +
    provisioningSource +
    "\n" +
    workerSource +
    "\n" +
    "globalThis.__provisioning = {" +
    "processNextAdmittedWorkerProvision," +
    "sweepRuntimeTargets," +
    "confirmProvisioningBootstrapSubmit," +
    "confirmProvisioningProjectOpen," +
    "noteProvisioningTabActivation," +
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
    setTimeout(callback, milliseconds) {
      clock += milliseconds;
      callback();
      return 1;
    },
    clearTimeout() {},
  });
  vm.runInContext(source, context, { filename: "service_worker.js" });
  provisioningApi = context.__provisioning;

  return {
    api: context.__provisioning,
    storage: sharedStorage,
    events,
    createCalls,
    registeredBodies,
    failedBodies,
    bootstrapMessages,
    hasTab(tabId) {
      return tabs.has(tabId);
    },
    patchTab(tabId, patch) {
      Object.assign(tabs.get(tabId), patch);
    },
    setWake(status, revision) {
      wakeStatus = status;
      wakeRevision = revision;
    },
    get createInFlight() {
      return createInFlight;
    },
    get maxCreateInFlight() {
      return maxCreateInFlight;
    },
    advance(milliseconds) {
      clock += milliseconds;
    },
    releaseCreates() {
      for (const resolve of pendingCreateResolvers.splice(0)) {
        resolve();
      }
    },
  };
}

function jsonResponse(payload, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() {
      return { ok: true, ...payload };
    },
  };
}

async function waitFor(condition) {
  for (let index = 0; index < 50; index += 1) {
    if (condition()) {
      return;
    }
    await new Promise((resolve) => setImmediate(resolve));
  }
  throw new Error("Timed out waiting for worker provisioning.");
}

async function provisionOneInOrder() {
  const request = admittedRequest(1);
  const runtime = makeProvisioningRuntime({ requests: [request] });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.provisioned, true);
  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.createCalls[0].url, "https://chatgpt.com/g/project-1");
  assert.deepEqual(runtime.events.slice(0, 5), [
    "next",
    "tabs.create",
    "provision:openProjectNewChat",
    "provision:bootstrap",
    "provision:providerState",
  ]);
  assert.ok(
    runtime.events.indexOf("registered") > runtime.events.indexOf("provision:bootstrap"),
    "callback must follow provider bootstrap",
  );
  assert.equal(runtime.registeredBodies.length, 1);
  assert.deepEqual(runtime.bootstrapMessages[0].appIdentity, {
    appRef: "plugin_asdk_app_custom",
    appLabel: "my-local-mcp",
  });
  const workerRegistration = Object.values(runtime.storage.wakeTargets)[0];
  assert.deepEqual(workerRegistration.appIdentity, {
    appRef: "plugin_asdk_app_custom",
    appLabel: "my-local-mcp",
  });
  assert.equal(
    workerRegistration.runtimeOpenedTabId,
    1,
    "the never-claimed provisioning tab must become the runtime-owned Worker tab",
  );
  assert.deepEqual(runtime.registeredBodies[0], {
    requestId: request.request_id,
    body: {
      conversation_id: "provisioned-1",
      wake_target_id: request.wake_target_id,
      admission_revision: request.admission_revision,
    },
  });
  assert.equal(runtime.api.openInFlightCount(), 0);
}

async function currentSluggedProjectLandingIsAccepted() {
  const request = admittedRequest(1, {
    adapter_metadata: {
      provider: "chatgpt",
      project_ref: "g-p-6a7a81380ff4819185c013db8ea9580f",
      project_path: "/g/g-p-6a7a81380ff4819185c013db8ea9580f",
      app_ref: "plugin_asdk_app_custom",
      app_label: "my-local-mcp",
    },
  });
  const runtime = makeProvisioningRuntime({
    requests: [request],
    projectRedirectUrl:
      "https://chatgpt.com/g/g-p-6a7a81380ff4819185c013db8ea9580f-testproject/project",
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.provisioned, true);
  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.registeredBodies.length, 1);
}

async function provisionedTabIsReusedForFirstWake() {
  const request = admittedRequest(31);
  const runtime = makeProvisioningRuntime({
    requests: [request],
    projectBinding: {
      project_id: request.project_id,
      provider: "chatgpt",
      project_ref: request.adapter_metadata.project_ref,
      project_path: request.adapter_metadata.project_path,
      app_identity: {
        app_ref: request.adapter_metadata.app_ref,
        app_label: request.adapter_metadata.app_label,
      },
      connector_epoch: 0,
      revision: 0,
    },
    omitHeartbeatEpoch: true,
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();
  assert.equal(outcome.provisioned, true);
  const registration = Object.values(runtime.storage.wakeTargets)[0];
  assert.equal(registration.runtimeOpenedTabId, 1);
  assert.equal(registration.bindingRevision, 0);
  assert.equal(registration.connectorEpoch, 0);
  assert.deepEqual(registration.provisionedConnectorProof, {
    requestId: request.request_id,
    wakeTargetId: request.wake_target_id,
    conversationId: "provisioned-1",
    admissionRevision: request.admission_revision,
    projectId: request.project_id,
    provider: "chatgpt",
    projectRef: request.adapter_metadata.project_ref,
    projectPath: request.adapter_metadata.project_path,
    appRef: request.adapter_metadata.app_ref,
    appLabel: request.adapter_metadata.app_label,
    bindingRevision: 0,
    connectorEpoch: 0,
    finalizedAt: 1000500,
  });

  runtime.patchTab(1, { discarded: true });
  runtime.setWake("waking", 1);
  await runtime.api.sweepRuntimeTargets();

  assert.equal(runtime.createCalls.length, 1);
  assert.equal(
    runtime.events.filter((event) => event === "tabs.reload").length,
    1,
    "the first wake must reload the adopted provisioning tab instead of consuming another open",
  );
  assert.equal(
    runtime.storage.wakeTargets[`conversation:provisioned-1`]
      .lastAdmittedWakeRevision,
    1,
  );
}

async function bindingChangeDuringFinalizeFailsClosed() {
  const request = admittedRequest(32);
  const binding = {
    project_id: request.project_id,
    provider: "chatgpt",
    project_ref: request.adapter_metadata.project_ref,
    project_path: request.adapter_metadata.project_path,
    app_identity: {
      app_ref: request.adapter_metadata.app_ref,
      app_label: request.adapter_metadata.app_label,
    },
    connector_epoch: 4,
    revision: 7,
  };
  const runtime = makeProvisioningRuntime({
    requests: [request],
    projectBindingSequence: [binding, { ...binding, revision: 8 }],
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.provisioned, false);
  assert.equal(outcome.reason, "connector_binding_changed");
  const registration = Object.values(runtime.storage.wakeTargets)[0];
  assert.equal(registration.provisionedConnectorProof, undefined);
  assert.equal(registration.retirementPending.reason, "connector_binding_changed");
}

async function provisionTwoStrictlySequential() {
  const first = admittedRequest(1);
  const second = admittedRequest(2, {
    adapter_metadata: clone(first.adapter_metadata),
  });
  const runtime = makeProvisioningRuntime({
    requests: [first, second],
    delayCreates: true,
  });

  const firstDelivery = runtime.api.processNextAdmittedWorkerProvision();
  await waitFor(() => runtime.createCalls.length === 1);
  assert.equal(runtime.createInFlight, 1);
  assert.equal(runtime.maxCreateInFlight, 1);

  const overlappingDelivery = await runtime.api.processNextAdmittedWorkerProvision();
  assert.equal(overlappingDelivery.reason, "busy_or_backoff");
  assert.equal(runtime.createCalls.length, 1);

  runtime.releaseCreates();
  const firstOutcome = await firstDelivery;
  assert.equal(firstOutcome.provisioned, true);
  assert.equal(runtime.registeredBodies.length, 1);

  runtime.advance(runtime.api.MIN_RUNTIME_OPEN_INTERVAL_MS);
  const secondOutcome = await runtime.api.processNextAdmittedWorkerProvision();
  assert.equal(secondOutcome.provisioned, true);
  assert.equal(runtime.createCalls.length, 2);
  assert.equal(runtime.maxCreateInFlight, 1);
  assert.equal(runtime.registeredBodies.length, 2);
  assert.ok(
    runtime.events.lastIndexOf("registered") > runtime.events.indexOf("tabs.create"),
  );
  const secondCreateIndex = runtime.events.lastIndexOf("tabs.create");
  const firstRegisteredIndex = runtime.events.indexOf("registered");
  assert.ok(
    firstRegisteredIndex < secondCreateIndex,
    "the second tab cannot be created before the first auto-registration callback",
  );
}

async function unsupportedProjectIdentityCreatesNothing() {
  const request = admittedRequest(1, {
    adapter_metadata: { provider: "chatgpt", project_ref: "project-ref-1" },
  });
  const runtime = makeProvisioningRuntime({ requests: [request] });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.provisioned, false);
  assert.equal(outcome.reason, "unsupported_project_identity");
  assert.equal(runtime.createCalls.length, 0);
  assert.equal(runtime.registeredBodies.length, 0);
  assert.deepEqual(runtime.failedBodies, [
    {
      requestId: request.request_id,
      body: {
        admission_revision: request.admission_revision,
        reason: "unsupported_project_identity",
        rate_limited: false,
      },
    },
  ]);
}

async function mismatchedProjectRefAndPathCreateNothing() {
  const request = admittedRequest(24, {
    adapter_metadata: {
      provider: "chatgpt",
      project_ref: "project-A",
      project_path: "/g/project-B",
    },
  });
  const runtime = makeProvisioningRuntime({ requests: [request] });
  const outcome = await runtime.api.processNextAdmittedWorkerProvision();
  assert.equal(outcome.reason, "unsupported_project_identity");
  assert.equal(runtime.createCalls.length, 0);
}

async function pendingRegistrationFinalizesAfterRestart() {
  const request = admittedRequest(25);
  const first = makeProvisioningRuntime({
    requests: [request],
    registeredStatus: 503,
  });
  const firstOutcome = await first.api.processNextAdmittedWorkerProvision();
  assert.equal(firstOutcome.reason, "registration_finalize_pending");
  assert.equal(first.failedBodies.length, 0);
  assert.equal(
    first.storage.workerProvisioningState.admissions[request.request_id].status,
    "registration_pending",
  );
  assert.equal(
    Object.values(first.storage.wakeTargets)[0].runtimeOpenedTabId,
    1,
    "a retryable callback failure must retain durable ownership of the created tab",
  );

  const restarted = makeProvisioningRuntime({
    requests: [],
    initialStorage: first.storage,
    now: 1000000 + first.api.MIN_RUNTIME_OPEN_INTERVAL_MS,
  });
  const restartOutcome = await restarted.api.processNextAdmittedWorkerProvision();
  assert.equal(restartOutcome.reason, "none");
  assert.equal(restarted.registeredBodies.length, 1);
  assert.equal(restarted.createCalls.length, 0);
  assert.equal(
    restarted.storage.workerProvisioningState.admissions[request.request_id].status,
    "registered",
  );
  assert.equal(
    Object.values(restarted.storage.wakeTargets)[0].runtimeOpenedTabId,
    1,
  );
}

async function registrationConflictDropsExactProvisionalRegistration() {
  const request = admittedRequest(27);
  const first = makeProvisioningRuntime({
    requests: [request],
    registeredStatus: 409,
  });

  const firstOutcome = await first.api.processNextAdmittedWorkerProvision();

  assert.equal(firstOutcome.reason, "registration_conflict");
  assert.equal(first.registeredBodies.length, 1);
  assert.equal(first.hasTab(1), false);
  assert.deepEqual(first.storage.wakeTargets, {});
  assert.equal(
    first.storage.workerProvisioningState.admissions[request.request_id].status,
    "registration_conflict",
  );

  const restarted = makeProvisioningRuntime({
    requests: [],
    initialStorage: first.storage,
    now: 1000000 + first.api.MIN_RUNTIME_OPEN_INTERVAL_MS,
  });
  const restartOutcome = await restarted.api.processNextAdmittedWorkerProvision();
  assert.equal(restartOutcome.reason, "none");
  assert.equal(restarted.registeredBodies.length, 0);
  assert.equal(restarted.createCalls.length, 0);
}

async function registrationConflictPreservesUserClaimedTab() {
  const request = admittedRequest(28);
  const runtime = makeProvisioningRuntime({
    requests: [request],
    registeredStatus: 409,
    claimTabDuringRegistrationConflict: true,
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.reason, "registration_conflict");
  assert.deepEqual(runtime.storage.wakeTargets, {});
  assert.equal(runtime.hasTab(1), true);
  assert.equal(runtime.events.includes("tabs.remove"), false);
}

async function sharedWakeCooldownDefersBeforeBackendAdmission() {
  const request = admittedRequest(26);
  const runtime = makeProvisioningRuntime({
    requests: [request],
    initialStorage: {
      runtimeOpenAdmission: { lastOpenedAt: 1000000 },
    },
  });
  const deferred = await runtime.api.processNextAdmittedWorkerProvision();
  assert.equal(deferred.reason, "browser_open_cooldown");
  assert.equal(runtime.events.includes("next"), false);
  assert.equal(runtime.failedBodies.length, 0);

  runtime.advance(runtime.api.MIN_RUNTIME_OPEN_INTERVAL_MS);
  const resumed = await runtime.api.processNextAdmittedWorkerProvision();
  assert.equal(resumed.provisioned, true);
  assert.equal(runtime.registeredBodies.length, 1);
}

async function terminalAdmissionCreatesNothing() {
  const request = admittedRequest(1, { status: { status: "cancelled" } });
  const runtime = makeProvisioningRuntime({ requests: [request] });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.reason, "none");
  assert.equal(runtime.createCalls.length, 0);
  assert.equal(runtime.registeredBodies.length, 0);
  assert.equal(runtime.failedBodies.length, 0);
}

async function duplicateOrStaleAdmissionDoesNotRepeatCallback() {
  const current = admittedRequest(2);
  const stale = admittedRequest(2, { admission_revision: 1 });
  const runtime = makeProvisioningRuntime({ requests: [current, stale] });

  assert.equal(
    (await runtime.api.processNextAdmittedWorkerProvision()).provisioned,
    true,
  );
  runtime.advance(runtime.api.MIN_RUNTIME_OPEN_INTERVAL_MS);
  const staleOutcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(staleOutcome.reason, "duplicate_or_stale");
  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.registeredBodies.length, 1);
  assert.equal(runtime.failedBodies.length, 0);
}

async function providerRateLimitStopsAdditionalProvisioning() {
  const first = admittedRequest(1);
  const second = admittedRequest(2);
  const runtime = makeProvisioningRuntime({
    requests: [first, second],
    registeredStatus: 429,
  });

  const firstOutcome = await runtime.api.processNextAdmittedWorkerProvision();
  const blockedOutcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(firstOutcome.reason, "provider_rate_limited");
  assert.equal(blockedOutcome.reason, "busy_or_backoff");
  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.registeredBodies.length, 1);
  assert.deepEqual(
    runtime.failedBodies,
    [],
    "a durable local registration pending an idempotent callback must not be failed",
  );
}

async function userClaimedProvisioningTabStopsAutomation() {
  const request = admittedRequest(1);
  const runtime = makeProvisioningRuntime({
    requests: [request],
    claimTabDuringProjectOpen: true,
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.provisioned, false);
  assert.equal(outcome.reason, "provisioning_tab_claimed");
  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.registeredBodies.length, 0);
  assert.equal(runtime.events.includes("provision:bootstrap"), false);
  assert.equal(runtime.hasTab(1), true, "a user-claimed tab must not be closed");
  assert.equal(runtime.events.includes("tabs.remove"), false);
  assert.deepEqual(runtime.failedBodies, [
    {
      requestId: request.request_id,
      body: {
        admission_revision: request.admission_revision,
        reason: "provisioning_tab_claimed",
        rate_limited: false,
      },
    },
  ]);
}

async function bootstrapSubmitRechecksActiveAndPinnedClaims() {
  for (const claim of ["active", "pinned"]) {
    const request = admittedRequest(claim === "active" ? 21 : 22);
    const runtime = makeProvisioningRuntime({
      requests: [request],
      claimTabDuringBootstrapConfirm: claim,
    });

    const outcome = await runtime.api.processNextAdmittedWorkerProvision();

    assert.equal(outcome.provisioned, false);
    assert.equal(outcome.reason, "bootstrap_ownership_lost");
    assert.equal(runtime.registeredBodies.length, 0);
    assert.equal(runtime.hasTab(1), true, `a ${claim} tab must remain user-owned`);
    assert.equal(runtime.events.includes("tabs.remove"), false);
  }
}

async function activatedThenDeactivatedProvisioningTabRemainsClaimed() {
  const request = admittedRequest(23);
  const runtime = makeProvisioningRuntime({
    requests: [request],
    claimThenReleaseDuringProjectOpen: true,
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.provisioned, false);
  assert.equal(outcome.reason, "provisioning_tab_claimed");
  assert.equal(runtime.registeredBodies.length, 0);
  assert.equal(runtime.hasTab(1), true);
  assert.equal(
    runtime.events.includes("tabs.remove"),
    false,
    "ever-claimed provisioning tabs must be preserved after deactivation",
  );
}

async function failedProvisioningCleansUpOnlyItsInactiveTab() {
  const request = admittedRequest(1);
  const runtime = makeProvisioningRuntime({
    requests: [request],
    bootstrapResponse: {
      ok: false,
      code: "bootstrap_unavailable",
      retryable: false,
    },
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();

  assert.equal(outcome.provisioned, false);
  assert.equal(outcome.reason, "bootstrap_unavailable");
  assert.equal(runtime.createCalls.length, 1);
  assert.equal(runtime.hasTab(1), false, "the failed runtime tab must be cleaned up");
  assert.equal(runtime.events.filter((event) => event === "tabs.remove").length, 1);
}

async function boundedAdmissionHistoryRetainsRecentStaleDefense() {
  const request = admittedRequest(999, { admission_revision: 7 });
  const admissions = Object.fromEntries(
    Array.from({ length: 160 }, (_, index) => [
      "prq_" + String(index + 1).padStart(32, "0"),
      {
        admissionRevision: index + 1,
        status: "registered",
        updatedAt: index,
      },
    ]),
  );
  admissions[request.request_id] = {
    admissionRevision: request.admission_revision,
    status: "registered",
    updatedAt: 1000000,
  };
  admissions["prq_ffffffffffffffffffffffffffffffff"] = {
    admissionRevision: 900,
    status: "in_flight",
    updatedAt: 1000001,
  };
  admissions["prq_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"] = {
    admissionRevision: 899,
    status: "in_flight",
    updatedAt: 1000002,
  };
  const runtime = makeProvisioningRuntime({
    requests: [request],
    initialStorage: {
      workerProvisioningState: { admissions, backoffUntil: 0 },
    },
  });

  const outcome = await runtime.api.processNextAdmittedWorkerProvision();
  const compacted = runtime.storage.workerProvisioningState.admissions;

  assert.equal(outcome.reason, "duplicate_or_stale");
  assert.equal(runtime.createCalls.length, 0);
  assert.ok(Object.keys(compacted).length <= 128);
  assert.deepEqual(compacted[request.request_id], {
    admissionRevision: request.admission_revision,
    status: "registered",
    updatedAt: 1000000,
  });
  assert.deepEqual(compacted["prq_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"], {
    admissionRevision: 899,
    status: "in_flight",
    updatedAt: 1000002,
  });
  assert.equal(compacted["prq_ffffffffffffffffffffffffffffffff"], undefined);
}

function browserPayloadStaysOpaque() {
  const request = admittedRequest(1);
  const runtime = makeProvisioningRuntime({ requests: [request] });
  return runtime.api.processNextAdmittedWorkerProvision().then(() => {
    const registered = runtime.registeredBodies[0].body;
    const allowedKeys = [
      "admission_revision",
      "conversation_id",
      "wake_target_id",
    ].sort();
    assert.deepEqual(Object.keys(registered).sort(), allowedKeys);
    const serialized = JSON.stringify(registered).toLowerCase();
    for (const forbidden of [
      "task",
      "role",
      "run",
      "dag",
      "capability",
      "broker_task_id",
      "assigned_task_id",
      "run_id",
      "workflow_id",
      "depends_on",
      "objective",
      "worker_handle",
    ]) {
      assert.equal(serialized.includes(forbidden), false);
    }
  });
}

function pendingRetirementRegistration(index) {
  const conversationId = `retiring-${index}`;
  const wakeTargetId = `wtgt_${String(index).padStart(32, "0")}`;
  return {
    kind: "worker",
    conversationId,
    conversationUrl: `https://chatgpt.com/c/${conversationId}`,
    projectId: "prj_runtime_12345678",
    wakeTargetId,
    retirementPending: {
      wakeTargetId,
      projectId: "prj_runtime_12345678",
      conversationId,
      runtimeOpenedTabId: null,
      markedAt: index,
    },
  };
}

async function pendingRetirementRevokesProvisioningAtEveryBoundary() {
  const beforeNext = admittedRequest(91);
  let insertedBeforeNext = false;
  const nextRuntime = makeProvisioningRuntime({
    requests: [beforeNext],
    async onRuntimeOpenAdmissionRead({ storage }) {
      if (insertedBeforeNext) {
        return;
      }
      insertedBeforeNext = true;
      storage.wakeTargets = {
        "conversation:retiring-901": pendingRetirementRegistration(901),
      };
    },
  });
  const nextOutcome = await nextRuntime.api.processNextAdmittedWorkerProvision();
  assert.equal(nextOutcome.reason, "worker_retirement_pending");
  assert.equal(nextRuntime.events.includes("next"), false);
  assert.equal(nextRuntime.createCalls.length, 0);

  const beforeCreate = admittedRequest(92);
  const createRuntime = makeProvisioningRuntime({
    requests: [beforeCreate],
    async onWorkerProvisioningNext({ storage }) {
      storage.wakeTargets = {
        "conversation:retiring-902": pendingRetirementRegistration(902),
      };
    },
  });
  const createOutcome = await createRuntime.api.processNextAdmittedWorkerProvision();
  assert.equal(createOutcome.processed, true);
  assert.equal(createOutcome.provisioned, false);
  assert.equal(createRuntime.events.includes("next"), true);
  assert.equal(createRuntime.createCalls.length, 0);
  assert.deepEqual(createRuntime.failedBodies, [
    {
      requestId: beforeCreate.request_id,
      body: {
        admission_revision: beforeCreate.admission_revision,
        reason: "worker_retirement_pending",
        rate_limited: false,
      },
    },
  ]);
}

async function runScenario(scenario) {
  switch (scenario) {
    case "n1-order":
      await provisionOneInOrder();
      return;
    case "slugged-project-landing":
      await currentSluggedProjectLandingIsAccepted();
      return;
    case "provisioned-tab-first-wake":
      await provisionedTabIsReusedForFirstWake();
      return;
    case "binding-change-during-finalize":
      await bindingChangeDuringFinalizeFailsClosed();
      return;
    case "n2-sequential":
      await provisionTwoStrictlySequential();
      return;
    case "unsupported-project":
      await unsupportedProjectIdentityCreatesNothing();
      return;
    case "mismatched-project-identity":
      await mismatchedProjectRefAndPathCreateNothing();
      return;
    case "pending-finalize-restart":
      await pendingRegistrationFinalizesAfterRestart();
      return;
    case "registration-conflict-cleanup":
      await registrationConflictDropsExactProvisionalRegistration();
      return;
    case "registration-conflict-claimed-tab":
      await registrationConflictPreservesUserClaimedTab();
      return;
    case "shared-open-cooldown":
      await sharedWakeCooldownDefersBeforeBackendAdmission();
      return;
    case "terminal-admission":
      await terminalAdmissionCreatesNothing();
      return;
    case "duplicate-stale":
      await duplicateOrStaleAdmissionDoesNotRepeatCallback();
      return;
    case "rate-limit":
      await providerRateLimitStopsAdditionalProvisioning();
      return;
    case "claimed-tab":
      await userClaimedProvisioningTabStopsAutomation();
      return;
    case "bootstrap-claimed-tab":
      await bootstrapSubmitRechecksActiveAndPinnedClaims();
      return;
    case "claimed-then-deactivated-tab":
      await activatedThenDeactivatedProvisioningTabRemainsClaimed();
      return;
    case "failed-tab-cleanup":
      await failedProvisioningCleansUpOnlyItsInactiveTab();
      return;
    case "bounded-admission-history":
      await boundedAdmissionHistoryRetainsRecentStaleDefense();
      return;
    case "payload-opaque":
      await browserPayloadStaysOpaque();
      return;
    case "retirement-admission-gap":
      await pendingRetirementRevokesProvisioningAtEveryBoundary();
      return;
    default:
      throw new Error("Unknown worker-provisioning scenario: " + scenario);
  }
}

runScenario(process.argv[2]).catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
