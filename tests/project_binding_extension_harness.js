"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const extensionRoot = path.join(
  path.resolve(__dirname, ".."),
  "chattocodex",
  "wake",
  "chrome_extension",
);
const extensionVersion = JSON.parse(
  fs.readFileSync(path.join(extensionRoot, "manifest.json"), "utf8"),
).version;

function makeRuntime() {
  const storage = {};
  const calls = [];
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
      onAlarm: { addListener() {} },
      async get() {
        return { name: "existing" };
      },
      async create() {},
    },
    tabs: {
      onActivated: { addListener() {} },
      onRemoved: { addListener() {} },
    },
    storage: {
      local: {
        async get(key) {
          return { [key]: storage[key] };
        },
        async set(patch) {
          Object.assign(storage, patch);
        },
      },
    },
  };

  async function fetchStub(rawUrl, options = {}) {
    const url = new URL(rawUrl);
    calls.push({ path: url.pathname, options });
    if (url.pathname.startsWith("/api/project-bindings/")) {
      return jsonResponse({ binding: {} });
    }
    if (url.pathname === "/api/main-runtime/review-targets/register") {
      return jsonResponse({ target: {} });
    }
    if (url.pathname.startsWith("/api/main-runtime/review-targets/")) {
      return jsonResponse({ target: { status: "idle" } });
    }
    throw new Error(`Unexpected loopback path ${url.pathname}`);
  }

  const source = [
    "runtime_conversation_identity.js",
    "runtime_tab_reopen_policy.js",
    "worker_provisioning_adapter.js",
    "service_worker.js",
  ]
    .map((file) => fs.readFileSync(path.join(extensionRoot, file), "utf8"))
    .join("\n")
    .replace(/importScripts\([^\n]+\);\n/g, "")
    .replace("\nvoid bootstrapRuntimeSweep();\n", "\n")
    .concat(
      "\nglobalThis.__projectBinding = { registerTarget, explicitChatGPTProjectContext };",
    );
  const context = vm.createContext({
    chrome,
    fetch: fetchStub,
    URL,
    crypto: {
      randomUUID: () => "00000000-0000-0000-0000-000000000000",
    },
    console,
  });
  vm.runInContext(source, context, { filename: "service_worker.js" });
  return { api: context.__projectBinding, calls, storage };
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

async function validProjectBindsBeforeMainRegistration() {
  const runtime = makeRuntime();
  const context = runtime.api.explicitChatGPTProjectContext(
    "https://chatgpt.com/g/project-a/c/conversation-a",
  );
  assert.deepEqual(JSON.parse(JSON.stringify(context)), {
    provider: "chatgpt",
    projectRef: "project-a",
    projectPath: "/g/project-a",
    conversationUrl: "https://chatgpt.com/g/project-a/c/conversation-a",
    conversationId: "conversation-a",
  });

  const registered = await runtime.api.registerTarget({
    conversationUrl: context.conversationUrl,
    projectContext: {
      ...context,
      appIdentity: {
        appRef: "plugin_asdk_app_alpha",
        appLabel: "my-local-mcp",
      },
      observedAt: "2026-08-12T00:00:00.000Z",
    },
    projectId: "prj_runtime_12345678",
    projectName: "Runtime",
    kind: "main",
  });
  assert.deepEqual(JSON.parse(JSON.stringify(registered.registration.appIdentity)), {
    appRef: "plugin_asdk_app_alpha",
    appLabel: "my-local-mcp",
  });

  assert.deepEqual(
    runtime.calls.map((call) => [call.path, call.options.method]),
    [
      ["/api/project-bindings/prj_runtime_12345678", "PUT"],
      ["/api/main-runtime/review-targets/register", "POST"],
    ],
  );
  const bindingBody = JSON.parse(runtime.calls[0].options.body);
  assert.deepEqual(Object.keys(bindingBody), ["adapter_metadata"]);
  assert.deepEqual(bindingBody.adapter_metadata, {
    provider: "chatgpt",
    project_ref: "project-a",
    project_path: "/g/project-a",
    app_ref: "plugin_asdk_app_alpha",
    app_label: "my-local-mcp",
    observed_at: "2026-08-12T00:00:00.000Z",
  });
  const serialized = JSON.stringify(bindingBody);
  for (const forbidden of [
    "project_root",
    "broker_task_id",
    "assigned_task_id",
    "run_id",
    "objective",
    "worker_handle",
  ]) {
    assert.equal(serialized.includes(forbidden), false);
  }
}

async function plainConversationFailsClosed() {
  const runtime = makeRuntime();
  assert.equal(
    runtime.api.explicitChatGPTProjectContext(
      "https://chatgpt.com/c/conversation-a",
    ),
    null,
  );
  await assert.rejects(
    runtime.api.registerTarget({
      conversationUrl: "https://chatgpt.com/c/conversation-a",
      projectId: "prj_runtime_12345678",
      projectName: "Runtime",
      kind: "main",
    }),
    /requires a current ChatGPT Project conversation/,
  );
  assert.equal(runtime.calls.length, 0);
}

async function labelOnlyConnectorFailsBeforeBinding() {
  const runtime = makeRuntime();
  const context = runtime.api.explicitChatGPTProjectContext(
    "https://chatgpt.com/g/project-a/c/conversation-a",
  );
  await assert.rejects(
    runtime.api.registerTarget({
      conversationUrl: context.conversationUrl,
      projectContext: {
        ...context,
        appIdentity: { appRef: null, appLabel: "arbitrary-mcp-name" },
      },
      projectId: "prj_runtime_12345678",
      projectName: "Runtime",
      kind: "main",
    }),
    /requires the app selected/,
  );
  assert.equal(runtime.calls.length, 0);
}

async function popupObservationRejectsLabelOnlySource() {
  const popupSource = fs.readFileSync(path.join(extensionRoot, "popup.js"), "utf8");
  const start = popupSource.indexOf("function observeSelectedChatGPTApp() {");
  const end = popupSource.indexOf("\n}\n\n// A Main binding", start) + 2;
  assert.ok(start >= 0 && end > start);
  const composer = {
    parentElement: null,
    querySelectorAll() {
      return [];
    },
  };
  const sourceRegion = {
    innerText: "arbitrary-mcp-name",
    querySelectorAll() {
      return [{ getAttribute: () => "arbitrary-mcp-name" }];
    },
  };
  const document = {
    querySelector(selector) {
      return selector.includes("prompt-textarea") ? composer : null;
    },
    querySelectorAll() {
      return [sourceRegion];
    },
  };
  const context = vm.createContext({ document });
  vm.runInContext(
    `${popupSource.slice(start, end)}\nglobalThis.observe = observeSelectedChatGPTApp;`,
    context,
  );
  assert.equal(context.observe(), null);
}

async function sluggedProjectConversationUsesStableProjectIdentity() {
  const runtime = makeRuntime();
  const context = runtime.api.explicitChatGPTProjectContext(
    "https://chatgpt.com/g/g-p-6a7a81380ff4819185c013db8ea9580f-testproject/c/conversation-a",
  );
  assert.deepEqual(JSON.parse(JSON.stringify(context)), {
    provider: "chatgpt",
    projectRef: "g-p-6a7a81380ff4819185c013db8ea9580f",
    projectPath: "/g/g-p-6a7a81380ff4819185c013db8ea9580f",
    conversationUrl:
      "https://chatgpt.com/g/g-p-6a7a81380ff4819185c013db8ea9580f-testproject/c/conversation-a",
    conversationId: "conversation-a",
  });
}

async function rereregistrationIsIdempotent() {
  const runtime = makeRuntime();
  const context = runtime.api.explicitChatGPTProjectContext(
    "https://chatgpt.com/g/project-a/c/conversation-a",
  );
  const registration = {
    conversationUrl: context.conversationUrl,
    projectContext: {
      ...context,
      appIdentity: {
        appRef: "plugin_asdk_app_alpha",
        appLabel: "my-local-mcp",
      },
      observedAt: "2026-08-12T00:00:00.000Z",
    },
    projectId: "prj_runtime_12345678",
    projectName: "Runtime",
    kind: "main",
  };
  await runtime.api.registerTarget(registration);
  await runtime.api.registerTarget(registration);
  assert.equal(
    runtime.calls.filter((call) => call.path.includes("project-bindings")).length,
    2,
  );
  assert.equal(
    runtime.calls.filter((call) => call.path.endsWith("/register")).length,
    1,
  );
  assert.equal(
    runtime.calls.filter((call) => call.path.includes("review-targets/mtgt_")).length,
    1,
  );
}

const scenarios = {
  "valid-project": validProjectBindsBeforeMainRegistration,
  "slugged-project": sluggedProjectConversationUsesStableProjectIdentity,
  "plain-conversation": plainConversationFailsClosed,
  "label-only-connector": labelOnlyConnectorFailsBeforeBinding,
  "popup-label-only-source": popupObservationRejectsLabelOnlySource,
  "reregistration": rereregistrationIsIdempotent,
};

const scenario = scenarios[process.argv[2]];
if (!scenario) {
  throw new Error(`Unknown project-binding scenario: ${process.argv[2]}`);
}
scenario().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
