import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const serviceSource = await readFile(
  new URL("./service_worker.js", import.meta.url),
  "utf8",
);
const chatSource = await readFile(
  new URL("./chat_transport.js", import.meta.url),
  "utf8",
);

function eventSource() {
  let listener = null;
  return {
    addListener(callback) {
      listener = callback;
    },
    emit(value) {
      return listener?.(value);
    },
  };
}

function createHarness(options = {}) {
  const messages = [];
  const timers = [];
  const scriptCalls = [];
  const localWrites = [];
  const removedTabs = [];
  const browser = {
    windowId: 701,
    tabId: 702,
    documentId: "document-a",
    url: "https://chatgpt.com/",
    status: "complete",
    ...(options.browser ?? {}),
  };
  const port = {
    onMessage: eventSource(),
    onDisconnect: eventSource(),
    postMessage(message) {
      messages.push(message);
    },
  };
  const local = {};
  const session = {};
  const runtime = {
    lastError: undefined,
    onStartup: eventSource(),
    onInstalled: eventSource(),
    connectNative() {
      return port;
    },
  };
  const context = vm.createContext({
    console,
    TextEncoder,
    URL,
    crypto: { randomUUID: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
    __HETERARCHY_CHAT_TEST__: true,
    setTimeout(callback, delay) {
      timers.push({ callback, delay });
      return timers.length;
    },
    clearTimeout() {},
  });
  const storageArea = (data) => ({
    async get(keys) {
      if (keys === null || keys === undefined) return { ...data };
      if (typeof keys === "string") return { [keys]: data[keys] };
      return Object.fromEntries(keys.map((key) => [key, data[key]]));
    },
    async set(values) {
      Object.assign(data, values);
      if (data === local) localWrites.push({ ...values });
    },
    async remove(keys) {
      for (const key of Array.isArray(keys) ? keys : [keys]) delete data[key];
    },
  });
  context.chrome = {
    runtime,
    storage: {
      local: storageArea(local),
      session: storageArea(session),
    },
    tabs: {
      onRemoved: eventSource(),
      async create(details) {
        browser.url = details.url;
        browser.status = "complete";
        return {
          id: browser.tabId,
          windowId: browser.windowId,
          status: browser.status,
          url: browser.url,
        };
      },
      async get(tabId) {
        return {
          id: tabId,
          windowId: browser.windowId,
          status: browser.status,
          url: browser.url,
          pinned: false,
        };
      },
      async query() {
        return [
          {
            id: browser.tabId,
            windowId: browser.windowId,
            status: browser.status,
            url: browser.url,
          },
        ];
      },
      async remove(tabId) {
        removedTabs.push(tabId);
      },
    },
    windows: {
      onRemoved: eventSource(),
      async create(details) {
        browser.url = details.url;
        browser.status = "complete";
        return {
          id: browser.windowId,
          tabs: [
            {
              id: browser.tabId,
              windowId: browser.windowId,
              status: browser.status,
              url: browser.url,
            },
          ],
        };
      },
      async get(windowId) {
        return { id: windowId };
      },
    },
  };
  context.chrome.scripting = {
    async executeScript(details) {
      scriptCalls.push(details);
      if (options.executeScript !== undefined) {
        return options.executeScript(details, {
          browser,
          context,
          local,
          scriptCalls,
        });
      }
      switch (details.func.name) {
        case "currentDocumentUrl":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: browser.url,
            },
          ];
        case "inspectBootstrapComposer":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { ready: true, reason: null },
            },
          ];
        case "prepareBootstrapPrompt":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { prepared: true, reason: null },
            },
          ];
        case "clickBootstrapPrompt":
          browser.url = "https://chatgpt.com/c/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { clicked: true, reason: null },
            },
          ];
        default:
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: null,
            },
          ];
      }
    },
  };
  context.importScripts = (name) => {
    assert.equal(name, "chat_transport.js");
    vm.runInContext(chatSource, context, { filename: name });
  };
  vm.runInContext(serviceSource, context, { filename: "service_worker.js" });
  return {
    context,
    port,
    messages,
    timers,
    local,
    localWrites,
    removedTabs,
    scriptCalls,
    browser,
  };
}

function tick() {
  return new Promise((resolve) => setImmediate(resolve));
}

function provisionDispatch(operationId) {
  return {
    operation_id: operationId,
    operation_revision: 1,
    profile_id: "profile-native",
    runtime_session_id: "runtime-native",
    provider: "chatgpt",
    action: "execute",
    effect_kind: "provision_new",
  };
}

test("actual injected bootstrap helper waits for asynchronous send readiness", async () => {
  const harness = createHarness();
  const context = harness.context;
  context.setTimeout = globalThis.setTimeout;
  context.clearTimeout = globalThis.clearTimeout;

  const composerSelector =
    'div[contenteditable="true"][role="textbox"][data-composer-markdown]';
  let sendButton = null;
  let insertCount = 0;
  let clickCount = 0;
  let documentState;

  class FakeElement {
    constructor() {
      this.isConnected = true;
      this.innerText = "";
      this.textContent = "";
      this.attributes = { contenteditable: "true" };
    }

    getAttribute(name) {
      return this.attributes[name] ?? null;
    }

    contains(node) {
      return node === this;
    }

    focus() {
      documentState.activeElement = this;
    }
  }

  class FakeForm extends FakeElement {
    querySelector(selector) {
      if (selector.includes("stop-button") || selector.includes("Stop ")) {
        return null;
      }
      if (
        selector.includes('aria-label="보내기"') ||
        selector.includes('[aria-label^="Send"')
      ) {
        return sendButton;
      }
      return null;
    }
  }

  class FakeButton extends FakeElement {
    constructor() {
      super();
      this.disabled = false;
    }

    click() {
      clickCount += 1;
    }
  }

  const form = new FakeForm();
  const composer = new FakeElement();
  composer.closest = (selector) => (selector === "form" ? form : null);
  documentState = {
    activeElement: null,
    querySelector(selector) {
      return selector.includes("data-composer-markdown") && composer.isConnected
        ? composer
        : null;
    },
    getSelection() {
      return {
        rangeCount: 0,
        anchorNode: null,
        focusNode: null,
        selectAllChildren(node) {
          this.rangeCount = 1;
          this.anchorNode = node;
          this.focusNode = node;
        },
      };
    },
    execCommand(command, _showUi, value) {
      assert.equal(command, "insertText");
      insertCount += 1;
      composer.innerText = value;
      composer.textContent = value;
      globalThis.setTimeout(() => {
        sendButton = new FakeButton();
      }, 0);
      return true;
    },
  };
  context.HTMLElement = FakeElement;
  context.HTMLFormElement = FakeForm;
  context.HTMLButtonElement = FakeButton;
  context.document = documentState;

  const api = context.__HETERARCHY_SERVICE_WORKER_TEST__;
  const inspection = api.inspectBootstrapComposer();
  assert.equal(inspection.ready, true);
  assert.equal(inspection.reason, null);
  const prompt = "Heterarchy bootstrap test";
  const preparation = await api.prepareBootstrapPrompt(prompt, 200, 1);
  assert.equal(preparation.prepared, true);
  assert.equal(preparation.reason, null);
  assert.equal(insertCount, 1);
  assert.equal(composer.innerText, prompt);
  assert.ok(sendButton instanceof FakeButton);
  const click = api.clickBootstrapPrompt(prompt);
  assert.equal(click.clicked, true);
  assert.equal(click.reason, null);
  assert.equal(clickCount, 1);
});

test("inserted bootstrap body without an enabled send button never clicks", async () => {
  const harness = createHarness();
  const context = harness.context;
  let clickCount = 0;
  let documentState;

  class FakeElement {
    constructor() {
      this.isConnected = true;
      this.innerText = "";
      this.textContent = "";
      this.attributes = { contenteditable: "true" };
    }

    getAttribute(name) {
      return this.attributes[name] ?? null;
    }

    contains(node) {
      return node === this;
    }

    focus() {
      documentState.activeElement = this;
    }
  }

  class FakeForm extends FakeElement {
    querySelector() {
      return null;
    }
  }

  class FakeButton extends FakeElement {
    click() {
      clickCount += 1;
    }
  }

  const form = new FakeForm();
  const composer = new FakeElement();
  composer.closest = (selector) => (selector === "form" ? form : null);
  documentState = {
    activeElement: null,
    querySelector() {
      return composer;
    },
    getSelection() {
      return {
        rangeCount: 0,
        anchorNode: null,
        focusNode: null,
        selectAllChildren(node) {
          this.rangeCount = 1;
          this.anchorNode = node;
          this.focusNode = node;
        },
      };
    },
    execCommand(command, _showUi, value) {
      assert.equal(command, "insertText");
      composer.innerText = value;
      composer.textContent = value;
      return true;
    },
  };
  context.HTMLElement = FakeElement;
  context.HTMLFormElement = FakeForm;
  context.HTMLButtonElement = FakeButton;
  context.document = documentState;

  const api = context.__HETERARCHY_SERVICE_WORKER_TEST__;
  const prompt = "Heterarchy bootstrap without send";
  const preparation = await api.prepareBootstrapPrompt(prompt, 0, 1);
  assert.equal(preparation.prepared, false);
  assert.equal(preparation.reason, "send_button_unavailable");
  const click = api.clickBootstrapPrompt(prompt);
  assert.equal(click.clicked, false);
  assert.equal(click.reason, "send_button_unavailable");
  assert.equal(clickCount, 0);
});

test("durable reports queue behind an in-flight poll and dispatch before later polls", async () => {
  const harness = createHarness();
  assert.deepEqual(JSON.parse(JSON.stringify(harness.messages[0])), { type: "next" });
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;
  const accepted = api.postNative({
    type: "effect_failed",
    operation_id: "operation-a",
    expected_revision: 2,
  });
  assert.equal(accepted, true);
  assert.deepEqual(JSON.parse(JSON.stringify(api.queue())), [
    { type: "effect_failed", operation_id: "operation-a", expected_revision: 2 },
  ]);
  await harness.port.onMessage.emit({ type: "idle" });
  await tick();
  assert.deepEqual(JSON.parse(JSON.stringify(harness.messages[1])), {
    type: "effect_failed",
    operation_id: "operation-a",
    expected_revision: 2,
  });
});

test("postMessage failure requeues the exact durable request", async () => {
  const harness = createHarness();
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;
  await harness.port.onMessage.emit({ type: "idle" });
  await tick();
  harness.port.postMessage = () => {
    throw new Error("simulated disconnect");
  };
  const durable = {
    type: "effect_unknown",
    operation_id: "operation-b",
    expected_revision: 4,
  };
  assert.equal(api.postNative(durable), true);
  assert.deepEqual(JSON.parse(JSON.stringify(api.queue())), [durable]);
});

test("provision inspect exhaustion reports unknown for the same operation", async () => {
  const harness = createHarness();
  harness.local["provision:operation-c"] = {
    schemaVersion: 3,
    operationRevision: 1,
    runtimeSessionId: "runtime-c",
    effectKind: "wake_existing_runtime",
    inspectRetries: 3,
    tabId: null,
    observedConversationId: null,
    activationBootstrapSubmitted: false,
  };
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;
  await api.inspectDispatch({
    operation_id: "operation-c",
    operation_revision: 1,
    profile_id: "profile-c",
    runtime_session_id: "runtime-c",
    provider: "chatgpt",
    action: "inspect",
    effect_kind: "wake_existing_runtime",
    provider_conversation_id: "conversation-c",
  });
  await harness.port.onMessage.emit({ type: "idle" });
  await tick();
  assert.ok(
    harness.messages.some(
      (message) => message.type === "effect_unknown" && message.operation_id === "operation-c",
    ),
  );
});

test("new provision records a static pre-click failure and cleans up its owned tab", async () => {
  const dispatch = provisionDispatch("operation-precondition");
  const harness = createHarness({
    executeScript(details, { browser, local }) {
      switch (details.func.name) {
        case "currentDocumentUrl":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: browser.url,
            },
          ];
        case "inspectBootstrapComposer":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { ready: true, reason: null },
            },
          ];
        case "prepareBootstrapPrompt":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { prepared: false, reason: "composer_insert_failed" },
            },
          ];
        default:
          assert.fail(`unexpected script ${details.func.name}`);
      }
    },
  });
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;

  await api.handleDispatch(dispatch);

  const journal = harness.local[`provision:${dispatch.operation_id}`];
  assert.equal(journal.failureCode, "composer_insert_failed");
  assert.equal(journal.ackPending, "failed");
  assert.deepEqual(harness.removedTabs, [harness.browser.tabId]);
  assert.equal(
    harness.scriptCalls.some((entry) => entry.func.name === "clickBootstrapPrompt"),
    false,
  );
  assert.ok(
    harness.context.__HETERARCHY_SERVICE_WORKER_TEST__
      .queue()
      .some(
        (message) =>
          message.type === "effect_failed" &&
          message.operation_id === dispatch.operation_id &&
          message.failure_code === "composer_insert_failed",
      ),
  );
  assert.equal(JSON.stringify(journal).includes("composer_insert_failed"), true);
  assert.equal(JSON.stringify(journal).includes("Heterarchy runtime bootstrap"), false);
});

test("initial readiness timeout preserves the last observed failure reason", async () => {
  const dispatch = provisionDispatch("operation-readiness-timeout");
  let now = 0;
  let readinessChecks = 0;
  const harness = createHarness({
    executeScript(details, { browser }) {
      switch (details.func.name) {
        case "currentDocumentUrl":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: browser.url,
            },
          ];
        case "inspectBootstrapComposer":
          readinessChecks += 1;
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { ready: false, reason: "composer_busy" },
            },
          ];
        default:
          assert.fail(`unexpected script ${details.func.name}`);
      }
    },
  });
  harness.context.Date = { now: () => now };
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;

  const pending = api.handleDispatch(dispatch);
  await tick();
  assert.equal(readinessChecks, 1);
  const deadlineTimer = harness.timers.find(
    (entry) => entry.delay === 500,
  );
  assert.ok(deadlineTimer);
  now = 30_000;
  deadlineTimer.callback();
  await pending;

  const journal = harness.local[`provision:${dispatch.operation_id}`];
  assert.equal(journal.failureCode, "composer_busy");
  assert.equal(journal.ackPending, "failed");
  assert.equal(
    harness.scriptCalls.some(
      (entry) => entry.func.name === "clickBootstrapPrompt",
    ),
    false,
  );
});

test("pre-click failure leaves a redirected owned tab in place", async () => {
  const dispatch = provisionDispatch("operation-redirected-precondition");
  const harness = createHarness({
    executeScript(details, { browser }) {
      switch (details.func.name) {
        case "currentDocumentUrl":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: browser.url,
            },
          ];
        case "inspectBootstrapComposer":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { ready: true, reason: null },
            },
          ];
        case "prepareBootstrapPrompt":
          browser.url = "https://chatgpt.com/c/cccccccc-cccc-4ccc-8ccc-cccccccccccc";
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { prepared: false, reason: "composer_insert_failed" },
            },
          ];
        default:
          assert.fail(`unexpected script ${details.func.name}`);
      }
    },
  });
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;

  await api.handleDispatch(dispatch);

  assert.deepEqual(harness.removedTabs, []);
  assert.equal(
    harness.local[`provision:${dispatch.operation_id}`].failureCode,
    "composer_insert_failed",
  );
});

test("new provision waits through late composer hydration and journals submitting before click", async () => {
  const dispatch = provisionDispatch("operation-late-hydration");
  let readinessChecks = 0;
  const journalStagesAtClick = [];
  const harness = createHarness({
    executeScript(details, { browser, local }) {
      switch (details.func.name) {
        case "currentDocumentUrl":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: browser.url,
            },
          ];
        case "inspectBootstrapComposer":
          readinessChecks += 1;
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result:
                readinessChecks === 1
                  ? { ready: false, reason: "composer_unavailable" }
                  : { ready: true, reason: null },
            },
          ];
        case "prepareBootstrapPrompt":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { prepared: true, reason: null },
            },
          ];
        case "clickBootstrapPrompt":
          journalStagesAtClick.push(
            local[`provision:${dispatch.operation_id}`]?.stage,
          );
          browser.url = "https://chatgpt.com/c/bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { clicked: true, reason: null },
            },
          ];
        default:
          assert.fail(`unexpected script ${details.func.name}`);
      }
    },
  });
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;
  const pending = api.handleDispatch(dispatch);
  await tick();
  assert.equal(readinessChecks, 1);
  const hydrationTimer = harness.timers.find((entry) => entry.delay === 500);
  assert.ok(hydrationTimer);
  hydrationTimer.callback();
  await pending;

  assert.equal(readinessChecks, 2);
  assert.deepEqual(journalStagesAtClick, ["submitting"]);
  assert.deepEqual(harness.removedTabs, []);
  assert.equal(
    harness.local[`provision:${dispatch.operation_id}`].activationBootstrapSubmitted,
    true,
  );
  assert.ok(
    harness.context.__HETERARCHY_SERVICE_WORKER_TEST__
      .queue()
      .some(
        (message) =>
          message.type === "effect_observed" &&
          message.operation_id === dispatch.operation_id,
      ),
  );
});

test("lost click reply becomes unknown and inspect never resends the bootstrap", async () => {
  const dispatch = provisionDispatch("operation-lost-click-reply");
  const harness = createHarness({
    executeScript(details, { browser }) {
      switch (details.func.name) {
        case "currentDocumentUrl":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: browser.url,
            },
          ];
        case "inspectBootstrapComposer":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { ready: true, reason: null },
            },
          ];
        case "prepareBootstrapPrompt":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { prepared: true, reason: null },
            },
          ];
        case "clickBootstrapPrompt":
          throw new Error("browser reply lost after possible click");
        default:
          assert.fail(`unexpected script ${details.func.name}`);
      }
    },
  });
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;

  await api.handleDispatch(dispatch);
  const clickCount = harness.scriptCalls.filter(
    (entry) => entry.func.name === "clickBootstrapPrompt",
  ).length;
  const journal = harness.local[`provision:${dispatch.operation_id}`];
  assert.equal(journal.failureCode, "click_reply_uncertain");
  assert.equal(journal.ackPending, "unknown");
  assert.deepEqual(harness.removedTabs, []);
  assert.ok(
    harness.context.__HETERARCHY_SERVICE_WORKER_TEST__
      .queue()
      .some(
        (message) =>
          message.type === "effect_unknown" &&
          message.operation_id === dispatch.operation_id,
      ),
  );

  await api.inspectDispatch({ ...dispatch, action: "inspect" });

  assert.equal(
    harness.scriptCalls.filter(
      (entry) => entry.func.name === "clickBootstrapPrompt",
    ).length,
    clickCount,
  );
  assert.deepEqual(harness.removedTabs, []);
});

test("ambiguous click reply becomes unknown and retains the owned tab", async () => {
  const dispatch = provisionDispatch("operation-ambiguous-click-reply");
  const harness = createHarness({
    executeScript(details, { browser }) {
      switch (details.func.name) {
        case "currentDocumentUrl":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: browser.url,
            },
          ];
        case "inspectBootstrapComposer":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { ready: true, reason: null },
            },
          ];
        case "prepareBootstrapPrompt":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: { prepared: true, reason: null },
            },
          ];
        case "clickBootstrapPrompt":
          return [
            {
              frameId: 0,
              documentId: browser.documentId,
              result: null,
            },
          ];
        default:
          assert.fail(`unexpected script ${details.func.name}`);
      }
    },
  });
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;

  await api.handleDispatch(dispatch);

  const journal = harness.local[`provision:${dispatch.operation_id}`];
  assert.equal(journal.failureCode, "click_reply_uncertain");
  assert.equal(journal.ackPending, "unknown");
  assert.deepEqual(harness.removedTabs, []);
  assert.ok(
    harness.context.__HETERARCHY_SERVICE_WORKER_TEST__
      .queue()
      .some(
        (message) =>
          message.type === "effect_unknown" &&
          message.operation_id === dispatch.operation_id,
      ),
  );
});


test("pre-journal failure propagates a static surface code without raw error text", async () => {
  const harness = createHarness();
  harness.context.chrome.windows.create = async () => { throw new Error("private browser failure detail"); };
  const dispatch = provisionDispatch("operation-surface-failure");
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;
  await api.handleDispatch(dispatch);
  const report = api.queue().find((message) => message.type === "effect_failed");
  assert.equal(report?.failure_code, "surface_open_failed");
  assert.equal(report?.operation_id, dispatch.operation_id);
  assert.equal(JSON.stringify(report).includes("private browser failure detail"), false);
  assert.equal(harness.removedTabs.length, 0);
});

test("journal persistence failure is distinct and does not discard an unproven owned tab", async () => {
  const harness = createHarness();
  harness.context.chrome.storage.local.set = async () => { throw new Error("private storage detail"); };
  const dispatch = provisionDispatch("operation-journal-failure");
  const api = harness.context.__HETERARCHY_SERVICE_WORKER_TEST__;
  await api.handleDispatch(dispatch);
  const report = api.queue().find((message) => message.type === "effect_failed");
  assert.equal(report?.failure_code, "journal_write_failed");
  assert.equal(report?.operation_id, dispatch.operation_id);
  assert.equal(JSON.stringify(report).includes("private storage detail"), false);
  assert.equal(harness.removedTabs.length, 0);
});


test("unexpected composer content is retained without a send click", async () => {
  for (const reason of ["composer_not_empty", "composer_body_mismatch"]) {
    const dispatch = provisionDispatch(`operation-retain-${reason}`);
    const harness = createHarness({
      executeScript(details, { browser }) {
        let result;
        switch (details.func.name) {
          case "currentDocumentUrl":
            result = browser.url;
            break;
          case "inspectBootstrapComposer":
            result = { ready: true, reason: null };
            break;
          case "prepareBootstrapPrompt":
            result = { prepared: false, reason };
            break;
          default:
            assert.fail(`unexpected script ${details.func.name}`);
        }
        return [{ frameId: 0, documentId: browser.documentId, result }];
      },
    });
    await harness.context.__HETERARCHY_SERVICE_WORKER_TEST__.handleDispatch(dispatch);
    assert.deepEqual(harness.removedTabs, [], reason);
    assert.equal(harness.scriptCalls.some((entry) => entry.func.name === "clickBootstrapPrompt"), false);
    const journal = harness.local[`provision:${dispatch.operation_id}`];
    assert.equal(journal.failureCode, reason);
    assert.equal(journal.ackPending, "failed");
  }
});
