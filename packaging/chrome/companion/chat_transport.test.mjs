import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("./chat_transport.js", import.meta.url), "utf8");

function createStorage(initial = {}) {
  const data = { ...initial };
  const writes = [];
  return {
    data,
    writes,
    local: {
      async get(keys) {
        if (keys === null || keys === undefined) return { ...data };
        if (typeof keys === "string") return { [keys]: data[keys] };
        const selected = {};
        for (const key of keys) selected[key] = data[key];
        return selected;
      },
      async set(values) {
        writes.push(structuredClone(values));
        Object.assign(data, values);
      },
      async remove(keys) {
        for (const key of Array.isArray(keys) ? keys : [keys]) delete data[key];
      },
    },
  };
}

function createChrome(storage, overrides = {}) {
  const chrome = {
    storage: { local: storage.local },
    tabs: {
      async get() {
        return {
          id: 7,
          windowId: 9,
          status: "complete",
          url: "https://chatgpt.com/c/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        };
      },
    },
    scripting: {
      async executeScript() {
        throw new Error("test must provide scripting behavior");
      },
    },
  };
  return Object.assign(chrome, overrides);
}

function loadTransport(chrome, testGlobals = {}, dependencyOverrides = {}) {
  const context = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
    ...testGlobals,
  });
  vm.runInContext(source, context, { filename: "chat_transport.js" });
  return {
    context,
    transport: context.createHeterarchyChatTransport({
      postNative: () => true,
      schedule: () => {},
      requestNext: () => {},
      currentBrowserSessionId: async () => "session-a",
      journalOwnsCurrentBrowserSession: async (record) =>
        record.browserSessionId === "session-a",
      openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
      reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
      storeRuntimeSurface: async () => {},
      topLevelInjectionResult: (results) => results[0] ?? null,
      conversationIdFromUrl: (value) =>
        typeof value === "string" && value.includes("/c/")
          ? value.split("/c/")[1]?.split("/")[0] ?? null
          : null,
      ...dependencyOverrides,
    }),
  };
}

function dispatch(overrides = {}) {
  return {
    status: "dispatch",
    message_id: "message-a",
    revision: 1,
    action: "execute",
    workspace_id: "workspace-a",
    profile_id: "profile-a",
    participation_id: "participation-a",
    runtime_session_id: "runtime-a",
    provider_conversation_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    body: "hello from the Workspace",
    ...overrides,
  };
}

function waitForTurn() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

test("rejects an oversized dispatch body before any browser effect", async () => {
  const storage = createStorage();
  const outgoing = [];
  const chrome = createChrome(storage);
  const observable = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
  });
  vm.runInContext(source, observable, { filename: "chat_transport.js" });
  const observedTransport = observable.createHeterarchyChatTransport({
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    schedule: () => {},
    requestNext: () => {},
    currentBrowserSessionId: async () => "session-a",
    journalOwnsCurrentBrowserSession: async () => true,
    openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
    reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
    storeRuntimeSurface: async () => {},
    topLevelInjectionResult: (results) => results[0] ?? null,
    conversationIdFromUrl: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  });
  await observedTransport.handleNativeResponse(
    dispatch({ body: "x".repeat(16 * 1024 + 1) }),
  );
  assert.equal(outgoing.length, 0);
  assert.equal(storage.writes.length, 0);
});

test("expired fresh execute fails before any browser DOM send", async () => {
  const storage = createStorage();
  const outgoing = [];
  let submitCalls = 0;
  const chrome = createChrome(storage, {
    scripting: {
      async executeScript({ func }) {
        if (func.name === "submitChatPrompt") submitCalls += 1;
        throw new Error("no browser effect should be attempted");
      },
    },
  });
  const loaded = loadTransport(chrome, {}, {
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
  });
  await loaded.transport.handleNativeResponse(
    dispatch({ expires_at_ms: Date.now() - 1 }),
  );
  assert.equal(submitCalls, 0);
  assert.equal(outgoing[0]?.request.operation, "fail");
  assert.equal(
    storage.data["heterarchy:chat-journal:message-a"]?.state,
    "failure_report_pending",
  );
});

test("expiration during async surface setup fails without submitting", async () => {
  const storage = createStorage();
  const outgoing = [];
  let submitCalls = 0;
  const chrome = createChrome(storage, {
    scripting: {
      async executeScript({ func }) {
        if (func.name === "submitChatPrompt") {
          submitCalls += 1;
          throw new Error("submit must not run after expiry");
        }
        return [
          {
            frameId: 0,
            documentId: "document-a",
            result: {
              url: "https://chatgpt.com/c/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
              turns: [],
            },
          },
        ];
      },
    },
  });
  const loaded = loadTransport(chrome, {}, {
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    reusableRuntimeSurface: async () => null,
    openRuntimeOwnedTab: async () => {
      await new Promise((resolve) => setTimeout(resolve, 30));
      return { tabId: 7, windowId: 9 };
    },
  });
  await loaded.transport.handleNativeResponse(
    dispatch({ expires_at_ms: Date.now() + 5 }),
  );
  assert.equal(submitCalls, 0);
  assert.equal(outgoing[0]?.request.operation, "fail");
  assert.equal(
    storage.data["heterarchy:chat-journal:message-a"]?.state,
    "failure_report_pending",
  );
});

test("captures public finished turns and preserves exact composer text", () => {
  class FakeElement {
    constructor(attributes = {}, text = "") {
      this.attributes = attributes;
      this.textContent = text;
      this.innerText = text;
      this.isConnected = true;
      this.parent = null;
    }

    getAttribute(name) {
      return this.attributes[name] ?? null;
    }

    closest(selector) {
      if (selector === "form") return this.parent;
      if (
        selector.includes("data-message-id") ||
        selector.includes("data-turn-id") ||
        selector.includes("data-content-search-unit-key") ||
        selector.includes("data-content-search-turn-key")
      ) {
        return this;
      }
      if (
        selector.includes("data-testid*=") &&
        typeof this.attributes["data-testid"] === "string"
      ) {
        const marker = this.attributes["data-testid"].toLowerCase();
        if (
          (selector.includes("tool") && marker.includes("tool")) ||
          (selector.includes("reason") && marker.includes("reason")) ||
          (selector.includes("thought") && marker.includes("thought")) ||
          (selector.includes("analysis") && marker.includes("analysis"))
        ) {
          return this;
        }
      }
      return null;
    }

    querySelector(selector) {
      const marker = this.attributes["data-testid"]?.toLowerCase();
      if (
        marker !== undefined &&
        selector.includes("data-testid*=") &&
        ((selector.includes("tool") && marker.includes("tool")) ||
          (selector.includes("reason") && marker.includes("reason")) ||
          (selector.includes("thought") && marker.includes("thought")) ||
          (selector.includes("analysis") && marker.includes("analysis")))
      ) {
        return this;
      }
      if (
        this.attributes["data-user-message-bubble"] === "true" &&
        (selector.includes("data-user-message-bubble") || selector.includes("[dir="))
      ) {
        return this;
      }
      if (
        this.attributes["data-markdown-text-style"] === "assistant-message" &&
        selector.includes("data-markdown-text-style")
      ) {
        return this;
      }
      if (selector.includes("whitespace-pre-wrap") || selector.includes("markdown")) {
        return this;
      }
      return null;
    }

    querySelectorAll(selector) {
      if (
        this.attributes["data-markdown-text-style"] === "assistant-message" &&
        selector.includes("복사")
      ) {
        return [new FakeButton()];
      }
      return [];
    }

    getClientRects() {
      return [{}];
    }

    contains(node) {
      return node === this;
    }

    focus() {
      fakeDocument.activeElement = this;
    }
  }

  class FakeForm extends FakeElement {
    querySelector(selector) {
      if (
        selector.includes("send-button") ||
        selector.includes("aria-label^=") ||
        selector.includes("보내기")
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
      this.clicked = false;
    }

    click() {
      this.clicked = true;
    }
  }

  const composer = new FakeElement({
    contenteditable: "true",
    role: "textbox",
    "data-composer-markdown": "true",
  });
  const form = new FakeForm();
  const sendButton = new FakeButton();
  composer.parent = form;
  const messages = [
    new FakeElement(
      {
        "data-content-search-unit-key": "turn-user:0:user",
        "data-user-message-bubble": "true",
      },
      "existing",
    ),
    new FakeElement(
      {
        "data-content-search-unit-key": "turn-assistant:2:assistant",
        "data-markdown-text-style": "assistant-message",
      },
      "finished answer",
    ),
    new FakeElement(
      {
        "data-content-search-unit-key": "turn-assistant-second:2:assistant",
        "data-markdown-text-style": "assistant-message",
      },
      "second finished answer",
    ),
    new FakeElement(
      {
        "data-content-search-unit-key": "turn-tool:2:assistant",
        "data-markdown-text-style": "assistant-message",
        "data-testid": "tool-call-result",
      },
      "hidden tool output",
    ),
    new FakeElement(
      {
        "data-content-search-unit-key": "turn-reasoning:2:assistant",
        "data-markdown-text-style": "assistant-message",
        "data-testid": "reasoning-block",
      },
      "hidden reasoning",
    ),
  ];
  const fakeDocument = {
    activeElement: null,
    querySelector(selector) {
      if (selector.includes("data-composer-markdown")) return composer;
      return null;
    },
    querySelectorAll() {
      return messages;
    },
    getSelection() {
      return {
        rangeCount: 1,
        anchorNode: composer,
        focusNode: composer,
        selectAllChildren() {},
      };
    },
    execCommand(_command, _showUi, body) {
      composer.textContent = body;
      composer.innerText = body;
      return true;
    },
  };
  const storage = createStorage();
  const chrome = createChrome(storage);
  const loaded = loadTransport(chrome, {
    document: fakeDocument,
    location: { href: "https://chatgpt.com/c/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
    HTMLElement: FakeElement,
    HTMLFormElement: FakeForm,
    HTMLButtonElement: FakeButton,
  });
  const helpers = loaded.context.__HETERARCHY_CHAT_TRANSPORT_TEST__;
  const submission = helpers.submitChatPrompt("exact body\nwith newline");
  assert.equal(submission.submitted, true);
  assert.equal(composer.textContent, "exact body\nwith newline");
  assert.equal(sendButton.clicked, true);
  const observation = helpers.observeChatDocument(true);
  assert.deepEqual(JSON.parse(JSON.stringify(observation.turns)), [
    {
      id: "turn-user:0:user",
      stableId: true,
      role: "user",
      body: "existing",
      finished: true,
    },
    {
      id: "turn-assistant:2:assistant",
      stableId: true,
      role: "assistant",
      body: "finished answer",
      finished: true,
    },
    {
      id: "turn-assistant-second:2:assistant",
      stableId: true,
      role: "assistant",
      body: "second finished answer",
      finished: true,
    },
  ]);
  composer.textContent = "";
  composer.innerText = "";
  sendButton.disabled = true;
  assert.equal(helpers.inspectChatComposer().ready, true);
  sendButton.disabled = false;
});

test("journals before submit and never resends a terminal unknown effect", async () => {
  const storage = createStorage();
  const outgoing = [];
  let submitCount = 0;
  let composerChecks = 0;
  let turns = [];
  const chrome = createChrome(storage, {
    scripting: {
      async executeScript({ func }) {
        if (func.name === "inspectChatComposer") {
          composerChecks += 1;
          return [
            {
              frameId: 0,
              documentId: "document-a",
              result:
                composerChecks === 1
                  ? { ready: false, reason: "composer_is_busy" }
                  : { ready: true, reason: null },
            },
          ];
        }
        if (func.name === "submitChatPrompt") {
          const latest = storage.writes.at(-1);
          const journal = latest?.["heterarchy:chat-journal:message-a"];
          assert.equal(journal?.state, "submitting");
          submitCount += 1;
          turns = [
            { id: "turn-user", stableId: true, role: "user", body: "hello from the Workspace", finished: true },
            { id: "turn-assistant", stableId: true, role: "assistant", body: "completed answer", finished: true },
          ];
          return [{ frameId: 0, documentId: "document-a", result: { submitted: true } }];
        }
        return [{
          frameId: 0,
          documentId: "document-a",
          result: {
            url: "https://chatgpt.com/c/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            turns,
          },
        }];
      },
    },
  });
  const context = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
  });
  vm.runInContext(source, context, { filename: "chat_transport.js" });
  const transport = context.createHeterarchyChatTransport({
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    schedule: () => {},
    requestNext: () => {},
    currentBrowserSessionId: async () => "session-a",
    journalOwnsCurrentBrowserSession: async () => true,
    openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
    reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
    storeRuntimeSurface: async () => {},
    topLevelInjectionResult: (results) => results[0] ?? null,
    conversationIdFromUrl: (value) => value.split("/c/")[1]?.split("/")[0] ?? null,
  });
  await transport.handleNativeResponse(dispatch({ revision: 2 }));
  assert.equal(submitCount, 1);
  assert.equal(
    storage.writes.some(
      (write) => write["heterarchy:chat-journal:message-a"]?.state === "waiting",
    ),
    true,
  );
  assert.equal(outgoing[0]?.request.operation, "observe_delivery");
  assert.equal(outgoing[0]?.request.expected_revision, 2);
  await transport.handleNativeResponse({
    status: "ack",
    message_id: "message-a",
    revision: 3,
    state: "delivered",
  });
  assert.equal(storage.data["heterarchy:chat-journal:message-a"]?.revision, 3);
  await waitForTurn();
  assert.equal(outgoing[1]?.request.operation, "incoming");
  const revisionBeforeMisroutedAck = storage.data["heterarchy:chat-journal:message-a"]?.revision;
  await transport.handleNativeResponse({
    status: "ack",
    message_id: "message-a",
    revision: 4,
    state: "delivered",
  });
  assert.equal(
    storage.data["heterarchy:chat-journal:message-a"]?.revision,
    revisionBeforeMisroutedAck,
  );
  await transport.handleNativeResponse({ status: "idle" });
  await waitForTurn();
  assert.equal(outgoing[2]?.request.operation, "incoming");
  assert.equal(outgoing[2]?.request.provider_turn_id, "turn-assistant");
  await transport.handleNativeResponse({ status: "error", code: "durable_failure" });
  await transport.handleNativeResponse(
    dispatch({ action: "execute", revision: 4 }),
  );
  assert.equal(submitCount, 1);
  assert.equal(outgoing[3]?.request.expected_revision, 4);
});

test("inspect resolves an unknown effect from the trusted exact document without resending", async () => {
  const expiredAt = Date.now() - 1;
  const storage = createStorage({
    "heterarchy:chat-journal:message-a": {
      schemaVersion: 1,
      browserSessionId: "session-a",
      messageId: "message-a",
      revision: 1,
      workspaceId: "workspace-a",
      profileId: "profile-a",
      participationId: "participation-a",
      runtimeSessionId: "runtime-a",
      providerConversationId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      expiresAtMs: expiredAt,
      body: "hello from the Workspace",
      state: "unknown",
      tabId: null,
      windowId: null,
      documentId: null,
      navigationEpoch: 0,
      baselineCaptured: true,
      baselineTurnIds: [],
      userTurnId: null,
      observedBody: null,
    },
  });
  const outgoing = [];
  let scriptCalls = 0;
  const chrome = createChrome(storage, {
    scripting: {
      async executeScript() {
        scriptCalls += 1;
        return [
          {
            frameId: 0,
            documentId: "document-a",
            result: {
              url: "https://chatgpt.com/c/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
              turns: [
                {
                  id: "turn-user",
                  stableId: true,
                  role: "user",
                  body: "hello from the Workspace",
                  finished: true,
                },
              ],
            },
          },
        ];
      },
    },
  });
  const context = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
  });
  vm.runInContext(source, context, { filename: "chat_transport.js" });
  const transport = context.createHeterarchyChatTransport({
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    schedule: () => {},
    requestNext: () => {},
    currentBrowserSessionId: async () => "session-a",
    journalOwnsCurrentBrowserSession: async () => true,
    openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
    reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
    storeRuntimeSurface: async () => {},
    topLevelInjectionResult: (results) => results[0] ?? null,
    conversationIdFromUrl: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  });
  await transport.handleNativeResponse(
    dispatch({ action: "inspect", expires_at_ms: expiredAt }),
  );
  assert.equal(scriptCalls, 1);
  assert.equal(outgoing[0]?.request.operation, "observe_delivery");
  assert.equal(outgoing[0]?.request.body, "hello from the Workspace");
});

test("lost delivery acknowledgement becomes unknown and reconciles at the newer native revision", async () => {
  const storage = createStorage({
    "heterarchy:chat-journal:message-a": {
      schemaVersion: 1,
      browserSessionId: "session-a",
      messageId: "message-a",
      revision: 2,
      workspaceId: "workspace-a",
      profileId: "profile-a",
      participationId: "participation-a",
      runtimeSessionId: "runtime-a",
      providerConversationId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      body: "hello from the Workspace",
      state: "delivery_reporting",
      tabId: 7,
      windowId: 9,
      documentId: "document-a",
      navigationEpoch: 0,
      baselineCaptured: true,
      baselineTurnIds: [],
      userTurnId: null,
      observedBody: null,
    },
  });
  const outgoing = [];
  const chrome = createChrome(storage, {
    scripting: {
      async executeScript() {
        return [
          {
            frameId: 0,
            documentId: "document-a",
            result: {
              url: "https://chatgpt.com/c/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
              turns: [
                {
                  id: "turn-user",
                  stableId: true,
                  role: "user",
                  body: "hello from the Workspace",
                  finished: true,
                },
              ],
            },
          },
        ];
      },
    },
  });
  const context = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
  });
  vm.runInContext(source, context, { filename: "chat_transport.js" });
  const transport = context.createHeterarchyChatTransport({
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    schedule: () => {},
    requestNext: () => {},
    currentBrowserSessionId: async () => "session-a",
    journalOwnsCurrentBrowserSession: async () => true,
    openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
    reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
    storeRuntimeSurface: async () => {},
    topLevelInjectionResult: (results) => results[0] ?? null,
    conversationIdFromUrl: (value) => value.split("/c/")[1]?.split("/")[0] ?? null,
  });
  await transport.handleNativeResponse(dispatch({ action: "inspect", revision: 2 }));
  assert.equal(outgoing[0]?.request.operation, "observe_delivery");
  assert.equal(outgoing[0]?.request.expected_revision, 2);
  await transport.handleNativeResponse({
    status: "ack",
    message_id: "message-a",
    revision: 1,
    state: "delivered",
  });
  assert.equal(storage.data["heterarchy:chat-journal:message-a"]?.revision, 2);
  await transport.handleNativeResponse(dispatch({ action: "inspect", revision: 3 }));
  assert.equal(outgoing[1]?.request.operation, "observe_delivery");
  assert.equal(outgoing[1]?.request.expected_revision, 3);
  transport.onNativeDisconnect();
  await waitForTurn();
  assert.equal(storage.data["heterarchy:chat-journal:message-a"]?.state, "unknown");
  await transport.handleNativeResponse(dispatch({ action: "inspect", revision: 4 }));
  assert.equal(outgoing[2]?.request.operation, "observe_delivery");
  assert.equal(outgoing[2]?.request.expected_revision, 4);
  assert.equal(outgoing[2]?.request.body, "hello from the Workspace");
});

test("terminal storage cleanup removes only owned terminal records", async () => {
  const initial = {};
  for (let index = 0; index < 128; index += 1) {
    initial[`heterarchy:chat-journal:terminal-${index}`] = {
      state: "failed",
      updatedAtMs: index,
    };
  }
  initial["heterarchy:chat-journal:active"] = {
    state: "submitting",
    updatedAtMs: 0,
  };
  initial["unrelated:key"] = { state: "delivered" };
  const storage = createStorage(initial);
  const chrome = createChrome(storage);
  loadTransport(chrome);
  await waitForTurn();
  const keys = Object.keys(storage.data).filter((key) =>
    key.startsWith("heterarchy:chat-journal:"),
  );
  assert.equal(keys.length, 128);
  assert.ok(storage.data["heterarchy:chat-journal:active"]);
  assert.ok(storage.data["unrelated:key"]);
});

test("fails before browser send when the journal is full of unresolved records", async () => {
  const initial = {};
  for (let index = 0; index < 128; index += 1) {
    initial[`heterarchy:chat-journal:unknown-${index}`] = {
      state: "unknown",
      messageId: `unknown-${index}`,
      updatedAtMs: index,
    };
  }
  const storage = createStorage(initial);
  const outgoing = [];
  let scriptCalls = 0;
  const chrome = createChrome(storage, {
    scripting: {
      async executeScript() {
        scriptCalls += 1;
        return [];
      },
    },
  });
  const context = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
  });
  vm.runInContext(source, context, { filename: "chat_transport.js" });
  const transport = context.createHeterarchyChatTransport({
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    schedule: () => {},
    requestNext: () => {},
    currentBrowserSessionId: async () => "session-a",
    journalOwnsCurrentBrowserSession: async () => true,
    openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
    reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
    storeRuntimeSurface: async () => {},
    topLevelInjectionResult: (results) => results[0] ?? null,
    conversationIdFromUrl: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  });
  await transport.handleNativeResponse(dispatch({ message_id: "message-new" }));
  assert.equal(scriptCalls, 0);
  assert.equal(outgoing[0]?.request.operation, "fail");
  assert.equal(
    Object.keys(storage.data).filter((key) => key.startsWith("heterarchy:chat-journal:")).length,
    128,
  );
});

test("failure reports remain recoverable until the native failure acknowledgement", async () => {
  const storage = createStorage({
    "heterarchy:chat-journal:message-a": {
      schemaVersion: 1,
      browserSessionId: "session-a",
      messageId: "message-a",
      revision: 2,
      workspaceId: "workspace-a",
      profileId: "profile-a",
      participationId: "participation-a",
      runtimeSessionId: "runtime-a",
      providerConversationId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      body: "hello from the Workspace",
      state: "failure_report_pending",
      failure: "composer was unavailable",
      updatedAtMs: 1,
    },
  });
  const outgoing = [];
  const chrome = createChrome(storage);
  const context = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
  });
  vm.runInContext(source, context, { filename: "chat_transport.js" });
  const transport = context.createHeterarchyChatTransport({
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    schedule: () => {},
    requestNext: () => {},
    currentBrowserSessionId: async () => "session-a",
    journalOwnsCurrentBrowserSession: async () => true,
    openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
    reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
    storeRuntimeSurface: async () => {},
    topLevelInjectionResult: (results) => results[0] ?? null,
    conversationIdFromUrl: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  });
  await transport.handleNativeResponse({ status: "idle" });
  assert.equal(outgoing[0]?.request.operation, "fail");
  assert.equal(outgoing[0]?.request.expected_revision, 2);
  transport.onNativeDisconnect();
  await waitForTurn();
  await transport.handleNativeResponse({ status: "idle" });
  assert.equal(outgoing[1]?.request.operation, "fail");
  await transport.handleNativeResponse({
    status: "ack",
    message_id: "message-a",
    revision: 3,
    state: "failed",
  });
  assert.equal(storage.data["heterarchy:chat-journal:message-a"]?.state, "failed");
  assert.equal(storage.data["heterarchy:chat-journal:message-a"]?.revision, 3);
});

test("inspect ACK readback accepts the terminal native state after a lost effect ACK", async () => {
  const storage = createStorage({
    "heterarchy:chat-journal:message-a": {
      schemaVersion: 1,
      browserSessionId: "session-a",
      messageId: "message-a",
      revision: 1,
      workspaceId: "workspace-a",
      profileId: "profile-a",
      participationId: "participation-a",
      runtimeSessionId: "runtime-a",
      providerConversationId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      body: "hello from the Workspace",
      state: "unknown",
      updatedAtMs: 1,
    },
  });
  const outgoing = [];
  const chrome = createChrome(storage);
  const context = vm.createContext({
    chrome,
    TextEncoder,
    URL,
    console,
    setTimeout,
    clearTimeout,
    __HETERARCHY_CHAT_TEST__: true,
  });
  vm.runInContext(source, context, { filename: "chat_transport.js" });
  const transport = context.createHeterarchyChatTransport({
    postNative: (message) => {
      outgoing.push(message);
      return true;
    },
    schedule: () => {},
    requestNext: () => {},
    currentBrowserSessionId: async () => "session-a",
    journalOwnsCurrentBrowserSession: async () => true,
    openRuntimeOwnedTab: async () => ({ tabId: 7, windowId: 9 }),
    reusableRuntimeSurface: async () => ({ tabId: 7, windowId: 9 }),
    storeRuntimeSurface: async () => {},
    topLevelInjectionResult: (results) => results[0] ?? null,
    conversationIdFromUrl: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  });
  await transport.handleNativeResponse({ status: "idle" });
  assert.equal(outgoing[0]?.request.operation, "inspect");
  await transport.handleNativeResponse({
    status: "ack",
    message_id: "message-a",
    revision: 2,
    state: "delivered",
  });
  assert.equal(storage.data["heterarchy:chat-journal:message-a"]?.state, "delivered");
  assert.equal(storage.data["heterarchy:chat-journal:message-a"]?.revision, 2);
});
