"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const extensionRoot = path.resolve(
  __dirname,
  "..",
  "chattocodex",
  "orchestrator_backend",
  "wake",
  "chrome_extension",
);

for (const fileName of [
  "runtime_conversation_identity.js",
  "persistent_worker_runtime_policy.js",
]) {
  vm.runInThisContext(
    fs.readFileSync(path.join(extensionRoot, fileName), "utf8"),
    { filename: fileName },
  );
}

const registration = {
  kind: "worker",
  projectId: "project-local-1",
  wakeTargetId: "wake-target-1",
  conversationId: "conversation-1",
};
const conversationUrl =
  "https://chatgpt.com/g/g-p-0123456789abcdef0123456789abcdef/c/conversation-1";

assert.equal(
  persistentWorkerConversationMatchesRegistration(registration, conversationUrl),
  true,
);
assert.equal(
  persistentWorkerConversationMatchesRegistration(
    registration,
    "https://chatgpt.com/g/g-p-0123456789abcdef0123456789abcdef/c/other",
  ),
  false,
);
assert.equal(
  persistentWorkerConversationMatchesRegistration(
    { ...registration, kind: "main" },
    conversationUrl,
  ),
  false,
);

assert.equal(
  persistentWorkerWakeMatchesRegistration(
    registration,
    {
      project_id: "project-local-1",
      wake_target_id: "wake-target-1",
      wake_revision: 3,
      status: "waking",
    },
    conversationUrl,
  ),
  true,
);
assert.equal(
  persistentWorkerWakeMatchesRegistration(
    registration,
    {
      project_id: "project-local-1",
      wake_target_id: "wake-target-1",
      wake_revision: 3,
      status: "idle",
    },
    conversationUrl,
  ),
  false,
);
assert.equal(
  persistentWorkerWakeMatchesRegistration(
    registration,
    {
      project_id: "project-local-1",
      wake_target_id: "different-target",
      wake_revision: 3,
      status: "waking",
    },
    conversationUrl,
  ),
  false,
);

console.log("persistent worker runtime policy harness: ok");
