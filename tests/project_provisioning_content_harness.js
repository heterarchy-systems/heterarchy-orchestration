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

function loadProjectContextMatcher(currentUrl) {
  const identitySource = fs.readFileSync(
    path.join(extensionRoot, "runtime_conversation_identity.js"),
    "utf8",
  );
  const contentSource = fs
    .readFileSync(
      path.join(extensionRoot, "project_bound_provisioning_content.js"),
      "utf8",
    )
    .replace(
      /\}\)\(\);\s*$/,
      "globalThis.__projectProvisioning = { isExplicitProjectContext };\n})();",
    );
  const context = vm.createContext({
    URL,
    location: { href: currentUrl },
    document: {
      visibilityState: "hidden",
      hasFocus: () => false,
      addEventListener() {},
    },
    window: { addEventListener() {} },
    chrome: {
      runtime: { onMessage: { addListener() {} } },
    },
  });
  vm.runInContext(`${identitySource}\n${contentSource}`, context);
  return context.__projectProvisioning.isExplicitProjectContext;
}

const canonicalRef = "g-p-6a7a81380ff4819185c013db8ea9580f";
const canonicalPath = `/g/${canonicalRef}`;

const acceptsCurrentSluggedProjectLanding = loadProjectContextMatcher(
  `https://chatgpt.com/g/${canonicalRef}-testproject/project`,
);
assert.equal(
  acceptsCurrentSluggedProjectLanding({
    projectPath: canonicalPath,
    projectRef: canonicalRef,
  }),
  true,
);

const rejectsDifferentStableProject = loadProjectContextMatcher(
  "https://chatgpt.com/g/g-p-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-testproject/project",
);
assert.equal(
  rejectsDifferentStableProject({
    projectPath: canonicalPath,
    projectRef: canonicalRef,
  }),
  false,
);
