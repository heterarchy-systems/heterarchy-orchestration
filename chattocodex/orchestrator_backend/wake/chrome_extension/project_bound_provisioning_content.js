(() => {
if (globalThis.__chatToCodexProjectProvisioningLoaded) {
  return;
}
globalThis.__chatToCodexProjectProvisioningLoaded = true;
let provisioningDocumentEverClaimed =
  document.visibilityState === "visible" ||
  (typeof document.hasFocus === "function" && document.hasFocus());
document.addEventListener?.("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    provisioningDocumentEverClaimed = true;
  }
});
window.addEventListener?.("focus", () => {
  provisioningDocumentEverClaimed = true;
});

// Keep provider selectors in this adapter-only file.  If ChatGPT changes this
// surface, fail closed and report unsupported instead of navigating a Chat
// outside the explicitly observed project context.
const PROJECT_NEW_CHAT_SELECTORS = [
  '[data-testid="project-new-chat-button"]',
  '[data-testid="project-page"] button[data-testid="new-chat-button"]',
  '[data-testid^="project"] button[data-testid="new-chat-button"]',
];
const PROJECT_NEW_CHAT_COMPOSER_SELECTORS = [
  '#prompt-textarea[contenteditable="true"]',
  'textarea#prompt-textarea',
  '[contenteditable="true"][data-virtualkeyboard="true"]',
];

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  if (message?.type !== "provision:openProjectNewChat") {
    return;
  }
  void openProjectBoundNewChat({
    projectPath: message.projectPath,
    projectRef: message.projectRef,
    ownershipToken: message.ownershipToken,
  })
    .then(sendResponse)
    .catch(() => sendResponse({ ok: false, code: "unsupported_project_ui" }));
  return true;
});

async function openProjectBoundNewChat({ projectPath, projectRef, ownershipToken }) {
  if (!isExplicitProjectContext({ projectPath, projectRef })) {
    return { ok: false, code: "unsupported_project_identity", retryable: false };
  }
  if (hasProviderRateLimitSignal()) {
    return { ok: false, code: "provider_rate_limited", rateLimited: true };
  }
  const newChatControl = findProjectNewChatControl();
  const newChatComposer = newChatControl ? null : findProjectNewChatComposer();
  const newChatSurface = newChatControl || newChatComposer;
  if (!newChatSurface) {
    // The content script can receive the service-worker message before the
    // provider hydrates its project composer. The adapter owns a bounded retry
    // loop; report this one condition as retryable instead of permanently
    // blocking an otherwise supported project UI.
    return { ok: false, code: "unsupported_project_ui", retryable: true };
  }
  if (!isProvisioningDocumentStillUnclaimed()) {
    return { ok: false, code: "provisioning_tab_claimed", retryable: false };
  }
  const ownership = await sendExtensionMessage({
    type: "provision:confirmProjectOpen",
    ownershipToken,
  });
  if (
    !ownership?.ok ||
    !ownership.safe ||
    (findProjectNewChatControl() || findProjectNewChatComposer()) !==
      newChatSurface ||
    !isProvisioningDocumentStillUnclaimed() ||
    hasProviderRateLimitSignal()
  ) {
    return { ok: false, code: "provisioning_tab_claimed", retryable: false };
  }
  // ChatGPT's current project landing is already a blank project-scoped
  // conversation composer. Older layouts expose a dedicated New Chat button.
  // Clicking is required only for the latter; bootstrap owns the blank
  // composer itself and will create the persistent conversation identity.
  if (newChatControl) {
    newChatControl.click();
  }
  selectWorkModeWhenAvailable();
  return { ok: true };
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

function sendExtensionMessage(message) {
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
          resolve(null);
        }
      });
    } catch (_error) {
      resolve(null);
    }
  });
}

function isExplicitProjectContext({ projectPath, projectRef }) {
  return projectLandingMatches({
    rawUrl: location.href,
    projectPath,
    projectRef,
  });
}

function projectLandingMatches({ rawUrl, projectPath, projectRef }) {
  const canonicalRef = canonicalProjectRef(projectRef);
  if (!canonicalRef || canonicalRef !== projectRef) {
    return false;
  }
  try {
    const expected = new URL(String(projectPath), "https://chatgpt.com");
    const current = new URL(String(rawUrl));
    const expectedSegments = expected.pathname.split("/").filter(Boolean);
    const currentSegments = current.pathname.split("/").filter(Boolean);
    return (
      expected.origin === "https://chatgpt.com" &&
      !expected.search &&
      !expected.hash &&
      expectedSegments.length === 2 &&
      expectedSegments[0] === "g" &&
      canonicalProjectRef(expectedSegments[1]) === canonicalRef &&
      current.origin === expected.origin &&
      !current.search &&
      !current.hash &&
      currentSegments[0] === "g" &&
      canonicalProjectRef(currentSegments[1]) === canonicalRef &&
      (currentSegments.length === 2 ||
        (currentSegments.length === 3 && currentSegments[2] === "project"))
    );
  } catch (_error) {
    return false;
  }
}

function canonicalProjectRef(rawSegment) {
  const segment = typeof rawSegment === "string" ? rawSegment.trim() : "";
  if (!segment || segment.includes("/")) {
    return null;
  }
  const stableMatch = segment.match(
    /^(g-p-[0-9a-f]{32})(?:-[a-z0-9][a-z0-9-]*)?$/i,
  );
  return stableMatch ? stableMatch[1] : segment;
}

function findProjectNewChatControl() {
  const allowHiddenControls = isProvisioningDocumentStillUnclaimed();
  for (const selector of PROJECT_NEW_CHAT_SELECTORS) {
    const candidate = document.querySelector(selector);
    if (
      candidate &&
      !candidate.disabled &&
      (isVisible(candidate) || allowHiddenControls)
    ) {
      return candidate;
    }
  }
  return null;
}

function findProjectNewChatComposer() {
  const allowHiddenControls = isProvisioningDocumentStillUnclaimed();
  for (const selector of PROJECT_NEW_CHAT_COMPOSER_SELECTORS) {
    const candidate = document.querySelector(selector);
    if (
      candidate &&
      !candidate.disabled &&
      (isVisible(candidate) || allowHiddenControls)
    ) {
      return candidate;
    }
  }
  return null;
}

function selectWorkModeWhenAvailable() {
  const candidates = document.querySelectorAll?.('[role="radio"]') || [];
  for (const candidate of candidates) {
    const text = (candidate.textContent || "").trim().toLowerCase();
    if (text !== "work" || !isVisible(candidate)) {
      continue;
    }
    if (candidate.getAttribute?.("aria-checked") !== "true") {
      candidate.click();
    }
    return true;
  }
  return false;
}

function hasProviderRateLimitSignal() {
  const bodyText = (document.body?.innerText || "").toLowerCase();
  return (
    bodyText.includes("too many requests") ||
    bodyText.includes("rate limit") ||
    bodyText.includes("요청이 너무 많")
  );
}

function isVisible(element) {
  const rect = element.getBoundingClientRect();
  return rect.width > 0 && rect.height > 0;
}

})();
