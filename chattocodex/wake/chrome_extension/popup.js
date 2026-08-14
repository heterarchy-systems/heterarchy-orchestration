const conversationElement = document.querySelector("#conversation");
const statusElement = document.querySelector("#status");
const projectElement = document.querySelector("#project");
const registerButton = document.querySelector("#register");
const hintElement = document.querySelector("#hint");
let conversationUrl = null;
let conversationTabId = null;
let conversationProjectContext = null;
let projects = [];

registerButton.addEventListener("click", () => {
  void registerCurrentChat();
});
void initialize();

async function initialize() {
  const activeConversation = await readActiveConversation();
  if (!activeConversation) {
    setStatus(
      "명시적인 ChatGPT Project 대화(/g/.../c/...)를 연 뒤 다시 눌러주세요.",
      "error",
    );
    conversationElement.textContent =
      "등록 가능한 Project Chat이 아닙니다.";
    return;
  }
  conversationUrl = activeConversation.url;
  conversationTabId = activeConversation.tabId;
  conversationProjectContext = {
    ...activeConversation.projectContext,
    appIdentity: activeConversation.appIdentity,
  };
  conversationElement.textContent = conversationUrl;

  const registrationResponse = await sendMessage({
    type: "wake:getRegistration",
    conversationUrl,
  });
  if (!registrationResponse?.ok) {
    setStatus(registrationResponse?.error || "등록 상태를 읽지 못했습니다.", "error");
    return;
  }
  if (registrationResponse.registration) {
    if (registrationResponse.registration.kind === "main") {
      if (!activeConversation.appIdentity) {
        setStatus(
          "Work 모드에서 사용할 앱을 선택한 뒤 확장을 다시 열어주세요.",
          "error",
        );
        return;
      }
      const refreshed = await sendMessage({
        type: "wake:register",
        conversationUrl,
        projectContext: {
          ...conversationProjectContext,
          observedAt: new Date().toISOString(),
        },
        projectId: registrationResponse.registration.projectId,
        projectName: registrationResponse.registration.projectName,
        kind: "main",
      });
      if (!refreshed?.ok) {
        setStatus(refreshed?.error || "앱 연결 정보를 갱신하지 못했습니다.", "error");
        return;
      }
      await ensureWakeContentScript();
      await consumeRegistrationDraft(activeConversation.appIdentity);
      showRegistered(refreshed.registration);
      return;
    }
    await ensureWakeContentScript();
    showRegistered(registrationResponse.registration);
    return;
  }

  const projectsResponse = await sendMessage({ type: "wake:listProjects" });
  if (!projectsResponse?.ok) {
    setStatus(
      projectsResponse?.error || "Control Panel에 연결하지 못했습니다.",
      "error",
    );
    return;
  }
  projects = Array.isArray(projectsResponse.projects)
    ? projectsResponse.projects
    : [];
  if (projects.length === 0) {
    setStatus("등록 가능한 프로젝트가 없습니다.", "error");
    return;
  }
  renderProjects(projects);
  registerButton.disabled = false;
  setStatus("프로젝트를 선택해 Main reviewer로 등록하세요.", "ready");
}

async function registerCurrentChat() {
  if (!conversationUrl) {
    return;
  }
  const selected = projects.find(
    (project) => project.project_id === projectElement.value,
  );
  if (!selected) {
    setStatus("프로젝트를 선택해주세요.", "error");
    return;
  }
  projectElement.disabled = true;
  registerButton.disabled = true;
  setStatus("Main reviewer 등록 중…", "ready");

  const response = await sendMessage({
    type: "wake:register",
    conversationUrl,
    projectContext: {
      ...conversationProjectContext,
      observedAt: new Date().toISOString(),
    },
    projectId: selected.project_id,
    projectName: selected.name,
    kind: "main",
  });
  if (!response?.ok) {
    projectElement.disabled = false;
    registerButton.disabled = false;
    setStatus(response?.error || "Chat 등록에 실패했습니다.", "error");
    return;
  }
  await ensureWakeContentScript();
  await consumeRegistrationDraft(conversationProjectContext.appIdentity);
  showRegistered(response.registration);
}

function renderProjects(items) {
  projectElement.replaceChildren();
  for (const project of items) {
    const option = document.createElement("option");
    option.value = project.project_id;
    option.textContent = project.name;
    projectElement.append(option);
  }
  projectElement.disabled = false;
}

function showRegistered(registration) {
  const kind = registration.kind || "worker";
  projectElement.replaceChildren();
  const option = document.createElement("option");
  option.value = registration.projectId;
  option.textContent = registration.projectName || registration.projectId;
  projectElement.append(option);
  projectElement.disabled = true;
  registerButton.disabled = true;
  if (kind === "worker") {
    registerButton.textContent = "자동 등록된 Worker";
    hintElement.textContent =
      "이 Worker는 backend admission으로 자동 생성·등록되었습니다. 여기서 project나 역할을 변경할 수 없습니다.";
    setStatus("이 Chat은 자동 Wake Worker로 등록되어 있습니다.", "success");
    return;
  }
  registerButton.textContent = "Main reviewer 등록 완료";
  hintElement.textContent =
    "Worker stage 완료 후 Main Review Gate가 열리면 이 Chat을 자동으로 깨웁니다.";
  setStatus(
    "이 Chat은 Main review 자동 Resume 대상으로 등록되어 있습니다.",
    "success",
  );
}

async function readActiveConversation() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  const tab = tabs[0];
  const rawUrl = tab?.url;
  if (!rawUrl || !Number.isInteger(tab?.id)) {
    return null;
  }
  try {
    const projectContext = explicitChatGPTProjectContext(rawUrl);
    if (!projectContext) {
      return null;
    }
    const observations = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      func: observeSelectedChatGPTApp,
    });
    return {
      url: projectContext.conversationUrl,
      tabId: tab.id,
      projectContext,
      appIdentity: observations?.[0]?.result || null,
    };
  } catch (_error) {
    return null;
  }
}

// Observe only the selected app pill nearest the current composer. The provider
// app reference is stable routing identity; the visible label is not assumed to
// have any particular value.
function observeSelectedChatGPTApp() {
  const composer =
    document.querySelector('#prompt-textarea[contenteditable="true"]') ||
    document.querySelector("textarea#prompt-textarea");
  if (!composer) {
    return null;
  }
  let scope = composer;
  for (let depth = 0; depth < 6 && scope; depth += 1, scope = scope.parentElement) {
    const candidates = scope.querySelectorAll?.('a[href*="/plugins/plugin_"]') || [];
    for (const candidate of candidates) {
      const label = (candidate.textContent || "").trim();
      const href = candidate.getAttribute?.("href") || "";
      const match = href.match(/\/plugins\/(plugin_[A-Za-z0-9_-]+)/);
      const rect = candidate.getBoundingClientRect?.();
      if (
        match &&
        label &&
        (!rect || (rect.width > 0 && rect.height > 0))
      ) {
        return { appRef: match[1], appLabel: label };
      }
    }
  }
  // A visible source label is mutable presentation text, not connector
  // identity. Fail closed unless the selected app exposes its stable plugin
  // reference in the current composer.
  return null;
}

// A Main binding is only safe when the active tab itself proves the provider
// project route.  Do not infer a project from page text, a stale registration,
// a plain /c/<conversation> URL, or any other browser hint.
function explicitChatGPTProjectContext(rawUrl) {
  return chatGPTProjectConversationContext(rawUrl);
}

async function ensureWakeContentScript() {
  if (!Number.isInteger(conversationTabId)) {
    throw new Error("ChatGPT tab is unavailable for Runtime Wake activation.");
  }
  await chrome.scripting.executeScript({
    target: { tabId: conversationTabId },
    files: ["content.js"],
  });
}

async function consumeRegistrationDraft(appIdentity) {
  if (!Number.isInteger(conversationTabId)) {
    return;
  }
  await chrome.tabs.sendMessage(conversationTabId, {
    type: "runtime:consumeRegistrationDraft",
    appIdentity,
  });
}

function setStatus(message, kind) {
  statusElement.textContent = message;
  statusElement.dataset.kind = kind;
}

function sendMessage(message) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(message, (response) => {
      if (chrome.runtime.lastError) {
        resolve({ ok: false, error: chrome.runtime.lastError.message });
        return;
      }
      resolve(response);
    });
  });
}
