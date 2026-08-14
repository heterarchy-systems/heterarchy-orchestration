const $ = (id) => document.getElementById(id);

const workspaceRoot = $("workspace-root");
const customOrigin = $("custom-origin");
const customDomainWrap = $("custom-domain-wrap");
const mcpPort = $("mcp-port");
const actionMessage = $("action-message");
const ownerToken = $("owner-token");
const connectorUrl = $("connector-url");
let lastStatus = null;
let initializedForm = false;

function selectedMode() {
  return document.querySelector('input[name="remote-mode"]:checked').value;
}

function setMode(mode) {
  const input = document.querySelector(`input[name="remote-mode"][value="${mode}"]`);
  if (input) input.checked = true;
  const quick = mode === "quick_tunnel";
  $("quick-card").classList.toggle("selected", quick);
  $("custom-card").classList.toggle("selected", !quick);
  customDomainWrap.classList.toggle("hidden", quick);
  $("quick-notice").classList.toggle("hidden", !quick);
}

function setMessage(message, type = "") {
  actionMessage.textContent = message || "";
  actionMessage.className = `action-message ${type}`.trim();
}

function setDot(id, ok, warn = false) {
  const dot = $(id);
  dot.classList.toggle("ok", ok);
  dot.classList.toggle("warn", !ok && warn);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    cache: "no-store",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.message || `Request failed (${response.status})`);
  }
  return payload;
}

function formPayload() {
  const mode = selectedMode();
  return {
    workspace_root: workspaceRoot.value.trim(),
    remote_mode: mode,
    custom_origin: mode === "custom_domain" ? customOrigin.value.trim() : null,
    mcp_port: Number(mcpPort.value),
  };
}

async function savePreferences(showMessage = true) {
  const status = await api("/api/preferences", {
    method: "POST",
    body: JSON.stringify(formPayload()),
  });
  renderStatus(status, false);
  if (showMessage) setMessage("설정을 저장했습니다.", "success");
  return status;
}

async function startRuntime() {
  lockActions(true);
  setMessage("설정을 확인하고 원격 MCP를 시작하는 중입니다…");
  try {
    await savePreferences(false);
    const status = await api("/api/start", { method: "POST", body: "{}" });
    renderStatus(status, false);
    setMessage("MCP와 원격 연결이 시작되었습니다. Connector URL을 ChatGPT에 등록하세요.", "success");
  } catch (error) {
    setMessage(error.message, "error");
    await refreshStatus(false);
  } finally {
    lockActions(false);
  }
}

async function stopRuntime() {
  lockActions(true);
  setMessage("MCP와 Tunnel을 중지하는 중입니다…");
  try {
    const status = await api("/api/stop", { method: "POST", body: "{}" });
    renderStatus(status, false);
    setMessage("중지했습니다.", "success");
  } catch (error) {
    setMessage(error.message, "error");
  } finally {
    lockActions(false);
  }
}

async function rotateOwnerToken() {
  try {
    const payload = await api("/api/owner-token/rotate", { method: "POST", body: "{}" });
    ownerToken.value = payload.owner_token;
    ownerToken.type = "text";
    $("copy-token-button").disabled = false;
    $("token-state").textContent = "설정됨";
    setMessage("새 Owner Token을 생성했습니다. ChatGPT OAuth 승인 시 사용하세요.", "success");
    await copyText(payload.owner_token, "Owner Token을 클립보드에 복사했습니다.");
  } catch (error) {
    setMessage(error.message, "error");
  }
}

async function browseFolder() {
  try {
    setMessage("macOS 폴더 선택 창을 열었습니다…");
    const payload = await api("/api/folder-picker", { method: "POST", body: "{}" });
    if (payload.selected_path) {
      workspaceRoot.value = payload.selected_path;
      setMessage("폴더를 선택했습니다. 설정 저장 또는 MCP 시작을 누르세요.", "success");
    } else {
      setMessage("폴더 선택을 취소했습니다.");
    }
  } catch (error) {
    setMessage(error.message, "error");
  }
}

async function copyText(value, message) {
  if (!value) return;
  try {
    await navigator.clipboard.writeText(value);
    setMessage(message, "success");
  } catch (_error) {
    const temporary = document.createElement("textarea");
    temporary.value = value;
    temporary.style.position = "fixed";
    temporary.style.opacity = "0";
    document.body.appendChild(temporary);
    temporary.select();
    document.execCommand("copy");
    temporary.remove();
    setMessage(message, "success");
  }
}

function lockActions(busy) {
  $("save-button").disabled = busy || Boolean(lastStatus?.mcp_process?.running);
  $("start-button").disabled = busy || Boolean(lastStatus?.mcp_process?.running);
  $("stop-button").disabled = busy || !Boolean(lastStatus?.mcp_process?.running || lastStatus?.tunnel_process?.running);
}

function renderStatus(status, populateForm = true) {
  lastStatus = status;
  const mcpRunning = status.mcp_process.running;
  const tunnelRunning = status.tunnel_process.running;

  setDot("configured-dot", status.configured);
  setDot("mcp-dot", mcpRunning);
  setDot("tunnel-dot", tunnelRunning, status.configured && !tunnelRunning);
  setDot("oauth-dot", status.owner_token_configured);
  $("configured-label").textContent = status.configured ? "저장됨" : "미설정";
  $("mcp-label").textContent = mcpRunning ? `실행 중 · PID ${status.mcp_process.pid}` : "중지";
  $("tunnel-label").textContent = tunnelRunning ? `연결 중 · PID ${status.tunnel_process.pid}` : "중지";
  $("oauth-label").textContent = status.owner_token_configured ? "준비됨" : "토큰 필요";
  $("project-count").textContent = `${status.project_count} projects`;
  $("token-state").textContent = status.owner_token_configured ? "설정됨" : "미설정";

  if (populateForm && !initializedForm && status.preferences) {
    workspaceRoot.value = status.preferences.workspace_root;
    mcpPort.value = status.preferences.mcp_port;
    setMode(status.preferences.remote_mode);
    customOrigin.value = status.preferences.custom_origin || "";
    initializedForm = true;
  }

  if (status.visible_owner_token) {
    ownerToken.value = status.visible_owner_token;
    ownerToken.type = "text";
    $("copy-token-button").disabled = false;
  } else if (status.owner_token_configured && !ownerToken.value) {
    ownerToken.type = "password";
    ownerToken.placeholder = "설정됨 · 원문이 필요하면 생성 / 교체";
  }

  if (status.connector_url && (mcpRunning || tunnelRunning)) {
    connectorUrl.textContent = status.connector_url;
    $("copy-url-button").disabled = false;
  } else {
    connectorUrl.textContent = "MCP를 시작하면 Connector URL이 표시됩니다.";
    $("copy-url-button").disabled = true;
  }

  $("mcp-logs").textContent = status.mcp_process.log_lines.length ? status.mcp_process.log_lines.join("\n") : "로그 없음";
  $("tunnel-logs").textContent = status.tunnel_process.log_lines.length ? status.tunnel_process.log_lines.join("\n") : "로그 없음";
  workspaceRoot.disabled = mcpRunning || tunnelRunning;
  customOrigin.disabled = mcpRunning || tunnelRunning;
  mcpPort.disabled = mcpRunning || tunnelRunning;
  document.querySelectorAll('input[name="remote-mode"]').forEach((input) => { input.disabled = mcpRunning || tunnelRunning; });
  $("browse-button").disabled = mcpRunning || tunnelRunning;
  lockActions(false);
}

async function refreshStatus(showError = true) {
  try {
    const status = await api("/api/status");
    renderStatus(status);
  } catch (error) {
    if (showError) setMessage(error.message, "error");
  }
}

document.querySelectorAll('input[name="remote-mode"]').forEach((input) => {
  input.addEventListener("change", () => setMode(selectedMode()));
});
$("browse-button").addEventListener("click", browseFolder);
$("save-button").addEventListener("click", async () => {
  try { await savePreferences(true); } catch (error) { setMessage(error.message, "error"); }
});
$("start-button").addEventListener("click", startRuntime);
$("stop-button").addEventListener("click", stopRuntime);
$("rotate-token-button").addEventListener("click", rotateOwnerToken);
$("copy-token-button").addEventListener("click", () => copyText(ownerToken.value, "Owner Token을 복사했습니다."));
$("copy-url-button").addEventListener("click", () => copyText(lastStatus?.connector_url, "Connector URL을 복사했습니다."));

setMode("quick_tunnel");
refreshStatus();
setInterval(() => {
  if (lastStatus?.mcp_process?.running || lastStatus?.tunnel_process?.running) refreshStatus(false);
}, 2000);
