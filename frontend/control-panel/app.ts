// @ts-check

/**
 * Source of truth for the loopback Runtime Console UI.
 * The packaged browser asset is copied to chattocodex/control_panel/static/app.js
 * without adding a frontend toolchain dependency.
 */

const API = {
  status: "/api/status",
  operations: "/api/operations",
  bindings: "/api/project-bindings",
  settings: "/api/preferences",
  runtimeStart: "/api/start",
  runtimeStop: "/api/stop",
  folderPicker: "/api/folder-picker",
  rotateToken: "/api/owner-token/rotate",
  browserSetup: "/api/worker-browser/setup",
  browserStart: "/api/worker-browser/start",
  browserStop: "/api/worker-browser/stop",
};

const SECTION_COPY = {
  overview: ["Operations", "Project companies, broker namespaces, consumer groups, workers and current runs."],
  projects: ["Projects", "Bind local projects to their exact ChatGPT Project identity."],
  broker: ["Broker", "Inspect the shared Broker Cluster, project namespaces and Group Coordinator ownership."],
  workers: ["Workers", "Inspect project-scoped consumer groups and persistent Worker chats."],
  runs: ["Runs", "Read recent orchestration activity without leaving the control plane."],
  settings: ["Settings", "Configure the local MCP runtime, profile and connection details."],
};

const state = {
  status: null,
  operations: null,
  bindings: new Map(),
  section: "overview",
  settingsHydrated: false,
  refreshing: false,
  mutating: false,
};

const q = (selector) => document.querySelector(selector);
const qa = (selector) => Array.from(document.querySelectorAll(selector));

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function fetchJson(url: string, options: RequestInit = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload || payload.ok === false) {
    const message = payload?.message || `Request failed with HTTP ${response.status}.`;
    throw new Error(message);
  }
  return payload;
}

function setNotice(message, tone = "neutral") {
  const notice = q("#notice");
  if (!notice) return;
  if (!message) {
    notice.hidden = true;
    notice.textContent = "";
    notice.removeAttribute("data-tone");
    return;
  }
  notice.hidden = false;
  notice.dataset.tone = tone;
  notice.textContent = message;
}

function setSection(section) {
  if (!SECTION_COPY[section]) return;
  state.section = section;
  qa(".sidebar .nav-item").forEach((button) => {
    const active = button.dataset.nav === section;
    button.classList.toggle("is-active", active);
    if (active) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  });
  qa(".page-section").forEach((sectionNode) => {
    sectionNode.classList.toggle("is-visible", sectionNode.dataset.section === section);
  });
  const [title, subtitle] = SECTION_COPY[section];
  q("#page-title").textContent = title;
  q("#page-subtitle").textContent = subtitle;
  window.scrollTo({ top: 0, behavior: "auto" });
}

function formatTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function shortId(value) {
  const text = String(value || "");
  if (text.length <= 18) return text;
  return `${text.slice(0, 9)}…${text.slice(-7)}`;
}

function toneForWorker(status) {
  return {
    idle: "success",
    waking: "warning",
    claiming: "claiming",
    busy: "accent",
  }[status] || "neutral";
}

function toneForRun(status) {
  return status === "active" ? "accent" : status === "completed" ? "success" : "neutral";
}

function workerStateSummary(group) {
  const values = [
    ["idle", group.idle || 0],
    ["waking", group.waking || 0],
    ["claiming", group.claiming || 0],
    ["busy", group.busy || 0],
  ];
  return values
    .map(
      ([name, count]) => `
        <span class="state-count">
          <span class="state-dot" data-worker-state="${name}"></span>
          <span>${name.toUpperCase()}</span>
          <strong>${count}</strong>
        </span>`,
    )
    .join("");
}

function renderStatus(status) {
  state.status = status;
  const runtimeOnline = Boolean(status.mcp_process?.running);
  q("#mcp-runtime-value").textContent = runtimeOnline ? "Online" : "Offline";
  q("#mcp-runtime-detail").textContent = runtimeOnline
    ? `PID ${status.mcp_process.pid ?? "—"}`
    : "MCP process";
  q("#sidebar-runtime-light").dataset.state = runtimeOnline ? "online" : "offline";
  q("#sidebar-runtime-title").textContent = runtimeOnline ? "Runtime online" : "Runtime offline";
  q("#sidebar-runtime-copy").textContent = runtimeOnline
    ? `${status.project_count} local project${status.project_count === 1 ? "" : "s"}`
    : "Waiting for local MCP";
  q("#connector-url").textContent = status.connector_url || "Not available";
  q("#owner-token").textContent = status.visible_owner_token
    || (status.owner_token_configured
      ? "Configured — rotate to reveal a new token"
      : "Not configured — starting MCP will create one");
  q("#mcp-log").textContent = (status.mcp_process?.log_lines || []).join("\n") || "No recent output.";
  q("#tunnel-log").textContent = (status.tunnel_process?.log_lines || []).join("\n") || "No recent output.";

  if (!state.settingsHydrated && status.preferences) {
    q("#workspace-root").value = status.preferences.workspace_root || "";
    q("#remote-mode").value = status.preferences.remote_mode || "quick_tunnel";
    q("#mcp-port").value = String(status.preferences.mcp_port || 8765);
    q("#custom-origin").value = status.preferences.custom_origin || "";
    state.settingsHydrated = true;
    syncCustomOriginVisibility();
  }
}

function renderOperations(operations) {
  state.operations = operations;
  const totals = operations?.totals || {
    consumer_group_count: 0,
    worker_count: 0,
    idle: 0,
    waking: 0,
    claiming: 0,
    busy: 0,
    provisioning_pending: 0,
    active_runs: 0,
  };

  q("#group-total-value").textContent = String(totals.consumer_group_count || 0);
  q("#worker-total-value").textContent = String(totals.worker_count || 0);
  q("#worker-total-detail").textContent = totals.worker_count
    ? `${totals.busy || 0} busy · ${totals.idle || 0} idle`
    : "No active workers";
  q("#run-total-value").textContent = String(totals.active_runs || 0);
  q("#run-total-detail").textContent = totals.provisioning_pending
    ? `${totals.provisioning_pending} provisioning`
    : "No provisioning backlog";

  q("#mix-idle").textContent = String(totals.idle || 0);
  q("#mix-waking").textContent = String(totals.waking || 0);
  q("#mix-claiming").textContent = String(totals.claiming || 0);
  q("#mix-busy").textContent = String(totals.busy || 0);
  q("#worker-mix-total").textContent = `${totals.worker_count || 0} total`;

  const browser = operations?.browser || { running: false, mode: null, profile_initialized: false };
  const browserLabel = browser.running
    ? browser.mode === "visible_bootstrap" ? "Setup open" : "Headless"
    : "Stopped";
  q("#browser-runtime-value").textContent = browserLabel;
  q("#browser-runtime-detail").textContent = browser.profile_initialized
    ? "Persistent profile ready"
    : "Profile setup required";
  q("#browser-status-chip").textContent = browserLabel;
  q("#browser-status-chip").dataset.tone = browser.running ? "success" : "neutral";
  q("#profile-state").textContent = browser.profile_initialized ? "Profile initialized" : "Not initialized";
  q("#browser-setup-button").textContent = browser.profile_initialized ? "Re-open profile setup" : "Open profile setup";
  q("#browser-setup-button").disabled = Boolean(browser.running && browser.mode !== "visible_bootstrap");
  q("#browser-start-button").disabled = Boolean(browser.running) || !browser.profile_initialized;
  q("#browser-stop-button").disabled = !browser.running;
  q("#browser-setup-button").className = browser.profile_initialized ? "secondary-button" : "primary-button";
  q("#browser-start-button").className = browser.profile_initialized ? "primary-button" : "secondary-button";

  const database = operations?.database || {
    enabled: false,
    state: "disabled",
    projected_at: null,
    message: null,
  };
  const databaseLabel = {
    disabled: "Disabled",
    ready: "Ready",
    driver_missing: "Driver missing",
    unavailable: "Unavailable",
  }[database.state] || "Unknown";
  q("#database-runtime-value").textContent = databaseLabel;
  q("#database-runtime-detail").textContent = database.state === "ready"
    ? `Projected ${formatTime(database.projected_at)}`
    : database.message || "File-backed runtime only";

  renderOverviewProjects(operations?.projects || []);
  renderProjectManagement(operations?.projects || []);
  renderBroker(operations?.broker || null, operations?.projects || []);
  renderWorkerGroups(operations?.projects || []);
  renderRuns(operations?.recent_runs || []);
}

function renderBroker(broker, projects) {
  const summary = q("#broker-summary");
  const namespaceBody = q("#broker-namespace-body");
  const coordinatorBody = q("#broker-coordinator-body");
  if (!summary || !namespaceBody || !coordinatorBody) return;
  if (!broker) {
    summary.innerHTML = emptyState("Broker metadata unavailable", "Refresh after the local runtime has initialized its broker management view.");
    namespaceBody.innerHTML = `<tr><td colspan="8" class="empty-table-cell">No broker namespace data.</td></tr>`;
    coordinatorBody.innerHTML = `<tr><td colspan="5" class="empty-table-cell">No Group Coordinator data.</td></tr>`;
    return;
  }

  const topologyLabel = broker.topology === "embedded_single_node"
    ? "Embedded single node"
    : "Multi-node cluster";
  summary.innerHTML = [
    ["Cluster", broker.cluster_id, topologyLabel],
    ["Controller", broker.controller_node_id, `${broker.nodes.length} broker node${broker.nodes.length === 1 ? "" : "s"}`],
    ["Queue", String(broker.ready_count || 0), `${broker.queued_count || 0} queued · ${broker.claimed_count || 0} claimed`],
    ["Worker slots", String((broker.idle_slot_count || 0) + (broker.busy_slot_count || 0)), `${broker.idle_slot_count || 0} idle · ${broker.busy_slot_count || 0} busy`],
  ]
    .map(([label, value, detail]) => `
      <article class="broker-summary-card">
        <span>${escapeHtml(label)}</span>
        <strong>${escapeHtml(value)}</strong>
        <small>${escapeHtml(detail)}</small>
      </article>`)
    .join("");

  const projectNames = new Map(projects.map((project) => [project.project_id, project.name]));
  namespaceBody.innerHTML = broker.namespaces.length
    ? broker.namespaces.map((namespace) => `
        <tr>
          <td><strong>${escapeHtml(projectNames.get(namespace.project_id) || namespace.project_id)}</strong><small class="table-subtext">${escapeHtml(shortId(namespace.project_id))}</small></td>
          <td>${namespace.queued_count}</td>
          <td><span class="mini-chip" data-tone="${namespace.ready_count ? "accent" : "neutral"}">${namespace.ready_count}</span></td>
          <td>${namespace.claimed_count}</td>
          <td>${namespace.completed_count}</td>
          <td>${namespace.failed_count}</td>
          <td>${namespace.idle_slot_count} idle · ${namespace.busy_slot_count} busy</td>
          <td>${namespace.consumer_group_count}</td>
        </tr>`).join("")
    : `<tr><td colspan="8" class="empty-table-cell">No broker namespaces.</td></tr>`;

  coordinatorBody.innerHTML = broker.group_coordinators.length
    ? broker.group_coordinators.map((group) => `
        <tr>
          <td><strong>${escapeHtml(group.group_id)}</strong></td>
          <td>${escapeHtml(projectNames.get(group.project_id) || shortId(group.project_id))}</td>
          <td><span class="mini-chip" data-tone="success">${escapeHtml(group.coordinator_node_id)}</span></td>
          <td>${group.generation}</td>
          <td>${group.member_count}</td>
        </tr>`).join("")
    : `<tr><td colspan="5" class="empty-table-cell">No initialized Consumer Groups.</td></tr>`;
}

function renderOverviewProjects(projects) {
  const container = q("#overview-projects");
  if (!projects.length) {
    container.innerHTML = emptyState("No project companies", "Select a workspace containing at least one recognized project.");
    return;
  }
  container.innerHTML = projects
    .map((project) => {
      const binding = project.binding;
      const bindingLabel = !binding ? "Unbound" : binding.connector_ready ? "Connected" : "Bound";
      const bindingTone = !binding ? "neutral" : binding.connector_ready ? "success" : "warning";
      const group = project.consumer_group;
      const groupLabel = group.initialized ? `Generation ${group.generation}` : "Not initialized";
      return `
        <article class="project-row">
          <div class="project-identity">
            <div class="project-title-line">
              <strong>${escapeHtml(project.name)}</strong>
              <span class="mini-chip" data-tone="${bindingTone}">${bindingLabel}</span>
            </div>
            <div class="project-meta">${escapeHtml(binding?.project_label || binding?.project_path || project.root)}</div>
          </div>
          <div>
            <div class="project-title-line">
              <strong>Consumer group</strong>
              <span class="mini-chip" data-tone="${group.initialized ? "accent" : "neutral"}">${escapeHtml(groupLabel)}</span>
            </div>
            <div class="worker-state-inline">${workerStateSummary(group)}</div>
          </div>
          <div class="capacity-line">
            <div><span>Desired</span><strong>${project.desired_capacity}</strong></div>
            <div><span>Spawn needed</span><strong>${project.spawn_needed}</strong></div>
            <div><span>Provisioning</span><strong>${project.provisioning_pending}</strong></div>
          </div>
        </article>`;
    })
    .join("");
}

function renderProjectManagement(projects) {
  const container = q("#project-management-list");
  if (!projects.length) {
    container.innerHTML = emptyState("No local projects", "Choose a workspace in Settings before binding a provider project.");
    return;
  }
  container.innerHTML = projects.map((project) => {
    const binding = project.binding;
    const metadata = binding || {};
    const provider = metadata.provider || "chatgpt";
    const projectRef = metadata.project_ref || "";
    const projectPath = metadata.project_path || "";
    const projectLabel = metadata.project_label || "";
    const appRef = state.bindings.get(project.project_id)?.adapter_metadata?.app_ref || "";
    const appLabel = state.bindings.get(project.project_id)?.adapter_metadata?.app_label || "";
    const connected = Boolean(binding?.connector_ready);
    return `
      <article class="project-management-row" data-project-id="${escapeHtml(project.project_id)}">
        <div class="project-management-header">
          <div>
            <div class="project-title-line">
              <h3>${escapeHtml(project.name)}</h3>
              <span class="mini-chip" data-tone="${connected ? "success" : binding ? "warning" : "neutral"}">
                ${connected ? "Connected" : binding ? "Bound" : "Unbound"}
              </span>
            </div>
            <p>${escapeHtml(project.root)}</p>
          </div>
          <span class="revision-label">${binding ? `rev ${binding.revision}` : "new binding"}</span>
        </div>
        <div class="binding-details">
          <div><span>Provider</span><strong>${escapeHtml(provider)}</strong></div>
          <div><span>Project ref</span><strong>${escapeHtml(projectRef || "—")}</strong></div>
          <div><span>Project path</span><strong>${escapeHtml(projectPath || "—")}</strong></div>
          <div><span>Connector</span><strong>${connected ? "Ready" : "Not ready"}</strong></div>
        </div>
        <form class="binding-editor" data-binding-form data-project-id="${escapeHtml(project.project_id)}">
          <label><span>ChatGPT Project URL</span><input name="project_url" type="url" autocomplete="off" spellcheck="false" placeholder="https://chatgpt.com/g/g-p-.../project" value="${escapeHtml(projectPath ? `https://chatgpt.com${projectPath}` : "")}" /></label>
          <label><span>Project label</span><input name="project_label" type="text" maxlength="255" value="${escapeHtml(projectLabel)}" placeholder="AI FOMO" /></label>
          <label><span>App reference</span><input name="app_ref" type="text" maxlength="255" value="${escapeHtml(appRef)}" placeholder="Optional connector identity" /></label>
          <label><span>App label</span><input name="app_label" type="text" maxlength="255" value="${escapeHtml(appLabel)}" placeholder="ChatOrchestration" /></label>
          <button class="primary-button" type="submit">${binding ? "Update binding" : "Connect project"}</button>
        </form>
      </article>`;
  }).join("");
}

function renderWorkerGroups(projects) {
  const container = q("#worker-groups");
  if (!projects.length) {
    container.innerHTML = emptyState("No consumer groups", "Consumer groups appear after a project is discovered by the local runtime.");
    return;
  }
  container.innerHTML = projects.map((project) => {
    const group = project.consumer_group;
    const workers = group.workers || [];
    return `
      <article class="worker-group">
        <div class="worker-group-header">
          <div>
            <div class="project-title-line">
              <h3>${escapeHtml(project.name)}</h3>
              <span class="mini-chip" data-tone="${group.initialized ? "accent" : "neutral"}">
                ${group.initialized ? `Generation ${group.generation}` : "Not initialized"}
              </span>
            </div>
            <p>${workers.length} reusable Worker Chat${workers.length === 1 ? "" : "s"} · desired capacity ${project.desired_capacity}</p>
          </div>
          <div class="worker-state-inline">${workerStateSummary(group)}</div>
        </div>
        <div class="table-shell">
          ${workers.length ? `
            <table>
              <thead><tr><th>Worker target</th><th>Status</th><th>Wake revision</th><th>Updated</th></tr></thead>
              <tbody>
                ${workers.map((worker) => `
                  <tr>
                    <td><code>${escapeHtml(shortId(worker.wake_target_id))}</code></td>
                    <td><span class="status-badge" data-tone="${toneForWorker(worker.status)}">${escapeHtml(worker.status.toUpperCase())}</span></td>
                    <td>${worker.wake_revision}</td>
                    <td>${formatTime(worker.updated_at)}</td>
                  </tr>`).join("")}
              </tbody>
            </table>` : emptyState("No registered Workers", project.spawn_needed ? `${project.spawn_needed} Worker${project.spawn_needed === 1 ? "" : "s"} need provisioning.` : "The group is ready to grow when work arrives.")}
        </div>
      </article>`;
  }).join("");
}

function renderRuns(runs) {
  const tableBody = q("#runs-table-body");
  const overview = q("#overview-runs");
  if (!runs.length) {
    tableBody.innerHTML = `<tr><td colspan="6" class="empty-table-cell">No orchestration runs yet.</td></tr>`;
    overview.innerHTML = emptyState("No recent runs", "Delegated work will appear here after Main creates a Run.");
    return;
  }
  tableBody.innerHTML = runs.map((run) => `
    <tr>
      <td><code>${escapeHtml(shortId(run.run_id))}</code></td>
      <td>${escapeHtml(run.project_name)}</td>
      <td class="goal-cell">${escapeHtml(run.goal)}</td>
      <td>${run.requested_parallelism}</td>
      <td><span class="status-badge" data-tone="${toneForRun(run.status)}">${escapeHtml(run.status.toUpperCase())}</span></td>
      <td>${formatTime(run.updated_at)}</td>
    </tr>`).join("");
  overview.innerHTML = runs.slice(0, 4).map((run) => `
    <button class="compact-run-row" type="button" data-nav="runs">
      <span class="run-state-dot" data-tone="${toneForRun(run.status)}"></span>
      <span><strong>${escapeHtml(run.project_name)}</strong><small>${escapeHtml(run.goal)}</small></span>
      <time>${formatTime(run.updated_at)}</time>
    </button>`).join("");
}

function emptyState(title, copy) {
  return `<div class="empty-state"><strong>${escapeHtml(title)}</strong><span>${escapeHtml(copy)}</span></div>`;
}

function parseChatGptProjectUrl(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) return null;
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("Enter a valid ChatGPT Project URL.");
  }
  if (parsed.protocol !== "https:" || parsed.hostname !== "chatgpt.com") {
    throw new Error("Project URL must use https://chatgpt.com.");
  }
  const match = parsed.pathname.match(/^\/g\/(g-p-[A-Za-z0-9_-]+)(?:\/project)?\/?$/);
  if (!match) {
    throw new Error("Use a ChatGPT Project URL shaped like https://chatgpt.com/g/g-p-.../project.");
  }
  return {
    provider: "chatgpt",
    project_ref: match[1],
    project_path: `/g/${match[1]}`,
  };
}

async function refreshAll({ quiet = false } = {}) {
  if (state.refreshing) return;
  state.refreshing = true;
  q("#refresh-button").disabled = true;
  try {
    const status = await fetchJson(API.status);
    renderStatus(status);
    if (!status.configured) {
      renderOperations(null);
      state.bindings = new Map();
      if (!quiet) setNotice("Configure a workspace in Settings to load runtime operations.", "neutral");
      return;
    }
    const [operations, bindings] = await Promise.all([
      fetchJson(API.operations),
      fetchJson(API.bindings),
    ]);
    state.bindings = new Map((bindings.bindings || []).map((binding) => [binding.project_id, binding]));
    renderOperations(operations);
    q("#last-sync").textContent = `Synced ${new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date())}`;
    if (!quiet) setNotice("Runtime state refreshed.", "success");
  } catch (error) {
    if (!quiet) setNotice(error instanceof Error ? error.message : String(error), "danger");
  } finally {
    state.refreshing = false;
    q("#refresh-button").disabled = false;
  }
}

async function runMutation(url, successMessage, options = {}) {
  if (state.mutating) return;
  state.mutating = true;
  setNotice("Working…", "neutral");
  try {
    const payload = await fetchJson(url, { method: "POST", ...options });
    if (url.startsWith("/api/worker-browser/")) {
      renderOperations(payload);
    } else {
      renderStatus(payload);
      await refreshAll({ quiet: true });
    }
    setNotice(successMessage, "success");
  } catch (error) {
    setNotice(error instanceof Error ? error.message : String(error), "danger");
  } finally {
    state.mutating = false;
  }
}

async function saveSettings(event) {
  event.preventDefault();
  const remoteMode = q("#remote-mode").value;
  const payload: Record<string, unknown> = {
    workspace_root: q("#workspace-root").value,
    remote_mode: remoteMode,
    custom_origin: remoteMode === "custom_domain" ? q("#custom-origin").value : null,
    mcp_port: Number(q("#mcp-port").value),
  };
  try {
    const status = await fetchJson(API.settings, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.settingsHydrated = false;
    renderStatus(status);
    await refreshAll({ quiet: true });
    setNotice("Settings saved.", "success");
  } catch (error) {
    setNotice(error instanceof Error ? error.message : String(error), "danger");
  }
}

async function saveBinding(form) {
  const projectId = form.dataset.projectId;
  if (!projectId) return;
  const formData = new FormData(form);
  const projectUrl = String(formData.get("project_url") || "");
  const parsed = parseChatGptProjectUrl(projectUrl);
  const current = state.bindings.get(projectId);
  const metadata: Record<string, unknown> = parsed
    ? {
      provider: parsed.provider,
      project_ref: parsed.project_ref,
      project_path: parsed.project_path,
      project_label: String(formData.get("project_label") || "").trim() || null,
      app_ref: String(formData.get("app_ref") || "").trim() || null,
      app_label: String(formData.get("app_label") || "").trim() || null,
      observed_at: new Date().toISOString(),
    }
    : null;
  const payload: Record<string, unknown> = {
    adapter_metadata: metadata,
  };
  if (current) payload.expected_revision = current.revision;
  try {
    await fetchJson(`${API.bindings}/${encodeURIComponent(projectId)}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    await refreshAll({ quiet: true });
    setNotice("Project binding saved.", "success");
  } catch (error) {
    setNotice(error instanceof Error ? error.message : String(error), "danger");
  }
}

async function chooseFolder() {
  try {
    const result = await fetchJson(API.folderPicker, { method: "POST" });
    if (result.selected_path) q("#workspace-root").value = result.selected_path;
  } catch (error) {
    setNotice(error instanceof Error ? error.message : String(error), "danger");
  }
}

async function rotateToken() {
  try {
    const result = await fetchJson(API.rotateToken, { method: "POST" });
    q("#owner-token").textContent = result.owner_token;
    setNotice("Owner token rotated. Store the new token now; the previous token is invalid.", "warning");
  } catch (error) {
    setNotice(error instanceof Error ? error.message : String(error), "danger");
  }
}

function syncCustomOriginVisibility() {
  const custom = q("#remote-mode").value === "custom_domain";
  q("#custom-origin-field").hidden = !custom;
  q("#custom-origin").required = custom;
}

function bindEvents() {
  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const button = target.closest("[data-nav]");
    if (button instanceof HTMLElement) setSection(button.dataset.nav);
  });
  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (form instanceof HTMLFormElement && form.matches("[data-binding-form]")) {
      event.preventDefault();
      saveBinding(form);
    }
  });
  q("#settings-form").addEventListener("submit", saveSettings);
  q("#remote-mode").addEventListener("change", syncCustomOriginVisibility);
  q("#refresh-button").addEventListener("click", () => refreshAll());
  q("#folder-picker-button").addEventListener("click", chooseFolder);
  q("#runtime-start-button").addEventListener("click", () => runMutation(API.runtimeStart, "Runtime started."));
  q("#runtime-stop-button").addEventListener("click", () => runMutation(API.runtimeStop, "Runtime stopped."));
  q("#rotate-token-button").addEventListener("click", rotateToken);
  q("#browser-setup-button").addEventListener("click", () => runMutation(API.browserSetup, "Worker browser profile setup opened."));
  q("#browser-start-button").addEventListener("click", () => runMutation(API.browserStart, "Headless Worker browser started with the persistent profile."));
  q("#browser-stop-button").addEventListener("click", () => runMutation(API.browserStop, "Worker browser stopped."));
}

bindEvents();
const initialSection = new URLSearchParams(window.location.search).get("section");
if (initialSection) setSection(initialSection);
refreshAll({ quiet: true });
