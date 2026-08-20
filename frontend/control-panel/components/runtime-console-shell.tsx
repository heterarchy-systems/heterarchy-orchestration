export function RuntimeConsoleShell() {
  return (
    <>
      <div className="app-shell">
            <aside className="sidebar" aria-label="Runtime Console navigation">
              <div className="brand-lockup">
                <span className="sr-only">ChatToCodex 설정</span>
                <div className="brand-mark" aria-hidden="true">
                  <span></span><span></span><span></span>
                </div>
                <div>
                  <strong>ChatToCodex</strong>
                  <span>Runtime Console</span>
                </div>
              </div>
      
              <nav className="sidebar-nav" aria-label="Console sections">
                <button className="nav-item is-active" type="button" data-nav="overview" aria-current="page">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 13h6V4H4v9Zm0 7h6v-4H4v4Zm10 0h6v-9h-6v9Zm0-16v4h6V4h-6Z"/></svg>
                  Overview
                </button>
                <button className="nav-item" type="button" data-nav="execution">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h5l2 3h9v9H4V6Zm3 8h3v2H7v-2Zm5-3h5v2h-5v-2Z"/></svg>
                  Execution
                </button>
                <button className="nav-item" type="button" data-nav="projects">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 5h6l2 2h8v12H4V5Z"/></svg>
                  Projects
                </button>
                <button className="nav-item" type="button" data-nav="broker">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 5h16v4H4V5Zm0 5h16v4H4v-4Zm0 5h16v4H4v-4Zm2-8h2v1H6V7Zm0 5h2v1H6v-1Zm0 5h2v1H6v-1Z"/></svg>
                  Broker
                </button>
                <button className="nav-item" type="button" data-nav="workers">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5a3 3 0 1 0 0 6 3 3 0 0 0 0-6Zm8 1a2.5 2.5 0 1 0 0 5 2.5 2.5 0 0 0 0-5ZM3 19c0-3 2-5 5-5s5 2 5 5H3Zm10.5 0c0-2.2 1-3.9 2.9-4.7 2.7.2 4.6 1.9 4.6 4.7h-7.5Z"/></svg>
                  Workers
                </button>
                <button className="nav-item" type="button" data-nav="runs">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m9 7 8 5-8 5V7Z"/></svg>
                  Runs
                </button>
                <button className="nav-item" type="button" data-nav="settings">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 8.2a3.8 3.8 0 1 0 0 7.6 3.8 3.8 0 0 0 0-7.6Zm8 3.8-2.1-1 .1-2.3-2.2-2.2-2.3.1-1-2.1H9.5l-1 2.1-2.3-.1L4 8.7l.1 2.3-2.1 1v3l2.1 1-.1 2.3 2.2 2.2 2.3-.1 1 2.1h3l1-2.1 2.3.1 2.2-2.2-.1-2.3 2.1-1v-3Z"/></svg>
                  Settings
                </button>
              </nav>
      
              <div className="sidebar-footer">
                <div className="sidebar-status">
                  <span className="status-light" id="sidebar-runtime-light" data-state="offline"></span>
                  <div>
                    <strong id="sidebar-runtime-title">Runtime offline</strong>
                    <span id="sidebar-runtime-copy">Waiting for local MCP</span>
                  </div>
                </div>
                <span className="build-label">Local control plane</span>
              </div>
            </aside>
      
            <main className="main-stage">
              <header className="topbar">
                <div>
                  <h1 id="page-title">Operations</h1>
                  <p id="page-subtitle">Project companies, consumer groups, workers and current runs.</p>
                </div>
                <div className="topbar-actions">
                  <span className="last-sync" id="last-sync">Not synced yet</span>
                  <button className="icon-button" id="refresh-button" type="button" aria-label="Refresh runtime data" title="Refresh runtime data">
                    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 8V4l-1.8 1.8A8 8 0 1 0 20 12h-2a6 6 0 1 1-2.2-4.6L15 8h4Z"/></svg>
                  </button>
                </div>
              </header>
      
              <div id="notice" className="notice" role="status" aria-live="polite" hidden></div>
      
              <section className="page-section is-visible" data-section="overview">
                <div className="runtime-strip" aria-label="Runtime status">
                  <div className="runtime-cell">
                    <span>Control plane</span>
                    <strong id="mcp-runtime-value">Offline</strong>
                    <small id="mcp-runtime-detail">MCP process</small>
                  </div>
                  <div className="runtime-cell">
                    <span>Worker browser</span>
                    <strong id="browser-runtime-value">Stopped</strong>
                    <small id="browser-runtime-detail">Persistent profile</small>
                  </div>
                  <div className="runtime-cell">
                    <span>Database</span>
                    <strong id="database-runtime-value">Disabled</strong>
                    <small id="database-runtime-detail">PostgreSQL read model</small>
                  </div>
                  <div className="runtime-cell">
                    <span>Runtime groups</span>
                    <strong id="group-total-value">0</strong>
                    <small>Orchestration topology</small>
                  </div>
                  <div className="runtime-cell">
                    <span>Workers</span>
                    <strong id="worker-total-value">0</strong>
                    <small id="worker-total-detail">No active workers</small>
                  </div>
                  <div className="runtime-cell">
                    <span>Active runs</span>
                    <strong id="run-total-value">0</strong>
                    <small id="run-total-detail">No queued work</small>
                  </div>
                </div>
      
                <div className="overview-grid">
                  <section className="workspace-panel" aria-labelledby="companies-heading">
                    <div className="section-heading-row">
                      <div>
                        <h2 id="companies-heading">Project companies</h2>
                        <p>Each local project is an isolated namespace inside the shared Broker Cluster, with its own provider binding and Worker groups.</p>
                      </div>
                      <button className="text-button" type="button" data-nav="projects">Manage bindings</button>
                    </div>
                    <div id="overview-projects" className="project-list" aria-live="polite"></div>
                  </section>
      
                  <aside className="right-rail">
                    <section className="rail-section" aria-labelledby="browser-heading">
                      <div className="rail-heading">
                        <h2 id="browser-heading">Worker browser</h2>
                        <span id="browser-status-chip" className="mini-chip">Stopped</span>
                      </div>
                      <p>One persistent Chrome profile acts as the delivery adapter. Task authority remains in the Broker.</p>
                      <div className="rail-detail-row"><span>ChatGPT profile</span><strong id="profile-state">Not initialized</strong></div>
                      <button id="browser-setup-button" className="primary-button" type="button">Open profile setup</button>
                      <button id="browser-start-button" className="secondary-button" type="button">Start headless runtime</button>
                      <button id="browser-stop-button" className="text-button wide-button" type="button">Stop worker browser</button>
                    </section>
      
                    <section className="rail-section" aria-labelledby="worker-mix-heading">
                      <div className="rail-heading">
                        <h2 id="worker-mix-heading">Worker state mix</h2>
                        <span id="worker-mix-total" className="rail-meta">0 total</span>
                      </div>
                      <div className="state-mix" id="state-mix">
                        <div><span className="state-dot" data-worker-state="idle"></span><span>IDLE</span><strong id="mix-idle">0</strong></div>
                        <div><span className="state-dot" data-worker-state="waking"></span><span>WAKING</span><strong id="mix-waking">0</strong></div>
                        <div><span className="state-dot" data-worker-state="claiming"></span><span>CLAIMING</span><strong id="mix-claiming">0</strong></div>
                        <div><span className="state-dot" data-worker-state="busy"></span><span>BUSY</span><strong id="mix-busy">0</strong></div>
                      </div>
                    </section>
      
                    <section className="rail-section" aria-labelledby="recent-run-heading">
                      <div className="rail-heading">
                        <h2 id="recent-run-heading">Recent runs</h2>
                        <button className="text-button" type="button" data-nav="runs">View all</button>
                      </div>
                      <div id="overview-runs" className="compact-runs"></div>
                    </section>
                  </aside>
                </div>
              </section>
      
              <section className="page-section" data-section="execution">
                <div className="section-intro execution-intro">
                  <div>
                    <div className="eyebrow-row"><span className="live-dot"></span><span>Live orchestration</span></div>
                    <h2>Execution control</h2>
                    <p>Follow each Run from orchestration planning through Broker ownership, Worker execution, and Main Review. Select a Run to see what is happening and what should happen next.</p>
                  </div>
                  <span className="status-chip" data-tone="success">AUTO REFRESH · 5S</span>
                </div>
                <div className="execution-layout">
                  <aside className="execution-list-panel" aria-label="Execution runs">
                    <div className="panel-heading">
                      <div><span>Runs</span><strong>Execution queue</strong></div>
                      <small id="execution-run-count">0 runs</small>
                    </div>
                    <div id="execution-run-list" className="execution-run-list"></div>
                  </aside>
                  <section className="execution-detail-panel" aria-live="polite">
                    <div id="execution-detail"></div>
                  </section>
                </div>
              </section>
      
              <section className="page-section" data-section="projects">
                <div className="section-intro">
                  <div>
                    <h2>Project bindings</h2>
                    <p>Bind a local project to its exact ChatGPT Project identity. The connector can enrich the binding later.</p>
                  </div>
                </div>
                <div id="project-management-list" className="management-list"></div>
              </section>
      
              <section className="page-section" data-section="broker">
                <div className="section-intro">
                  <div>
                    <h2>Shared Broker Cluster</h2>
                    <p>Consumer Groups are Broker-authoritative Agent Companies. Provider runtimes remain an orchestration concern, so GPT, CLI, and future agents can participate without changing Broker identity semantics.</p>
                  </div>
                </div>
                <div id="broker-summary" className="broker-summary-grid"></div>
      
                <div className="section-intro compact broker-subsection-heading">
                  <div>
                    <h2>Namespaces</h2>
                    <p>Per-project queue and reusable Worker Slot pressure inside the shared cluster.</p>
                  </div>
                </div>
                <div className="table-shell">
                  <table>
                    <thead>
                      <tr><th>Namespace</th><th>Queued</th><th>Ready</th><th>Claimed</th><th>Done</th><th>Failed</th><th>Slots</th><th>Groups</th></tr>
                    </thead>
                    <tbody id="broker-namespace-body"></tbody>
                  </table>
                </div>
      
                <div className="section-intro compact broker-subsection-heading">
                  <div>
                    <h2>Consumer groups</h2>
                    <p>Authoritative Broker groups managed by the Broker-side GroupCoordinator.</p>
                  </div>
                </div>
                <div className="table-shell">
                  <table>
                    <thead>
                      <tr><th>Consumer group</th><th>Namespace</th><th>Generation</th><th>Revision</th><th>Consumers</th></tr>
                    </thead>
                    <tbody id="broker-group-body"></tbody>
                  </table>
                </div>
              </section>
      
              <section className="page-section" data-section="workers">
                <div className="section-intro">
                  <div>
                    <h2>Worker runtimes</h2>
                    <p>Orchestration-owned runtime topology: reusable Worker targets, wake state, and capacity. Broker Consumer Group authority is shown separately.</p>
                  </div>
                </div>
                <div id="worker-groups" className="worker-groups"></div>
              </section>
      
              <section className="page-section" data-section="runs">
                <div className="section-intro">
                  <div>
                    <h2>Run activity</h2>
                    <p>Recent orchestration runs ordered by their latest state change.</p>
                  </div>
                </div>
                <div className="table-shell">
                  <table>
                    <thead>
                      <tr><th>Run</th><th>Project</th><th>Goal</th><th>Parallelism</th><th>Status</th><th>Updated</th></tr>
                    </thead>
                    <tbody id="runs-table-body"></tbody>
                  </table>
                </div>
              </section>
      
              <section className="page-section" data-section="settings">
                <div className="settings-layout">
                  <section className="settings-section" aria-labelledby="workspace-heading">
                    <div className="section-intro compact">
                      <div>
                        <h2 id="workspace-heading">Local runtime</h2>
                        <p>Workspace discovery, MCP exposure and owner capability.</p>
                      </div>
                    </div>
                    <form id="settings-form" className="settings-form">
                      <label>
                        <span>Workspace folder</span>
                        <div className="input-action-row">
                          <input id="workspace-root" name="workspace_root" type="text" autoComplete="off" spellCheck="false" placeholder="/Users/you/Projects" required />
                          <button id="folder-picker-button" className="secondary-button compact-button" type="button">Choose</button>
                        </div>
                      </label>
                      <div className="field-grid">
                        <label>
                          <span>Remote mode</span>
                          <select id="remote-mode" name="remote_mode">
                            <option value="quick_tunnel">Quick tunnel</option>
                            <option value="custom_domain">Custom domain</option>
                          </select>
                        </label>
                        <label>
                          <span>MCP port</span>
                          <input id="mcp-port" name="mcp_port" type="number" min="1024" max="65535" value="8765" required />
                        </label>
                      </div>
                      <label id="custom-origin-field" hidden>
                        <span>Custom HTTPS origin</span>
                        <input id="custom-origin" name="custom_origin" type="url" autoComplete="off" spellCheck="false" placeholder="https://mcp.example.com" />
                      </label>
                      <div className="settings-actions">
                        <button id="settings-save-button" className="primary-button" type="submit">Save settings</button>
                        <button id="runtime-start-button" className="secondary-button" type="button">Start runtime</button>
                        <button id="runtime-stop-button" className="secondary-button" type="button">Stop runtime</button>
                      </div>
                    </form>
                  </section>
      
                  <section className="settings-section" aria-labelledby="connection-heading">
                    <div className="section-intro compact">
                      <div>
                        <h2 id="connection-heading">Connector</h2>
                        <p>Use these values when attaching ChatGPT to the local MCP runtime.</p>
                      </div>
                    </div>
                    <div className="connection-stack">
                      <div className="copy-block"><span>Connector URL</span><code id="connector-url">Not available</code></div>
                      <div className="copy-block"><span>Owner token</span><code id="owner-token">Not configured</code></div>
                      <button id="rotate-token-button" className="secondary-button" type="button">Rotate owner token</button>
                    </div>
                  </section>
      
                  <section className="settings-section settings-section-wide" aria-labelledby="logs-heading">
                    <div className="section-intro compact">
                      <div>
                        <h2 id="logs-heading">Runtime output</h2>
                        <p>Recent MCP and tunnel output. Secrets are redacted by the local supervisor.</p>
                      </div>
                    </div>
                    <div className="log-grid">
                      <div><span>MCP</span><pre id="mcp-log">No recent output.</pre></div>
                      <div><span>Tunnel</span><pre id="tunnel-log">No recent output.</pre></div>
                    </div>
                  </section>
                </div>
              </section>
            </main>
          </div>
      
          <template id="empty-state-template">
            <div className="empty-state"><strong></strong><span></span></div>
          </template>
    </>
  );
}
