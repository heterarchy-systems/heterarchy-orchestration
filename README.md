# ChatToCodex

ChatToCodex lets one Main ChatGPT conversation delegate work to reusable generic Worker Chats through a local MCP runtime.

The normal user flow is intentionally small:

```text
One-time setup
  Open the Main conversation inside the target GPT Project.
  Register that Main Chat once and bind it to the canonical local project.

Main Chat
  "이 프로젝트를 병렬로 작업해줘. 끝나면 네가 직접 검수하고 문제가 있으면 다시 시켜."

ChatToCodex reuses, wakes, or provisions the required Worker capacity.
Main Chat wakes again when the Main Review Gate opens.
```

The initial Worker count may be zero. The user does not create Worker Chats, register them from the extension popup, choose a project for each Worker, type `작업해`, or copy and manage Run IDs, Worker IDs, Slot IDs, capability handles, Broker Task IDs, dependency IDs, or wake routing IDs.

## User flow

### One-time Main registration and Project binding

Load the unpacked Chrome extension from:

```text
chattocodex/wake/chrome_extension
```

Open the intended Main conversation from inside the target GPT Project. Its URL must have the explicit `https://chatgpt.com/g/<project-ref>/c/<conversation-id>` shape. Click **ChatToCodex Runtime Wake**, choose the canonical local project, then click **이 Chat을 Main reviewer로 등록**. A plain `/c/<conversation-id>` route is rejected because it does not prove the GPT Project context.

This single operation first records GPT Project adapter metadata against the server-owned local `project_id` and `project_root`, then registers the Main reviewer. The GPT Project observation cannot authorize or replace the canonical local identity. Repeating the same binding is idempotent; a stable provider/reference pair cannot be rebound to another local project.

The extension stores Main and Worker conversation locations only in Chrome local storage. Worker provisioning reports an opaque `wake_target_id` and conversation ID; Main review registration reports an opaque `main_target_id`. The server does not receive a Chat URL through either loopback boundary.

After registration or automatic provisioning, Chat tabs do not need to remain open. While Chrome and the Runtime Wake extension are running, the extension keeps a separate runtime heartbeat for provisioned Worker targets. A Worker with no open Chat tab is treated as a recoverable dormant consumer; when work becomes `WAKING`, the extension reopens that conversation in an inactive background tab. The Main reviewer uses the same background reopen behavior when the Main Review Gate opens.

Only tabs opened by the Runtime Adapter are eligible for automatic parking. After the target returns to `IDLE`, the adapter waits through an idle grace period and asks the content script whether ChatGPT has finished generating and the composer is empty. It then closes only its own inactive, unpinned tab. If the user activates or pins a runtime-opened tab, runtime ownership is relinquished and the tab is left open.

### Project binding API

Main registration uses the following loopback-only binding API. The Control Panel can use the same boundary. The server resolves `project_id` and `project_root` from the configured workspace; browser-supplied provider references, labels, and observed paths are optional metadata only.

```text
GET /api/project-bindings
GET /api/project-bindings/<project_id>
PUT /api/project-bindings/<project_id>
```

The PUT body may contain `adapter_metadata` and an optional `expected_revision`, but must not contain a browser-selected canonical root. Repeating the same binding is idempotent. Stable adapter provider/reference values cannot be rebound after the first observation; labels and observed paths may be refreshed. Task, role, objective, Run, DAG, and capability data never cross this binding boundary.

### Main Chat

Ask for delegated work naturally:

```text
@<등록할 때 선택한 앱 이름>
이 프로젝트 3명이서 검토해줘.
보안 / 백엔드 / 제품 관점으로 보고 마지막에 종합해줘.
```

For simple fire-and-report delegation, ChatToCodex can still use `delegate_work`. When the user asks the Main Chat to inspect Worker results, approve completion, or request corrective work, the Main Chat uses `orchestrate_work`. The durable Run Control Coordinator owns the execution round, Main Review Gate, and bounded rework loop while the Broker continues to own logical Task readiness.

Worker completion automatically refreshes the coordinated Run. When every Worker Task in the current execution round is terminal, the workflow moves to `WAITING_MAIN_REVIEW`. The Main Chat checks `workflow_status`, inspects reports/results/diffs/tests, then calls `review_workflow` with `approve`, `rework`, or `fail`. A rework decision creates a new execution round with a new corrective Worker DAG. Rework rounds are bounded by `max_rework_rounds`.

The registered Main reviewer polls only its opaque review-target state. When the Main Review Gate opens, the Runtime Wake adapter sends:

```text
@<등록할 때 선택한 앱 이름> 검수해
main_target_id: <opaque routing id>
main_wake_revision: <opaque delivery epoch>
```

The Main Chat calls `resume_main_review` with both opaque values, which resolves the pending workflow server-side and rotates a fresh review capability. The browser does not receive workflow IDs, Run IDs, review instructions, or capability values. A stale Main wake revision is rejected. An unconsumed Main review wake expires after 90 seconds and enters bounded exponential cooldown; another registered Main reviewer can then claim the same waiting Workflow, while N=1 retries the same Main Chat later with a new revision. The Main then inspects the requested evidence and calls `review_workflow` with `approve`, `rework`, or `fail`. After rework completes, the same Main reviewer can be woken again for the next review round.

If a Main reviewer disappears after `resume_main_review` has already activated the review session, the Runtime first reopens that same Chat and sends `@<the app selected at registration> 검수 계속해` so the existing review context and workflow capability can continue. The app reference and current display label are observed from the registered Main composer; no MCP display name is hard-coded. The actual Main Chat content script renews a separate chat-presence heartbeat; background extension polling does not fake that presence. If the Main Chat still does not return within 120 seconds, the ACTIVE review claim is released and another Main reviewer can take the same `WAITING_MAIN_REVIEW` Workflow. When that replacement calls `resume_main_review`, the Workflow review capability rotates again, invalidating the abandoned Main's old handle.

### Worker Chat

READY work drives capacity automatically. The coordinator computes bounded desired capacity from requested parallelism, READY Task count, and project/global Worker limits, then applies this order:

```text
Reuse > Wake > Spawn
```

An existing recoverable Worker is reused before ChatToCodex provisions another conversation. If the project has zero recoverable Workers and one READY Task, backend admission opens a new Chat inside the bound GPT Project, bootstraps it, obtains its persistent conversation ID, registers its opaque Worker target, joins it to the Consumer Group, and lets the normal wake path deliver work. The popup never registers a Worker.

When two new Workers are required, provisioning is strictly sequential. The global provider-facing open/spawn gate allows at most one in-flight request, and the default minimum admission interval is 12 seconds (the initial safe range is 10–15 seconds). A provider rate-limit signal stops further provisioning and advances the configured bounded backoff through `30s → 60s → 120s`; the adapter does not start a parallel retry burst.

An automatically provisioned Worker can be either active (its Chat tab is open) or dormant (the extension is alive but its Chat tab is closed). Active Workers are preferred. A dormant Worker remains eligible for READY capacity through the extension runtime heartbeat; when it becomes `WAKING`, the extension opens its registered conversation in the background if necessary and the content script sends:

```text
@<등록할 때 선택한 앱 이름> 작업해.
사용자가 Main Chat에서 Worker 위임을 요청했습니다.
이 서버 발급 wake pair는 현재 배정을 조회하기 위한 work 호출만 승인합니다.
파괴적·비가역적·자격증명 필요·외부 프로덕션 작업의 별도 승인을 뜻하지 않습니다.
wake_target_id: <opaque routing id>
wake_revision: <opaque delivery epoch>
```

The authorization wording is deliberately narrow: the user initiated Main delegation, and the fresh single-use wake pair authorizes only retrieving the current backend assignment with `work`. It never attests separate approval for destructive, irreversible, credential-gated, or external-production effects. The browser still does not receive Task content, role, Run ID, DAG data, repository instructions, or Worker capability values. The Worker obtains the authoritative assignment only by calling `work` after waking, and stale or fabricated wake values fail at the backend boundary.

A fresh Worker Slot cannot be created by calling `work` manually without the current Wake delivery. This is a server-side safety boundary: after `delegate_work`, the Main Chat is structurally unable to self-claim its own delegated tasks. The first Worker claim must carry both the opaque `wake_target_id` and the current `wake_revision` supplied by the registered Chrome Wake adapter. A stale revision is rejected before fresh Broker work is accepted. Later turns reuse the Slot identity kept inside that Worker conversation and omit fresh wake fields. After the assignment becomes terminal, the Worker calls `work_done` and returns the physical Chat to IDLE.

Wake delivery is leased rather than trusted indefinitely. A `WAKING` reservation that is not claimed within 90 seconds is revoked, the physical target enters exponential cooldown (`30s → 60s → 120s → 300s`), and another ACTIVE/DORMANT consumer can receive the still-READY work. With only one consumer, the same Chat is retried after cooldown with a new `wake_revision`.

After a Worker has already claimed work, the Runtime first tries to preserve Chat context: if the BUSY runtime-owned tab disappears it is reopened and receives `@<the app selected at registration> 작업 계속해`, which resumes the established Slot. If physical Chat presence still does not return within the assignment recovery window, the old temporary Task is terminally revoked, the logical Broker Task is re-queued, and another consumer may execute a fresh temporary assignment. An already-terminal Task is preserved instead of being re-run.

A physical Chat has no permanent role. After the first run returns Workers A and B to `IDLE` or parks their runtime-owned tabs, a second run that needs two Workers reuses those same physical conversations and creates zero new Chats:

```text
Run 1
  Worker A = accessibility reviewer
  Worker B = data-integrity reviewer

Run 2
  Worker A = code-architecture reviewer
  Worker B = test reviewer
```

## Public MCP surface

User-facing orchestration:

- `delegate_work` — Main Chat creates a Run and the complete logical task DAG in one call.
- `delegation_status` — Main Chat checks delegated-work progress and completion.
- `orchestrate_work` — Main Chat creates a durable coordinated workflow when Worker execution must be followed by Main review and optional bounded rework.
- `workflow_status` — refreshes the current execution round and reports whether the workflow is waiting at the Main Review Gate.
- `review_workflow` — Main Chat records `approve`, `rework`, or `fail`; rework starts a new corrective execution round.
- `resume_main_review` — Main Chat consumes the current opaque `main_target_id` + `main_wake_revision` from the local Runtime Wake adapter, resolves the pending review server-side, and rotates a fresh workflow review capability.
- `work` — A fresh Worker Chat claims work only when the Chrome Wake adapter supplies the current opaque `wake_target_id` + `wake_revision`; established Workers safely resume with their existing Slot capability and no fresh wake fields.
- `work_done` — Worker Chat releases a terminal assignment and becomes reusable again.

Worker execution primitives used internally by the Chat:

- `worker_context`
- `task_update`
- `project_tree`
- `project_file_read`
- `project_text_search`
- `worker_workspace_prepare`
- `worker_file_create`
- `worker_file_replace`
- `worker_result_commit`
- `worker_report_submit`
- `run_results`
- `run_reports`
- `integration_preview`
- `integration_apply`

Low-level Broker actions and Wake actions are deliberately not registered as public MCP actions. Broker state, Task dependencies, reusable Slot identity, temporary capabilities, and browser routing stay behind the facade.

## Internal architecture

```text
                         Main Chat
                    Planner / Reviewer
                           │
                    orchestrate_work
                           │
                           ▼
                ┌─────────────────────┐
                │ Run Control         │
                │ Coordinator         │
                │                     │
                │ execution rounds    │
                │ Main Review Gate    │
                │ approve/rework/fail │
                └─────────┬───────────┘
                          │ publish round DAG
                          ▼
                 ┌──────────────────┐
                 │ Durable Broker   │
                 │ Task DAG         │
                 │ Slot ownership   │
                 └────────┬─────────┘
                          │ READY projection
                          ▼
                 ┌──────────────────┐
                 │ Consumer Group   │
                 │ Coordinator      │
                 │                  │
                 │ membership       │
                 │ generation       │
                 │ lease recovery   │
                 │ coop rebalance   │
                 └────────┬─────────┘
                          │ opaque wake state only
                          ▼
                 Worker Pool Runtime
              ACTIVE / DORMANT / OFFLINE
                          │
                          ▼
                  Chrome Wake Adapter
             ┌────────────┼────────────┐
             ▼            ▼            ▼
         Worker A     Worker B     Worker C
             │            │            │
             └──── authoritative work() ┘
                          │
                    terminal round
                          │
                          ▼
                 WAITING_MAIN_REVIEW
                          │
                 Main Runtime Adapter
                    opaque wake
                          │
                          ▼
                    resume_main_review
                          │
                     Main review
                    ┌─────┴─────┐
                 approve      rework
                    │            │
                 complete    new round DAG
```

A logical role belongs to the assignment, not to the physical Chat. A task is claimable only after every prerequisite completes. Claiming work creates a temporary run-scoped Worker identity for existing repository/report/Git capability checks.

Each project has one internal Worker Consumer Group. `ACTIVE` and recoverable `DORMANT` Worker targets are members; `OFFLINE` targets are removed. A change to that recoverable member set advances the durable group generation. ACTIVE-to-DORMANT transitions do not create a new generation because the same physical Consumer is still recoverable.

Rebalancing is cooperative rather than stop-the-world. A generation change never steals an in-flight Broker assignment from a healthy BUSY Worker. New or recovered members participate in subsequent READY work. Only the existing assignment lease can revoke a dead BUSY attempt; recovery then re-queues the logical Broker Task so a current group member can claim a fresh temporary execution. Broker Task/Slot state remains the sole source of truth for assignment ownership; Consumer Group state persists only membership and generation.

If a Worker calls `work` again while an assignment is active, ChatToCodex safely resumes it and rotates the temporary Worker capability. If the underlying task is already terminal but `work_done` was skipped, the next `work` releases the old assignment before claiming new work.

Completing a task can unlock DAG successors. `work_done` recalculates READY capacity, reuses or wakes an eligible recoverable target, and records a project-bound provisioning deficit only when existing capacity is insufficient.

### Shared Broker Cluster management

The management model follows Kafka's separation of cluster, broker, namespace-like ownership, and Consumer Group coordination without copying Kafka topic/partition/offset semantics. ChatToCodex has one provider-independent Broker Cluster over the configured execution root. Every local Project is isolated as a Broker namespace, while provider adapters remain outside this layer.

```text
Shared Broker Cluster
├── Controller: broker-0
├── Broker node: broker-0
│
├── Project namespace A
│   ├── Broker Tasks / Worker Slots / Runs
│   └── Consumer Group(s)
│
├── Project namespace B
│   ├── Broker Tasks / Worker Slots / Runs
│   └── Consumer Group(s)
│
└── Group Coordinator Directory
    ├── group-A -> broker-0
    └── group-B -> broker-0
```

The current MVP is an embedded single-node cluster, so `broker-0` combines the Broker and Controller roles. Group Coordinator ownership is nevertheless resolved through a stable group-id hash over eligible Broker nodes. This keeps coordinator ownership independent from ChatGPT, Chrome, or any future provider runtime and leaves a direct seam for multiple Broker nodes later.

Today's persisted Consumer Group topology is still one default Group per Project. Broker management exposes it as `<project_id>:default`; this is a compatibility identity, not a long-term requirement that Project and Consumer Group remain 1:1. Future multiple Consumer Groups per Project can replace that persistence shape without changing Broker namespace management or provider adapters.

The Runtime Console exposes the cluster, Controller, per-Project namespace queue pressure, reusable Worker Slot pressure, and Group Coordinator ownership. PostgreSQL mirrors the same management view as a read model; the file-backed Broker state remains the execution source of truth.

## Durable state

The current PoC separates global control-plane state from project execution state. OAuth, the Owner Token, and the configured workspace remain under the global ChatToCodex state root (by default `~/.chattocodex`). Runs and all execution artifacts live under the configured working project instead:

```text
~/.chattocodex/
├── config.json
└── auth / owner-control state

<configured-project>/.chattocodex/
├── broker/
│   ├── tasks/<broker_task_id>.json
│   └── slots/<slot_id>.json
├── consumer_groups/
│   └── <project_id>.json
├── run_control/
│   └── records/<workflow_id>.json
├── main_runtime/
│   ├── review_targets/<main_target_id>.json
│   ├── review_delivery_leases/<main_target_id>.json
│   └── chat_presence/<main_target_id>.json
├── worker_runtime/
│   ├── presence/<wake_target_id>.json
│   └── wake_delivery_leases/<wake_target_id>.json
├── wake/
│   └── targets/<wake_target_id>.json
├── project_bindings/
│   └── <project_id>.json
├── worker_provisioning/
│   └── state.json
├── worktrees/<run_id>/<worker_id>/
├── integrations/
└── runs/<run_id>/
    ├── orchestration.json
    ├── workers/<worker_id>.json
    └── tasks/<task_id>.json
```

ChatToCodex adds `/.chattocodex/` to the project's local `.git/info/exclude` before Git cleanliness checks. The execution directory therefore stays physically beside the project while remaining outside source diffs, Worker commits, and integration output. The project's tracked `.gitignore` is not modified. Legacy execution records that were created under the global `~/.chattocodex` root are not automatically deleted; after the Control Panel/MCP runtime is restarted on this layout, new scheduling uses only the configured project's execution root.

Plaintext mutation capabilities are returned only to the relevant Chat; only SHA-256 digests are persisted. Task mutations use compare-and-swap revisions and in-process coordination locks. State writes use atomic replace and read-back validation.

`integration_apply` remains Broker-gated for simple delegation. If a Run belongs to a coordinated workflow, integration is additionally hard-gated by the Main Review Gate: the workflow must be `COMPLETED` before integration can proceed. Main-review approval is therefore enforced by the server rather than only by prompting instructions.

## Settings Control Panel

Run the local Control Panel:

```bash
uv sync
uv run chattocodex app
```

Keep the shared development `.venv` in editable mode. Do not replace
`site-packages/chattocodex` with a symlink to this repository or alternate that
environment with `uv run --no-editable`; uninstalling a non-editable package through
such a symlink can remove source files. The Make targets fail closed when that unsafe
layout is detected. Deployment-style non-editable checks belong in an isolated
environment.

The loopback-only page (default `http://127.0.0.1:8790`) configures the workspace, Quick Tunnel or custom Cloudflare hostname, local Owner Token, MCP runtime, and ChatGPT Connector URL.

The configured workspace is also the execution-state boundary. Changing the workspace requires restarting the Control Panel/MCP runtime so both processes bind to the same `<workspace>/.chattocodex` execution root.

Cross-process lock identity is stored in a user-local runtime namespace outside that
rotatable execution root. When upgrading from a version that stored locks under
`.chattocodex/locks` or `auth/locks`, stop every older Control Panel and MCP process
before starting the new version; mixed old/new processes do not share a lock namespace.

The same loopback process exposes the Worker boundary under `/api/wake/*` and the Main review boundary under `/api/main-runtime/review-targets/*`. The Worker boundary includes an opaque per-target runtime-heartbeat endpoint so the extension can advertise recoverability without pretending the Chat tab itself is open. These are local Chrome Runtime Adapter boundaries and are not added to the public MCP catalog. Neither loopback response carries Workflow or Run identity.

Create the ChatGPT Custom MCP App in Developer Mode with the Connector URL shown by the Control Panel, complete OAuth, and enter the Owner Token only on the local ChatToCodex approval page.

### PostgreSQL operations read model

The Broker, Consumer Group coordinator, wake leases, and Run Control state remain file-backed execution authorities. PostgreSQL is deliberately a **management read model**, not a second assignment authority. The Runtime Console projects the current durable file snapshot into PostgreSQL and reads that projection back for the UI. If PostgreSQL or its Python driver is unavailable, Worker execution continues from the file-backed state and the Runtime Console reports the database as degraded instead of changing assignment semantics.

The local development database is defined in `compose.yaml` and binds only to loopback. Copy `.env.example` if you want to override the development database name, credentials, or port, then start PostgreSQL:

```bash
make db-config
make db-up
```

The host process that runs `chattocodex app` must receive `CHATTOCODEX_DATABASE_URL`. The example value is:

```text
postgresql://chattocodex:chattocodex-local@127.0.0.1:55432/chattocodex
```

PostgreSQL access is loaded lazily through `psycopg`; a normal file-backed runtime does not import the driver. The first successful database projection applies the packaged SQL migrations in `chattocodex/database/migrations/`, then refreshes normalized Project, Consumer Group, Worker, and Run tables plus a validated JSONB operations snapshot. Database credentials are never returned by `/api/operations`.

The Worker Browser remains a host-side process because it owns a local persistent Chrome profile and loopback-only Runtime Adapter. `compose.yaml` containerizes PostgreSQL only; it does not move Chrome, the local workspace, or the MCP trust boundary into a container.

Stop the local database with:

```bash
make db-down
```

## Current Wake MVP boundary

- Chrome itself and the Runtime Wake extension must remain running; the adapter does not wake a powered-off/sleeping machine.
- The popup registers only the Main reviewer from an explicit GPT Project conversation and establishes the one-time project binding. Worker conversations are created and registered only through backend-admitted project-bound provisioning.
- Registered Chat tabs may be closed. The extension can reopen dormant provisioned Worker/Main conversations as inactive background tabs when their opaque target becomes `WAKING`.
- Chat registration and tab matching use the ChatGPT conversation ID rather than the complete URL path, so `/g/.../c/<id>` and `/c/<id>` cannot become duplicate runtime registrations for the same conversation. Legacy URL-keyed registrations are collapsed to one conversation-ID record when the extension reads its local registry.
- A Runtime sweep has a bounded new-tab budget and an in-flight per-conversation open lock. Adding one automatically provisioned Worker does not trigger a full Worker-pool reopen sweep.
- Automatic close applies only to tabs created by the Runtime Adapter. User-opened, activated, or pinned tabs are never automatically closed.
- The extension never overwrites a non-empty ChatGPT composer and waits while ChatGPT is generating a response. Runtime-owned tabs are parked only after the target is `IDLE`, an idle grace period passes, and the content script reports that generation has ended and the composer is empty.
- Fresh wake delivery uses a durable revisioned lease. An unclaimed WAKING delivery expires after 90 seconds, then uses bounded exponential cooldown before retry/failover.
- Main review wake delivery uses the same failover principle with a Main-specific durable lease and `main_wake_revision`; a dead WAKING Main target cannot hold `WAITING_MAIN_REVIEW` forever.
- An ACTIVE Main review session also has physical Chat liveness recovery: the same Main Chat is reopened first and receives `검수 계속해`; after 120 seconds without actual Chat heartbeat, its ACTIVE claim is released so another Main reviewer can resume the Workflow with a newly rotated review capability.
- A missing BUSY Worker is reopened first to resume the same Chat context; after the assignment recovery window, coordinator-only recovery revokes the dead temporary assignment and re-queues the logical Broker Task.
- Consumer Group generation and membership never cross the browser or public MCP boundary. The browser carries only opaque physical routing identity plus the wake delivery revision; assignment semantics and rebalance state stay server-side.
- ChatGPT DOM selectors and app-mention behavior are browser-UI integration points and must be dogfooded after ChatGPT UI changes.

## Verification

```bash
make ci
```

The default environment remains the compatibility-first GIL-enabled CPython build.
An additional opt-in gate verifies the same suite on uv's isolated CPython 3.14
free-threaded build and fails if importing the native runtime dependencies silently
re-enables the GIL:

```bash
uv python install 3.14t
make test_free_threaded
```

ChatToCodex does not rely on the GIL for correctness. Shared durable transitions use
entity-scoped filesystem locks and compare-and-swap revisions, while unrelated
projects, Workers, Tasks, and I/O-bound service calls remain independently runnable.
The free-threaded gate is intentionally separate because CPython's standard build is
still the deployment default and third-party extension compatibility can change.

The suite covers the high-level delegation facade, logical DAG validation, project-local execution-state placement, local Git exclusion of `.chattocodex`, nested isolated Worker worktrees, conversation-ID browser deduplication, bounded runtime tab reopening, Consumer Group membership/generation, cooperative rebalance with sticky BUSY assignments, dead-member assignment recovery/requeue, wake-target state transitions, READY-capacity wake scheduling, revisioned wake-delivery failover/backoff, stale-revision rejection, DAG successor re-wake, opaque browser-boundary enforcement, automatic Worker registration, safe assignment resume, Slot reuse across changing roles, forgotten-release recovery, capability isolation, CAS concurrency, reports/results, Broker-gated integration, multi-session Streamable HTTP execution, restart recovery, and OAuth remote MCP access/refresh.

`make ci` verifies deterministic backend and browser-harness contracts; it does not prove the current live ChatGPT DOM, GPT Project navigation, app-mention behavior, or provider rate-limit UI. Do not report live Chrome E2E success until the procedure and evidence criteria in [`docs/actual-chatgpt-multichat-dogfood.md`](docs/actual-chatgpt-multichat-dogfood.md) pass against the real ChatGPT UI.
