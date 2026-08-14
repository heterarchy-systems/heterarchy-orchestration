# Actual ChatGPT Multi-Chat Dogfood

## Status and purpose

This document is a live Chrome execution procedure, not a record of a completed run. A green repository test suite verifies deterministic contracts but does not prove the current ChatGPT DOM, GPT Project navigation, app-mention behavior, or provider rate-limit UI.

Use this procedure to prove that one Main Chat request can create project-bound Worker capacity from `Worker=0`, execute N=1 and N=2 work without manual Worker setup, and reuse the same physical conversations on a second run.

## Product invariants

- Register only the Main Chat from the extension popup.
- Bind one canonical local project to one observed GPT Project during that Main registration.
- Start the provisioning proof with zero recoverable Workers.
- Apply `Reuse > Wake > Spawn` whenever READY work changes capacity demand.
- Keep provider-facing Worker creation globally sequential.
- Keep Task, role, Run, DAG, repository, and capability data out of browser provisioning and wake payloads.
- Assign roles dynamically through `work`; never bind a permanent role to a physical Worker Chat.

## 1. Start ChatToCodex

From the repository root:

```bash
uv sync
uv run chattocodex app
```

Start the MCP runtime or tunnel from the loopback Control Panel, then refresh the ChatGPT Custom MCP App so it loads the current public catalog.

Low-level Broker, Wake, project-binding, and Worker-provisioning actions must not appear in the public MCP catalog. They are loopback/runtime boundaries, not tools for the Main or Worker Chat.

## 2. Load the Runtime Wake extension

In Chrome, open `chrome://extensions`, enable Developer mode, choose **Load unpacked**, and select:

```text
chattocodex/wake/chrome_extension
```

Keep Chrome and the extension running throughout the test. The adapter can reopen an inactive ChatGPT tab; it cannot wake a powered-off or sleeping machine.

## 3. Register Main and bind the GPT Project once

1. Open the intended Main conversation from inside the target GPT Project.
2. Confirm that the URL has the exact `https://chatgpt.com/g/<project-ref>/c/<conversation-id>` shape. A plain `/c/<conversation-id>` page is not sufficient evidence of project identity.
3. Click **ChatToCodex Runtime Wake**.
4. Choose the canonical local project.
5. Click **이 Chat을 Main reviewer로 등록**.

This operation must complete the project binding before it registers the opaque Main review target. The server remains authoritative for local `project_id` and `project_root`; the browser contributes only observed provider metadata such as the GPT Project reference/path.

Confirm these conditions before continuing:

- repeating the same Main registration is idempotent;
- the same GPT Project observation cannot bind to a different local project;
- no Worker Chat was created or registered by the user;
- the selected project has zero recoverable Worker consumers.

## 4. N=1 automatic provisioning from Worker=0

In the registered Main Chat, send one natural request:

```text
@<등록할 때 선택한 앱 이름>
이 프로젝트를 한 명에게 맡겨서 README 첫 줄을 확인하게 해줘.
```

Do not create a Worker Chat, open the extension popup in another Chat, select a Worker project, copy a routing ID, or type `작업해` yourself.

Expected sequence:

1. Main creates the Run and logical Task DAG.
2. One Task becomes READY.
3. Capacity reconciliation observes `recoverable=0` and `deficit=1`.
4. Backend admission selects the already-bound GPT Project.
5. The adapter opens that GPT Project in an inactive tab and requests one new project-scoped conversation.
6. The adapter bootstraps the conversation and obtains its persistent conversation ID.
7. The callback registers the server-issued opaque Worker target and confirms Consumer Group membership.
8. The provisioning request becomes `ACTIVE` and its target is present in the project's recoverable membership.
9. The normal wake path selects the app observed during Main registration and sends the narrow work-retrieval authorization wording plus `wake_target_id` and `wake_revision`; the label is not hard-coded, no Task payload is exposed to the browser, the wording never attests approval for destructive or external effects, and the backend rejects stale or fabricated wake values.
10. Worker calls `work`, receives the authoritative assignment, completes it, calls `work_done`, and returns to `IDLE` or an automatically parked state.

Pass only if exactly one persistent Worker conversation is created and no user action occurs between the Main request and the Worker claim.

## 5. N=2 strictly sequential provisioning

Use a separate freshly bound test project with zero recoverable Workers, or otherwise establish `Worker=0` without deleting or altering unrelated production state. In its registered Main Chat, send:

```text
@<등록할 때 선택한 앱 이름>
이 프로젝트를 두 명이 동시에 검토해줘.
한 명은 보안, 한 명은 데이터 정합성 관점으로 봐줘.
```

Expected sequence:

1. Two independent Tasks become READY and desired capacity becomes two.
2. Worker A is admitted and created first.
3. ChatToCodex obtains A's persistent conversation ID, completes automatic target registration, marks the provisioning request `ACTIVE`, and confirms Consumer Group membership.
4. The global provider-facing admission gate remains single-flight.
5. Worker B is not admitted until the minimum interval has elapsed. The default is 12 seconds, within the initial 10–15 second safety range.
6. Worker B then follows the same create, bootstrap, registration, membership, wake, and claim sequence.
7. A and B claim different logical Tasks; neither can mutate the other's assignment.

Fail the run if two browser opens/spawns overlap or if the implementation uses a parallel creation fan-out such as `Promise.all`.

### Provider rate-limit branch

If ChatGPT reports a provider rate limit during provisioning:

1. stop additional provisioning immediately;
2. preserve the unmet durable capacity demand;
3. verify that the configured retry delay advances through `30s → 60s → 120s`;
4. verify that no second provider-facing request starts during the active cooldown;
5. record the provider signal and timestamps without repeatedly provoking the limit.

Do not treat a real provider rate limit as required for the normal N=2 pass. The deterministic harness covers this branch without aggressively retrying the live provider.

## 6. Second-run reuse with zero new Chats

After Workers A and B finish the first run, wait until both are recoverable (`IDLE` or parked/dormant). Record their persistent conversation IDs. Then ask the same Main Chat for two different roles:

```text
@<등록할 때 선택한 앱 이름>
같은 프로젝트를 두 명이 다시 검토해줘.
이번에는 한 명은 코드 구조, 한 명은 테스트 관점으로 봐줘.
```

Expected result:

```text
Run 1
  Worker A = security reviewer
  Worker B = data-integrity reviewer

Run 2
  Worker A = code-architecture reviewer
  Worker B = test reviewer

Run 2 new Chat count = 0
```

The existing A/B conversations must wake and receive their new roles only from the assignments returned by `work`. A role shown in an earlier conversation must not become scheduling authority.

## 7. DAG successor reuse

With A and B already provisioned, send:

```text
@<등록할 때 선택한 앱 이름>
보안과 백엔드 관점으로 각각 검토하고,
두 검토가 모두 끝난 뒤 마지막 작업이 결과를 종합하게 해줘.
```

Expected behavior:

1. Only the two prerequisite review Tasks are READY initially.
2. Existing A and B wake; no new Worker Chat is created.
3. Synthesis remains queued until both prerequisites are terminal.
4. `work_done` recalculates READY capacity after the second prerequisite finishes.
5. One now-recoverable physical Worker wakes again, receives the synthesis role from `work`, and completes it.

This proves that DAG semantics stay in the backend and that a physical Chat remains a generic reusable slot.

## 8. Browser and repository safety checks

Verify these conditions throughout the run:

- A runtime sweep does not open all registered conversations merely because registrations exist.
- At most one new runtime/provisioning tab is in flight globally.
- Automatic wake never overwrites a human draft and waits while ChatGPT is generating.
- A user-activated, pinned, or otherwise claimed tab is not automatically removed or parked.
- Browser-visible wake text contains no role, objective, Run ID, dependency, repository path, or capability.
- A stale wake or provisioning callback cannot duplicate a claim or registration.
- Separate concurrent Workers receive isolated worktrees.
- The original project working tree does not change before explicit integration.

If app-mention selection fails after a ChatGPT UI change, record the exact DOM/UI behavior and stop expanding browser responsibilities. Repair only the mention/wake adapter.

## 9. Evidence to capture

Record evidence without secrets, capabilities, OAuth codes, or full Chat URLs:

| Gate | Required evidence |
| --- | --- |
| Binding | Local `project_id`, redacted GPT Project reference, binding revision, and idempotent repeat result |
| N=1 | Initial recoverable count 0, one READY Task, one admitted request, one `ACTIVE` provisioning result, membership confirmation, and one terminal Worker result |
| N=2 | Initial recoverable count 0, two distinct request/target/conversation identifiers, A/B admission timestamps at least the configured minimum interval apart, and no overlapping provider open |
| Rate limit | Redacted provider signal plus 30/60/120-second cooldown state; use harness evidence if live provider does not rate-limit |
| Reuse | A/B identifiers before and after run 2, two new assignments with different roles, and `new Chat count = 0` |
| Safety | Composer preservation, single-flight browser admission, opaque wake payload, isolated worktrees, and unchanged original repository before integration |

## Success criteria

Live Chrome E2E passes only when all of these hold:

1. One Main popup registration establishes the GPT Project binding and Main review target.
2. N=1 starts from `Worker=0` and creates exactly one project-bound Worker without user intervention.
3. N=2 starts from `Worker=0`, creates exactly two Workers sequentially, and respects the configured 10–15 second admission interval.
4. Provider rate-limit handling is single-flight and observes bounded 30/60/120-second backoff without an aggressive retry burst.
5. Workers claim different READY Tasks and cannot mutate each other's assignments.
6. The second two-Worker run creates zero new Chats and reuses the same physical A/B conversations with new dynamic roles.
7. Browser payloads remain opaque and contain no assignment semantics.
8. DAG dependencies prevent premature downstream claims and completion wakes an eligible existing Worker.
9. `work_done` returns physical targets to a recoverable state.
10. Main Review resume, OAuth reconnect/refresh, and existing Git integration guards remain intact.

## Stop conditions

Stop and record the exact failure if:

- Main registration succeeds from a plain `/c/<conversation-id>` route;
- a user must create or register a Worker, select its project, copy an ID, or type `작업해`;
- provisioning opens a Chat outside the bound GPT Project;
- more than one provider-facing open/spawn is in flight;
- a provider rate limit triggers an immediate or parallel retry burst;
- run 2 creates a new Chat while A/B are recoverable;
- a browser payload contains Task, role, Run, DAG, repository, or capability information;
- the extension overwrites a user draft or removes a user-claimed tab;
- two Chats claim the same logical Task;
- a dependent Task wakes before its prerequisites complete;
- original repository content changes before explicit integration.

Until the full procedure passes against the live ChatGPT UI with the required evidence, report the live Chrome E2E status as **not verified** rather than inferring success from `make ci`.
