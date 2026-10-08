# Heterarchy Orchestration

Orchestration owns organization state and global admission: which work may run,
where, and within what resource and approval policy. Forge owns repository work
and its evidence, including Workspace, Activity, Goal/DAG, source changes,
commands, leases, and verification. Broker owns distributed claims and fencing
where its contract applies.

## Product direction

The selected product target is a headless Rust backend with no GUI and no MCP
server. The migration is in progress. The workspace still contains transition
code and historical documentation; a crate, route, or package declaration does
not prove that the corresponding capability has a working external Source, a
target consumer, or accepted behavior.

The former desktop tree was removed after a caller inventory found that its
commands delegated into existing native owners. That retirement does not retire
the business requirements those commands exposed. The current capability and
acceptance status is tracked in
[`docs/repair/2026-10-03-headless-capability-matrix.md`](docs/repair/2026-10-03-headless-capability-matrix.md).

## Backend API source inventory

The Rust backend declares `GET /health` and `GET /ready`. Its `/internal/v1/*`
routes are behind bearer authentication. The current source includes profile
and logical-session operations; provisioning and Agent-request workflows;
membership, presence, inbox, handoff, and departure; teams and leadership;
approval; work-credit; Node-Team and Node-Chat; and global and Workspace event
streams. Selected Forge read operations are exposed through
`/internal/v1/forge/*` and `/internal/v1/workspaces/{workspace_id}/forge/*`.
The operator-only operations import lifecycle is exposed through
`/internal/v1/operations/imports/*`; each page or verification call carries a
full durable progress fence and performs one bounded Forge observation.

This is a route inventory only. The repair matrix records which requirements
still need authoritative Forge or Broker behavior, a real consumer, and
executed acceptance. No capability is considered complete because a handler,
client type, or test fixture exists in source.

## Admission issuer startup

The backend can persist or verify one explicit Forge admission issuer binding
before it serves readiness. Configure all three variables together:

```text
HETERARCHY_ORCHESTRATION_ADMISSION_ISSUER_ID
HETERARCHY_ORCHESTRATION_ADMISSION_BACKEND_ID
HETERARCHY_ORCHESTRATION_ADMISSION_TTL_MS
```

The IDs must be canonical non-nil UUIDs, and the TTL must be between 1,000 and
3,600,000 milliseconds. The values come from the Forge host configuration;
they are never derived from bearer credentials or imported Workspace data. When
the group is absent, existing consumers retain the previous startup behavior.
When configured, a conflicting durable binding fails startup before the HTTP
listener is opened; an exact restart reuses the stored binding and timestamp.

The same Backend executable also provides `session-authority` for the
existing authenticated read-only logical Session resolver used by Forge.
See the operator guide for the distinct issuer secret and exact existing
catalog; this lookup does not grant admission or coordinator authority.

## Operating and migration references

- [Headless capability and acceptance matrix](docs/repair/2026-10-03-headless-capability-matrix.md) — current route families, retained requirements, and external gaps.
- [MCP replacement matrix](docs/mcp-backend-replacement-matrix.md) — historical business-operation inventory; its status claims predate this repair and do not establish completion.
- [Headless migration progress](docs/glm-headless-backend-progress.md) — dated implementation and verification record, including blocked or unverified gates.
- [macOS packaging and operator notes](packaging/macos/README.md) — package workflow and the in-progress token-issue operator CLI.

The target remains headless and MCP-free. Do not remove a missing business
operation from the requirement set simply because its former GUI or MCP route
has been retired. Completion requires an owner-preserving headless API path and
executed evidence for its Source, consumer, authorization, recovery, and runtime
behavior.

## Development checks

Read `AGENTS.md` and the repository Rust harness before implementation. Use the
canonical gates when they are available:

```sh
make rules
make fmt
make check
make test
make ci
```

Report only gates that ran successfully. Keep Forge repository effects and
evidence under Forge authority; Orchestration stores opaque Forge references
and scheduling state rather than a second Goal/DAG.
