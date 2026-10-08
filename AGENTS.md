# AGENTS.md — heterarchy-orchestration

This file is the thin repository entrypoint for coding agents, including GPT-6/Astra-class agents. It routes to repository-local authority; it does not duplicate the full Engineering Harness.

## Read order before changing code

1. `AGENTS.md`
2. `.agents/rust_dev_harness/PROJECT_PROFILE.md` — semantic project authority
3. `.agents/rust_dev_harness/HARNESS.toml` — machine-verifiable projection
4. `.agents/rust_dev_harness/HARNESS_MANIFEST.toml` — Harness identity/provenance
5. `.agents/rust_dev_harness/rules/README.md` and the active rule families
6. `.agents/rust_dev_harness/skills/rust-engineering/SKILL.md`
7. Actual `Cargo.toml`, `rust-toolchain.toml`, source, tests, `xtask`, and CI

## Rust backend profile and framework routing

The current target is the Rust organization/operations backend. Read `.agents/rust_dev_harness/BACKEND_PROFILE.md` together with `PROJECT_PROFILE.md`; it defines the adopted scope without claiming product migration is complete.

For HTTP work, load `.agents/rust_dev_harness/profiles/axum-backend/README.md` and `.agents/rust_dev_harness/skills/axum-backend/SKILL.md`. For long-lived streams, additionally load `.agents/rust_dev_harness/profiles/realtime-backend/README.md` and `.agents/rust_dev_harness/skills/realtime-backend/SKILL.md`. The framework-specific rules supplement normal/type/async, not replace them.

The product target is a GUI-free Rust Backend with HTTP command/query and SSE. The user removed the TypeScript harness, desktop host, MCP crate and OAuth implementation; do not restore them. Their deletion does not prove business API parity. Remaining browser integration assets are consumer material, not a Backend authority or a verified provider worker. Top-level `.agents/skills` design assets are retained for archival, are not backend engineering authority, and must not be deleted before complete archival verification and explicit removal authorization.

If prose and executable repository policy disagree, treat the mismatch as drift. Inspect the concrete repository state and repair the authority chain; do not guess.

## Canonical product boundary

**Orchestration owns when/where/how-many work may run and whether it is admitted by global policy.** Its domain includes bounded admission, queueing, scheduling, priority/fairness, resource budgets, retry/defer policy, approval intent, runtime/worker allocation, and public operational projection.

**Forge owns repository-work truth and effects.** Forge remains authority for Location/Workspace, Goal/DAG truth, node lifecycle, Writer Lease, source/worktree mutation, command execution, ChangeSet/Diff, verification, integration, and evidence/provenance.

**Broker owns distributed claim/lease/fencing authority where the Broker contract applies.**

Never create a second Orchestration DAG source of truth. Orchestration stores opaque Forge references and scheduling state, then Forge revalidates exact authority before effects.

## GPT-6/Astra execution discipline

- Read the exact current source and active Goal/DAG before proposing edits.
- Preserve unrelated dirty work. Never use reset/clean/stash/checkout--/broad revert as a shortcut.
- Prefer the smallest coherent change that fits an existing correct owner; do not invent manager/service/trait/config layers without a demonstrated invariant or lifecycle owner.
- Make ownership, boundedness, cancellation, failure semantics, and evidence explicit. Avoid hidden process-global state.
- Do not preserve Python/PyO3/PostgreSQL or other removed architecture through compatibility shims.
- Do not claim success from intent, logs alone, or manifest presence. PASS requires executed verification evidence.
- Keep identifiers/revisions/fences exact. After an unknown outcome, read durable state before retrying a mutation.
- For async/concurrent work, use bounded admission and queues, structured task ownership, explicit cancellation, and no blocking/lock-held-across-await paths.

## Source mutation authority

Repository mutations normally use Forge with exact Workspace, Activity, Agent, Goal/DAG/node, and Writer Lease attribution. Never steal a live Writer Lease. Recover only exact-revision expired leases through the supported recovery lifecycle. For this explicitly user-authorized harness/documentation task, ChatGPTtoCodex may perform exact project-scoped edits while the Forge bridge is unavailable. This exception grants no runtime authority and is not a Forge ChangeSet or execution verification.

## Required verification order

Use focused tests first, then the canonical repository gates when available:

```text
make rules
make fmt
make check
make test
make ci
```

The Makefile delegates to the repository `xtask` crate with `cargo run --quiet --locked -p xtask -- <command>`; no Cargo alias or lockfile mutation is required.

If a gate does not yet exist, do not fabricate a PASS. Record the missing verifier and use the strongest real Cargo checks available until the canonical xtask surface is completed.
