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

If prose and executable repository policy disagree, treat the mismatch as drift. Inspect the concrete repository state and repair the authority chain; do not guess.

## Canonical product boundary

**Orchestration owns when/where/how-many work may run and whether it is admitted by global policy.** Its domain includes bounded admission, queueing, scheduling, priority/fairness, resource budgets, retry/defer policy, approval intent, runtime/worker allocation, and GUI-facing global projection.

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

Repository mutations are performed through Forge with exact Workspace, Activity, Agent, Goal/DAG/node, and Writer Lease attribution. Never steal a live Writer Lease. Recover only exact-revision expired leases through the supported recovery lifecycle.

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
