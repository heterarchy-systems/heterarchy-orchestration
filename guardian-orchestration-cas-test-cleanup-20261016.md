# Operational-message CAS test cleanup plan

## Scope and target

Clean only the tests in `crates/heterarchy-orchestration-operations/src/operational_messages.rs` around the queued-cancel/dispatch cases at lines 5065–5504. Preserve all assertions and behavior. Do not edit production operations, backend/provider-worker tests, NativeMembership tests, database/schema/wire contracts, dependencies, or runtime behavior.

## Changes

- Replace test-only `expect`, timestamp `unwrap`, thread-join `unwrap`, and unexpected-outcome `panic!` with contextual error propagation.
- Split `concurrent_cas_cancel_vs_dispatch_two_connections` into separate two-connection cancellation-wins, dispatch-wins, and true concurrent-race tests. Give each scenario its own fixture and preserve its complete state/revision/timestamp/readback assertions.
- Keep the existing unit-test module and fixture ownership; add no generic test framework or lint suppression.

## Verification

Run the affected cancellation/CAS tests, then the operations crate all-target Clippy and scoped formatting through the parent-approved orchestration Rust SSD launcher. Main owns canonical full-CI acceptance; no production/runtime or deployment claim is part of this tranche.

## Result

The first two-connection races are now separate tests for cancel-wins and dispatch-wins; the concurrent CAS race remains a third test with its original state/revision/timestamp assertions. All seven test-only `expect` calls were replaced with contextual `io::Error` propagation, and the concurrent test now uses `?` for time construction/join observation plus a typed `Err` for impossible outcomes. No production function or contract changed.

Executed evidence: `cargo test -p heterarchy-orchestration-operations cancel --locked` passed 9 matching tests; `cargo clippy -p heterarchy-orchestration-operations --all-targets --locked -- -D warnings` passed; `cargo fmt -p heterarchy-orchestration-operations -- --check` passed. The captured full `make ci` log still contains backend/provider-worker Clippy findings owned by another lane; this tranche does not claim full CI acceptance.
