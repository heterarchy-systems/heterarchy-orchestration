# Private Rust CI inputs

Both repositories' CI workflows run their canonical Rust gates only after restoring the private Rust Harness from a configured Git commit. Forge Release uses the same provisioning contract before its CI and package gates. The Harness stays ignored under `.agents/rust_dev_harness` and is removed by an `always()` cleanup step. The workflows do not cache or upload the private checkout or restored Harness.

Configure these GitHub Actions values for Forge and Orchestration, preferably at the organization level when both repositories share access:

| Name | Kind | Value |
| --- | --- | --- |
| `HETERARCHY_HARNESS_REPOSITORY` | Variable | Private repository in `owner/repository` form. |
| `HETERARCHY_HARNESS_REVISION` | Variable | Full immutable 40-character commit SHA. |
| `HETERARCHY_HARNESS_READ_TOKEN` | Secret | Read-only token with Contents: Read access to the Harness repository. |

Store `HETERARCHY_HARNESS_READ_TOKEN` only as a secret in the protected GitHub Environment named `private-rust-ci`. Do not create a repository-level or organization-level secret with this name. Require environment reviewers and restrict deployments to protected `main` for CI plus reviewed version tags for Forge Release. The Orchestration workflow uses the same environment for `main` CI. Configure these protections outside the workflow; this change does not create or modify GitHub settings.

The pinned tree must contain both project exports:

```text
projects/forge/rust_dev_harness/
projects/orchestration/rust_dev_harness/
```

Orchestration CI also needs immutable source revisions for its path dependencies. It checks out `heterarchy-systems/heterarchy-forge` and `heterarchy-systems/heterarchy-broker` beside the Orchestration checkout, then verifies their actual commits and clean source trees before running Cargo.

| Name | Kind | Value |
| --- | --- | --- |
| `HETERARCHY_FORGE_REVISION` | Variable | Full immutable Forge commit SHA. |
| `HETERARCHY_BROKER_REVISION` | Variable | Full immutable Broker commit SHA. |

The same read-only secret is used when any of these repositories are private. Grant it Contents: Read access only to the repositories the workflows must fetch. Checkout credentials are not persisted. Submodules and Git LFS are disabled.

Missing or malformed configuration fails before any Cargo command. Untrusted pull request refs must not receive the environment secret or private Harness; they may be blocked by environment protection or fail on the missing token, and cannot claim full CI. The workflows do not use `pull_request_target`, skip the private gate, or select a moving branch.

The action rejects an existing staging path, a mismatched commit, modified tracked files, untracked or ignored Harness files, symlinks, special files, more than 4,096 Harness entries, and Harness content larger than 128 MiB by logical file size. The private checkout's allocated on-disk size is capped at 128 MiB. Failed or partial setup is cleaned only after the preflight has claimed empty CI-owned paths.

This helper is for isolated GitHub-hosted CI workspaces. Do not use it to stage or clean a shared developer checkout.

GitHub runners place Cargo targets and temporary files under their `RUNNER_TEMP` directory, overriding Forge's local SSD target path. Local Rust work still uses the workspace's SSD launcher.

Run the synthetic provisioning contract tests locally through the workspace SSD launcher:

```bash
../.codex/rust-ssd forge bash .github/scripts/test-private-rust-harness.sh forge
```

In Orchestration, use:

```bash
../.codex/rust-ssd orchestration bash .github/scripts/test-private-rust-harness.sh orchestration
```

These checks validate the provisioning boundary. They do not claim that source compilation without the private Harness is supported or prove live Broker-cluster or product end-to-end readiness.
