# Permanent Home transfer Source dispatch

Operations schema 68 records the immutable Source request, its first-send
attempt and the original Forge result for an existing permanent-transfer
preparation. These operations do not change Home membership, consume the
destination Team hold, release a prepared fence or activate a new work scope.
`Prepared` proves Source capture and fencing, not work settlement.

## Explicit host configuration

`HETERARCHY_ORCHESTRATION_FORGE_SOURCE_CONFIG` names an absolute JSON file.
Its existing `address`, absolute `bearer_file` and trusted `scopes` select the
Forge HTTP authority. To enable Home dispatch and remote reconciliation, add a
`home_transfer` object with all four fields:

| Field | Required value |
| --- | --- |
| `backend_id` | The configured Forge backend's non-nil UUID |
| `destination_id` | The exact delegated Operations destination UUID |
| `issuer_id` | The configured Home Source service issuer's non-nil UUID |
| `grant_validity_ms` | An explicitly configured positive integer duration |

The installation must supply the real service identities and grant duration.
There is no default duration, credential-derived identity or fallback from the
optional `effect_facts` object. Unknown fields and invalid values reject the
configuration. The existing trusted Workspace/Location scope checks still
apply. Configuration alone does not establish a Source delegation.

The first attempt pins the address, service identities, full initial scope,
canonical request and exact transport body identity. Remote recovery requires
the same address, service and Location/locator; its trusted outer Workspace
revision may advance but cannot go backwards. A changed pin never retargets the
original request. Local historical readback remains available without a
working remote Source configuration, subject to its authenticated read ACL.

## Commands and recovery

The bearer-authenticated prefix is
`/internal/v1/workspaces/{workspace_id}/teams/permanent-transfers/source`.
The Rust SDK exposes the corresponding typed request and response models.

| HTTP command | SDK method | Effect |
| --- | --- | --- |
| `POST /dispatch` | `dispatch_team_permanent_transfer_source` | Authorize and commit one immutable attempt, then perform its first Source Prepare outside the catalog transaction |
| `POST /readback` | `read_team_permanent_transfer_source` | Read local attempt and any stored original Source receipt |
| `POST /reconcile` | `reconcile_team_permanent_transfer_source` | Read the existing attempt, query its exact Source Get and persist a matching original receipt |

First dispatch requires current Orchestrator authority and the current transfer,
consent and preparation conditions. Its body must fit the Source request bound,
and transport capacity is reserved before the attempt commits. Only the newly
confirmed commit returns a one-shot send permit. A historical record or a
replayed attempt does not convey that permit.

After a lost response or uncertain commit, retain the original request and use
`recover_team_permanent_transfer_source`: it reads local history first and can
explicitly reconcile through Source Get. Recovery never sends Prepare. An absent
local attempt or Source result is not proof of cancellation or permission to
release a slot. A recorded attempt can remain pending even if a crash occurred
before its first network send. There is no automatic resend or grant refresh.

An authenticated Source original is checked against the exact stored request,
service and envelope before immutable ingestion. Its original grant window is
preserved, even when it differs from the local attempt's window. Receipt
ingestion does not require the old business leader to remain in office and
does not authorize a later membership change. Later settlement and cutover must
revalidate their own current authority.

## Verification boundary

The isolated Broker/Forge/Orchestration HTTP test drops the Forge Prepare and
Orchestration dispatch replies in the Source-dispatch subflow; its earlier
preparation subflow also drops a reply. It checks local readback, one preflight
Get and one recovery Get, unchanged Team rosters/capacity configuration and
Workspace population, and one Prepare without redispatch. Pending hold
preservation is supported by the owner implementation; this HTTP test does not
directly re-read the pending hold after dispatch. It uses one synthetic
principal across logical roles. It does not
prove independent provider actions, power-loss durability, Source settlement,
membership cutover or new-scope activation. Full D1–D5 acceptance remains open.
