//! Operator CLI subcommands for the Orchestration Autonomy Loop surface.
//!
//! Follows the `parse` + `execute` contract of [`crate::token_cli`]: raw bearer
//! tokens are never accepted on `argv`, `--bearer-file` is read with bounded
//! secret hygiene, and deterministic pretty-printed JSON is returned for `stdout`.

mod parse_subcommands;

use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

use crate::cli_support::{
    CliConnectionOptions, CliPayload, CliSupportError, ParsedSubcommandHead, format_json_output,
    parse_canonical_uuid, parse_subcommand_head, read_bounded_json_file,
};
use crate::wire::autonomy_loop::{
    AssignReviewerRequest, DagSettlementAuthorityCheckRequest, DagSettlementGovernanceQuery,
    ExecutionSnapshotQuery, ExecutionSnapshotQueryResponse, MAX_AUTONOMY_LOOP_BODY_BYTES,
    PrepareExecutionSnapshotRequest, RecordReviewVerdictRequest, VerifyNodeCompletionGateRequest,
    WorkspaceScopeMutationRequest, WorkspaceScopeQueryResponse,
};

/// Usage summary for the `autonomy` CLI subcommand family.
pub const AUTONOMY_CLI_USAGE: &str = "usage: heterarchy-orchestration-backend autonomy <scope-get|scope-mutate|reviewer-assign|reviewer-verdict|reviewer-verify-gate|snapshot-prepare|snapshot-get|dag-settlement-authority|dag-settlement-governance> --bearer-file <path> [--addr <ip:port>] [--request <json-file> | <flags...>]";

/// Payload source alias for `autonomy` CLI subcommands.
pub type AutonomyPayload<T> = CliPayload<T>;

/// Parsed `autonomy` CLI dispatch result.
#[derive(Debug, PartialEq, Eq)]
pub enum AutonomyCliCommand {
    /// Arguments belong to another top-level command or daemon mode.
    NotAutonomyCommand,
    /// Operator requested `autonomy` usage output.
    Help,
    /// Validated `autonomy` subcommand ready for execution.
    Run(Box<AutonomyCliOptions>),
}

/// Typed `autonomy` action paired with its validated payload source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutonomyAction {
    ScopeGet {
        workspace_id: Option<Uuid>,
        request_file: Option<PathBuf>,
    },
    ScopeMutate(AutonomyPayload<WorkspaceScopeMutationRequest>),
    ReviewerAssign {
        workspace_id: Uuid,
        payload: AutonomyPayload<AssignReviewerRequest>,
    },
    ReviewerVerdict {
        workspace_id: Uuid,
        payload: AutonomyPayload<RecordReviewVerdictRequest>,
    },
    ReviewerVerifyGate {
        workspace_id: Uuid,
        payload: AutonomyPayload<VerifyNodeCompletionGateRequest>,
    },
    SnapshotPrepare {
        workspace_id: Uuid,
        payload: AutonomyPayload<PrepareExecutionSnapshotRequest>,
    },
    SnapshotGet {
        workspace_id: Uuid,
        payload: AutonomyPayload<ExecutionSnapshotQuery>,
    },
    DagSettlementAuthority {
        workspace_id: Uuid,
        payload: AutonomyPayload<DagSettlementAuthorityCheckRequest>,
    },
    DagSettlementGovernance {
        workspace_id: Uuid,
        payload: AutonomyPayload<DagSettlementGovernanceQuery>,
    },
}

/// Validated `autonomy` CLI invocation options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutonomyCliOptions {
    connection: CliConnectionOptions,
    action: AutonomyAction,
}

impl AutonomyCliOptions {
    #[must_use]
    pub const fn connection(&self) -> &CliConnectionOptions {
        &self.connection
    }

    #[must_use]
    pub const fn action(&self) -> &AutonomyAction {
        &self.action
    }
}

/// Safe, redacted errors returned while parsing or executing `autonomy` CLI commands.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct AutonomyCliError(#[from] pub CliSupportError);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScopeGetFileSelector {
    #[serde(default)]
    workspace_id: Option<String>,
}

/// Parse `autonomy` subcommands using ambient `HETERARCHY_*` fallbacks.
///
/// # Errors
/// Rejects malformed flags, forbidden raw bearer flags, missing `--bearer-file`,
/// or non-loopback `--addr` values.
pub fn parse(arguments: &[OsString]) -> Result<AutonomyCliCommand, AutonomyCliError> {
    parse_with_env(arguments, |key| env::var_os(key))
}

/// Pure, deterministic parser accepting an explicit environment lookup closure for testing.
///
/// # Errors
/// Returns [`AutonomyCliError`] when arguments or connection options are invalid.
pub fn parse_with_env<F>(
    arguments: &[OsString],
    env_lookup: F,
) -> Result<AutonomyCliCommand, AutonomyCliError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let (subcommand, mut index, mut conn) = match parse_subcommand_head("autonomy", arguments)? {
        ParsedSubcommandHead::NotThisCommand => return Ok(AutonomyCliCommand::NotAutonomyCommand),
        ParsedSubcommandHead::Help => return Ok(AutonomyCliCommand::Help),
        ParsedSubcommandHead::Subcommand {
            name,
            next_index,
            conn,
        } => (name, next_index, conn),
    };
    let action =
        parse_subcommands::dispatch(subcommand, arguments, &mut index, &mut conn, &env_lookup)?;
    let connection = conn.resolve_from_env(&env_lookup)?;
    Ok(AutonomyCliCommand::Run(Box::new(AutonomyCliOptions {
        connection,
        action,
    })))
}

/// Execute a validated `autonomy` CLI command against the loopback `OrchestrationHttpClient`.
///
/// # Errors
/// Returns redacted bearer-file, request-JSON, or bounded HTTP client failures.
pub async fn execute(options: &AutonomyCliOptions) -> Result<String, AutonomyCliError> {
    let (client, bearer) = options.connection.open_client_and_bearer()?;
    let auth = bearer.header_value();
    Ok(execute_action(&client, auth, &options.action).await?)
}

async fn execute_action(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &AutonomyAction,
) -> Result<String, CliSupportError> {
    match action {
        AutonomyAction::ScopeGet {
            workspace_id,
            request_file,
        } => execute_scope_get(client, auth, *workspace_id, request_file.as_ref()).await,
        AutonomyAction::ScopeMutate(payload) => {
            let req = payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?;
            format_json_output(&client.mutate_workspace_scope(auth, &req).await?)
        }
        AutonomyAction::DagSettlementAuthority {
            workspace_id,
            payload,
        } => {
            let req = payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?;
            format_json_output(
                &client
                    .check_dag_settlement_authority(auth, *workspace_id, &req)
                    .await?,
            )
        }
        AutonomyAction::DagSettlementGovernance {
            workspace_id,
            payload,
        } => {
            let query = payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?;
            format_json_output(
                &client
                    .get_dag_settlement_governance(auth, *workspace_id, &query)
                    .await?,
            )
        }
        other => execute_reviewer_or_snapshot(client, auth, other).await,
    }
}

async fn execute_scope_get(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    workspace_id: Option<Uuid>,
    request_file: Option<&PathBuf>,
) -> Result<String, CliSupportError> {
    let selected_ws = match (workspace_id, request_file) {
        (Some(ws), None) => Some(ws),
        (None, Some(path)) => {
            let selector: ScopeGetFileSelector =
                read_bounded_json_file(path, MAX_AUTONOMY_LOOP_BODY_BYTES)?;
            selector
                .workspace_id
                .as_deref()
                .map(parse_canonical_uuid)
                .transpose()?
        }
        (None, None) => None,
        (Some(_), Some(_)) => return Err(CliSupportError::InvalidArguments),
    };
    if let Some(ws) = selected_ws {
        let scope = client.get_workspace_scope(auth, ws).await?;
        format_json_output(&WorkspaceScopeQueryResponse {
            workspace_id: ws,
            scope,
        })
    } else {
        format_json_output(&client.list_workspace_scopes(auth).await?)
    }
}

async fn execute_reviewer_or_snapshot(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &AutonomyAction,
) -> Result<String, CliSupportError> {
    match action {
        AutonomyAction::ReviewerAssign {
            workspace_id,
            payload,
        } => format_json_output(
            &client
                .assign_reviewer(
                    auth,
                    *workspace_id,
                    &payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?,
                )
                .await?,
        ),
        AutonomyAction::ReviewerVerdict {
            workspace_id,
            payload,
        } => format_json_output(
            &client
                .record_reviewer_verdict(
                    auth,
                    *workspace_id,
                    &payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?,
                )
                .await?,
        ),
        AutonomyAction::ReviewerVerifyGate {
            workspace_id,
            payload,
        } => format_json_output(
            &client
                .verify_reviewer_gate(
                    auth,
                    *workspace_id,
                    &payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?,
                )
                .await?,
        ),
        AutonomyAction::SnapshotPrepare {
            workspace_id,
            payload,
        } => format_json_output(
            &client
                .prepare_execution_snapshot(
                    auth,
                    *workspace_id,
                    &payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?,
                )
                .await?,
        ),
        AutonomyAction::SnapshotGet {
            workspace_id,
            payload,
        } => {
            let query = payload.resolve(MAX_AUTONOMY_LOOP_BODY_BYTES)?;
            let attempt = query
                .to_attempt_key(*workspace_id)
                .map_err(|_| CliSupportError::InvalidRequestJson)?;
            let record = client
                .get_execution_snapshot(auth, *workspace_id, &query)
                .await?;
            format_json_output(&ExecutionSnapshotQueryResponse {
                workspace_id: *workspace_id,
                goal_id: attempt.goal_id().into_uuid(),
                dag_revision: attempt.dag_revision().get(),
                node_id: attempt.node_id().into_uuid(),
                assignment_id: attempt.assignment_id().into_uuid(),
                attempt: attempt.attempt(),
                record,
            })
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

#[cfg(test)]
mod tests;
