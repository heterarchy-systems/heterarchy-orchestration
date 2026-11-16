//! Operator and spawned-worker CLI subcommands for Native Swarm coordination.
//!
//! Follows the `parse` + `execute` contract of [`crate::token_cli`]: raw bearer
//! tokens are never accepted on `argv`, `--bearer-file` is read with bounded
//! secret hygiene, spawned-worker `HETERARCHY_*` environment variables supply
//! fallback `workspace_id`, `swarm_run_id`, and `member_id` identities when
//! omitted on `argv`, and deterministic pretty-printed JSON is returned for `stdout`.

mod parse_bridge;
mod parse_subcommands;
mod parse_turns;

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
use crate::client::ClientError;
use crate::wire::swarm::{
    AbortSwarmRunRequest, AddSwarmMemberRequest, ClaimSwarmWakeupRequest, CompleteSwarmTurnRequest,
    ConvergeSwarmRequest, CreateSwarmChannelRequest, CreateSwarmRunRequest,
    CreateSwarmVerificationPanelRequest, DispatchSwarmTurnRequest, FailSwarmTurnRequest,
    ForgeConvergeSwarmRequest, ForgeSettleSwarmRequest, KillSwarmMemberRequest, ListSwarmRunsQuery,
    MAX_SWARM_HTTP_BODY_BYTES, PostSwarmMessageRequest, ReadSwarmMessagesQuery,
    RecordSwarmVerdictRequest, SettleSwarmRequest, SubscribeSwarmChannelRequest,
    SwarmStallCheckRequest, SwitchSwarmMemberModelRequest, SyncAutonomyOutboxRequest,
};

/// Usage summary for the `swarm` CLI subcommand family.
pub const SWARM_CLI_USAGE: &str = "usage: heterarchy-orchestration-backend swarm <create|list|status|channel-create|subscribe|member-add|member-kill|post|read|claim|complete|fail|dispatch|model-switch|panel-create|verdict|converge|forge-converge|settle|forge-settle|outbox-sync|abort|stall-check> --bearer-file <path> [--addr <ip:port>] [--request <json-file> | <flags...>]";

/// Payload source alias for `swarm` CLI subcommands.
pub type SwarmPayload<T> = CliPayload<T>;

/// Parsed `swarm` CLI dispatch result.
#[derive(Debug, PartialEq, Eq)]
pub enum SwarmCliCommand {
    /// Arguments belong to another top-level command or daemon mode.
    NotSwarmCommand,
    /// Operator requested `swarm` usage output.
    Help,
    /// Validated `swarm` subcommand ready for execution.
    Run(Box<SwarmCliOptions>),
}

/// Typed `swarm` action paired with its validated payload source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwarmAction {
    Create {
        workspace_id: Uuid,
        payload: SwarmPayload<CreateSwarmRunRequest>,
    },
    List {
        workspace_id: Uuid,
        payload: SwarmPayload<ListSwarmRunsQuery>,
    },
    Status {
        workspace_id: Uuid,
        swarm_run_id: Option<Uuid>,
        request_file: Option<PathBuf>,
    },
    ChannelCreate {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<CreateSwarmChannelRequest>,
    },
    Subscribe {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        channel_id: Uuid,
        payload: SwarmPayload<SubscribeSwarmChannelRequest>,
    },
    MemberAdd {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<AddSwarmMemberRequest>,
    },
    MemberKill {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        payload: SwarmPayload<KillSwarmMemberRequest>,
    },
    Post {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        channel_name: Option<String>,
        payload: SwarmPayload<PostSwarmMessageRequest>,
    },
    Read {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        channel_name: Option<String>,
        payload: SwarmPayload<ReadSwarmMessagesQuery>,
    },
    Claim {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        payload: SwarmPayload<ClaimSwarmWakeupRequest>,
    },
    Complete {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        payload: SwarmPayload<CompleteSwarmTurnRequest>,
    },
    Fail {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        payload: SwarmPayload<FailSwarmTurnRequest>,
    },
    Dispatch {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        payload: SwarmPayload<DispatchSwarmTurnRequest>,
    },
    ModelSwitch {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        member_id: Uuid,
        payload: SwarmPayload<SwitchSwarmMemberModelRequest>,
    },
    PanelCreate {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<CreateSwarmVerificationPanelRequest>,
    },
    Verdict {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<RecordSwarmVerdictRequest>,
    },
    Converge {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<ConvergeSwarmRequest>,
    },
    Settle {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<SettleSwarmRequest>,
    },
    Abort {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<AbortSwarmRunRequest>,
    },
    StallCheck {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<SwarmStallCheckRequest>,
    },
    ForgeConverge {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<ForgeConvergeSwarmRequest>,
    },
    ForgeSettle {
        workspace_id: Uuid,
        swarm_run_id: Uuid,
        payload: SwarmPayload<ForgeSettleSwarmRequest>,
    },
    OutboxSync {
        workspace_id: Uuid,
        payload: SwarmPayload<SyncAutonomyOutboxRequest>,
    },
}

/// Validated `swarm` CLI invocation options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmCliOptions {
    connection: CliConnectionOptions,
    action: SwarmAction,
}

impl SwarmCliOptions {
    #[must_use]
    pub const fn connection(&self) -> &CliConnectionOptions {
        &self.connection
    }

    #[must_use]
    pub const fn action(&self) -> &SwarmAction {
        &self.action
    }
}

/// Safe, redacted errors returned while parsing or executing `swarm` CLI commands.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct SwarmCliError(#[from] pub CliSupportError);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatusFileSelector {
    swarm_run_id: String,
}

/// Parse `swarm` subcommands using ambient `HETERARCHY_*` fallbacks.
///
/// # Errors
/// Rejects malformed flags, forbidden raw bearer flags, missing `--bearer-file`,
/// or non-loopback `--addr` values.
pub fn parse(arguments: &[OsString]) -> Result<SwarmCliCommand, SwarmCliError> {
    parse_with_env(arguments, |key| env::var_os(key))
}

/// Pure, deterministic parser accepting an explicit environment lookup closure for testing.
///
/// # Errors
/// Returns [`SwarmCliError`] when arguments or connection options are invalid.
pub fn parse_with_env<F>(
    arguments: &[OsString],
    env_lookup: F,
) -> Result<SwarmCliCommand, SwarmCliError>
where
    F: Fn(&str) -> Option<OsString>,
{
    let (subcommand, mut index, mut conn) = match parse_subcommand_head("swarm", arguments)? {
        ParsedSubcommandHead::NotThisCommand => return Ok(SwarmCliCommand::NotSwarmCommand),
        ParsedSubcommandHead::Help => return Ok(SwarmCliCommand::Help),
        ParsedSubcommandHead::Subcommand {
            name,
            next_index,
            conn,
        } => (name, next_index, conn),
    };
    let action =
        parse_subcommands::dispatch(subcommand, arguments, &mut index, &mut conn, &env_lookup)?;
    let connection = conn.resolve_from_env(&env_lookup)?;
    Ok(SwarmCliCommand::Run(Box::new(SwarmCliOptions {
        connection,
        action,
    })))
}

/// Execute a validated `swarm` CLI command against the loopback `OrchestrationHttpClient`.
///
/// # Errors
/// Returns redacted bearer-file, request-JSON, or bounded HTTP client failures.
pub async fn execute(options: &SwarmCliOptions) -> Result<String, SwarmCliError> {
    let (client, bearer) = options.connection.open_client_and_bearer()?;
    let auth = bearer.header_value();
    let json = execute_action(&client, auth, &options.action).await?;
    Ok(json)
}

async fn execute_action(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &SwarmAction,
) -> Result<String, CliSupportError> {
    match action {
        SwarmAction::Create { .. }
        | SwarmAction::List { .. }
        | SwarmAction::Status { .. }
        | SwarmAction::ChannelCreate { .. }
        | SwarmAction::Subscribe { .. } => execute_run_or_channel(client, auth, action).await,
        SwarmAction::MemberAdd { .. }
        | SwarmAction::MemberKill { .. }
        | SwarmAction::Post { .. }
        | SwarmAction::Read { .. } => execute_member_or_board(client, auth, action).await,
        SwarmAction::Claim { .. }
        | SwarmAction::Complete { .. }
        | SwarmAction::Fail { .. }
        | SwarmAction::Dispatch { .. }
        | SwarmAction::ModelSwitch { .. } => execute_turn_action(client, auth, action).await,
        SwarmAction::PanelCreate { .. }
        | SwarmAction::Verdict { .. }
        | SwarmAction::Converge { .. }
        | SwarmAction::Settle { .. }
        | SwarmAction::Abort { .. }
        | SwarmAction::StallCheck { .. } => execute_governance_action(client, auth, action).await,
        SwarmAction::ForgeConverge { .. }
        | SwarmAction::ForgeSettle { .. }
        | SwarmAction::OutboxSync { .. } => execute_bridge_action(client, auth, action).await,
    }
}

async fn execute_run_or_channel(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &SwarmAction,
) -> Result<String, CliSupportError> {
    match action {
        SwarmAction::Create {
            workspace_id,
            payload,
        } => format_json_output(
            &client
                .create_swarm_run(
                    auth,
                    *workspace_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::List {
            workspace_id,
            payload,
        } => format_json_output(
            &client
                .list_swarm_runs(
                    auth,
                    *workspace_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Status {
            workspace_id,
            swarm_run_id,
            request_file,
        } => {
            let run_id = resolve_status_run_id(*swarm_run_id, request_file.as_ref())?;
            format_json_output(
                &client
                    .get_swarm_run_snapshot(auth, *workspace_id, run_id)
                    .await?,
            )
        }
        SwarmAction::ChannelCreate {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .create_swarm_channel(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Subscribe {
            workspace_id,
            swarm_run_id,
            channel_id,
            payload,
        } => format_json_output(
            &client
                .subscribe_swarm_channel(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    *channel_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

async fn execute_member_or_board(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &SwarmAction,
) -> Result<String, CliSupportError> {
    match action {
        SwarmAction::MemberAdd {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .add_swarm_member(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::MemberKill {
            workspace_id,
            swarm_run_id,
            member_id,
            payload,
        } => format_json_output(
            &client
                .kill_swarm_member(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    *member_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Post {
            workspace_id,
            swarm_run_id,
            channel_name,
            payload,
        } => {
            let mut req = payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?;
            if let Some(name) = channel_name.as_deref()
                && name != heterarchy_orchestration_operations::SWARM_MAIN_CHANNEL_NAME
            {
                let channel_id =
                    resolve_channel_name_to_id(client, auth, *workspace_id, *swarm_run_id, name)
                        .await?;
                req.channel_id = Some(channel_id.to_string());
            }
            format_json_output(
                &client
                    .post_swarm_message(auth, *workspace_id, *swarm_run_id, &req)
                    .await?,
            )
        }
        SwarmAction::Read {
            workspace_id,
            swarm_run_id,
            channel_name,
            payload,
        } => {
            let mut query = payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?;
            if let Some(name) = channel_name.as_deref() {
                let channel_id =
                    resolve_channel_name_to_id(client, auth, *workspace_id, *swarm_run_id, name)
                        .await?;
                query.channel_id = Some(channel_id.to_string());
            }
            format_json_output(
                &client
                    .read_swarm_messages(auth, *workspace_id, *swarm_run_id, &query)
                    .await?,
            )
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

async fn resolve_channel_name_to_id(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    workspace_id: Uuid,
    swarm_run_id: Uuid,
    channel_name: &str,
) -> Result<Uuid, CliSupportError> {
    let snapshot = client
        .get_swarm_run_snapshot(auth, workspace_id, swarm_run_id)
        .await?;
    snapshot
        .channels
        .iter()
        .find(|ch| ch.name == channel_name)
        .map(|ch| ch.channel_id)
        .ok_or(CliSupportError::Client(ClientError::InvalidRequest))
}

async fn execute_turn_action(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &SwarmAction,
) -> Result<String, CliSupportError> {
    match action {
        SwarmAction::Claim {
            workspace_id,
            swarm_run_id,
            member_id,
            payload,
        } => format_json_output(
            &client
                .claim_swarm_turn(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    *member_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Complete {
            workspace_id,
            swarm_run_id,
            member_id,
            payload,
        } => format_json_output(
            &client
                .complete_swarm_turn(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    *member_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Fail {
            workspace_id,
            swarm_run_id,
            member_id,
            payload,
        } => format_json_output(
            &client
                .fail_swarm_turn(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    *member_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Dispatch {
            workspace_id,
            swarm_run_id,
            member_id,
            payload,
        } => format_json_output(
            &client
                .dispatch_swarm_member_turn(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    *member_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::ModelSwitch {
            workspace_id,
            swarm_run_id,
            member_id,
            payload,
        } => format_json_output(
            &client
                .switch_swarm_member_model(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    *member_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

async fn execute_governance_action(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &SwarmAction,
) -> Result<String, CliSupportError> {
    match action {
        SwarmAction::PanelCreate {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .create_swarm_verification_panel(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Verdict {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .record_swarm_verdict(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Converge {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .converge_swarm_run(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Settle {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .settle_swarm_run(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::Abort {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .abort_swarm_run(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::StallCheck {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .evaluate_swarm_stall_check(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

async fn execute_bridge_action(
    client: &crate::client::OrchestrationHttpClient,
    auth: &hyper::header::HeaderValue,
    action: &SwarmAction,
) -> Result<String, CliSupportError> {
    match action {
        SwarmAction::ForgeConverge {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .forge_converge_swarm_run(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::ForgeSettle {
            workspace_id,
            swarm_run_id,
            payload,
        } => format_json_output(
            &client
                .forge_settle_swarm_run(
                    auth,
                    *workspace_id,
                    *swarm_run_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        SwarmAction::OutboxSync {
            workspace_id,
            payload,
        } => format_json_output(
            &client
                .sync_autonomy_outbox(
                    auth,
                    *workspace_id,
                    &payload.resolve(MAX_SWARM_HTTP_BODY_BYTES)?,
                )
                .await?,
        ),
        _ => Err(CliSupportError::InvalidArguments),
    }
}

fn resolve_status_run_id(
    swarm_run_id: Option<Uuid>,
    request_file: Option<&PathBuf>,
) -> Result<Uuid, CliSupportError> {
    match (swarm_run_id, request_file) {
        (Some(id), None) => Ok(id),
        (None, Some(path)) => {
            let selector: StatusFileSelector =
                read_bounded_json_file(path, MAX_SWARM_HTTP_BODY_BYTES)?;
            parse_canonical_uuid(&selector.swarm_run_id)
        }
        _ => Err(CliSupportError::InvalidArguments),
    }
}

#[cfg(test)]
mod tests;
