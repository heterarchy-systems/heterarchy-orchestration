//! Swarm run lifecycle and channel creation/subscription persistence.

use forge_domain::WorkspaceId;
use rusqlite::{Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::super::sql::{
    INSERT_SWARM_CHANNEL_SQL, INSERT_SWARM_CHANNEL_SUBSCRIPTION_SQL, INSERT_SWARM_RUN_SQL,
    SELECT_SWARM_RUNS_BY_WORKSPACE_SQL,
};
use super::super::{
    MAX_SWARM_BOARD_PAGE_SIZE, MAX_SWARM_CHANNELS_PER_RUN, SWARM_MAIN_CHANNEL_NAME, SwarmError,
    check_timestamp_non_negative, map_sqlite_conflict, validate_channel_name,
    validate_non_nil_uuid, validate_workspace_id,
};
use super::codecs::{
    decode_run_row, load_channel_by_id, load_channel_by_name, load_channels_by_run,
    load_doorbells_by_run, load_members_by_run, load_run_by_id, load_run_replay,
    load_subscriptions_by_run, read_raw_run_row, require_channel_by_id, require_member_by_id,
    require_run_by_id,
};
use super::{
    CreateSwarmChannelCommand, CreateSwarmRunCommand, SWARM_MAIN_CHANNEL_ID_DOMAIN,
    SWARM_RUN_DIGEST_DOMAIN, SWARM_RUN_ID_DOMAIN, SubscribeSwarmChannelCommand, SwarmChannelRecord,
    SwarmMemberState, SwarmRunMode, SwarmRunRecord, SwarmRunSnapshot,
};
use crate::sqlite_codecs::{derive_deterministic_uuid, hash_str};
use crate::{OperationsCatalog, OperationsError};

impl OperationsCatalog {
    /// Create a Swarm run and auto-create its `main` board channel, or idempotently replay.
    ///
    /// # Errors
    /// Rejects invalid bounds, conflicting `request_id` payload reuse, and storage errors.
    pub fn create_swarm_run(
        &mut self,
        command: &CreateSwarmRunCommand,
    ) -> Result<SwarmRunRecord, SwarmError> {
        command.validate()?;
        let digest = run_create_digest(command);
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        if let Some(replayed) = load_run_replay(&tx, command.request_id, digest)? {
            tx.commit().map_err(OperationsError::from)?;
            return Ok(replayed);
        }
        let swarm_run_id =
            derive_deterministic_uuid(SWARM_RUN_ID_DOMAIN, self.destination, command.request_id);
        let main_channel_id = derive_deterministic_uuid(
            SWARM_MAIN_CHANNEL_ID_DOMAIN,
            swarm_run_id,
            command.request_id,
        );
        insert_run_row(&tx, swarm_run_id, command, digest)?;
        tx.execute(
            INSERT_SWARM_CHANNEL_SQL,
            params![
                main_channel_id.as_bytes(),
                swarm_run_id.as_bytes(),
                command.workspace_id.into_uuid().as_bytes(),
                SWARM_MAIN_CHANNEL_NAME,
                Option::<&str>::None,
                Option::<&[u8]>::None,
                command.created_at_ms,
            ],
        )
        .map_err(|err| map_sqlite_conflict(err, "swarm main channel create"))?;
        let created = load_run_by_id(&tx, command.workspace_id, swarm_run_id)?
            .ok_or(OperationsError::Corrupt("created swarm run missing"))?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(created)
    }

    /// Read a durable [`SwarmRunRecord`] by `(workspace_id, swarm_run_id)`.
    ///
    /// # Errors
    /// Rejects invalid workspace/run IDs or corrupt stored rows.
    pub fn get_swarm_run(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<Option<SwarmRunRecord>, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let run = load_run_by_id(&tx, workspace_id, swarm_run_id)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(run)
    }

    /// List bounded Swarm runs for a Workspace ordered newest first.
    ///
    /// # Errors
    /// Rejects invalid workspace IDs, out-of-range limits, or corrupt stored rows.
    pub fn list_swarm_runs(
        &self,
        workspace_id: WorkspaceId,
        limit: usize,
    ) -> Result<Vec<SwarmRunRecord>, SwarmError> {
        validate_workspace_id(workspace_id)?;
        if !(1..=MAX_SWARM_BOARD_PAGE_SIZE).contains(&limit) {
            return Err(OperationsError::Bounds("swarm run list limit").into());
        }
        let sql_limit =
            i64::try_from(limit).map_err(|_| OperationsError::Bounds("swarm run list limit"))?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let mut stmt = tx
            .prepare(SELECT_SWARM_RUNS_BY_WORKSPACE_SQL)
            .map_err(OperationsError::from)?;
        let runs = stmt
            .query_map(
                params![workspace_id.into_uuid().as_bytes(), sql_limit],
                read_raw_run_row,
            )
            .map_err(OperationsError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(OperationsError::from)?;
        drop(stmt);
        let decoded = runs
            .into_iter()
            .map(decode_run_row)
            .collect::<Result<Vec<_>, _>>()?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(decoded)
    }

    /// Read a consistent single-transaction snapshot of a Swarm run, its channels, members, subscriptions, and pending doorbells.
    ///
    /// # Errors
    /// Rejects invalid identities or corrupt stored rows.
    pub fn get_swarm_run_snapshot(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
    ) -> Result<Option<SwarmRunSnapshot>, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let Some(run) = load_run_by_id(&tx, workspace_id, swarm_run_id)? else {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(None);
        };
        let channels = load_channels_by_run(&tx, swarm_run_id)?;
        let members = load_members_by_run(&tx, swarm_run_id)?;
        let subscriptions = load_subscriptions_by_run(&tx, swarm_run_id)?;
        let pending_doorbells = load_doorbells_by_run(&tx, swarm_run_id)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(Some(SwarmRunSnapshot {
            run,
            channels,
            members,
            subscriptions,
            pending_doorbells,
        }))
    }

    /// Create an isolated area channel inside an active Swarm run.
    ///
    /// # Errors
    /// Rejects invalid channel names, terminal runs, channel cap overflow, or duplicate names.
    pub fn create_swarm_channel(
        &mut self,
        command: &CreateSwarmChannelCommand,
    ) -> Result<SwarmChannelRecord, SwarmError> {
        command.validate()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot create channel on terminal Swarm run",
            ));
        }
        if command.created_at_ms < run.created_at_ms {
            return Err(OperationsError::Bounds(
                "swarm channel created_at_ms precedes run creation",
            )
            .into());
        }
        if let Some(existing) = load_channel_by_id(&tx, command.swarm_run_id, command.channel_id)? {
            if existing.workspace_id != command.workspace_id
                || existing.name != command.name
                || existing.purpose != command.purpose
                || existing.created_by_member_id != command.created_by_member_id
            {
                return Err(OperationsError::Conflict(
                    "swarm channel_id already exists with different specification",
                )
                .into());
            }
            tx.commit().map_err(OperationsError::from)?;
            return Ok(existing);
        }
        if let Some(creator_id) = command.created_by_member_id {
            let creator = require_member_by_id(&tx, command.swarm_run_id, creator_id)?;
            if creator.member_state == SwarmMemberState::Killed {
                return Err(SwarmError::InvalidStateTransition(
                    "killed member cannot create channel",
                ));
            }
        }
        let existing_channels = load_channels_by_run(&tx, command.swarm_run_id)?;
        if existing_channels.len() >= MAX_SWARM_CHANNELS_PER_RUN {
            return Err(OperationsError::Bounds("swarm max channels per run").into());
        }
        tx.execute(
            INSERT_SWARM_CHANNEL_SQL,
            params![
                command.channel_id.as_bytes(),
                command.swarm_run_id.as_bytes(),
                command.workspace_id.into_uuid().as_bytes(),
                command.name.as_str(),
                command.purpose.as_deref(),
                command.created_by_member_id.as_ref().map(Uuid::as_bytes),
                command.created_at_ms,
            ],
        )
        .map_err(|err| map_sqlite_conflict(err, "swarm channel duplicate name"))?;
        if let Some(creator_id) = command.created_by_member_id {
            tx.execute(
                INSERT_SWARM_CHANNEL_SUBSCRIPTION_SQL,
                params![
                    command.swarm_run_id.as_bytes(),
                    command.channel_id.as_bytes(),
                    creator_id.as_bytes(),
                    command.created_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        }
        let channel = require_channel_by_id(&tx, command.swarm_run_id, command.channel_id)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(channel)
    }

    /// Look up a Swarm channel by name inside a run.
    ///
    /// # Errors
    /// Rejects invalid workspace/run IDs, invalid channel names, or corrupt rows.
    pub fn get_swarm_channel_by_name(
        &self,
        workspace_id: WorkspaceId,
        swarm_run_id: Uuid,
        name: &str,
    ) -> Result<Option<SwarmChannelRecord>, SwarmError> {
        validate_workspace_id(workspace_id)?;
        validate_non_nil_uuid(swarm_run_id, "swarm_run_id")?;
        validate_channel_name(name)?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(OperationsError::from)?;
        let Some(run) = load_run_by_id(&tx, workspace_id, swarm_run_id)? else {
            crate::catalog::commit_read(tx, self.destination)?;
            return Ok(None);
        };
        let channel = load_channel_by_name(&tx, run.swarm_run_id, name)?;
        crate::catalog::commit_read(tx, self.destination)?;
        Ok(channel)
    }

    /// Subscribe an active Swarm member to a Swarm channel (`true` if newly subscribed).
    ///
    /// # Errors
    /// Rejects missing run/channel/member, killed members, or storage errors.
    pub fn subscribe_swarm_channel(
        &mut self,
        command: SubscribeSwarmChannelCommand,
    ) -> Result<bool, SwarmError> {
        validate_workspace_id(command.workspace_id)?;
        validate_non_nil_uuid(command.swarm_run_id, "swarm_run_id")?;
        validate_non_nil_uuid(command.channel_id, "swarm channel_id")?;
        validate_non_nil_uuid(command.member_id, "swarm member_id")?;
        check_timestamp_non_negative(command.subscribed_at_ms, "swarm subscribed_at_ms")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(OperationsError::from)?;
        let run = require_run_by_id(&tx, command.workspace_id, command.swarm_run_id)?;
        if run.state.is_terminal() {
            return Err(SwarmError::InvalidStateTransition(
                "cannot subscribe on terminal Swarm run",
            ));
        }
        let channel = require_channel_by_id(&tx, command.swarm_run_id, command.channel_id)?;
        if channel.workspace_id != command.workspace_id {
            return Err(SwarmError::NotFound("swarm channel in workspace"));
        }
        let member = require_member_by_id(&tx, command.swarm_run_id, command.member_id)?;
        if member.member_state == SwarmMemberState::Killed {
            return Err(SwarmError::InvalidStateTransition(
                "killed member cannot subscribe to channel",
            ));
        }
        let changed = tx
            .execute(
                INSERT_SWARM_CHANNEL_SUBSCRIPTION_SQL,
                params![
                    command.swarm_run_id.as_bytes(),
                    command.channel_id.as_bytes(),
                    command.member_id.as_bytes(),
                    command.subscribed_at_ms,
                ],
            )
            .map_err(OperationsError::from)?;
        tx.commit().map_err(OperationsError::from)?;
        Ok(changed == 1)
    }
}

fn run_create_digest(command: &CreateSwarmRunCommand) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(SWARM_RUN_DIGEST_DOMAIN);
    hasher.update(command.workspace_id.into_uuid().as_bytes());
    hash_str(&mut hasher, &command.title);
    hasher.update(command.mode.mode_kind().to_le_bytes());
    if let SwarmRunMode::DagNodeHybrid {
        goal_id,
        dag_revision,
        node_id,
        declared_write_manifest_blake3,
    } = command.mode
    {
        hasher.update(goal_id.into_uuid().as_bytes());
        hasher.update(dag_revision.get().to_le_bytes());
        hasher.update(node_id.into_uuid().as_bytes());
        hasher.update(declared_write_manifest_blake3);
    }
    hasher.update(command.max_members.to_le_bytes());
    hasher.update([command.max_spawn_depth]);
    hasher.update(command.max_total_turns.to_le_bytes());
    hasher.update(command.required_verifier_quorum.to_le_bytes());
    hasher.update([u8::from(command.allow_worker_all_mention)]);
    hash_str(&mut hasher, command.baseline_commit_oid.as_str());
    hasher.update(command.baseline_tree_blake3);
    hasher.finalize().into()
}

fn insert_run_row(
    tx: &Transaction<'_>,
    swarm_run_id: Uuid,
    command: &CreateSwarmRunCommand,
    digest: [u8; 32],
) -> Result<(), SwarmError> {
    let (goal_id, dag_rev, node_id, manifest) = match command.mode {
        SwarmRunMode::Standalone => (None, None, None, None),
        SwarmRunMode::DagNodeHybrid {
            goal_id,
            dag_revision,
            node_id,
            declared_write_manifest_blake3,
        } => (
            Some(goal_id.into_uuid()),
            Some(dag_revision.get()),
            Some(node_id.into_uuid()),
            Some(declared_write_manifest_blake3),
        ),
    };
    tx.execute(
        INSERT_SWARM_RUN_SQL,
        params![
            swarm_run_id.as_bytes(),
            command.workspace_id.into_uuid().as_bytes(),
            command.request_id.as_bytes(),
            digest.as_slice(),
            command.title.as_str(),
            command.mode.mode_kind(),
            goal_id.as_ref().map(Uuid::as_bytes),
            dag_rev,
            node_id.as_ref().map(Uuid::as_bytes),
            manifest.as_ref().map(<[u8; 32]>::as_slice),
            i64::from(command.max_members),
            i64::from(command.max_spawn_depth),
            crate::catalog::signed(command.max_total_turns)?,
            i64::from(command.required_verifier_quorum),
            i64::from(command.allow_worker_all_mention),
            command.baseline_commit_oid.as_str(),
            command.baseline_tree_blake3.as_slice(),
            command.created_at_ms,
        ],
    )
    .map_err(|err| map_sqlite_conflict(err, "swarm run insert"))?;
    Ok(())
}
