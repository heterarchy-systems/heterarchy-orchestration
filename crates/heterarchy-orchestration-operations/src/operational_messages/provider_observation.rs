//! Pure provider observations; no dispatch, retry or request-equivalence grant.

use rusqlite::params;
use uuid::Uuid;

use super::{
    OperationsCatalog, OperationsError, ProviderMessageOutbox, ProviderMessageTarget,
    WorkspaceMessage, catalog_commit_read, load_delivered_provider_turn, load_outbox,
    load_provider_turn, require_verified_workspace, validate_provider_id, validate_recipient,
};
use crate::wire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMessageInFlightPage {
    pub outboxes: Vec<ProviderMessageOutbox>,
    pub next_message_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTurnObservation {
    pub message: WorkspaceMessage,
    pub observed_body: String,
}

impl OperationsCatalog {
    /// Read a bounded UUID-keyset page of current pending/unknown message rows.
    ///
    /// # Errors
    /// Rejects invalid cursors/limits, unverified scopes and corrupt catalog data.
    pub fn provider_message_in_flight_page(
        &self,
        after_message_id: Option<Uuid>,
        limit: u32,
    ) -> Result<ProviderMessageInFlightPage, OperationsError> {
        if limit == 0 || limit > 32 || after_message_id.is_some_and(|id| id.is_nil()) {
            return Err(OperationsError::Bounds(
                "provider in-flight observation page",
            ));
        }
        let tx = self.connection.unchecked_transaction()?;
        let after = after_message_id.map(Uuid::into_bytes);
        let mut statement = tx.prepare(
            "SELECT substr(message_id,1,17) FROM ops_provider_message_outbox
             WHERE state IN(2,5) AND (?1 IS NULL OR message_id>?1)
             ORDER BY message_id LIMIT ?2",
        )?;
        let ids = statement
            .query_map(
                params![
                    after.as_ref().map(<[u8; 16]>::as_slice),
                    i64::from(limit) + 1
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .map(|row| wire::id(row?).map(Uuid::from_bytes))
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let truncated = ids.len() > limit as usize;
        let mut outboxes = Vec::with_capacity(limit as usize);
        for id in ids.into_iter().take(limit as usize) {
            let row = load_outbox(&tx, id)?.ok_or(OperationsError::Corrupt(
                "provider in-flight message disappeared",
            ))?;
            require_verified_workspace(&tx, row.target().workspace_id(), self.destination)?;
            outboxes.push(row);
        }
        let next_message_id = if truncated {
            outboxes.last().map(ProviderMessageOutbox::message_id)
        } else {
            None
        };
        catalog_commit_read(tx, self.destination)?;
        Ok(ProviderMessageInFlightPage {
            outboxes,
            next_message_id,
        })
    }

    /// Observe a recorded public provider turn, including a delivered user turn.
    ///
    /// # Errors
    /// Rejects malformed selectors, unverified/foreign routes and corrupt records.
    pub fn incoming_provider_turn(
        &self,
        target: &ProviderMessageTarget,
        provider_turn_id: &str,
    ) -> Result<Option<ProviderTurnObservation>, OperationsError> {
        validate_provider_id(provider_turn_id, "provider turn observation")?;
        let tx = self.connection.unchecked_transaction()?;
        require_verified_workspace(&tx, target.workspace_id(), self.destination)?;
        validate_recipient(&tx, target)?;
        let observation = if let Some((message, observed_body)) =
            load_delivered_provider_turn(&tx, target, provider_turn_id)?
        {
            Some(ProviderTurnObservation {
                message,
                observed_body,
            })
        } else {
            load_provider_turn(
                &tx,
                target.workspace_id(),
                target.provider_conversation_id(),
                provider_turn_id,
            )?
            .map(|message| ProviderTurnObservation {
                observed_body: message.body().to_owned(),
                message,
            })
        };
        catalog_commit_read(tx, self.destination)?;
        Ok(observation)
    }
}
