//! Official bounded client for the operator-only Forge operations import.

use forge_backend::ops::agent_operations_migration::ExportCursorWire;
use hyper::Method;
use hyper::header::HeaderValue;
use uuid::Uuid;

use crate::wire::operations_import::{
    ImportPrepareRequest, ImportProgressResponse, ImportProgressWire, ImportStepRequest,
};

use super::{
    ClientError, OperationsImportReadback, OrchestrationHttpClient, ReadbackTarget,
    require_identity,
};

const MAX_SCOPE_LOCATOR_BYTES: usize = 4_096;

impl OrchestrationHttpClient {
    /// Prepare or exactly replay one operator import.
    ///
    /// # Errors
    /// Rejects invalid identities or progress fences, source rejections, and
    /// invalid replies. An uncertain mutation retains its original readback intent.
    pub async fn import_prepare(
        &self,
        authorization: &HeaderValue,
        request: &ImportPrepareRequest,
    ) -> Result<ImportProgressWire, ClientError> {
        validate_prepare(request)?;
        let readback = ReadbackTarget::OperationsImport(Box::new(
            OperationsImportReadback::Prepare(Box::new(request.clone())),
        ));
        let response: ImportProgressWire = self
            .call(
                authorization,
                Method::POST,
                "/internal/v1/operations/imports/prepare".to_owned(),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_progress(
            &response,
            request.migration_id,
            request.scope.workspace_id,
            request.destination_id,
        ) {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Import one bounded next page without retrying or looping.
    ///
    /// # Errors
    /// Rejects invalid identities or progress fences, source rejections, and
    /// invalid replies. An uncertain mutation retains its original readback intent.
    pub async fn import_next_page(
        &self,
        authorization: &HeaderValue,
        migration_id: Uuid,
        request: &ImportStepRequest,
    ) -> Result<ImportProgressWire, ClientError> {
        validate_step(migration_id, request)?;
        let readback =
            ReadbackTarget::OperationsImport(Box::new(OperationsImportReadback::ImportNextPage {
                migration_id,
                request: Box::new(request.clone()),
            }));
        let response: ImportProgressWire = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/operations/imports/{migration_id}/import-next-page"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        let expected_pages = request
            .expected_pages
            .checked_add(1)
            .ok_or(ClientError::InvalidRequest)?;
        if !valid_progress(
            &response,
            migration_id,
            request.scope.workspace_id,
            request.destination_id,
        ) || !matches!(
            response.phase,
            crate::wire::operations_import::ImportPhaseWire::Importing
                | crate::wire::operations_import::ImportPhaseWire::Imported
        ) || response.page_count != expected_pages
            || response.verified_pages != request.expected_verified_pages
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Verify one bounded next page without retrying or looping.
    ///
    /// # Errors
    /// Rejects invalid identities or progress fences, source rejections, and
    /// invalid replies. An uncertain mutation retains its original readback intent.
    pub async fn import_verify_next_page(
        &self,
        authorization: &HeaderValue,
        migration_id: Uuid,
        request: &ImportStepRequest,
    ) -> Result<ImportProgressWire, ClientError> {
        validate_step(migration_id, request)?;
        let readback =
            ReadbackTarget::OperationsImport(Box::new(OperationsImportReadback::VerifyNextPage {
                migration_id,
                request: Box::new(request.clone()),
            }));
        let response: ImportProgressWire = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/operations/imports/{migration_id}/verify-next-page"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        let expected_verified_pages = request
            .expected_verified_pages
            .checked_add(1)
            .ok_or(ClientError::InvalidRequest)?;
        let terminal_replay = response.phase
            == crate::wire::operations_import::ImportPhaseWire::Verified
            && request.expected_verified_pages == request.expected_pages;
        if !valid_progress(
            &response,
            migration_id,
            request.scope.workspace_id,
            request.destination_id,
        ) || !matches!(
            response.phase,
            crate::wire::operations_import::ImportPhaseWire::Imported
                | crate::wire::operations_import::ImportPhaseWire::Verified
        ) || response.page_count != request.expected_pages
            || (!terminal_replay && response.verified_pages != expected_verified_pages)
            || (terminal_replay && response.verified_pages != request.expected_verified_pages)
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }

    /// Read one exact import progress projection. Missing migrations are an
    /// explicit `progress: null` observation.
    ///
    /// # Errors
    /// Rejects invalid identities, unavailable sources, and malformed or
    /// incorrectly correlated progress observations.
    pub async fn import_progress(
        &self,
        authorization: &HeaderValue,
        migration_id: Uuid,
    ) -> Result<Option<ImportProgressWire>, ClientError> {
        require_identity(migration_id)?;
        let response: ImportProgressResponse = self
            .call::<_, ()>(
                authorization,
                Method::GET,
                format!("/internal/v1/operations/imports/{migration_id}/progress"),
                None,
                None,
            )
            .await?;
        if let Some(progress) = &response.progress
            && !valid_progress(
                progress,
                migration_id,
                progress.workspace_id,
                progress.destination_id,
            )
        {
            return Err(ClientError::Protocol);
        }
        Ok(response.progress)
    }

    /// Abort one pre-membership import under its exact progress fence.
    ///
    /// # Errors
    /// Rejects invalid identities or progress fences, source rejections, and
    /// invalid replies. An uncertain mutation retains its original readback intent.
    pub async fn import_abort(
        &self,
        authorization: &HeaderValue,
        migration_id: Uuid,
        request: &ImportStepRequest,
    ) -> Result<ImportProgressWire, ClientError> {
        validate_step(migration_id, request)?;
        let readback =
            ReadbackTarget::OperationsImport(Box::new(OperationsImportReadback::Abort {
                migration_id,
                request: Box::new(request.clone()),
            }));
        let response: ImportProgressWire = self
            .call(
                authorization,
                Method::POST,
                format!("/internal/v1/operations/imports/{migration_id}/abort"),
                Some(request),
                Some(readback.clone()),
            )
            .await?;
        if !valid_progress(
            &response,
            migration_id,
            request.scope.workspace_id,
            request.destination_id,
        ) || response.phase != crate::wire::operations_import::ImportPhaseWire::Aborted
            || response.page_count != request.expected_pages
            || response.verified_pages != request.expected_verified_pages
        {
            return Err(ClientError::UnknownOutcome { readback });
        }
        Ok(response)
    }
}

fn validate_prepare(request: &ImportPrepareRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(request.migration_id)?;
    require_identity(request.destination_id)
}

fn validate_step(migration_id: Uuid, request: &ImportStepRequest) -> Result<(), ClientError> {
    validate_scope(&request.scope)?;
    require_identity(migration_id)?;
    require_identity(request.destination_id)?;
    if request.expected_pages > i64::MAX as u64
        || request.expected_verified_pages > i64::MAX as u64
        || request.expected_verified_pages > request.expected_pages
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn validate_scope(scope: &crate::wire::WorkspaceQueryScope) -> Result<(), ClientError> {
    if scope.workspace_id.is_nil()
        || scope.location_id.is_nil()
        || scope.location_revision <= 0
        || scope.workspace_revision <= 0
        || scope.locator.is_empty()
        || scope.locator.len() > MAX_SCOPE_LOCATOR_BYTES
        || scope.locator.chars().any(char::is_control)
    {
        return Err(ClientError::InvalidRequest);
    }
    Ok(())
}

fn valid_progress(
    progress: &ImportProgressWire,
    migration_id: Uuid,
    workspace_id: Uuid,
    destination_id: Uuid,
) -> bool {
    progress.migration_id == migration_id
        && progress.workspace_id == workspace_id
        && progress.destination_id == destination_id
        && !progress.workspace_id.is_nil()
        && !progress.destination_id.is_nil()
        && progress.source_revision > 0
        && i64::try_from(progress.source_revision).is_ok()
        && i64::try_from(progress.page_count).is_ok()
        && i64::try_from(progress.record_count).is_ok()
        && progress.verified_pages <= progress.page_count
        && match progress.phase {
            crate::wire::operations_import::ImportPhaseWire::Importing => {
                progress.next_cursor.is_some()
            }
            crate::wire::operations_import::ImportPhaseWire::Imported
            | crate::wire::operations_import::ImportPhaseWire::Verified => {
                progress.next_cursor.is_none() && progress.page_count >= 5
            }
            crate::wire::operations_import::ImportPhaseWire::Aborted => true,
        }
        && (progress.phase != crate::wire::operations_import::ImportPhaseWire::Verified
            || (progress.verified_pages == progress.page_count
                && progress.import_digest == progress.verify_digest))
        && valid_digest(&progress.import_digest)
        && valid_digest(&progress.verify_digest)
        && progress.next_cursor.as_ref().is_none_or(valid_cursor)
}

fn valid_cursor(cursor: &ExportCursorWire) -> bool {
    match cursor {
        ExportCursorWire::Participants { .. }
        | ExportCursorWire::Presence { .. }
        | ExportCursorWire::Events { .. }
        | ExportCursorWire::Coordinators { .. } => true,
        ExportCursorWire::Inbox {
            after_recipient_agent_id,
            after_recipient_session_id,
            after_event_id,
        } => matches!(
            (
                after_recipient_agent_id,
                after_recipient_session_id,
                after_event_id
            ),
            (None, None, None) | (Some(_), Some(_), Some(_))
        ),
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::tests::fixture;
    use crate::client::tests::reply;
    use crate::wire::operations_import::{ImportPhaseWire, ImportProgressWire};
    use hyper::header::HeaderValue;

    fn scope() -> crate::wire::WorkspaceQueryScope {
        crate::wire::WorkspaceQueryScope {
            workspace_id: Uuid::from_u128(1),
            location_id: Uuid::from_u128(2),
            location_revision: 1,
            workspace_revision: 1,
            locator: "workspace".to_owned(),
        }
    }

    #[test]
    fn step_validation_rejects_unbounded_expected_counts() {
        let request = ImportStepRequest {
            scope: scope(),
            destination_id: Uuid::from_u128(3),
            expected_pages: i64::MAX as u64 + 1,
            expected_verified_pages: 0,
        };
        assert_eq!(
            validate_step(Uuid::from_u128(4), &request),
            Err(ClientError::InvalidRequest)
        );

        let inconsistent = ImportStepRequest {
            scope: scope(),
            destination_id: Uuid::from_u128(3),
            expected_pages: 0,
            expected_verified_pages: 1,
        };
        assert_eq!(
            validate_step(Uuid::from_u128(4), &inconsistent),
            Err(ClientError::InvalidRequest)
        );
    }

    #[tokio::test]
    async fn prepare_accepts_imported_verified_and_aborted_exact_replays()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = ImportPrepareRequest {
            scope: scope(),
            migration_id: Uuid::from_u128(4),
            destination_id: Uuid::from_u128(3),
        };
        let digest = "00".repeat(32);
        let responses = [
            ImportProgressWire {
                migration_id: request.migration_id,
                workspace_id: request.scope.workspace_id,
                destination_id: request.destination_id,
                source_revision: 1,
                phase: ImportPhaseWire::Imported,
                next_cursor: None,
                page_count: 5,
                record_count: 5,
                import_digest: digest.clone(),
                verified_pages: 0,
                verify_digest: digest.clone(),
            },
            ImportProgressWire {
                migration_id: request.migration_id,
                workspace_id: request.scope.workspace_id,
                destination_id: request.destination_id,
                source_revision: 1,
                phase: ImportPhaseWire::Verified,
                next_cursor: None,
                page_count: 5,
                record_count: 5,
                import_digest: digest.clone(),
                verified_pages: 5,
                verify_digest: digest.clone(),
            },
            ImportProgressWire {
                migration_id: request.migration_id,
                workspace_id: request.scope.workspace_id,
                destination_id: request.destination_id,
                source_revision: 1,
                phase: ImportPhaseWire::Aborted,
                next_cursor: Some(ExportCursorWire::default()),
                page_count: 0,
                record_count: 0,
                import_digest: digest.clone(),
                verified_pages: 0,
                verify_digest: digest,
            },
        ];
        for expected in responses {
            let response = reply("200 OK", &serde_json::to_value(&expected)?);
            let (client, server) = fixture(Some(response)).await?;
            assert_eq!(
                client
                    .import_prepare(&HeaderValue::from_static("Bearer operator"), &request,)
                    .await?,
                expected
            );
            let _captured = server.await??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn lost_mutation_keeps_exact_import_readback() -> Result<(), Box<dyn std::error::Error>> {
        let (client, server) = fixture(None).await?;
        let request = ImportStepRequest {
            scope: scope(),
            destination_id: Uuid::from_u128(3),
            expected_pages: 0,
            expected_verified_pages: 0,
        };
        let migration_id = Uuid::from_u128(4);
        let result = client
            .import_next_page(
                &HeaderValue::from_static("Bearer original-caller"),
                migration_id,
                &request,
            )
            .await;
        assert_eq!(
            result,
            Err(ClientError::UnknownOutcome {
                readback: ReadbackTarget::OperationsImport(Box::new(
                    OperationsImportReadback::ImportNextPage {
                        migration_id,
                        request: Box::new(request.clone()),
                    },
                ))
            })
        );
        let _captured = server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn unconfirmed_import_commit_requires_exact_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let (client, server) = fixture(Some(reply(
            "409 Conflict",
            &serde_json::json!({
                "code": "COMMITTED_BUT_UNCONFIRMED",
                "message": "redacted",
                "retryable": false,
            }),
        )))
        .await?;
        let request = ImportStepRequest {
            scope: scope(),
            destination_id: Uuid::from_u128(3),
            expected_pages: 0,
            expected_verified_pages: 0,
        };
        let migration_id = Uuid::from_u128(4);
        assert_eq!(
            client
                .import_next_page(
                    &HeaderValue::from_static("Bearer original-caller"),
                    migration_id,
                    &request,
                )
                .await,
            Err(ClientError::ReadbackRequired {
                readback: ReadbackTarget::OperationsImport(Box::new(
                    OperationsImportReadback::ImportNextPage {
                        migration_id,
                        request: Box::new(request),
                    },
                )),
            })
        );
        let _captured = server.await??;
        Ok(())
    }
}
