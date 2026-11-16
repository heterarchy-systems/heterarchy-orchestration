//! Shared SQLite row decoding, principal encoding, SHA-256 framing, and
//! confirmed-commit readback helpers for Operations persistence modules.

use forge_domain::{AuthenticatedPrincipal, DirectTokenId, OAuthGrantId, WorkspaceId};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::OperationsError;

/// Decode a 16-byte SQLite BLOB into a non-nil [`Uuid`].
pub(crate) fn decode_non_nil_uuid(
    bytes: &[u8],
    label: &'static str,
) -> Result<Uuid, OperationsError> {
    let fixed: [u8; 16] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    let value = Uuid::from_bytes(fixed);
    if value.is_nil() {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(value)
}

/// Decode an optional 16-byte SQLite BLOB into `Option<Uuid>`, rejecting nil UUIDs.
pub(crate) fn decode_optional_non_nil_uuid(
    bytes: Option<&[u8]>,
    label: &'static str,
) -> Result<Option<Uuid>, OperationsError> {
    bytes.map(|raw| decode_non_nil_uuid(raw, label)).transpose()
}

/// Decode a 32-byte SQLite BLOB into a non-zero SHA-256 / BLAKE3 digest.
pub(crate) fn decode_non_zero_digest(
    bytes: &[u8],
    label: &'static str,
) -> Result<[u8; 32], OperationsError> {
    let digest: [u8; 32] = bytes
        .try_into()
        .map_err(|_| OperationsError::Corrupt(label))?;
    if digest == [0_u8; 32] {
        return Err(OperationsError::Corrupt(label));
    }
    Ok(digest)
}

/// Decode an owned UTF-8 byte vector from a SQLite BLOB/TEXT projection.
pub(crate) fn decode_utf8(bytes: Vec<u8>, label: &'static str) -> Result<String, OperationsError> {
    String::from_utf8(bytes).map_err(|_| OperationsError::Corrupt(label))
}

/// Decode a non-negative `INTEGER` column into `u64`.
pub(crate) fn decode_non_negative_u64(
    value: i64,
    label: &'static str,
) -> Result<u64, OperationsError> {
    u64::try_from(value).map_err(|_| OperationsError::Corrupt(label))
}

/// Decode a strictly positive (`>= 1`) `INTEGER` column into `u64`.
pub(crate) fn decode_positive_u64(value: i64, label: &'static str) -> Result<u64, OperationsError> {
    u64::try_from(value)
        .ok()
        .filter(|parsed| *parsed >= 1)
        .ok_or(OperationsError::Corrupt(label))
}

/// Decode a strictly positive (`>= 1`) `INTEGER` column into `u32`.
pub(crate) fn decode_positive_u32(value: i64, label: &'static str) -> Result<u32, OperationsError> {
    u32::try_from(value)
        .ok()
        .filter(|parsed| *parsed >= 1)
        .ok_or(OperationsError::Corrupt(label))
}

/// Decode a SQLite `0`/`1` integer flag into a `bool`.
pub(crate) fn decode_bool_flag(value: i64, label: &'static str) -> Result<bool, OperationsError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(OperationsError::Corrupt(label)),
    }
}

/// Check that an [`AuthenticatedPrincipal`] has no nil token or grant identifier.
pub(crate) fn principal_is_valid(principal: AuthenticatedPrincipal) -> bool {
    match principal {
        AuthenticatedPrincipal::TrustedStdio => true,
        AuthenticatedPrincipal::DirectToken(id) => !id.into_uuid().is_nil(),
        AuthenticatedPrincipal::OAuthGrant(id) => !id.into_uuid().is_nil(),
    }
}

/// Encode an [`AuthenticatedPrincipal`] into `(principal_kind, optional_uuid)`.
pub(crate) fn encode_principal(principal: AuthenticatedPrincipal) -> (i64, Option<Uuid>) {
    match principal {
        AuthenticatedPrincipal::TrustedStdio => (0, None),
        AuthenticatedPrincipal::DirectToken(id) => (1, Some(id.into_uuid())),
        AuthenticatedPrincipal::OAuthGrant(id) => (2, Some(id.into_uuid())),
    }
}

/// Decode a `(principal_kind, optional_16_byte_id)` SQLite pair into [`AuthenticatedPrincipal`].
pub(crate) fn decode_principal(
    kind: i64,
    id_bytes: Option<&[u8]>,
    label: &'static str,
) -> Result<AuthenticatedPrincipal, OperationsError> {
    match (kind, id_bytes) {
        (0, None) => Ok(AuthenticatedPrincipal::TrustedStdio),
        (1, Some(bytes)) => Ok(AuthenticatedPrincipal::DirectToken(
            DirectTokenId::from_uuid(decode_non_nil_uuid(bytes, label)?),
        )),
        (2, Some(bytes)) => Ok(AuthenticatedPrincipal::OAuthGrant(OAuthGrantId::from_uuid(
            decode_non_nil_uuid(bytes, label)?,
        ))),
        _ => Err(OperationsError::Corrupt(label)),
    }
}

/// Derive a deterministic domain-separated UUIDv5-shaped identifier from `(domain, namespace, seed)`.
pub(crate) fn derive_deterministic_uuid(domain: &[u8], namespace: Uuid, seed: Uuid) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(namespace.as_bytes());
    hasher.update(seed.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Encode a `usize` length prefix as little-endian `u64` bytes for canonical hashing.
pub(crate) fn len_u64(length: usize) -> [u8; 8] {
    u64::try_from(length).unwrap_or(u64::MAX).to_le_bytes()
}

/// Feed a length-prefixed UTF-8 string into a [`Sha256`] hasher.
pub(crate) fn hash_str(hasher: &mut Sha256, value: &str) {
    hasher.update(len_u64(value.len()));
    hasher.update(value.as_bytes());
}

/// Feed an optional [`Uuid`] with an explicit presence tag into a [`Sha256`] hasher.
pub(crate) fn hash_optional_uuid(hasher: &mut Sha256, value: Option<Uuid>) {
    match value {
        Some(id) => {
            hasher.update([1_u8]);
            hasher.update(id.as_bytes());
        }
        None => hasher.update([0_u8]),
    }
}

/// Finalize a SQLite `commit()` with durable readback recovery on unknown commit outcomes.
pub(crate) fn finalize_operation_commit<T>(
    commit_result: Result<(), rusqlite::Error>,
    fresh: T,
    workspace_id: WorkspaceId,
    request_id: Uuid,
    readback: impl FnOnce() -> Result<Option<T>, OperationsError>,
) -> Result<T, OperationsError> {
    match commit_result {
        Ok(()) => Ok(fresh),
        Err(source) => match readback() {
            Ok(Some(replayed)) => Ok(replayed),
            Err(conflict @ OperationsError::Conflict(_)) => Err(conflict),
            Ok(None) | Err(_) => Err(OperationsError::OperationCommitUnconfirmed {
                workspace_id,
                request_id,
                source,
            }),
        },
    }
}
