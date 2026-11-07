//! Durable first-attempt dispatch to Forge's Home-transfer source owner.
//!
//! This records a pinned request and an original Source outcome. It never
//! performs network I/O, resends a prior attempt, releases a preparation
//! barrier, or changes Home membership.

mod model;
pub(crate) mod schema;
mod store;

pub(crate) use store::load_home_preparation_for_handoff_in_transaction;

pub use model::{
    TeamPermanentTransferSourceDispatchCommand, TeamPermanentTransferSourceDispatchOutcome,
    TeamPermanentTransferSourceDispatchPermit, TeamPermanentTransferSourceDispatchReadback,
    TeamPermanentTransferSourceDispatchReceipt, TeamPermanentTransferSourceDispatchReplay,
    TeamPermanentTransferSourceEnvelopeDigest, TeamPermanentTransferSourceReceiptCommand,
    TeamPermanentTransferSourceReceiptOutcome, TeamPermanentTransferSourceReceiptRecord,
    TeamPermanentTransferSourceWireIdentity, derive_team_permanent_transfer_source_request,
};
