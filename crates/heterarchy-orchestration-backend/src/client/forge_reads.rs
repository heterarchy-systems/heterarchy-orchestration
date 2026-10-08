//! Bounded client methods for the Forge source read surface.
//!
//! Read-only source observations over the shared loopback transport. Reads
//! pass `None` readback targets: they are observations with no mutation
//! outcome, so there is no unknown-outcome identity to recover.

mod coordinator;
mod file;
mod goal_dag;
mod leases;
mod workspace;
