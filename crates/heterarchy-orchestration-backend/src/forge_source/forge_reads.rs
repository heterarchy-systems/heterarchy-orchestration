//! Typed, bounded Source read observations for the Forge catalog families.
//!
//! Each function mirrors one covered Forge HTTP command operation and returns
//! a source observation only. Readbacks never become permissions: no result
//! grants mutation rights, lease ownership, scope membership, or actor
//! identity. Hosts without the configured HTTP source keep these reads
//! fail-closed instead of switching transports.

pub mod file;
pub mod goal_dag;
pub mod leases;
pub mod workspace;
