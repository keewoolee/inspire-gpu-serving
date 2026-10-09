//! PIR serving front: database generations on the GPU (with zero-downtime
//! flips), a batch scheduler that owns each (single-threaded) `GpuServer`
//! handle, a sidecar store for post-snapshot changes, a chain-follower loop
//! (with [`names`] for a table of primary names), and a small HTTP API. The
//! binary in `main.rs` wires them up.

pub mod follower;
pub mod generation;
pub mod http;
pub mod names;
pub mod scheduler;
pub mod sidecar;
