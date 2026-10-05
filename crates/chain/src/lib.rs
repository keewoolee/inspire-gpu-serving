//! Ethereum data-source adapter (the one application-specific crate in this
//! workspace): JSON-RPC block tracking, per-block account and contract-storage
//! changes, and snapshot resync. The server's live loop and the standalone
//! `resync` tool both build on [`rpc`].

pub mod rpc;
