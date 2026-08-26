//! Keyword-PIR layer: turns "look up this key privately" into positional PIR
//! queries against the inspire-gpu backend. Application-agnostic; key/value
//! sizes are parameters (the Ethereum deployment uses 20-byte addresses and
//! 40-byte account values).
//!
//! - [`cuckoo`]: cuckoo hashing (2 SipHash functions) with capacity-2
//!   buckets — one PIR entry = one 120-byte bucket of two 60-byte account
//!   cells, so a lookup is still 2 PIR queries while the feasible load
//!   factor rises from 0.5 to ~0.897.
//! - [`slots`]: 15-bit byte↔slot packing and the row-major slot matrix the
//!   backend ingests.
//! - [`manifest`]: the static service manifest (fetched once at client
//!   setup) and the sidecar-broadcast wire types.

pub mod cuckoo;
pub mod manifest;
pub mod slots;
