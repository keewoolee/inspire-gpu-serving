//! The service manifest: the static configuration a cold client needs to
//! build queries — cuckoo-hashing parameters and PIR parameters, including
//! the fixed CRS seed. Published by the server (JSON), fetched once at
//! client setup; it does NOT change at generation flips, so a client never
//! re-fetches it in steady state. Per-generation state (snapshot block,
//! stash) travels in the sidecar broadcast instead.

use crate::cuckoo::CuckooHash;
use serde::{Deserialize, Serialize};

/// Cuckoo-hashing side: lets the client map an address to its two candidate
/// bucket indices (= PIR entry indices).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CuckooManifest {
    pub num_buckets: usize,
    pub key_size: usize,
    pub value_size: usize,
    /// Cells (key-value pairs) per bucket; one PIR entry = one bucket.
    pub bucket_capacity: usize,
    pub num_hashes: usize,
    /// 16-byte SipHash seed, hex.
    pub seed_hex: String,
}

impl CuckooManifest {
    pub fn seed(&self) -> Result<[u8; 16], String> {
        let raw = hex::decode(&self.seed_hex).map_err(|e| e.to_string())?;
        raw.try_into().map_err(|_| "seed must be 16 bytes".into())
    }

    pub fn hasher(&self) -> Result<CuckooHash, String> {
        Ok(CuckooHash::new_from_seed(
            self.seed()?,
            self.num_hashes,
            self.num_buckets,
        ))
    }
}

/// PIR side: mirror of the backend's `ipir_params` (capi.h). The client feeds
/// these to `ipir_query_build`; `crs_seed_hex` is the 64-byte CRS seed, fixed
/// for the lifetime of the service (queries stay valid across database
/// rebuilds — privacy rests on the fresh per-query secret, not on CRS
/// freshness).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PirManifest {
    pub n_entries: usize,
    pub entry_bytes: usize,
    pub db_rows: usize,
    pub db_cols: usize,
    pub n_packed: usize,
    pub interp_d: usize,
    pub num_cts: usize,
    pub ring_n: usize,
    pub d_eff: usize,
    pub crs_seed_hex: String,
}

impl PirManifest {
    pub fn crs_seed(&self) -> Result<[u8; 64], String> {
        let raw = hex::decode(&self.crs_seed_hex).map_err(|e| e.to_string())?;
        raw.try_into().map_err(|_| "CRS seed must be 64 bytes".into())
    }
}

/// One sidecar broadcast entry: a key changed after the serving snapshot.
/// Shared wire type between server and client.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SidecarEntry {
    /// Key (20-byte address), hex.
    pub address_hex: String,
    /// Value (40 bytes), hex.
    pub value_hex: String,
    /// Block that produced this change.
    pub block: u64,
}

/// One entry that overflowed the cuckoo table's eviction bound, living
/// outside the PIR matrix. Served in the clear through the sidecar
/// broadcast: every client downloads the same list, so reading it leaks
/// nothing. Expected empty at sane load factors.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct StashEntry {
    /// Key (20-byte address), hex.
    pub address_hex: String,
    /// Value (40 bytes), hex.
    pub value_hex: String,
}

/// The `/sidecar` response body: everything per-generation a lookup needs,
/// in one broadcast fetched per lookup. Identical bytes for every client.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct SidecarBroadcast {
    /// Chain block the serving snapshot reflects; `entries` covers blocks
    /// after this.
    pub snapshot_block: u64,
    /// Cuckoo-overflow entries living outside the PIR matrix this snapshot.
    #[serde(default)]
    pub stash: Vec<StashEntry>,
    /// Changes since the snapshot, oldest first.
    #[serde(default)]
    pub entries: Vec<SidecarEntry>,
}

impl SidecarBroadcast {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("sidecar broadcast serializes")
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| e.to_string())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Manifest {
    pub cuckoo: CuckooManifest,
    pub pir: PirManifest,
}

impl Manifest {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("manifest serializes")
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| e.to_string())
    }

    /// Short fingerprint of the service configuration (the CRS seed pins
    /// everything else). Part of every response stamp, so a client that
    /// reaches a server with a DIFFERENT configuration — another deployment,
    /// or an operator reconfiguration — detects it and re-fetches the
    /// manifest.
    pub fn config_fp(&self) -> String {
        self.pir.crs_seed_hex[..16.min(self.pir.crs_seed_hex.len())].to_string()
    }

    /// The X-Snapshot stamp for responses served at `snapshot_block`:
    /// "<snapshot block>:<config fingerprint>". The snapshot block is
    /// globally meaningful (chain height), so stamps stay comparable across
    /// machines in a rolling swap. Lookup responses are internally
    /// consistent by construction; the stamp serves monitoring, and its
    /// fingerprint half tells a client to refetch the manifest after an
    /// operator reconfiguration.
    pub fn stamp_for(&self, snapshot_block: u64) -> String {
        format!("{}:{}", snapshot_block, self.config_fp())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cuckoo::DETERMINISTIC_SEED;

    #[test]
    fn test_manifest_roundtrip() {
        let m = Manifest {
            cuckoo: CuckooManifest {
                num_buckets: 1 << 27,
                key_size: 20,
                value_size: 40,
                bucket_capacity: 2,
                num_hashes: 2,
                seed_hex: hex::encode(DETERMINISTIC_SEED),
            },
            pir: PirManifest {
                n_entries: 1 << 27,
                entry_bytes: 120,
                db_rows: 32768,
                db_cols: 262144,
                n_packed: 128,
                interp_d: 512,
                num_cts: 1,
                ring_n: 2048,
                d_eff: 2,
                crs_seed_hex: hex::encode([0x5Au8; 64]),
            },
        };
        let m2 = Manifest::from_json(&m.to_json()).unwrap();
        assert_eq!(m, m2);
        assert_eq!(m2.cuckoo.seed().unwrap(), DETERMINISTIC_SEED);
        assert_eq!(m2.pir.crs_seed().unwrap(), [0x5Au8; 64]);
        let h = m2.cuckoo.hasher().unwrap();
        assert_eq!(h.num_buckets, 1 << 27);
        assert_eq!(m2.stamp_for(42), format!("42:{}", "5a".repeat(8)));
    }

    #[test]
    fn test_sidecar_broadcast_roundtrip() {
        let b = SidecarBroadcast {
            snapshot_block: 24_644_657,
            stash: vec![StashEntry {
                address_hex: hex::encode([0x11u8; 20]),
                value_hex: hex::encode([0x22u8; 40]),
            }],
            entries: vec![SidecarEntry {
                address_hex: hex::encode([0x33u8; 20]),
                value_hex: hex::encode([0x44u8; 40]),
                block: 24_644_660,
            }],
        };
        let b2 = SidecarBroadcast::from_json(&b.to_json()).unwrap();
        assert_eq!(b, b2);
    }
}
