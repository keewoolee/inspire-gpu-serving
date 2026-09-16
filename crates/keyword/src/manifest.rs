//! The service manifest: the static configuration a cold client needs to
//! build queries — cuckoo-hashing parameters and PIR parameters, including
//! the fixed CRS seed. Published by the server (JSON), fetched once at
//! client setup; it does NOT change at generation flips, so a client never
//! re-fetches it in steady state. Per-generation state (snapshot block,
//! stash) travels in the sidecar broadcast instead.

use crate::cuckoo::CuckooHash;
use serde::{Deserialize, Serialize};

/// How a lookup key is derived from an account address. A client has to agree
/// with the table it is querying, so the server publishes which one is in use
/// rather than leaving it to be configured on both sides.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum KeyDerivation {
    /// The address itself.
    #[default]
    Address,
    /// keccak256 of the address, cut to the key size. A snapshot read out of a
    /// node's state trie arrives this way, because the trie is keyed by that
    /// hash and a hash cannot be turned back into an address. It costs the
    /// client nothing, since it knows the address it is asking about.
    Keccak,
}

impl KeyDerivation {
    /// The lookup key for `address` under this derivation.
    pub fn key(&self, address: &[u8], key_size: usize) -> Vec<u8> {
        match self {
            KeyDerivation::Address => address.to_vec(),
            KeyDerivation::Keccak => {
                use sha3::Digest;
                sha3::Keccak256::digest(address)[..key_size.min(32)].to_vec()
            }
        }
    }
}

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
    /// Absent in a manifest written before this was published, which could
    /// only have been an address-keyed table.
    #[serde(default)]
    pub key_derivation: KeyDerivation,
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
                key_derivation: KeyDerivation::Address,
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

    /// A manifest written before key_derivation existed can only describe an
    /// address-keyed table, and has to keep loading.
    #[test]
    fn a_manifest_without_key_derivation_reads_as_address() {
        let json = r#"{"num_buckets":16,"key_size":20,"value_size":40,
            "bucket_capacity":2,"num_hashes":2,"seed_hex":"00"}"#;
        let c: CuckooManifest = serde_json::from_str(json).unwrap();
        assert_eq!(c.key_derivation, KeyDerivation::Address);
    }

    #[test]
    fn key_derivation_names_are_stable_on_the_wire() {
        assert_eq!(
            serde_json::to_string(&KeyDerivation::Keccak).unwrap(),
            "\"keccak\""
        );
        assert_eq!(
            serde_json::to_string(&KeyDerivation::Address).unwrap(),
            "\"address\""
        );
    }

    /// The address derivation hands the address back; the keccak one hands back
    /// the hash the state trie is keyed by, cut to the key size.
    #[test]
    fn derivations_produce_the_keys_the_tables_are_built_with() {
        // keccak256 of vitalik.eth's address, as the trie stores it.
        let address = hex::decode("d8da6bf26964af9d7eed9e03e53415d37aa96045").unwrap();
        assert_eq!(KeyDerivation::Address.key(&address, 20), address);
        assert_eq!(
            hex::encode(KeyDerivation::Keccak.key(&address, 20)),
            "06e120c2c3547c60ee47f712d32e5acf38b35d1c"
        );
    }
}
