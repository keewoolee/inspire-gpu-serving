//! Client for the PIR serving front. Pure CPU (links no CUDA); this is the
//! reference for wallet integration.
//!
//! The service manifest (cuckoo + PIR parameters, fixed CRS seed) is fetched
//! ONCE at setup and never changes at generation flips. A private lookup is
//! then a SINGLE round with a FIXED request shape — the server sees the same
//! traffic no matter which key is looked up, where it is stored, or whether
//! it exists (PIR hides query contents, not query counts):
//!
//!   one POST /lookup carrying exactly 2 self-contained PIR queries, one per
//!   cuckoo candidate bucket (fresh secret each) — always both, never an
//!   early exit.
//!
//! The server answers the queries first and then attaches the sidecar
//! broadcast for the snapshot that answered them (snapshot block, stash,
//! and the changes since that snapshot), so the response is one consistent
//! view by construction — a generation flip mid-lookup changes nothing.
//! The client picks the answer locally: the freshest sidecar entry wins,
//! else the matching cell from either decrypted bucket, else the stash; no
//! match anywhere is a proven non-membership at that snapshot.
//!
//! The CRS is fixed, so queries are valid against every generation; the
//! X-Snapshot stamp ("<snapshot block>:<config fingerprint>") only guards
//! against an operator RECONFIGURATION — a changed fingerprint makes the
//! client refetch the manifest and retry once.

use pir_backend_ffi::{pack_query, ClientQuery, Params};
use pir_keyword::cuckoo::{CuckooHash, CANARY_ADDRESS};
use pir_keyword::manifest::{Manifest, SidecarBroadcast};
use pir_keyword::slots::unpack_bytes;

/// Where a lookup result came from.
#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    /// Retrieved through PIR (or the stash) from the serving snapshot.
    Snapshot { block: u64 },
    /// Overlaid from the sidecar broadcast (fresher than the snapshot).
    Sidecar { block: u64 },
}

#[derive(Clone, Debug)]
pub struct Lookup {
    pub value: Vec<u8>,
    pub source: Source,
}

/// Balance/nonce view of a 40-byte account value
/// ([16B zero][16B balance BE][8B nonce BE]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AccountValue {
    pub balance: u128,
    pub nonce: u64,
}

pub fn parse_account_value(v: &[u8]) -> Option<AccountValue> {
    if v.len() != 40 {
        return None;
    }
    Some(AccountValue {
        balance: u128::from_be_bytes(v[16..32].try_into().unwrap()),
        nonce: u64::from_be_bytes(v[32..40].try_into().unwrap()),
    })
}

/// Cap on a response body. A lookup carries the whole sidecar broadcast, which
/// grows with every block until the next generation flip folds it in, so the
/// 10 MiB default trips exactly when a server is catching up.
const MAX_RESPONSE_BYTES: u64 = 128 * 1024 * 1024;

pub struct PirClient {
    base: String,
    pub manifest: Manifest,
    params: Params,
    hasher: CuckooHash,
    /// Snapshot block of the last completed lookup (display/monitoring).
    pub last_snapshot_block: u64,
}

impl PirClient {
    /// Fetch the (static) manifest and set up. `base` like
    /// "http://host:8080".
    pub fn connect(base: &str) -> Result<PirClient, String> {
        let base = base.trim_end_matches('/').to_string();
        let (manifest, params, hasher) = Self::fetch_manifest(&base)?;
        Ok(PirClient {
            base,
            manifest,
            params,
            hasher,
            last_snapshot_block: 0,
        })
    }

    fn fetch_manifest(base: &str) -> Result<(Manifest, Params, CuckooHash), String> {
        let mut resp = ureq::get(&format!("{}/manifest", base))
            .call()
            .map_err(|e| format!("manifest fetch failed: {}", e))?;
        let body = resp
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_string()
            .map_err(|e| e.to_string())?;
        let manifest = Manifest::from_json(&body)?;
        let params = Params::from_manifest(&manifest.pir)?;
        let hasher = manifest.cuckoo.hasher()?;
        Ok((manifest, params, hasher))
    }

    /// Re-fetch the manifest. Only needed when the service configuration
    /// changed (the stamp's fingerprint half differs) — never at an
    /// ordinary generation flip.
    pub fn refresh(&mut self) -> Result<(), String> {
        let (m, p, h) = Self::fetch_manifest(&self.base)?;
        self.manifest = m;
        self.params = p;
        self.hasher = h;
        Ok(())
    }

    /// Find `key` among the cells of a retrieved bucket; returns the value.
    fn match_cell(&self, bucket: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let ks = self.manifest.cuckoo.key_size;
        let cs = ks + self.manifest.cuckoo.value_size;
        for c in 0..self.manifest.cuckoo.bucket_capacity {
            let cell = &bucket[c * cs..(c + 1) * cs];
            if &cell[..ks] == key {
                return Some(cell[ks..cs].to_vec());
            }
        }
        None
    }

    /// Private lookup: one POST carrying both bucket queries. Retries once
    /// through a manifest refresh if the server was reconfigured.
    /// Look up an account by address, deriving the key the way the served
    /// table was built. Prefer this over `lookup` unless you already hold a
    /// key: a snapshot read out of a node's state trie is keyed by the hash of
    /// the address, and querying such a table with a raw address quietly finds
    /// nothing rather than failing.
    pub fn lookup_address(&mut self, address: &[u8]) -> Result<Option<Lookup>, String> {
        let cuckoo = &self.manifest.cuckoo;
        let key = cuckoo.key_derivation.key(address, cuckoo.key_size);
        self.lookup(&key)
    }

    pub fn lookup(&mut self, key: &[u8]) -> Result<Option<Lookup>, String> {
        match self.lookup_once(key)? {
            LookupOutcome::Done(r) => Ok(r),
            LookupOutcome::Reconfigured => {
                self.refresh()?;
                match self.lookup_once(key)? {
                    LookupOutcome::Done(r) => Ok(r),
                    LookupOutcome::Reconfigured => {
                        Err("server configuration kept changing".into())
                    }
                }
            }
        }
    }

    fn lookup_once(&mut self, key: &[u8]) -> Result<LookupOutcome, String> {
        // Build both queries (fresh secret each), concurrently.
        let [p0, p1] = self.hasher.positions_2(key);
        let (q0, q1) = std::thread::scope(|s| {
            let h0 = s.spawn(|| ClientQuery::build(&self.params, p0 as u64));
            let h1 = s.spawn(|| ClientQuery::build(&self.params, p1 as u64));
            (h0.join().unwrap(), h1.join().unwrap())
        });
        let (q0, q1) = (q0?, q1?);
        let mut body = pack_query(&self.params, &q0.flat)?;
        body.extend_from_slice(&pack_query(&self.params, &q1.flat)?);

        let mut resp = ureq::post(&format!("{}/lookup", self.base))
            .send(&body[..])
            .map_err(|e| format!("lookup failed: {}", e))?;
        let stamp = resp
            .headers()
            .get("x-snapshot")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if stamp.split(':').nth(1) != Some(self.manifest.config_fp().as_str()) {
            return Ok(LookupOutcome::Reconfigured);
        }
        let bytes = resp
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_vec()
            .map_err(|e| e.to_string())?;

        // [4B LE json length][SidecarBroadcast JSON][response 0][response 1]
        let rb = self.params.response_compressed_bytes();
        if bytes.len() < 4 {
            return Err("lookup response too short".into());
        }
        let json_len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        if bytes.len() != 4 + json_len + 2 * rb {
            return Err(format!(
                "lookup response is {} bytes, expected 4 + {} + 2x{}",
                bytes.len(),
                json_len,
                rb
            ));
        }
        let sc = SidecarBroadcast::from_json(
            std::str::from_utf8(&bytes[4..4 + json_len]).map_err(|e| e.to_string())?,
        )?;
        let r0 = &bytes[4 + json_len..4 + json_len + rb];
        let r1 = &bytes[4 + json_len + rb..];

        let cs = self.manifest.cuckoo.key_size + self.manifest.cuckoo.value_size;
        let bucket_bytes = self.manifest.cuckoo.bucket_capacity * cs;
        let b0 = unpack_bytes(&q0.extract_compressed(&self.params, r0)?, bucket_bytes);
        let b1 = unpack_bytes(&q1.extract_compressed(&self.params, r1)?, bucket_bytes);

        self.last_snapshot_block = sc.snapshot_block;

        // Pick locally: freshest sidecar entry, else either bucket's
        // matching cell, else the stash — all describing one snapshot.
        if let Some(e) = sc
            .entries
            .iter()
            .rev()
            .find(|e| hex::decode(&e.address_hex).as_deref() == Ok(key))
        {
            return Ok(LookupOutcome::Done(Some(Lookup {
                value: hex::decode(&e.value_hex).map_err(|x| x.to_string())?,
                source: Source::Sidecar { block: e.block },
            })));
        }
        let mut found = self
            .match_cell(&b0, key)
            .or_else(|| self.match_cell(&b1, key));
        if found.is_none() {
            if let Some(e) = sc
                .stash
                .iter()
                .find(|e| hex::decode(&e.address_hex).as_deref() == Ok(key))
            {
                found = Some(hex::decode(&e.value_hex).map_err(|x| x.to_string())?);
            }
        }
        Ok(LookupOutcome::Done(found.map(|value| Lookup {
            value,
            source: Source::Snapshot {
                block: sc.snapshot_block,
            },
        })))
    }

    /// Retrieve the canary entry: proves which snapshot block answered.
    /// Goes through lookup() so even canary checks share the request shape.
    pub fn canary_block(&mut self) -> Result<Option<u64>, String> {
        Ok(self
            .lookup(&CANARY_ADDRESS)?
            .and_then(|l| parse_account_value(&l.value))
            .map(|a| a.nonce))
    }
}

enum LookupOutcome {
    Done(Option<Lookup>),
    /// The stamp's config fingerprint differs from the manifest's: the
    /// service was reconfigured (never a mere flip).
    Reconfigured,
}
