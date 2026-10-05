//! Database generations and the flip between them.
//!
//! A generation = one GPU handle serving one snapshot of the database. The
//! CRS seed is FIXED for the lifetime of the service (set once at startup),
//! so queries stay valid across flips; what changes per generation is the
//! database content, the snapshot block, and the stash. The HTTP layer
//! stamps every response with X-Snapshot ("<snapshot block>:<config
//! fingerprint>") for monitoring and reconfiguration detection. The builder
//! owns the host-side truth (cuckoo table + slot matrix), applies chain
//! updates incrementally, and stands up a new GPU generation on demand;
//! swapping the serving pointer retires the old one (its scheduler exits
//! and frees the GPU once in-flight requests drain).

use crate::scheduler::{self, QueryJob};
use crate::sidecar::Sidecar;
use pir_backend_ffi::{GpuServer, Params};
use pir_keyword::cuckoo::{canary_value, CuckooTable, CANARY_ADDRESS};
use pir_keyword::manifest::{CuckooManifest, Manifest, StashEntry};
use pir_keyword::slots::{slot_db_cols, to_slot_db, write_bucket_to_slot_db};
use pir_keyword::storage::storage_key;
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// One live generation, shared with the HTTP workers.
pub struct Generation {
    /// The static service manifest (identical across generations; kept here
    /// so the HTTP layer serves it without extra locking).
    pub manifest: Manifest,
    pub manifest_json: String,
    pub params: Params,
    /// Chain block this generation's snapshot reflects.
    pub snapshot_block: u64,
    /// Cuckoo-overflow entries outside the PIR matrix, published through the
    /// sidecar broadcast.
    pub stash: Vec<StashEntry>,
    /// X-Snapshot response stamp: "<snapshot block>:<config fingerprint>".
    pub stamp: String,
    /// Channel into this generation's scheduler thread.
    pub jobs: Sender<QueryJob>,
}

/// What the HTTP layer sees: the current generation (swappable) + sidecar.
pub struct ServingState {
    current: RwLock<Arc<Generation>>,
    pub sidecar: Arc<Sidecar>,
}

impl ServingState {
    pub fn new(first: Generation, sidecar: Arc<Sidecar>) -> Self {
        ServingState {
            current: RwLock::new(Arc::new(first)),
            sidecar,
        }
    }

    pub fn current(&self) -> Arc<Generation> {
        self.current.read().unwrap().clone()
    }

    /// Flip to a new generation. The old one keeps answering its in-flight
    /// jobs and is freed once the last request drops its Arc (the scheduler
    /// thread then exits and the GPU memory is released). Because of that,
    /// callers truncate the sidecar only through the RETIRING generation's
    /// snapshot (one-flip retention lag): lookups the old generation is
    /// still answering need its suffix to remain complete.
    pub fn swap(&self, next: Generation) {
        *self.current.write().unwrap() = Arc::new(next);
    }
}

/// Owns the host-side database truth and builds GPU generations from it.
pub struct GenerationBuilder {
    pub table: CuckooTable,
    slot_db: Vec<u16>,
    db_rows: usize,
    db_cols: usize,
    max_batch: usize,
    window: Duration,
    /// Fixed for the lifetime of the service; every generation is built
    /// against it.
    crs_seed: [u8; 64],
    /// Human-readable build counter (logs only; the protocol identifies
    /// generations by snapshot block).
    builds: u64,
}

impl GenerationBuilder {
    pub fn new(
        table: CuckooTable,
        db_rows: usize,
        max_batch: usize,
        window: Duration,
        crs_seed: [u8; 64],
    ) -> GenerationBuilder {
        let slot_db = to_slot_db(&table, db_rows);
        let db_cols = slot_db_cols(&table, db_rows);
        GenerationBuilder {
            table,
            slot_db,
            db_rows,
            db_cols,
            max_batch,
            window,
            crs_seed,
            builds: 0,
        }
    }

    /// Apply one key-value change to the host-side truth (table + slot
    /// matrix). Takes effect in the NEXT built generation; until then the
    /// sidecar carries it to clients.
    /// Apply an account's new value, deriving its key the way this table is
    /// keyed, and hand the key back because the sidecar has to carry the same
    /// one. Always reach for this rather than `apply_update` when what you
    /// hold is an address: a table keyed by address hashes takes a raw address
    /// as a different account and quietly grows a second copy of it.
    pub fn apply_account(&mut self, address: &[u8], value: &[u8]) -> Vec<u8> {
        let params = &self.table.params;
        let key = params.key_derivation.key(address, params.key_size);
        self.apply_update(&key, value);
        key
    }

    /// Apply a storage slot's new value in a storage table, deriving its key
    /// the way the table is keyed, and hand the key back for the sidecar. The
    /// same trap as for accounts applies: a key derived any other way is a
    /// different entry.
    pub fn apply_storage(&mut self, contract: &[u8; 20], slot: &[u8; 32], value: &[u8]) -> Vec<u8> {
        let key = storage_key(contract, slot, self.table.params.key_size);
        self.apply_update(&key, value);
        key
    }

    /// Apply a value under a key that is already derived. The canary is a
    /// reserved key rather than an account, so it goes in as itself.
    pub fn apply_update(&mut self, key: &[u8], value: &[u8]) {
        for bucket in self.table.upsert(key, value) {
            write_bucket_to_slot_db(
                &mut self.slot_db,
                bucket,
                self.table.bucket_slice(bucket),
                self.db_rows,
                self.db_cols,
            );
        }
    }

    /// Preprocess the current truth onto the GPU as the next generation
    /// (same fixed CRS) and return it ready to swap in. `snapshot_block` is
    /// what this generation reflects; the canary is updated to it.
    pub fn build(&mut self, snapshot_block: u64) -> Result<Generation, String> {
        self.apply_update(&CANARY_ADDRESS, &canary_value(snapshot_block));

        let t0 = Instant::now();
        let srv = GpuServer::create(
            self.table.params.num_buckets,
            self.table.params.bucket_bytes(),
            self.db_rows,
            self.max_batch,
            &self.slot_db,
            Some(&self.crs_seed),
        )?;
        // The counter only advances on success, so a failing GPU build
        // retried in a loop does not burn numbers in the logs.
        self.builds += 1;
        // Cuckoo insertion degrades past ~0.9 of the cells, so the load is
        // worth watching on a table that grows.
        let params = &self.table.params;
        let cells: usize = self.table.used.iter().map(|&u| u as usize).sum();
        let load = cells as f64 / (params.num_buckets * params.bucket_capacity) as f64;
        eprintln!(
            "generation {} built in {:.1}s (snapshot block #{}, {:.2} GB resident, {:.1}% of cells used)",
            self.builds,
            t0.elapsed().as_secs_f64(),
            snapshot_block,
            srv.caps().resident_bytes as f64 / 1e9,
            100.0 * load,
        );

        let ks = self.table.params.key_size;
        let stash: Vec<StashEntry> = self
            .table
            .stash
            .iter()
            .map(|cell| StashEntry {
                address_hex: hex::encode(&cell[..ks]),
                value_hex: hex::encode(&cell[ks..]),
            })
            .collect();
        if !stash.is_empty() {
            eprintln!(
                "snapshot #{}: {} stash entries served via the sidecar broadcast",
                snapshot_block,
                stash.len()
            );
        }
        let manifest = Manifest {
            cuckoo: CuckooManifest {
                num_buckets: self.table.params.num_buckets,
                key_size: self.table.params.key_size,
                value_size: self.table.params.value_size,
                bucket_capacity: self.table.params.bucket_capacity,
                num_hashes: self.table.params.num_hashes,
                seed_hex: hex::encode(self.table.params.seed),
                key_derivation: self.table.params.key_derivation,
                contracts: self
                    .table
                    .params
                    .contracts
                    .iter()
                    .map(|c| format!("0x{}", hex::encode(c)))
                    .collect(),
            },
            pir: srv.params().to_manifest(),
        };
        let params = *srv.params();
        let (jobs, _handle) = scheduler::spawn(srv, self.max_batch, self.window);
        Ok(Generation {
            manifest_json: manifest.to_json(),
            stamp: manifest.stamp_for(snapshot_block),
            manifest,
            params,
            snapshot_block,
            stash,
            jobs,
        })
    }
}
