//! Live loop: feed account changes into the sidecar and the host-side truth,
//! and periodically rebuild + flip the generation. Two sources: the real
//! chain (JSON-RPC) or a simulator (random updates on a block clock).

use crate::generation::{Generation, GenerationBuilder, ServingState};
use crate::names::NamesTracker;
use pir_chain::rpc::{account_update_to_value, EthRpc};
use pir_keyword::cuckoo::address_from_index;
use pir_keyword::storage::storage_value;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How many recent blocks a node can trace (128 on ethrex). A storage table
/// has no other source of changes, so once the next block it needs is older
/// than this, it can never catch up again and needs a fresh dump.
pub const TRACE_WINDOW: u64 = 128;

/// Blocks taken before the flip timer is looked at again. A server far behind
/// the chain keeps flipping while it catches up, so its sidecar, which every
/// lookup carries whole, holds a few flips' worth of changes rather than all
/// of the catch-up.
const BLOCKS_PER_PASS: u64 = 16;

pub struct FollowerConfig {
    /// Rebuild + flip the generation this often.
    pub rebuild_every: Duration,
    /// Poll the chain head this often.
    pub poll_every: Duration,
    /// What the served table holds, and so what to take from each block.
    pub feed: Feed,
    /// Save a storage table this often, so a restart can start from it.
    pub checkpoint: Option<Checkpoint>,
}

/// Where and how often a table saves itself, replacing the file whole so a
/// crash mid-write leaves the last complete save. A restart within the trace
/// window of the last save picks up where it left off; a longer outage needs
/// a fresh dump.
pub struct Checkpoint {
    pub path: PathBuf,
    pub every: CheckpointEvery,
    pub format: CheckpointFormat,
}

pub enum CheckpointEvery {
    Time(Duration),
    /// Counted in blocks, which is how the trace window is counted: a save
    /// at most this many blocks old, plus the restart, has to fit inside it.
    Blocks(u64),
}

pub enum CheckpointFormat {
    /// A storage snapshot CSV, saved over the file the server started from.
    StorageCsv,
    /// The table as it sits in memory (`CuckooTable::write_checkpoint`).
    Binary,
}

/// Save a copy of the table as of `block`.
fn save_checkpoint(table: &pir_keyword::cuckoo::CuckooTable, cp_path: &PathBuf, binary: bool, block: u64) {
    let t0 = Instant::now();
    let partial = PathBuf::from(format!("{}.partial", cp_path.display()));
    let written = if binary {
        table.write_checkpoint(&partial, block).map(|bytes| format!("{:.1} GB", bytes as f64 / 1e9))
    } else {
        table.write_storage_csv(&partial, block).map(|n| format!("{n} slots"))
    };
    match written.and_then(|what| std::fs::rename(&partial, cp_path).map(|_| what)) {
        Ok(what) => eprintln!(
            "checkpoint: {} at block #{} saved to {} in {:.1}s",
            what,
            block,
            cp_path.display(),
            t0.elapsed().as_secs_f64()
        ),
        Err(e) => eprintln!("checkpoint at block #{} failed: {}", block, e),
    }
}

/// The generation the last flip retired, until its GPU memory is free. The
/// next generation is built only then, so the card never holds three: the
/// retired one answers what it already received and lets go, and a request
/// holds a generation only while its reply is built.
#[derive(Default)]
struct Retired {
    gpu: Option<Weak<()>>,
    waiting_since: Option<Instant>,
}

impl Retired {
    fn hold(&mut self, gen: Arc<Generation>) {
        self.gpu = Some(gen.gpu_held.clone());
    }

    /// Whether the GPU is free for the next generation. A wait that is not
    /// over at once is logged when it ends.
    fn released(&mut self) -> bool {
        let held = self.gpu.as_ref().is_some_and(|w| w.upgrade().is_some());
        if held {
            self.waiting_since.get_or_insert_with(Instant::now);
            return false;
        }
        self.gpu = None;
        if let Some(t) = self.waiting_since.take() {
            eprintln!(
                "retired generation released the GPU after {:.1}s more; building the next one",
                t.elapsed().as_secs_f64()
            );
        }
        true
    }
}

/// What a table follows the chain for.
pub enum Feed {
    /// Every account's balance and nonce.
    Accounts,
    /// Every storage slot of these contracts.
    Storage(Vec<[u8; 20]>),
    /// Every address's primary names. The tracker takes blocks itself, since
    /// what it asks depends on what each block changed.
    Names(Box<NamesTracker>),
}

/// Apply one block's changes to the host-side truth and the sidecar, and
/// return how many there were.
fn ingest_block(
    rpc: &EthRpc,
    feed: &Feed,
    block: u64,
    builder: &Mutex<GenerationBuilder>,
    state: &ServingState,
) -> Result<usize, String> {
    match feed {
        Feed::Accounts => {
            let (_, updates) = rpc.fetch_block_updates(block)?;
            let mut bld = builder.lock().unwrap();
            for u in &updates {
                let value = account_update_to_value(u);
                let key = bld.apply_account(&u.address, &value);
                state.sidecar.push(&key, &value, block);
            }
            Ok(updates.len())
        }
        Feed::Storage(contracts) => {
            let updates = rpc.fetch_block_storage_updates(block, contracts)?;
            let mut bld = builder.lock().unwrap();
            for u in &updates {
                // An emptied slot is written as zero rather than deleted: it
                // reads the same, and the sidecar can carry it.
                let value = storage_value(&u.value);
                let key = bld.apply_storage(&u.contract, &u.slot, &value);
                state.sidecar.push(&key, &value, block);
            }
            Ok(updates.len())
        }
        Feed::Names(_) => unreachable!("a name table is followed by its tracker"),
    }
}

/// One tracker step: take what it changed into the host-side truth and the
/// sidecar. Changes are stamped with the block the tracker reached, which
/// their answers reflect at least.
fn step_names(
    tracker: &mut NamesTracker,
    builder: &Mutex<GenerationBuilder>,
    state: &ServingState,
) -> Result<usize, String> {
    let changes = tracker.step()?;
    let block = tracker.synced_to;
    if let Some(list) = tracker.take_stealth_registry() {
        eprintln!("stealth registry: {} registrations as of block #{block}", list.len());
        *state.stealth_registry.lock().unwrap() = list;
    }
    if let Some(list) = tracker.take_stealth_texts() {
        eprintln!("stealth texts: {} set records as of block #{block}", list.len());
        *state.stealth_texts.lock().unwrap() = list;
    }
    let mut bld = builder.lock().unwrap();
    for c in &changes {
        let key = bld.apply_account(&c.address, &c.value);
        state.sidecar.push(&key, &c.value, block);
    }
    // Names change a few times an hour, so each one is worth a line.
    if changes.len() <= 50 {
        for c in &changes {
            eprintln!("names: 0x{} -> {}", hex::encode(c.address), describe_names(&c.value));
        }
    }
    Ok(changes.len())
}

fn describe_names(value: &[u8]) -> String {
    use pir_keyword::names::{NameEntry, NameSystem, Names};
    let Ok(names) = Names::unpack(value) else { return "unreadable value".into() };
    if names.is_empty() {
        return "no name".into();
    }
    NameSystem::ALL
        .iter()
        .filter_map(|&s| {
            names.get(s).as_ref().map(|e| match e {
                NameEntry::Name(n) => format!("{} {}", s.label(), n),
                NameEntry::TooLong => format!("{} (too long)", s.label()),
            })
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Runs forever. Call from a dedicated thread.
pub fn run(
    rpc: EthRpc,
    builder: Arc<Mutex<GenerationBuilder>>,
    state: Arc<ServingState>,
    start_block: u64,
    mut cfg: FollowerConfig,
) {
    let mut synced_to = start_block;
    let mut last_rebuild = Instant::now();
    // Snapshot of the generation currently serving; sidecar entries are
    // deleted only once the generation that still needs them retires, i.e.
    // each flip truncates through the PREVIOUS snapshot, not the new one.
    let mut serving_snapshot = start_block;
    // Escalating backoff on RPC failures: retrying a rate-limited endpoint
    // every poll tick keeps it rate-limited.
    let mut fail_streak: u32 = 0;
    let backoff = |streak: u32| Duration::from_secs((1u64 << streak.min(6)).min(60));
    let mut last_checkpoint = Instant::now();
    let mut checkpoint_writer: Option<JoinHandle<()>> = None;
    // Whether the last pass stopped short of the chain head.
    let mut behind = false;
    let mut retired = Retired::default();
    let mut last_checkpoint_block = start_block;

    loop {
        // 1. Pull new blocks into the sidecar + host truth.
        if let Feed::Names(tracker) = &mut cfg.feed {
            match step_names(tracker, &builder, &state) {
                Ok(changes) => {
                    fail_streak = 0;
                    synced_to = tracker.synced_to;
                    if changes > 0 {
                        eprintln!(
                            "block #{}: {} name changes (sidecar {} entries)",
                            synced_to,
                            changes,
                            state.sidecar.len()
                        );
                    }
                }
                Err(e) => {
                    // A name table has no source but the state diffs, so it
                    // stops once the next block is out of reach, as storage does.
                    let behind = rpc.block_number().map(|head| head.saturating_sub(synced_to + 1));
                    if matches!(behind, Ok(b) if b >= TRACE_WINDOW) {
                        eprintln!(
                            "block #{} can no longer be traced ({}). Stopping, since this \
                             table cannot catch up without starting over",
                            synced_to + 1,
                            e
                        );
                        std::process::exit(1);
                    }
                    fail_streak += 1;
                    let wait = backoff(fail_streak);
                    eprintln!("names step failed: {} (backing off {}s)", e, wait.as_secs());
                    std::thread::sleep(wait);
                }
            }
        } else {
            match rpc.block_number() {
                Ok(head) if head > synced_to => {
                    behind = head > synced_to + BLOCKS_PER_PASS;
                    for b in (synced_to + 1)..=head.min(synced_to + BLOCKS_PER_PASS) {
                        match ingest_block(&rpc, &cfg.feed, b, &builder, &state) {
                            Ok(changes) => {
                                fail_streak = 0;
                                synced_to = b;
                                if changes > 0 {
                                    eprintln!(
                                        "block #{}: {} {} changes (sidecar {} entries)",
                                        b,
                                        changes,
                                        match cfg.feed {
                                            Feed::Accounts => "account",
                                            Feed::Storage(_) => "storage",
                                            Feed::Names(_) => "name",
                                        },
                                        state.sidecar.len()
                                    );
                                }
                            }
                            Err(e) => {
                                // Accounts fall back to weaker sources inside the
                                // fetch. Storage cannot, so once the block is out
                                // of reach, serving on would only serve stale
                                // values. Stopping makes the outage visible.
                                if matches!(cfg.feed, Feed::Storage(_))
                                    && head.saturating_sub(b) >= TRACE_WINDOW
                                {
                                    eprintln!(
                                        "block #{} can no longer be traced ({}). Stopping, since this \
                                         table cannot catch up without a fresh dump",
                                        b, e
                                    );
                                    std::process::exit(1);
                                }
                                fail_streak += 1;
                                let wait = backoff(fail_streak);
                                eprintln!(
                                    "block #{} fetch failed: {} (backing off {}s)",
                                    b,
                                    e,
                                    wait.as_secs()
                                );
                                std::thread::sleep(wait);
                                behind = false;
                                break;
                            }
                        }
                    }
                }
                Ok(_) => behind = false,
                Err(e) => {
                    fail_streak += 1;
                    let wait = backoff(fail_streak);
                    eprintln!("block_number failed: {} (backing off {}s)", e, wait.as_secs());
                    std::thread::sleep(wait);
                    behind = false;
                }
            }
        }

        // 2. Periodic generation flip: rebuild from the maintained truth,
        //    swap, truncate the sidecar through the retiring snapshot.
        if last_rebuild.elapsed() >= cfg.rebuild_every && retired.released() {
            let next = {
                let mut bld = builder.lock().unwrap();
                bld.build(synced_to)
            };
            match next {
                Ok(g) => {
                    retired.hold(state.swap(g));
                    // One-flip retention lag: in-flight lookups answered by
                    // the retiring generation still need its suffix.
                    state.sidecar.truncate_through(serving_snapshot);
                    serving_snapshot = synced_to;
                    last_rebuild = Instant::now();
                    eprintln!(
                        "flipped to snapshot #{} ({} sidecar entries retained)",
                        synced_to,
                        state.sidecar.len()
                    );

                    // Save from a copy, so following goes on while it writes.
                    // The copy is taken here, where the truth is exactly the
                    // state at `synced_to`.
                    let writer_idle = checkpoint_writer.as_ref().map_or(true, |w| w.is_finished());
                    if let Some(cp) = &cfg.checkpoint {
                        let due = match cp.every {
                            CheckpointEvery::Time(every) => last_checkpoint.elapsed() >= every,
                            CheckpointEvery::Blocks(n) => synced_to >= last_checkpoint_block + n,
                        };
                        if due && writer_idle {
                            let table = builder.lock().unwrap().table.clone();
                            let (path, block) = (cp.path.clone(), synced_to);
                            let binary = matches!(cp.format, CheckpointFormat::Binary);
                            checkpoint_writer = Some(std::thread::spawn(move || {
                                save_checkpoint(&table, &path, binary, block)
                            }));
                            last_checkpoint = Instant::now();
                            last_checkpoint_block = synced_to;
                        }
                    }
                }
                Err(e) => {
                    // Back off a full rebuild interval; retrying every poll
                    // tick would hammer a failing GPU.
                    last_rebuild = Instant::now();
                    eprintln!("generation rebuild failed: {}", e);
                }
            }
        }

        // A server still catching up goes straight on to the next pass.
        if !behind {
            std::thread::sleep(cfg.poll_every);
        }
    }
}

// ============================================================================
// Simulated chain
// ============================================================================

pub struct SimulatorConfig {
    /// Account updates arriving per block.
    pub updates_per_block: usize,
    /// Block time (mainnet: 12 s).
    pub block_secs: u64,
    /// Rebuild + flip the generation this often.
    pub rebuild_every: Duration,
    /// The initial synthetic account count; updates mostly touch these, with
    /// a trickle of brand-new accounts above this index.
    pub initial_accounts: usize,
}

/// Value for simulated account `idx` last touched at `block`:
/// balance = idx, nonce = block — so any lookup shows exactly when the
/// account was last updated.
pub fn sim_value(idx: usize, block: u64) -> Vec<u8> {
    let mut v = vec![0u8; 40];
    v[16..32].copy_from_slice(&(idx as u128).to_be_bytes());
    v[32..40].copy_from_slice(&block.to_be_bytes());
    v
}

/// Runs forever. Same shape as `run`, with random updates instead of RPC.
/// Update j=0 of every block touches account 0 (a heartbeat the demo client
/// can watch); 1 in 8 updates creates a new account; the rest touch random
/// existing ones.
pub fn run_simulated(
    builder: Arc<Mutex<GenerationBuilder>>,
    state: Arc<ServingState>,
    start_block: u64,
    cfg: SimulatorConfig,
) {
    let mut block = start_block;
    let mut next_new = cfg.initial_accounts;
    let mut last_rebuild = Instant::now();
    // One-flip retention lag, as in `run`.
    let mut serving_snapshot = start_block;
    let mut retired = Retired::default();

    loop {
        std::thread::sleep(Duration::from_secs(cfg.block_secs));
        block += 1;
        {
            let mut bld = builder.lock().unwrap();
            for j in 0..cfg.updates_per_block {
                let idx = if j == 0 {
                    0
                } else if j % 8 == 7 {
                    next_new += 1;
                    next_new - 1
                } else {
                    fastrand::usize(0..cfg.initial_accounts)
                };
                let addr = address_from_index(idx);
                let value = sim_value(idx, block);
                let key = bld.apply_account(&addr, &value);
                state.sidecar.push(&key, &value, block);
            }
        }
        eprintln!(
            "block #{}: +{} simulated updates (sidecar {} entries)",
            block,
            cfg.updates_per_block,
            state.sidecar.len()
        );

        if last_rebuild.elapsed() >= cfg.rebuild_every && retired.released() {
            let next = {
                let mut bld = builder.lock().unwrap();
                bld.build(block)
            };
            match next {
                Ok(g) => {
                    retired.hold(state.swap(g));
                    state.sidecar.truncate_through(serving_snapshot);
                    serving_snapshot = block;
                    last_rebuild = Instant::now();
                    eprintln!(
                        "flipped to snapshot #{} ({} sidecar entries retained)",
                        block,
                        state.sidecar.len()
                    );
                }
                Err(e) => {
                    last_rebuild = Instant::now();
                    eprintln!("generation rebuild failed: {}", e);
                }
            }
        }
    }
}
