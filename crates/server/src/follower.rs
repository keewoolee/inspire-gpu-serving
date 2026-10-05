//! Live loop: feed account changes into the sidecar and the host-side truth,
//! and periodically rebuild + flip the generation. Two sources: the real
//! chain (JSON-RPC) or a simulator (random updates on a block clock).

use crate::generation::{GenerationBuilder, ServingState};
use pir_chain::rpc::{account_update_to_value, EthRpc};
use pir_keyword::cuckoo::address_from_index;
use pir_keyword::storage::storage_value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct FollowerConfig {
    /// Rebuild + flip the generation this often.
    pub rebuild_every: Duration,
    /// Poll the chain head this often.
    pub poll_every: Duration,
    /// What the served table holds, and so what to take from each block.
    pub feed: Feed,
}

/// What a table follows the chain for.
pub enum Feed {
    /// Every account's balance and nonce.
    Accounts,
    /// Every storage slot of these contracts.
    Storage(Vec<[u8; 20]>),
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
    }
}

/// Runs forever. Call from a dedicated thread.
pub fn run(
    rpc: EthRpc,
    builder: Arc<Mutex<GenerationBuilder>>,
    state: Arc<ServingState>,
    start_block: u64,
    cfg: FollowerConfig,
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

    loop {
        // 1. Pull new blocks into the sidecar + host truth.
        match rpc.block_number() {
            Ok(head) if head > synced_to => {
                for b in (synced_to + 1)..=head {
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
                                    },
                                    state.sidecar.len()
                                );
                            }
                        }
                        Err(e) => {
                            fail_streak += 1;
                            let wait = backoff(fail_streak);
                            eprintln!(
                                "block #{} fetch failed: {} (backing off {}s)",
                                b,
                                e,
                                wait.as_secs()
                            );
                            std::thread::sleep(wait);
                            break;
                        }
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                fail_streak += 1;
                let wait = backoff(fail_streak);
                eprintln!("block_number failed: {} (backing off {}s)", e, wait.as_secs());
                std::thread::sleep(wait);
            }
        }

        // 2. Periodic generation flip: rebuild from the maintained truth,
        //    swap, truncate the sidecar through the retiring snapshot.
        if last_rebuild.elapsed() >= cfg.rebuild_every {
            let next = {
                let mut bld = builder.lock().unwrap();
                bld.build(synced_to)
            };
            match next {
                Ok(g) => {
                    state.swap(g);
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
                }
                Err(e) => {
                    // Back off a full rebuild interval; retrying every poll
                    // tick would hammer a failing GPU.
                    last_rebuild = Instant::now();
                    eprintln!("generation rebuild failed: {}", e);
                }
            }
        }

        std::thread::sleep(cfg.poll_every);
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

        if last_rebuild.elapsed() >= cfg.rebuild_every {
            let next = {
                let mut bld = builder.lock().unwrap();
                bld.build(block)
            };
            match next {
                Ok(g) => {
                    state.swap(g);
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
