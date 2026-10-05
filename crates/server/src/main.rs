//! The serving binary: load a snapshot (accounts or contract storage, from
//! CSV) or synthetic accounts, build the first generation, and serve the HTTP
//! API. With --eth-rpc it also follows
//! the chain (sidecar + periodic generation flips); without it, it serves the
//! snapshot statically.

use clap::Parser;
use pir_keyword::cuckoo::{CuckooParams, CuckooTable, DETERMINISTIC_SEED};
use pir_server::follower::{self, Checkpoint, Feed, FollowerConfig, SimulatorConfig, TRACE_WINDOW};
use pir_server::generation::{GenerationBuilder, ServingState};
use pir_server::http::serve;
use pir_server::sidecar::Sidecar;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "pir-server", about = "InsPIRe GPU serving front")]
struct Args {
    /// Accounts CSV snapshot (address,nonce,balance_wei; `# block=N` header).
    #[clap(long, conflicts_with = "synthetic")]
    accounts_csv: Option<String>,

    /// Storage CSV snapshot instead (key,value; `# block=N` and
    /// `# contracts=…` header), as `ethrex-statedump --storage-of` writes it.
    /// The follower then tracks those contracts' storage, not accounts.
    #[clap(long, conflicts_with_all = ["accounts_csv", "synthetic", "simulate"])]
    storage_csv: Option<String>,

    /// With --storage-csv: every this many seconds, save the table over that
    /// file, so a restart starts from recent state. A storage table cannot
    /// catch up from further back than the node traces, so without this a
    /// restart needs a fresh dump.
    #[clap(long, requires = "storage_csv")]
    checkpoint_secs: Option<u64>,

    /// Serve N synthetic accounts instead of a snapshot.
    #[clap(long)]
    synthetic: Option<usize>,

    /// Cuckoo bucket count = PIR entry count (120 B/bucket: 2^23 = 1 GB).
    /// Must be a multiple of db-rows.
    #[clap(long, default_value_t = 1 << 23)]
    buckets: usize,

    /// PIR db_rows geometry (must divide buckets).
    #[clap(long, default_value_t = 32768)]
    db_rows: usize,

    /// GPU batch pool size. Must be even: a lookup's bucket pair enters
    /// the batch as one unit.
    #[clap(long, default_value_t = 8)]
    max_batch: usize,

    /// How long the scheduler waits to fill a batch after the first query, ms.
    #[clap(long, default_value_t = 20)]
    window_ms: u64,

    /// Listen address.
    #[clap(long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// HTTP worker threads (each can hold one in-flight query).
    #[clap(long, default_value_t = 32)]
    http_workers: usize,

    /// Ethereum JSON-RPC URL; enables the chain follower + generation flips.
    #[clap(long)]
    eth_rpc: Option<String>,

    /// Simulate a chain instead: this many random account updates arrive per
    /// block (needs --synthetic; conflicts with --eth-rpc).
    #[clap(long, conflicts_with = "eth_rpc")]
    simulate: Option<usize>,

    /// Simulated block time in seconds.
    #[clap(long, default_value_t = 12)]
    block_secs: u64,

    /// Rebuild + flip the generation this often (seconds; used by both
    /// --eth-rpc and --simulate). Flips cost clients nothing, so this can
    /// go as low as the GPU preprocess time; a short cadence keeps the
    /// sidecar broadcast small.
    #[clap(long, default_value_t = 60)]
    rebuild_secs: u64,

    /// Fixed CRS seed, 128 hex chars (64 bytes). Every machine serving the
    /// same database must use the same seed (a rolling swap included) —
    /// queries target the CRS, and it never changes at a flip. Omitted: a
    /// fresh one is drawn and printed at startup.
    #[clap(long)]
    crs_seed: Option<String>,
}

fn main() {
    let args = Args::parse();
    let t0 = Instant::now();

    if args.max_batch == 0 || args.max_batch % 2 != 0 {
        eprintln!("error: --max-batch must be even (a lookup's bucket pair is batched as a unit)");
        std::process::exit(2);
    }

    // 1. Accounts → cuckoo table.
    let params = CuckooParams::new(args.buckets, 20, 40, DETERMINISTIC_SEED);
    let mut table = CuckooTable::new(params);
    let snapshot_block;
    let mut feed = Feed::Accounts;
    match (&args.accounts_csv, &args.storage_csv, args.synthetic) {
        (Some(csv), _, _) => {
            let (block, n) = table
                .build_from_csv(Path::new(csv))
                .expect("failed to load snapshot CSV");
            eprintln!("Snapshot: {} accounts at block #{}", n, block);
            snapshot_block = block;
        }
        (None, Some(csv), _) => {
            if let Some(url) = &args.eth_rpc {
                refuse_unreachable_snapshot(Path::new(csv), url);
            }
            let (block, n) = table
                .build_from_storage_csv(Path::new(csv))
                .expect("failed to load storage CSV");
            eprintln!("Snapshot: {} storage slots at block #{}", n, block);
            snapshot_block = block;
            feed = Feed::Storage(table.params.contracts.clone());
        }
        (None, None, Some(n)) => {
            eprintln!("Building {} synthetic accounts...", n);
            table.build_accounts_parallel(n);
            snapshot_block = 0;
        }
        (None, None, None) => {
            eprintln!("error: pass --accounts-csv, --storage-csv or --synthetic N");
            std::process::exit(2);
        }
    }
    // Cuckoo-overflow entries (rare) are published through the sidecar
    // broadcast and served in the clear to every client.

    // 2. Fixed CRS seed: supplied (so a second machine can serve the same
    // queries) or drawn fresh and printed for the operator to copy.
    let crs_seed: [u8; 64] = match &args.crs_seed {
        Some(hex_str) => hex::decode(hex_str)
            .ok()
            .and_then(|v| v.try_into().ok())
            .unwrap_or_else(|| {
                eprintln!("error: --crs-seed must be 128 hex chars (64 bytes)");
                std::process::exit(2);
            }),
        None => {
            let mut seed = [0u8; 64];
            let mut rng = fastrand::Rng::new();
            for b in &mut seed {
                *b = rng.u8(..);
            }
            seed
        }
    };
    eprintln!("CRS seed: {}", hex::encode(crs_seed));

    // 3. First generation.
    eprintln!(
        "Building slot matrix ({} buckets, db_rows={})...",
        args.buckets, args.db_rows
    );
    let mut builder = GenerationBuilder::new(
        table,
        args.db_rows,
        args.max_batch,
        Duration::from_millis(args.window_ms),
        crs_seed,
    );
    let first = builder.build(snapshot_block).expect("GPU preprocess failed");
    let state = Arc::new(ServingState::new(first, Arc::new(Sidecar::new())));

    // 4. Chain source (optional): real follower or simulator.
    if let Some(updates_per_block) = args.simulate {
        let initial_accounts = args.synthetic.unwrap_or_else(|| {
            eprintln!("error: --simulate needs --synthetic N");
            std::process::exit(2);
        });
        let builder2 = Arc::new(Mutex::new(builder));
        let state2 = Arc::clone(&state);
        let cfg = SimulatorConfig {
            updates_per_block,
            block_secs: args.block_secs,
            rebuild_every: Duration::from_secs(args.rebuild_secs),
            initial_accounts,
        };
        std::thread::spawn(move || {
            follower::run_simulated(builder2, state2, snapshot_block, cfg)
        });
        eprintln!(
            "Simulated chain on: {} updates per {}s block, flip every {}s.",
            updates_per_block, args.block_secs, args.rebuild_secs
        );
    } else if let Some(url) = &args.eth_rpc {
        let rpc = pir_chain::rpc::EthRpc::new(url);
        let builder = Arc::new(Mutex::new(builder));
        let state2 = Arc::clone(&state);
        let cfg = FollowerConfig {
            rebuild_every: Duration::from_secs(args.rebuild_secs),
            poll_every: Duration::from_secs(3),
            feed,
            checkpoint: args.checkpoint_secs.map(|secs| Checkpoint {
                path: PathBuf::from(args.storage_csv.as_ref().unwrap()),
                every: Duration::from_secs(secs),
            }),
        };
        std::thread::spawn(move || follower::run(rpc, builder, state2, snapshot_block, cfg));
        eprintln!(
            "Chain follower on (rebuild every {}s).",
            args.rebuild_secs
        );
    }

    // 5. HTTP.
    let http = tiny_http::Server::http(&args.listen).expect("failed to bind");
    eprintln!(
        "Serving on {} ({} workers), ready {:.1}s after start.",
        args.listen,
        args.http_workers,
        t0.elapsed().as_secs_f64()
    );
    serve(http, state, args.http_workers);
}

/// Refuse a storage snapshot the follower could never bring current, because
/// the first block it needs is already older than the node can trace. Checked
/// before loading, so a restart after a long outage fails at once and says a
/// fresh dump is needed, rather than loading gigabytes and then serving stale
/// values.
fn refuse_unreachable_snapshot(csv: &Path, url: &str) {
    let stamp = std::fs::File::open(csv).ok().and_then(|f| {
        BufReader::new(f)
            .lines()
            .take(8)
            .map_while(Result::ok)
            .find_map(|line| line.trim().strip_prefix("# block=")?.trim().parse::<u64>().ok())
    });
    // A file without a stamp is the loader's to reject, with its own message.
    let Some(block) = stamp else { return };
    match pir_chain::rpc::EthRpc::new(url).block_number() {
        Ok(head) if head.saturating_sub(block + 1) >= TRACE_WINDOW => {
            eprintln!(
                "error: {} is stamped at block #{}, {} blocks behind the head, and the node \
                 traces only its last {}. The table could never catch up. Take a fresh dump.",
                csv.display(),
                block,
                head - block,
                TRACE_WINDOW
            );
            std::process::exit(1);
        }
        Ok(_) => {}
        Err(e) => {
            eprintln!("error: cannot reach {} to check the snapshot's age: {}", url, e);
            std::process::exit(1);
        }
    }
}
