//! Standalone tool to resync an Ethereum accounts CSV to the current chain head.
//!
//! Usage:
//!   cargo run --release --bin resync -- --snapshot accounts.csv \
//!     --eth-rpc https://eth-mainnet.g.alchemy.com/v2/KEY
//!
//! This loads the CSV, backfills missing blocks via JSON-RPC, and writes
//! an updated CSV (overwriting the input or writing to --output).

use clap::Parser;
use std::path::Path;
use std::time::Instant;

use pir_chain::rpc::EthRpc;
use pir_keyword::cuckoo::*;

#[derive(Parser)]
#[command(name = "resync", about = "Resync Ethereum accounts CSV to chain head")]
struct Args {
    /// Input CSV snapshot file.
    #[clap(long)]
    snapshot: String,

    /// Ethereum JSON-RPC URL.
    #[clap(long)]
    eth_rpc: String,

    /// Output CSV file (defaults to overwriting the input).
    #[clap(long)]
    output: Option<String>,

    /// Number of buckets (must match server config).
    #[clap(long, default_value_t = 536_870_912)]
    buckets: usize,

    /// Stop at this block number instead of chain head.
    #[clap(long)]
    target_block: Option<u64>,
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let t_total = Instant::now();

    // Load CSV into cuckoo table
    let seed = DETERMINISTIC_SEED;
    let cuckoo_params = CuckooParams::new(args.buckets, 20, 40, seed);
    let mut table = CuckooTable::new(cuckoo_params);

    eprintln!("Loading snapshot: {}", args.snapshot);
    let (snap_block, num_accounts) = table
        .build_from_csv(Path::new(&args.snapshot))
        .expect("failed to parse CSV");
    eprintln!("Loaded {} accounts at block #{}.", num_accounts, snap_block);

    // Connect to RPC and resync
    let rpc = EthRpc::new(&args.eth_rpc);
    let head = rpc.block_number().expect("failed to get chain head");

    if let Some(t) = args.target_block {
        if t <= snap_block {
            eprintln!(
                "Snapshot is already at or ahead of target block #{}. Nothing to do.",
                t
            );
            return;
        }
        eprintln!("Chain head: #{}. Syncing to fixed target #{}...", head, t);
    } else {
        if head <= snap_block {
            eprintln!("Snapshot is already at chain head #{}. Nothing to do.", head);
            return;
        }
        eprintln!(
            "Chain head: #{}. Snapshot is {} blocks behind. Resyncing (will repeat until caught up)...",
            head,
            head - snap_block
        );
    }

    let result = rpc
        .resync(&mut table, snap_block, args.target_block)
        .expect("resync failed");

    // Write updated CSV
    let output_path = args.output.as_deref().unwrap_or(&args.snapshot);
    eprintln!("Writing updated snapshot to: {}", output_path);
    let exported = table
        .export_csv(Path::new(output_path), result.last_block)
        .expect("failed to write CSV");

    eprintln!(
        "\nDone. {} accounts, block #{} -> #{}, {} block changes, {:.1}s total.",
        exported,
        snap_block,
        result.last_block,
        result.total_changes,
        t_total.elapsed().as_secs_f64()
    );
}
