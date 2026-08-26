//! CLI for private lookups against a pir-server.
//!
//!   pir-client --server http://host:8080 lookup 0xADDRESS
//!   pir-client --server http://host:8080 synthetic 12345
//!   pir-client --server http://host:8080 canary
//!   pir-client --server http://host:8080 manifest

use clap::{Parser, Subcommand};
use pir_client::{parse_account_value, PirClient, Source};
use pir_keyword::cuckoo::address_from_index;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "pir-client", about = "Private lookups against a pir-server")]
struct Cli {
    /// Server base URL, e.g. http://127.0.0.1:8080
    #[clap(long)]
    server: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Look up an Ethereum address (0x... 20 bytes).
    Lookup { address: String },
    /// Look up synthetic account i (address = SHAKE-256(i), for demo DBs).
    Synthetic { index: usize },
    /// Retrieve the canary: which snapshot block is this server answering from?
    Canary,
    /// Print the current generation manifest.
    Manifest,
}

fn main() {
    let cli = Cli::parse();
    let mut client = PirClient::connect(&cli.server).unwrap_or_else(|e| {
        eprintln!("connect failed: {}", e);
        std::process::exit(1);
    });

    match cli.cmd {
        Cmd::Manifest => {
            println!("{}", client.manifest.to_json());
        }
        Cmd::Canary => {
            let t0 = Instant::now();
            match client.canary_block().unwrap_or_else(die) {
                Some(block) => println!(
                    "canary: serving snapshot of block #{} ({:.0} ms)",
                    block,
                    t0.elapsed().as_secs_f64() * 1e3
                ),
                None => println!("canary entry not found (server without canary?)"),
            }
        }
        Cmd::Lookup { address } => {
            let hexpart = address.strip_prefix("0x").unwrap_or(&address);
            let key = hex::decode(hexpart).unwrap_or_else(|e| {
                eprintln!("bad address hex: {}", e);
                std::process::exit(1);
            });
            if key.len() != client.manifest.cuckoo.key_size {
                eprintln!("address must be {} bytes", client.manifest.cuckoo.key_size);
                std::process::exit(1);
            }
            run_lookup(&mut client, &key);
        }
        Cmd::Synthetic { index } => {
            let key = address_from_index(index);
            println!("address: 0x{}", hex::encode(&key));
            run_lookup(&mut client, &key);
        }
    }
}

fn run_lookup(client: &mut PirClient, key: &[u8]) {
    let t0 = Instant::now();
    match client.lookup(key).unwrap_or_else(die) {
        Some(l) => {
            let ms = t0.elapsed().as_secs_f64() * 1e3;
            match parse_account_value(&l.value) {
                Some(a) => println!("balance: {} wei\nnonce:   {}", a.balance, a.nonce),
                None => println!("value: 0x{}", hex::encode(&l.value)),
            }
            match l.source {
                Source::Snapshot { block } => println!(
                    "source:  PIR (snapshot block #{}), {:.0} ms",
                    block, ms
                ),
                Source::Sidecar { block } => {
                    println!("source:  sidecar broadcast (block #{}), {:.0} ms", block, ms)
                }
            }
        }
        None => {
            println!(
                "not found (proven non-membership at snapshot block #{}), {:.0} ms",
                client.last_snapshot_block,
                t0.elapsed().as_secs_f64() * 1e3
            );
        }
    }
}

fn die<T>(e: String) -> T {
    eprintln!("error: {}", e);
    std::process::exit(1);
}
