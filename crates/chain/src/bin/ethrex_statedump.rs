//! Dump every mainnet account from an ethrex node's database.
//!
//! The chain follower keeps the served set current but never backfills, so the
//! service needs one full snapshot to start from. An ethrex node already holds
//! one: `account_flatkeyvalue` is a direct address-hash to account-leaf table
//! it maintains for fast reads, so a linear scan of that column family is the
//! snapshot. This reads it through a RocksDB secondary instance, which is
//! read-only and leaves the running node alone.
//!
//! Keys are the account trie's 64-nibble path, which is keccak256(address), and
//! a hash cannot be turned back into an address. That costs nothing here: a
//! client always knows the address it is asking about, so it hashes locally and
//! looks the result up, exactly as the state trie does. The first 20 bytes of
//! the hash are written as the key, which keeps the entry the same size as an
//! address-keyed one.
//!
//! A scan takes long enough that blocks arrive during it, so the snapshot is
//! not one clean block boundary. The header records a block below whatever the
//! data reflects, and the follower replays from there: applying diffs in order
//! leaves every account at its head state, so an early start is always safe.
//!
//! ```text
//! ethrex-statedump --datadir ~/.local/share/ethrex/mainnet --out accounts.csv
//! ```

use clap::Parser;
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, MergeOperands, Options, DB};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

const ACCOUNT_FLATKEYVALUE: &str = "account_flatkeyvalue";
/// ethrex attaches this to `transaction_locations`. RocksDB refuses to open a
/// column family whose recorded merge operator is missing, and this tool never
/// reads that family, so a stub under the same name is enough to get in.
const TX_LOCATIONS_MERGE: &str = "tx_locations_merge";

#[derive(Parser)]
#[command(
    name = "ethrex-statedump",
    about = "Dump all accounts from an ethrex database into a snapshot CSV"
)]
struct Args {
    /// ethrex data directory (the one holding the .sst files).
    #[clap(long)]
    datadir: PathBuf,

    /// Output CSV.
    #[clap(long, default_value = "accounts.csv")]
    out: PathBuf,

    /// Scratch directory for the secondary instance's own files.
    #[clap(long, default_value = "/tmp/ethrex-statedump-secondary")]
    secondary: PathBuf,

    /// JSON-RPC endpoint, used only to stamp the snapshot's block number.
    #[clap(long, default_value = "http://127.0.0.1:8545")]
    eth_rpc: String,

    /// How far below the chain head to stamp the snapshot. The table trails the
    /// node, because state settles into it a layer at a time rather than a block
    /// at a time, and sampling accounts that had just changed put that lag at
    /// exactly 128 blocks. A larger margin is not safer: a node keeps 128 blocks
    /// of state history, so it cannot trace further back than that either, and
    /// a stamp below the window leaves the follower with blocks it can never
    /// replay. The two numbers are the same depth, so 128 is both the floor and
    /// the ceiling.
    #[clap(long, default_value_t = 128)]
    margin: u64,

    /// Skip accounts holding less than this many wei. `--min-balance 1` drops
    /// the ones that hold nothing, which is about half of them, and halves the
    /// table with it. The cost is that a client asking about a dropped account
    /// gets non-membership rather than its nonce; such an account cannot pay
    /// for a transaction anyway, and it rejoins the served set through the
    /// sidecar the moment it receives anything.
    #[clap(long, default_value_t = 0)]
    min_balance: u128,

    /// Stop after this many accounts. For checking the output before committing
    /// to a full scan.
    #[clap(long)]
    limit: Option<u64>,

    /// Read one address and print what the table holds for it, instead of
    /// dumping. A dumped key is a hash and cannot be turned back into an
    /// address, so this is how a dump gets checked against the node: the same
    /// address through `eth_getBalance` has to agree.
    #[clap(long)]
    probe: Vec<String>,
}

fn main() {
    let args = Args::parse();

    let head = match chain_head(&args.eth_rpc) {
        Ok(head) => head,
        Err(e) => {
            eprintln!("could not read the chain head from {}: {e}", args.eth_rpc);
            std::process::exit(1);
        }
    };
    let stamp = head.saturating_sub(args.margin);
    println!("chain head {head}, stamping the snapshot at block {stamp}");

    let db = match open_secondary(&args.datadir, &args.secondary) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("could not open {} as a secondary: {e}", args.datadir.display());
            eprintln!("(the node can keep running; this needs only read access to its files)");
            std::process::exit(1);
        }
    };
    if let Err(e) = db.try_catch_up_with_primary() {
        eprintln!("warning: could not catch up with the primary: {e}");
    }

    let cf = match db.cf_handle(ACCOUNT_FLATKEYVALUE) {
        Some(cf) => cf,
        None => {
            eprintln!("this database has no {ACCOUNT_FLATKEYVALUE} column family");
            std::process::exit(1);
        }
    };

    if !args.probe.is_empty() {
        for address in &args.probe {
            probe_address(&db, &cf, address);
        }
        return;
    }

    let file = match std::fs::File::create(&args.out) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("could not write {}: {e}", args.out.display());
            std::process::exit(1);
        }
    };
    let mut out = BufWriter::with_capacity(16 << 20, file);
    // Every comment goes above the column header, and none of them may contain
    // a column name: the loader treats the first line mentioning one as the
    // header row.
    writeln!(out, "# block={stamp}").unwrap();
    // The server reads this and publishes it in the manifest, so a client
    // derives keys the same way without being told.
    writeln!(out, "# key_derivation=keccak").unwrap();
    if args.min_balance > 0 {
        writeln!(out, "# accounts holding under {} wei are left out", args.min_balance).unwrap();
    }
    writeln!(out, "address,nonce,balance_wei").unwrap();

    let t0 = Instant::now();
    let (mut written, mut skipped, mut filtered) = (0u64, 0u64, 0u64);

    for item in db.iterator_cf(&cf, IteratorMode::Start) {
        let (key, value) = match item {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("\niteration stopped at {written} accounts: {e}");
                break;
            }
        };
        let hash = match nibbles_to_hash(&key) {
            Some(hash) => hash,
            None => {
                skipped += 1;
                continue;
            }
        };
        let (nonce, balance) = match decode_account(&value) {
            Some(account) => account,
            None => {
                skipped += 1;
                continue;
            }
        };
        if balance < args.min_balance {
            filtered += 1;
            continue;
        }
        writeln!(out, "{},{},{}", hex::encode(&hash[..20]), nonce, balance).unwrap();

        written += 1;
        if written % 5_000_000 == 0 {
            let secs = t0.elapsed().as_secs_f64();
            println!(
                "  {written} accounts ({:.0}k/s, {:.1} min elapsed)",
                written as f64 / secs / 1000.0,
                secs / 60.0
            );
        }
        if args.limit.is_some_and(|limit| written >= limit) {
            break;
        }
    }

    out.flush().unwrap();
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "wrote {written} accounts to {} in {:.1} min",
        args.out.display(),
        secs / 60.0
    );
    if filtered > 0 {
        let seen = written + filtered;
        println!(
            "  {filtered} of {seen} held less than {} wei and were left out ({:.1}%)",
            args.min_balance,
            100.0 * filtered as f64 / seen as f64
        );
    }
    if skipped > 0 {
        println!("  {skipped} entries could not be read");
    }
}

/// Open every column family read-only next to the running node.
fn open_secondary(primary: &PathBuf, secondary: &PathBuf) -> Result<DB, rocksdb::Error> {
    let mut opts = Options::default();
    opts.create_if_missing(false);

    // Every existing family has to be opened, so ask the database which ones
    // there are rather than tracking ethrex's list.
    let names = DB::list_cf(&Options::default(), primary)?;
    let descriptors = names.into_iter().map(|name| {
        let mut cf_opts = Options::default();
        if name == "transaction_locations" {
            cf_opts.set_merge_operator_associative(TX_LOCATIONS_MERGE, keep_newest_operand);
        }
        ColumnFamilyDescriptor::new(name, cf_opts)
    });

    DB::open_cf_descriptors_as_secondary(&opts, primary, secondary, descriptors)
}

/// Read one address out of the flat table, for checking a dump against the
/// node. Tries the key both with and without the leaf terminator and says which
/// one the table actually uses.
fn probe_address<C: rocksdb::AsColumnFamilyRef>(db: &DB, cf: &C, address: &str) {
    let trimmed = address.strip_prefix("0x").unwrap_or(address);
    let bytes = match hex::decode(trimmed) {
        Ok(bytes) if bytes.len() == 20 => bytes,
        _ => {
            println!("{address}: not a 20-byte hex address");
            return;
        }
    };
    let hash = keccak256(&bytes);
    println!("{address}");
    println!("  key           {}", hex::encode(&hash[..20]));

    for (shape, key) in [
        ("64 nibbles + terminator", hash_to_nibbles(&hash, true)),
        ("64 nibbles", hash_to_nibbles(&hash, false)),
    ] {
        match db.get_cf(cf, &key) {
            Ok(Some(value)) => match decode_account(&value) {
                Some((nonce, balance)) => {
                    println!("  found as     {shape}");
                    println!("  nonce         {nonce}");
                    println!("  balance       {balance} wei");
                    return;
                }
                None => println!("  {shape}: leaf present but undecodable"),
            },
            Ok(None) => {}
            Err(e) => println!("  {shape}: read failed: {e}"),
        }
    }
    println!("  not present under either key shape");
}

fn keccak256(bytes: &[u8]) -> [u8; 32] {
    use sha3::Digest;
    sha3::Keccak256::digest(bytes).into()
}

/// The table's key: one nibble per byte, high nibble first.
fn hash_to_nibbles(hash: &[u8; 32], terminator: bool) -> Vec<u8> {
    let mut key: Vec<u8> = Vec::with_capacity(65);
    for &byte in hash {
        key.push(byte >> 4);
        key.push(byte & 0xf);
    }
    if terminator {
        key.push(16);
    }
    key
}

/// Stands in for ethrex's merge operator on a family this tool never reads.
fn keep_newest_operand(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    operands
        .into_iter()
        .last()
        .map(|operand| operand.to_vec())
        .or_else(|| existing.map(|existing| existing.to_vec()))
}

/// Pack the trie path back into the address hash it came from. ethrex stores
/// one nibble per byte, high nibble first, with a terminator past the 64th.
fn nibbles_to_hash(key: &[u8]) -> Option<[u8; 32]> {
    if key.len() < 64 {
        return None;
    }
    let mut hash = [0u8; 32];
    for (i, byte) in hash.iter_mut().enumerate() {
        let (high, low) = (key[2 * i], key[2 * i + 1]);
        if high > 0xf || low > 0xf {
            return None;
        }
        *byte = (high << 4) | low;
    }
    Some(hash)
}

/// An account leaf is RLP `[nonce, balance, storage_root, code_hash]`.
/// Only the first two are served, and a balance fits u128 many times over.
fn decode_account(value: &[u8]) -> Option<(u64, u128)> {
    let (fields, _) = rlp_list(value)?;
    let (nonce, rest) = rlp_item(fields)?;
    let (balance, _) = rlp_item(rest)?;
    Some((be_u64(nonce)?, be_u128(balance)?))
}

/// One RLP byte string: its payload, and whatever follows it.
fn rlp_item(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let (&first, tail) = input.split_first()?;
    match first {
        0x00..=0x7f => Some((&input[..1], tail)),
        0x80..=0xb7 => split_at_checked(tail, (first - 0x80) as usize),
        0xb8..=0xbf => rlp_long(tail, (first - 0xb7) as usize),
        _ => None,
    }
}

/// An RLP list header: its payload, and whatever follows it.
fn rlp_list(input: &[u8]) -> Option<(&[u8], &[u8])> {
    let (&first, tail) = input.split_first()?;
    match first {
        0xc0..=0xf7 => split_at_checked(tail, (first - 0xc0) as usize),
        0xf8..=0xff => rlp_long(tail, (first - 0xf7) as usize),
        _ => None,
    }
}

fn rlp_long(tail: &[u8], length_bytes: usize) -> Option<(&[u8], &[u8])> {
    let (raw, rest) = split_at_checked(tail, length_bytes)?;
    split_at_checked(rest, usize::try_from(be_u64(raw)?).ok()?)
}

fn split_at_checked(input: &[u8], at: usize) -> Option<(&[u8], &[u8])> {
    (input.len() >= at).then(|| input.split_at(at))
}

/// RLP integers are minimal big-endian, and zero is the empty string.
fn be_u64(bytes: &[u8]) -> Option<u64> {
    (bytes.len() <= 8).then(|| bytes.iter().fold(0u64, |acc, &b| (acc << 8) | b as u64))
}

fn be_u128(bytes: &[u8]) -> Option<u128> {
    (bytes.len() <= 16).then(|| bytes.iter().fold(0u128, |acc, &b| (acc << 8) | b as u128))
}

fn chain_head(url: &str) -> Result<u64, String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []
    });
    let mut response = ureq::post(url)
        .header("Content-Type", "application/json")
        .send_json(&body)
        .map_err(|e| e.to_string())?;
    let json: serde_json::Value = response
        .body_mut()
        .read_json()
        .map_err(|e| e.to_string())?;
    let hex = json["result"].as_str().ok_or("no result field")?;
    u64::from_str_radix(hex.strip_prefix("0x").unwrap_or(hex), 16).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nibbles_pack_back_into_the_address_hash() {
        let hash: Vec<u8> = (0u8..32).collect();
        let mut key: Vec<u8> = hash.iter().flat_map(|b| [b >> 4, b & 0xf]).collect();
        key.push(16); // leaf terminator
        assert_eq!(nibbles_to_hash(&key).unwrap().to_vec(), hash);
    }

    #[test]
    fn short_or_malformed_paths_are_rejected() {
        assert!(nibbles_to_hash(&[0u8; 63]).is_none());
        let mut bad = vec![0u8; 65];
        bad[7] = 0xff; // not a nibble
        assert!(nibbles_to_hash(&bad).is_none());
    }

    /// An account leaf, as the trie stores it. Zero is the empty string, and a
    /// balance past u64 has to survive.
    #[test]
    fn decodes_an_account_leaf() {
        // [nonce=1, balance=0x0de0b6b3a7640000, storage_root, code_hash]
        let mut leaf = vec![0xf8, 0x4c, 0x01, 0x88];
        leaf.extend_from_slice(&0x0de0_b6b3_a764_0000u64.to_be_bytes());
        leaf.push(0xa0);
        leaf.extend_from_slice(&[0x11; 32]);
        leaf.push(0xa0);
        leaf.extend_from_slice(&[0x22; 32]);
        assert_eq!(decode_account(&leaf), Some((1, 1_000_000_000_000_000_000)));
    }

    #[test]
    fn decodes_an_empty_account() {
        // [nonce=0, balance=0, storage_root, code_hash]
        let mut leaf = vec![0xf8, 0x44, 0x80, 0x80, 0xa0];
        leaf.extend_from_slice(&[0x11; 32]);
        leaf.push(0xa0);
        leaf.extend_from_slice(&[0x22; 32]);
        assert_eq!(decode_account(&leaf), Some((0, 0)));
    }

    #[test]
    fn decodes_a_balance_past_u64() {
        // 26780479053084510844 wei, which needs nine bytes.
        let mut leaf = vec![0xf8, 0x4d, 0x09, 0x89];
        leaf.extend_from_slice(&[0x01, 0x73, 0xa7, 0x5f, 0xcf, 0x49, 0xe4, 0x4a, 0x7c]);
        leaf.push(0xa0);
        leaf.extend_from_slice(&[0x11; 32]);
        leaf.push(0xa0);
        leaf.extend_from_slice(&[0x22; 32]);
        assert_eq!(decode_account(&leaf), Some((9, 26_780_479_053_084_510_844)));
    }
}
