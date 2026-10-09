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
//! The table trails the node's head by `--margin` blocks, which is also as far
//! back as the node can trace. So before it reads the table, a dump replays
//! those blocks through the node's state diffs and folds them in, and stamps the
//! file at the head it reached. Blocks that arrive while the table is read are
//! left to the follower, from that stamp: applying diffs in order leaves every
//! account at its head state, so starting from a block the data already
//! reflects is safe.
//!
//! An account delegated under EIP-7702 holds the designator `0xef0100 ‖
//! delegate` as its code, so the leaf's code hash leads to it. Codes sit in
//! `account_codes` by hash, with their lengths in `account_code_metadata`, so
//! only a 23-byte code is ever read, and each distinct hash once.
//!
//! Contract storage sits in a second table, `storage_flatkeyvalue`, keyed by
//! the contract's address hash followed by each slot's hash. `--storage-of`
//! dumps the slots of chosen contracts, such as tokens, into a storage
//! snapshot. `--count-storage` counts one contract's slots there, for sizing
//! such a table before building one, and `--probe-mapping` reads one mapping
//! entry, such as a token balance, and compares it with the node.
//!
//! ```text
//! ethrex-statedump --datadir ~/.local/share/ethrex/mainnet --out accounts.csv
//! ```

use clap::Parser;
use pir_chain::rpc::EthRpc;
use pir_keyword::account::delegate_from_code;
use pir_keyword::storage::{mapping_slot, storage_key_from_hashes};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, MergeOperands, Options, DB};
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

const ACCOUNT_FLATKEYVALUE: &str = "account_flatkeyvalue";
const ACCOUNT_CODES: &str = "account_codes";
const ACCOUNT_CODE_METADATA: &str = "account_code_metadata";
/// keccak256 of empty code, which every account without code carries.
const EMPTY_CODE_HASH: [u8; 32] = [
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
];
const STORAGE_FLATKEYVALUE: &str = "storage_flatkeyvalue";
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

    /// How far the table trails the chain head. State settles into it a layer
    /// at a time rather than a block at a time, and sampling accounts that had
    /// just changed put that lag at exactly 128 blocks, which is also as far
    /// back as the node can trace. A dump replays these blocks from the node
    /// before reading the table, so the file it writes is stamped at the head.
    #[clap(long, default_value_t = 128)]
    margin: u64,

    /// Skip accounts holding less than this many wei. `--min-balance 1` drops
    /// the ones that hold nothing, which is about half of them, and halves the
    /// table with it. The cost is that a client asking about a dropped account
    /// gets non-membership rather than its nonce; such an account cannot pay
    /// for a transaction anyway, and it rejoins the served set through the
    /// sidecar the moment it receives anything. An account that delegates its
    /// code is kept whatever it holds, since a wallet asks for its code too,
    /// and a sponsor can pay for its transactions.
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

    /// Dump the storage of this contract instead of the accounts, into a
    /// storage snapshot stamped at the chain head. Repeatable: every contract
    /// named goes into the one file.
    #[clap(long)]
    storage_of: Vec<String>,

    /// Count the storage slots of this contract instead of dumping accounts,
    /// and how many of them hold a value too large to be a token balance.
    /// Repeatable.
    #[clap(long)]
    count_storage: Vec<String>,

    /// Read one mapping entry, CONTRACT:KEY:SLOT, and compare it with the node:
    /// the entry for address KEY in the mapping declared at storage slot SLOT,
    /// such as a token's balance of KEY. Repeatable.
    #[clap(long)]
    probe_mapping: Vec<String>,
}

fn main() {
    let args = Args::parse();
    if !args.storage_of.is_empty() {
        dump_storage(&args);
        return;
    }
    if args.probe.is_empty() && args.count_storage.is_empty() && args.probe_mapping.is_empty() {
        dump_accounts(&args);
        return;
    }

    let head = match chain_head(&args.eth_rpc) {
        Ok(head) => head,
        Err(e) => {
            eprintln!("could not read the chain head from {}: {e}", args.eth_rpc);
            std::process::exit(1);
        }
    };
    let stamp = head.saturating_sub(args.margin);
    println!("chain head {head}, and the table reflects block {stamp}");

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

    if !args.count_storage.is_empty() || !args.probe_mapping.is_empty() {
        let cf = match db.cf_handle(STORAGE_FLATKEYVALUE) {
            Some(cf) => cf,
            None => {
                eprintln!("this database has no {STORAGE_FLATKEYVALUE} column family");
                std::process::exit(1);
            }
        };
        for spec in &args.probe_mapping {
            probe_mapping(&db, &cf, spec, &args.eth_rpc, stamp);
        }
        for address in &args.count_storage {
            count_storage(&db, &cf, address);
        }
        return;
    }

    let cf = match db.cf_handle(ACCOUNT_FLATKEYVALUE) {
        Some(cf) => cf,
        None => {
            eprintln!("this database has no {ACCOUNT_FLATKEYVALUE} column family");
            std::process::exit(1);
        }
    };

    for address in &args.probe {
        probe_address(&db, &cf, address);
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
    let mut codes = Codes::new(db);
    let Some(bytes) = parse_address(address) else {
        println!("{address}: not a 20-byte hex address");
        return;
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
                Some((nonce, balance, code_hash)) => {
                    println!("  found as     {shape}");
                    println!("  nonce         {nonce}");
                    println!("  balance       {balance} wei");
                    println!("  code hash     0x{}", hex::encode(code_hash));
                    match codes.delegate(&code_hash) {
                        Some(d) => println!("  delegate      0x{}", hex::encode(d)),
                        None => println!("  delegate      none"),
                    }
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

/// Account values the replay has seen, by address hash: (nonce, balance,
/// delegate).
type ReplayedAccounts = HashMap<[u8; 32], (u64, u128, Option<[u8; 20]>)>;

/// The delegate each code hash names, read from the node's code tables once
/// per hash.
struct Codes<'a> {
    db: &'a DB,
    seen: HashMap<[u8; 32], Option<[u8; 20]>>,
    /// Code hashes the node holds no code for, which read as no delegate.
    missing: u64,
}

impl<'a> Codes<'a> {
    fn new(db: &'a DB) -> Self {
        Codes { db, seen: HashMap::new(), missing: 0 }
    }

    fn delegate(&mut self, code_hash: &[u8; 32]) -> Option<[u8; 20]> {
        if *code_hash == EMPTY_CODE_HASH {
            return None;
        }
        if let Some(&known) = self.seen.get(code_hash) {
            return known;
        }
        let found = self.read(code_hash);
        self.seen.insert(*code_hash, found);
        found
    }

    /// A designator is 23 bytes, so a code of any other length is skipped
    /// from its length alone, without reading the code.
    fn read(&mut self, code_hash: &[u8; 32]) -> Option<[u8; 20]> {
        let get = |family: &str| {
            let cf = self.db.cf_handle(family).unwrap_or_else(|| fail("no code table", family));
            self.db
                .get_cf(&cf, code_hash)
                .unwrap_or_else(|e| fail(&format!("reading {family}"), e))
        };
        if let Some(length) = get(ACCOUNT_CODE_METADATA) {
            if length.len() == 8 && u64::from_be_bytes(length[..].try_into().unwrap()) != 23 {
                return None;
            }
        }
        let Some(stored) = get(ACCOUNT_CODES) else {
            self.missing += 1;
            return None;
        };
        // The bytecode comes first, as an RLP string, then the node's jump
        // table.
        rlp_item(&stored).and_then(|(code, _)| delegate_from_code(code))
    }
}

/// Write every account as a snapshot CSV, stamped at the chain head, for the
/// reason `dump_storage` gives. Accounts do have a fallback for blocks the node
/// can no longer trace, but it misses accounts changed inside contract calls,
/// so a dump stamped at the table's lag would leave those stale.
fn dump_accounts(args: &Args) {
    let rpc = EthRpc::new(&args.eth_rpc);
    let head = rpc
        .block_number()
        .unwrap_or_else(|e| fail("could not read the chain head", e));
    let mut next = head.saturating_sub(args.margin) + 1;
    let mut replayed = ReplayedAccounts::new();
    let t0 = Instant::now();
    replay_accounts(&rpc, &mut next, &mut replayed)
        .unwrap_or_else(|e| fail("replay failed (rerun if the oldest block left the window)", e));

    let db = open_secondary(&args.datadir, &args.secondary)
        .unwrap_or_else(|e| fail("could not open the database as a secondary", e));
    if let Err(e) = db.try_catch_up_with_primary() {
        eprintln!("warning: could not catch up with the primary: {e}");
    }
    let cf = db
        .cf_handle(ACCOUNT_FLATKEYVALUE)
        .unwrap_or_else(|| fail("no account table", ACCOUNT_FLATKEYVALUE));
    replay_accounts(&rpc, &mut next, &mut replayed).unwrap_or_else(|e| fail("replay failed", e));
    let stamp = next - 1;
    println!(
        "replayed to block {stamp} in {:.1} s ({} accounts touched)",
        t0.elapsed().as_secs_f64(),
        replayed.len()
    );

    let file = std::fs::File::create(&args.out)
        .unwrap_or_else(|e| fail(&format!("could not write {}", args.out.display()), e));
    let mut out = BufWriter::with_capacity(16 << 20, file);
    // Every comment goes above the column header, and none of them may contain
    // a column name: the loader treats the first line mentioning one as the
    // header row.
    writeln!(out, "# block={stamp}").unwrap();
    // The server reads this and publishes it in the manifest, so a client
    // derives keys the same way without being told.
    writeln!(out, "# key_derivation=keccak").unwrap();
    if args.min_balance > 0 {
        writeln!(
            out,
            "# accounts holding under {} wei are left out, unless they delegate",
            args.min_balance
        )
        .unwrap();
    }
    writeln!(out, "address,nonce,balance_wei,delegate").unwrap();

    let t1 = Instant::now();
    let (mut written, mut skipped, mut filtered) = (0u64, 0u64, 0u64);
    let (mut delegated, mut delegated_poor) = (0u64, 0u64);
    let mut write = |out: &mut BufWriter<std::fs::File>,
                     hash: &[u8; 32],
                     nonce: u64,
                     balance: u128,
                     delegate: Option<[u8; 20]>| {
        let poor = balance < args.min_balance;
        if let Some(delegate) = delegate {
            delegated += 1;
            delegated_poor += poor as u64;
            writeln!(out, "{},{},{},0x{}", hex::encode(&hash[..20]), nonce, balance, hex::encode(delegate))
                .unwrap();
            return true;
        }
        if poor {
            filtered += 1;
            return false;
        }
        writeln!(out, "{},{},{},", hex::encode(&hash[..20]), nonce, balance).unwrap();
        true
    };
    let mut codes = Codes::new(&db);

    let mut limited = false;
    for item in db.iterator_cf(&cf, IteratorMode::Start) {
        let (key, value) = item.unwrap_or_else(|e| fail("iteration stopped", e));
        let Some(hash) = nibbles_to_hash(&key) else {
            skipped += 1;
            continue;
        };
        let account = match replayed.remove(&hash) {
            Some(account) => account,
            None => match decode_account(&value) {
                Some((nonce, balance, code_hash)) => (nonce, balance, codes.delegate(&code_hash)),
                None => {
                    skipped += 1;
                    continue;
                }
            },
        };
        if !write(&mut out, &hash, account.0, account.1, account.2) {
            continue;
        }
        written += 1;
        if written % 5_000_000 == 0 {
            let secs = t1.elapsed().as_secs_f64();
            println!(
                "  {written} accounts ({:.0}k/s, {:.1} min elapsed)",
                written as f64 / secs / 1000.0,
                secs / 60.0
            );
        }
        if args.limit.is_some_and(|limit| written >= limit) {
            limited = true;
            break;
        }
    }

    // Accounts the replay created that the table did not have yet. A dump cut
    // short by --limit is a sample and leaves them out.
    let mut added = 0u64;
    if !limited {
        for (hash, (nonce, balance, delegate)) in replayed.drain() {
            if write(&mut out, &hash, nonce, balance, delegate) {
                added += 1;
            }
        }
    }

    out.flush().unwrap();
    println!(
        "wrote {} accounts to {} in {:.1} min ({added} new since the table), stamped at block {stamp}",
        written + added,
        args.out.display(),
        t1.elapsed().as_secs_f64() / 60.0
    );
    if filtered > 0 {
        let seen = written + added + filtered;
        println!(
            "  {filtered} of {seen} held less than {} wei and were left out ({:.1}%)",
            args.min_balance,
            100.0 * filtered as f64 / seen as f64
        );
    }
    println!(
        "  {delegated} delegate their code ({delegated_poor} of them kept despite holding under {} wei), \
         from {} distinct code hashes read",
        args.min_balance,
        codes.seen.len()
    );
    if codes.missing > 0 {
        println!("  {} code hashes had no code in the node, read as no delegate", codes.missing);
    }
    if skipped > 0 {
        println!("  {skipped} entries could not be read");
    }
}

/// Replay blocks into account values, from the same state diffs the follower
/// reads, and with no fallback: a block it cannot trace stops the dump.
fn replay_accounts(rpc: &EthRpc, next: &mut u64, replayed: &mut ReplayedAccounts) -> Result<(), String> {
    replay_to_head(rpc, next, |block| {
        for update in rpc.fetch_block_updates_via_diff(block)? {
            let balance = be_u128(minimal(&update.balance))
                .ok_or_else(|| format!("balance past 128 bits at block {block}"))?;
            replayed.insert(keccak256(&update.address), (update.nonce, balance, update.delegate));
        }
        Ok(())
    })
}

/// Slot values the replay has seen, by contract and slot hash: the hash is
/// what the table is keyed by.
type Replayed = HashMap<([u8; 20], [u8; 32]), [u8; 32]>;

/// Write the storage of the `--storage-of` contracts as a snapshot CSV,
/// stamped at the chain head rather than at the table's lag.
///
/// The table trails the node by `--margin` blocks, which is also as far back as
/// the node can trace, so a snapshot stamped at the table leaves its first
/// blocks at the edge of what a follower can replay, and they fall out of reach
/// while the file is moved and loaded. Accounts have a weaker fallback for
/// that and storage has none. So this replays those blocks itself, starting
/// the moment it reads the head, and folds them into the dump: a slot a
/// replayed block touched takes its value from the last such block, and every
/// other slot is as the table holds it.
fn dump_storage(args: &Args) {
    let contracts: Vec<[u8; 20]> = args
        .storage_of
        .iter()
        .map(|address| {
            parse_address(address).unwrap_or_else(|| {
                eprintln!("{address}: not a 20-byte hex address");
                std::process::exit(2);
            })
        })
        .collect();
    let rpc = EthRpc::new(&args.eth_rpc);

    // The oldest block stays traceable only until the next one arrives, so
    // the replay starts before anything else, the table included.
    let head = rpc
        .block_number()
        .unwrap_or_else(|e| fail("could not read the chain head", e));
    let mut next = head.saturating_sub(args.margin) + 1;
    let mut replayed = Replayed::new();
    let t0 = Instant::now();
    replay_storage(&rpc, &contracts, &mut next, &mut replayed)
        .unwrap_or_else(|e| fail("replay failed (rerun if the oldest block left the window)", e));

    let db = open_secondary(&args.datadir, &args.secondary)
        .unwrap_or_else(|e| fail("could not open the database as a secondary", e));
    if let Err(e) = db.try_catch_up_with_primary() {
        eprintln!("warning: could not catch up with the primary: {e}");
    }
    let cf = db
        .cf_handle(STORAGE_FLATKEYVALUE)
        .unwrap_or_else(|| fail("no storage table", STORAGE_FLATKEYVALUE));
    // The head moved while the table opened. Anything after this pass is left
    // to the follower, from the block the file is stamped with.
    replay_storage(&rpc, &contracts, &mut next, &mut replayed)
        .unwrap_or_else(|e| fail("replay failed", e));
    let stamp = next - 1;
    println!(
        "replayed to block {stamp} in {:.1} s ({} slots touched)",
        t0.elapsed().as_secs_f64(),
        replayed.len()
    );

    let file = std::fs::File::create(&args.out)
        .unwrap_or_else(|e| fail(&format!("could not write {}", args.out.display()), e));
    let mut out = BufWriter::with_capacity(16 << 20, file);
    let listed: Vec<String> = contracts.iter().map(|c| format!("0x{}", hex::encode(c))).collect();
    writeln!(out, "# block={stamp}").unwrap();
    writeln!(out, "# key_derivation=storage").unwrap();
    writeln!(out, "# contracts={}", listed.join(",")).unwrap();
    writeln!(out, "key,value").unwrap();

    for (contract, name) in contracts.iter().zip(&listed) {
        let t1 = Instant::now();
        let contract_hash = keccak256(contract);
        let prefix = storage_prefix(contract);
        let (mut written, mut emptied, mut unreadable) = (0u64, 0u64, 0u64);
        let write = |out: &mut BufWriter<std::fs::File>, slot_hash: &[u8; 32], value: &[u8]| {
            let key = storage_key_from_hashes(&contract_hash, slot_hash, 20);
            writeln!(out, "{},{}", hex::encode(key), hex::encode(value)).unwrap();
        };

        for item in db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward)) {
            let (key, value) = item.unwrap_or_else(|e| fail("iteration stopped", e));
            if !key.starts_with(&prefix) {
                break;
            }
            let Some(slot_hash) = nibbles_to_hash(&key[prefix.len()..]) else {
                unreadable += 1;
                continue;
            };
            let current = match replayed.remove(&(*contract, slot_hash)) {
                Some(word) => minimal(&word).to_vec(),
                None => match decode_storage_value(&value) {
                    Some(bytes) => bytes.to_vec(),
                    None => {
                        unreadable += 1;
                        continue;
                    }
                },
            };
            if current.is_empty() {
                emptied += 1;
                continue;
            }
            write(&mut out, &slot_hash, &current);
            written += 1;
        }

        // Slots the replay filled that the table did not have yet.
        let fresh: Vec<([u8; 32], [u8; 32])> = replayed
            .iter()
            .filter(|((c, _), _)| c == contract)
            .map(|((_, slot_hash), word)| (*slot_hash, *word))
            .collect();
        let mut added = 0u64;
        for (slot_hash, word) in fresh {
            replayed.remove(&(*contract, slot_hash));
            if !minimal(&word).is_empty() {
                write(&mut out, &slot_hash, minimal(&word));
                added += 1;
            }
        }

        println!(
            "{name}: {} slots written in {:.1} s ({added} new since the table, {emptied} emptied)",
            written + added,
            t1.elapsed().as_secs_f64()
        );
        if unreadable > 0 {
            println!("  {unreadable} entries could not be read");
        }
    }
    out.flush().unwrap();
    println!("wrote {} stamped at block {stamp}", args.out.display());
}

fn fail(what: &str, e: impl std::fmt::Display) -> ! {
    eprintln!("{what}: {e}");
    std::process::exit(1);
}

/// Run `apply` on every block from `next` to the head, re-reading the head
/// until it stops moving. Later blocks overwrite earlier ones, so what is kept
/// is each touched value as of the last block replayed.
fn replay_to_head(
    rpc: &EthRpc,
    next: &mut u64,
    mut apply: impl FnMut(u64) -> Result<(), String>,
) -> Result<(), String> {
    loop {
        let head = rpc.block_number()?;
        if *next > head {
            return Ok(());
        }
        while *next <= head {
            apply(*next)?;
            *next += 1;
        }
    }
}

/// Replay blocks into slot values of `contracts`.
fn replay_storage(
    rpc: &EthRpc,
    contracts: &[[u8; 20]],
    next: &mut u64,
    replayed: &mut Replayed,
) -> Result<(), String> {
    replay_to_head(rpc, next, |block| {
        for update in rpc.fetch_block_storage_updates(block, contracts)? {
            replayed.insert((update.contract, keccak256(&update.slot)), update.value);
        }
        Ok(())
    })
}

/// A big-endian integer without its leading zero bytes, as the table stores
/// it. Zero comes out empty.
fn minimal(word: &[u8]) -> &[u8] {
    let start = word.iter().position(|&b| b != 0).unwrap_or(word.len());
    &word[start..]
}

/// Count one contract's slots in the storage table, and how many hold 2^96 or
/// more. No supply of the tokens this is meant for comes near that, so those
/// slots are something other than balances: mostly allowances, many of them
/// unlimited (2^256 - 1).
fn count_storage<C: rocksdb::AsColumnFamilyRef>(db: &DB, cf: &C, address: &str) {
    let Some(contract) = parse_address(address) else {
        println!("{address}: not a 20-byte hex address");
        return;
    };
    let prefix = storage_prefix(&contract);
    let t0 = Instant::now();
    let (mut slots, mut large, mut unlimited, mut unreadable) = (0u64, 0u64, 0u64, 0u64);

    for item in db.iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward)) {
        let (key, value) = match item {
            Ok(pair) => pair,
            Err(e) => {
                println!("  iteration stopped after {slots} slots: {e}");
                break;
            }
        };
        if !key.starts_with(&prefix) {
            break;
        }
        match decode_storage_value(&value) {
            Some(bytes) => {
                slots += 1;
                if bytes.len() > 12 {
                    large += 1;
                }
                if bytes.len() == 32 && bytes.iter().all(|&b| b == 0xff) {
                    unlimited += 1;
                }
            }
            None => unreadable += 1,
        }
    }

    println!(
        "{address}: {slots} slots in {:.1} s, {large} of them at 2^96 or more \
         ({unlimited} at 2^256 - 1)",
        t0.elapsed().as_secs_f64()
    );
    if unreadable > 0 {
        println!("  {unreadable} entries could not be read");
    }
}

/// Read the entry for address KEY in the mapping at slot SLOT of CONTRACT, out
/// of the table and from the node. The table trails the node, so the node is
/// asked at the stamp block as well as at its head.
fn probe_mapping<C: rocksdb::AsColumnFamilyRef>(
    db: &DB,
    cf: &C,
    spec: &str,
    url: &str,
    stamp: u64,
) {
    let parsed = match spec.split(':').collect::<Vec<_>>().as_slice() {
        [contract, key, base] => parse_address(contract)
            .zip(parse_address(key))
            .zip(base.parse::<u64>().ok()),
        _ => None,
    };
    let Some(((contract, key), base)) = parsed else {
        println!("{spec}: expected CONTRACT:KEY:SLOT, two hex addresses and a slot number");
        return;
    };
    let slot = mapping_slot(&key, base);
    let mut flat_key = storage_prefix(&contract);
    flat_key.extend(hash_to_nibbles(&keccak256(&slot), true));

    println!("{spec}");
    println!("  slot          0x{}", hex::encode(slot));
    match db.get_cf(cf, &flat_key) {
        Ok(Some(value)) => match decode_storage_value(&value) {
            Some(bytes) => println!("  table         {}", hex_quantity(bytes)),
            None => println!("  table         undecodable leaf {}", hex::encode(&value)),
        },
        Ok(None) => println!("  table         absent, so zero"),
        Err(e) => println!("  table         read failed: {e}"),
    }
    for (label, block) in [("node @stamp", Some(stamp)), ("node @latest", None)] {
        match storage_at(url, &contract, &slot, block) {
            Ok(value) => println!("  {label:<13} {value}"),
            Err(e) => println!("  {label:<13} {e}"),
        }
    }
}

/// Where a contract's slots begin in the storage table. ethrex writes the
/// address hash as nibbles with a leaf terminator (16), then 17 to keep storage
/// paths apart from account paths, then each slot's own path: the slot hash as
/// nibbles, terminated the same way.
fn storage_prefix(contract: &[u8; 20]) -> Vec<u8> {
    let mut prefix = hash_to_nibbles(&keccak256(contract), true);
    prefix.push(17);
    prefix
}

/// A storage leaf is the RLP of the slot's value, a minimal big-endian integer
/// of at most 32 bytes.
fn decode_storage_value(value: &[u8]) -> Option<&[u8]> {
    let (bytes, rest) = rlp_item(value)?;
    (rest.is_empty() && bytes.len() <= 32).then_some(bytes)
}

fn parse_address(s: &str) -> Option<[u8; 20]> {
    hex::decode(s.strip_prefix("0x").unwrap_or(s)).ok()?.try_into().ok()
}

/// A big-endian integer as a JSON-RPC quantity: 0x, then no leading zeros.
fn hex_quantity(bytes: &[u8]) -> String {
    let digits = hex::encode(bytes);
    let trimmed = digits.trim_start_matches('0');
    format!("0x{}", if trimmed.is_empty() { "0" } else { trimmed })
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

/// An account leaf is RLP `[nonce, balance, storage_root, code_hash]`. The
/// code hash leads to the account's delegate, if it has one, and a balance
/// fits u128 many times over.
fn decode_account(value: &[u8]) -> Option<(u64, u128, [u8; 32])> {
    let (fields, _) = rlp_list(value)?;
    let (nonce, rest) = rlp_item(fields)?;
    let (balance, rest) = rlp_item(rest)?;
    let (_storage_root, rest) = rlp_item(rest)?;
    let (code_hash, _) = rlp_item(rest)?;
    Some((be_u64(nonce)?, be_u128(balance)?, code_hash.try_into().ok()?))
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

fn rpc_call(url: &str, method: &str, params: serde_json::Value) -> Result<serde_json::Value, String> {
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": method, "params": params
    });
    let mut response = ureq::post(url)
        .header("Content-Type", "application/json")
        .send_json(&body)
        .map_err(|e| e.to_string())?;
    let mut json: serde_json::Value = response
        .body_mut()
        .read_json()
        .map_err(|e| e.to_string())?;
    if let Some(error) = json.get("error") {
        return Err(error.to_string());
    }
    json.get_mut("result")
        .map(serde_json::Value::take)
        .ok_or_else(|| "no result field".to_string())
}

fn chain_head(url: &str) -> Result<u64, String> {
    let result = rpc_call(url, "eth_blockNumber", serde_json::json!([]))?;
    let hex = result.as_str().ok_or("no result field")?;
    u64::from_str_radix(hex.strip_prefix("0x").unwrap_or(hex), 16).map_err(|e| e.to_string())
}

/// One storage slot as the node reports it, at a block or at its head.
fn storage_at(
    url: &str,
    contract: &[u8; 20],
    slot: &[u8; 32],
    block: Option<u64>,
) -> Result<String, String> {
    let tag = block.map_or_else(|| "latest".to_string(), |b| format!("0x{b:x}"));
    let params = serde_json::json!([
        format!("0x{}", hex::encode(contract)),
        format!("0x{}", hex::encode(slot)),
        tag
    ]);
    let result = rpc_call(url, "eth_getStorageAt", params)?;
    let word = result.as_str().ok_or("no result field")?;
    // Pad odd-length hex rather than reject it: a node may trim leading zeros.
    let digits = word.strip_prefix("0x").unwrap_or(word);
    let even = if digits.len() % 2 == 1 { format!("0{digits}") } else { digits.to_string() };
    let bytes = hex::decode(even).map_err(|e| e.to_string())?;
    Ok(hex_quantity(&bytes))
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
        assert_eq!(decode_account(&leaf), Some((1, 1_000_000_000_000_000_000, [0x22; 32])));
    }

    #[test]
    fn decodes_an_empty_account() {
        // [nonce=0, balance=0, storage_root, code_hash]
        let mut leaf = vec![0xf8, 0x44, 0x80, 0x80, 0xa0];
        leaf.extend_from_slice(&[0x11; 32]);
        leaf.push(0xa0);
        leaf.extend_from_slice(&[0x22; 32]);
        assert_eq!(decode_account(&leaf), Some((0, 0, [0x22; 32])));
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
        assert_eq!(decode_account(&leaf), Some((9, 26_780_479_053_084_510_844, [0x22; 32])));
    }

    #[test]
    fn empty_code_hash_is_keccak_of_nothing() {
        assert_eq!(keccak256(&[]), EMPTY_CODE_HASH);
    }

    #[test]
    fn minimal_drops_leading_zero_bytes() {
        let mut word = [0u8; 32];
        assert!(minimal(&word).is_empty());
        word[30] = 1;
        assert_eq!(minimal(&word), &[1, 0]);
    }

    /// The prefix is the contract's address path (the USDC address hash, again
    /// checked against a node), its terminator, then 17.
    #[test]
    fn storage_prefix_is_the_address_path_then_17() {
        let usdc = parse_address("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap();
        let prefix = storage_prefix(&usdc);
        assert_eq!(prefix.len(), 66);
        assert_eq!(&prefix[64..], &[16, 17]);
        assert_eq!(
            hex::encode(nibbles_to_hash(&prefix[..64]).unwrap()),
            "7b5855bb92cd7f3f78137497df02f6ccb9badda93d9782e0f230c807ba728be0"
        );
    }

    #[test]
    fn decodes_storage_values() {
        assert_eq!(decode_storage_value(&[0x05]), Some(&[0x05][..]));
        assert_eq!(decode_storage_value(&[0x82, 0x01, 0x00]), Some(&[0x01, 0x00][..]));
        let mut unlimited = vec![0xa0];
        unlimited.extend_from_slice(&[0xff; 32]);
        assert_eq!(decode_storage_value(&unlimited).map(<[u8]>::len), Some(32));
        assert!(decode_storage_value(&[0x82, 0x01]).is_none()); // truncated
        assert!(decode_storage_value(&[0x05, 0x06]).is_none()); // trailing bytes
    }

    #[test]
    fn hex_quantities_have_no_leading_zeros() {
        assert_eq!(hex_quantity(&[0x00, 0x0a]), "0xa");
        assert_eq!(hex_quantity(&[0x01, 0x00]), "0x100");
        assert_eq!(hex_quantity(&[0, 0]), "0x0");
        assert_eq!(hex_quantity(&[]), "0x0");
    }
}
