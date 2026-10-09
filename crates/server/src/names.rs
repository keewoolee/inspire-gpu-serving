//! Following primary names: which addresses have one, and which blocks can
//! have changed an answer.
//!
//! Every address with a name is evaluated the way a wallet would ask (see
//! [`pir_chain::names`]), and the storage slots that answer read are kept in
//! an index. A block's state diff then says which slots changed, and only the
//! addresses that read one of them are asked again. That covers anything on
//! chain an answer depends on, however it changed, including a resolver that
//! emits no event and a change made far up the name tree.
//!
//! Finding the addresses in the first place is the part that differs between
//! nodes. An ethrex node keys its state by address hash, so the starting set
//! is every address-like value in the name contracts' storage (a primary name
//! needs a forward record pointing at the address, and an on-chain one stores
//! it there). After that, each block adds:
//!
//! - every account of a transaction that wrote name-contract storage, which
//!   catches an address setting its own name;
//! - the address in `ReverseClaimed`, `NameForAddrChanged` and
//!   `PrimaryNameSet`, which catches a name set on its behalf, by an operator
//!   or by signature;
//! - every account seen in a diff for the first time, asked once, so an
//!   address the starting set missed joins as soon as it transacts.
//!
//! A node that keeps addresses in the clear (reth) only changes where the
//! starting set comes from.
//!
//! Two things have no on-chain signal and are polled instead: GNS and WNS
//! names expire with time, and an ENS name whose forward record lives behind
//! a gateway can change there.
//!
//! Asking about the whole starting set takes many node calls, so they go out
//! as several batches at once, and the tracker saves its state to a file now
//! and then. A restart loads the file and follows on from where it stopped,
//! as long as the node can still trace the next block.

use pir_chain::names::{
    access_list_request, addresses_in_word, call_request, classify_reverse, decode_first_string,
    decode_reverse_half, find_resolver_call, forward_half_call, ns_reverse_call, parse_access_list,
    parse_address, parse_call, resolve_offchain, reverse_call, reverse_half_call, reverse_name,
    Address, CallOutcome, EnsOffchain, EnsOnchain, Gateway, Read, ENS_DEFAULT_REVERSE_REGISTRAR,
    ENS_REVERSE_REGISTRAR, GNS, KNOWN_CONTRACTS, NAME_FOR_ADDR_CHANGED, PRIMARY_NAME_SET,
    REVERSE_CLAIMED, UNIVERSAL_RESOLVER, WNS,
};
use pir_chain::rpc::EthRpc;
use pir_keyword::names::{NameEntry, Names};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read as _, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Addresses asked per JSON-RPC batch. Each costs up to three calls.
const BATCH: usize = 100;
/// Blocks taken per step, so evaluation keeps moving while catching up.
const BLOCKS_PER_STEP: usize = 16;
/// Rounds of concurrent batches a step spends on first-seen accounts and on
/// the starting set, so a step stays short and blocks keep being followed.
const FRESH_ROUNDS: usize = 2;
const BULK_ROUNDS: usize = 4;
/// Time a step may spend on gateways.
const OFFCHAIN_BUDGET: Duration = Duration::from_secs(3);
/// Gateway lookups per second, at least. Most offchain names sit behind one
/// gateway (Coinbase's), which answers a burst with 429, so lookups are paced
/// rather than sent all at once.
const MIN_OFFCHAIN_RATE: f64 = 3.0;
/// Gateway lookups in flight at once.
const OFFCHAIN_WORKERS: usize = 4;

const STATE_MAGIC: &[u8; 10] = b"PIRNAMES1\n";

/// One address's new value for the table.
pub struct Change {
    pub address: Address,
    pub value: Vec<u8>,
}

pub struct TrackerConfig {
    /// How often to ask the gateways again for names behind one.
    pub offchain_poll: Duration,
    /// Batches of node calls in flight at once, and gateway lookups.
    pub concurrency: usize,
    /// Where to save the tracker's state, and how often.
    pub state: Option<(PathBuf, Duration)>,
}

/// Read a starting set: either a storage snapshot (`ethrex-statedump
/// --storage-of`), whose address-like values are taken, or a plain list of
/// addresses, one per line. `strict` as in [`addresses_in_word`].
pub fn read_candidates(path: &Path, strict: bool) -> io::Result<Vec<Address>> {
    let file = File::open(path)?;
    let mut out = HashSet::new();
    let mut storage = false;
    for line in BufReader::new(file).lines() {
        let line = line?;
        let line = line.trim();
        if line.starts_with('#') {
            storage |= line == "# key_derivation=storage";
            continue;
        }
        if line.is_empty() || line == "key,value" {
            continue;
        }
        if storage {
            let Some((_, value)) = line.split_once(',') else { continue };
            let digits = value.trim().trim_start_matches("0x");
            if digits.len() > 64 {
                continue;
            }
            let Some(word) = hex::decode(format!("{:0>64}", digits))
                .ok()
                .and_then(|v| <[u8; 32]>::try_from(v).ok())
            else {
                continue;
            };
            out.extend(addresses_in_word(&word, strict));
        } else if let Some(address) = parse_address(line) {
            out.insert(address);
        }
    }
    let mut out: Vec<Address> = out.into_iter().collect();
    out.sort();
    Ok(out)
}

#[derive(Clone, Copy)]
struct Job {
    address: Address,
    /// Ask GNS and WNS too.
    ns: bool,
}

struct Answer {
    ens: EnsOnchain,
    gns: Option<String>,
    wns: Option<String>,
}

/// What the node said about one job: the answer, and what it read when the
/// index should hold the address.
struct Fetched {
    answer: Answer,
    reads: Option<Vec<Read>>,
}

pub struct NamesTracker {
    rpc: EthRpc,
    gateway: Gateway,
    cfg: TrackerConfig,
    /// Every block up to here has been taken into account.
    pub synced_to: u64,
    known: HashSet<Address>,
    ns_contracts: [Address; 2],

    // The index: what each address's answer read, and who read each thing.
    ids: HashMap<Address, u32>,
    addresses: Vec<Address>,
    reads_of: Vec<Vec<u32>>,
    read_ids: HashMap<Read, u32>,
    read_list: Vec<Read>,
    readers: Vec<HashSet<u32>>,
    /// Contracts some answer reads, with how many reads point at each.
    contract_refs: HashMap<Address, u32>,

    names: HashMap<Address, Names>,
    /// Addresses asked about GNS and WNS as well as ENS.
    ns_tracked: HashSet<Address>,
    /// Addresses holding a GNS or WNS name, checked every block for expiry.
    ns_named: HashSet<Address>,
    /// Addresses whose ENS answer goes through a gateway.
    offchain: HashSet<Address>,
    /// Offchain addresses in the order they are next asked: each goes to the
    /// back once asked, a new one to the front.
    offchain_queue: VecDeque<Address>,
    offchain_queued: HashSet<Address>,
    offchain_credit: f64,
    last_offchain_tick: Instant,
    gateway_failures: u64,
    /// Gateway failures by what went wrong, for the summary.
    failures_by: HashMap<String, u64>,

    seen: HashSet<Address>,
    urgent: VecDeque<Address>,
    urgent_ns: HashMap<Address, bool>,
    fresh: VecDeque<Address>,
    bulk: VecDeque<Job>,
    last_log: Instant,
    last_save: Instant,
}

impl NamesTracker {
    fn empty(rpc_url: &str, block: u64, cfg: TrackerConfig) -> Self {
        let parse = |s: &str| parse_address(s).expect("constant address");
        NamesTracker {
            rpc: EthRpc::new(rpc_url),
            gateway: Gateway::new(),
            cfg,
            synced_to: block,
            known: KNOWN_CONTRACTS.iter().map(|c| parse(c)).collect(),
            ns_contracts: [parse(GNS), parse(WNS)],
            ids: HashMap::new(),
            addresses: Vec::new(),
            reads_of: Vec::new(),
            read_ids: HashMap::new(),
            read_list: Vec::new(),
            readers: Vec::new(),
            contract_refs: HashMap::new(),
            names: HashMap::new(),
            ns_tracked: HashSet::new(),
            ns_named: HashSet::new(),
            offchain: HashSet::new(),
            offchain_queue: VecDeque::new(),
            offchain_queued: HashSet::new(),
            offchain_credit: 0.0,
            last_offchain_tick: Instant::now(),
            gateway_failures: 0,
            failures_by: HashMap::new(),
            seen: HashSet::new(),
            urgent: VecDeque::new(),
            urgent_ns: HashMap::new(),
            fresh: VecDeque::new(),
            bulk: VecDeque::new(),
            last_log: Instant::now(),
            last_save: Instant::now(),
        }
    }

    /// Start from `block` with the starting sets for ENS and for GNS/WNS. The
    /// starting set is asked over the following steps, while blocks keep
    /// being followed, so the answers stay consistent with the chain however
    /// long that takes.
    pub fn new(
        rpc_url: &str,
        block: u64,
        ens_candidates: Vec<Address>,
        ns_candidates: Vec<Address>,
        cfg: TrackerConfig,
    ) -> Self {
        let mut t = Self::empty(rpc_url, block, cfg);
        t.ns_tracked = ns_candidates.iter().copied().collect();
        t.seen = ens_candidates.iter().copied().collect();
        t.seen.extend(ns_candidates.iter().copied());
        let mut starting: Vec<Address> = t.seen.iter().copied().collect();
        starting.sort();
        t.bulk = starting
            .into_iter()
            .map(|address| Job { address, ns: t.ns_tracked.contains(&address) })
            .collect();
        t
    }

    /// Whether the starting set has been asked in full. Gateway answers may
    /// still be arriving.
    pub fn starting_set_done(&self) -> bool {
        self.bulk.is_empty() && self.urgent.is_empty()
    }

    /// Every address with a name, for building a table from a loaded state.
    pub fn named(&self) -> impl Iterator<Item = (&Address, &Names)> {
        self.names.iter()
    }

    pub fn summary(&self) -> String {
        let count = |f: fn(&Names) -> bool| self.names.values().filter(|n| f(n)).count();
        let mut why: Vec<(&String, &u64)> = self.failures_by.iter().collect();
        why.sort_by(|a, b| b.1.cmp(a.1));
        let why: Vec<String> = why.iter().take(4).map(|(w, n)| format!("{n} {w}")).collect();
        let why = if why.is_empty() { String::new() } else { format!(" [{}]", why.join("; ")) };
        format!(
            "block #{}: {} addresses named (ENS {}, GNS {}, WNS {}), {} indexed over {} reads, \
             {} behind gateways ({} queued, {} gateway failures{}), {} addresses seen, queues: {} urgent, {} fresh, {} starting",
            self.synced_to,
            self.names.len(),
            count(|n| n.ens.is_some()),
            count(|n| n.gns.is_some()),
            count(|n| n.wns.is_some()),
            self.ids.len(),
            self.read_ids.len(),
            self.offchain.len(),
            self.offchain_queue.len(),
            self.gateway_failures,
            why,
            self.seen.len(),
            self.urgent.len(),
            self.fresh.len(),
            self.bulk.len(),
        )
    }

    /// Follow new blocks, then ask whatever they and the starting set queued.
    /// Returns the table changes, each to take effect as of `synced_to`.
    pub fn step(&mut self) -> Result<Vec<Change>, String> {
        let head = self.rpc.block_number()?;
        for _ in 0..BLOCKS_PER_STEP {
            if self.synced_to >= head {
                break;
            }
            self.ingest_block(self.synced_to + 1)?;
            self.synced_to += 1;
        }

        let round = self.cfg.concurrency.max(1) * BATCH;
        let mut changes = Vec::new();
        while !self.urgent.is_empty() {
            let n = self.urgent.len().min(round);
            let jobs: Vec<Job> = self
                .urgent
                .drain(..n)
                .map(|address| Job {
                    address,
                    ns: self.urgent_ns.remove(&address).unwrap_or(false),
                })
                .collect();
            match self.evaluate(&jobs) {
                Ok(c) => changes.extend(c),
                Err(e) => {
                    for j in jobs.into_iter().rev() {
                        self.push_urgent(j.address, j.ns);
                    }
                    return Err(e);
                }
            }
        }
        for _ in 0..FRESH_ROUNDS {
            let n = self.fresh.len().min(round);
            if n == 0 {
                break;
            }
            // Asked about ENS only: an address with a GNS or WNS name set it
            // itself, in a transaction the diff already showed.
            let jobs: Vec<Job> = self.fresh.drain(..n).map(|address| Job { address, ns: false }).collect();
            match self.evaluate(&jobs) {
                Ok(c) => changes.extend(c),
                Err(e) => {
                    for j in jobs.into_iter().rev() {
                        self.fresh.push_front(j.address);
                    }
                    return Err(e);
                }
            }
        }
        for _ in 0..BULK_ROUNDS {
            let n = self.bulk.len().min(round);
            if n == 0 {
                break;
            }
            let jobs: Vec<Job> = self.bulk.drain(..n).collect();
            match self.evaluate(&jobs) {
                Ok(c) => changes.extend(c),
                Err(e) => {
                    for j in jobs.into_iter().rev() {
                        self.bulk.push_front(j);
                    }
                    return Err(e);
                }
            }
        }
        changes.extend(self.poll_offchain()?);

        if self.last_log.elapsed() >= Duration::from_secs(30) {
            eprintln!("names: {}", self.summary());
            self.last_log = Instant::now();
        }
        self.maybe_save();
        Ok(changes)
    }

    fn push_urgent(&mut self, address: Address, ns: bool) {
        let ns = ns || self.ns_tracked.contains(&address);
        match self.urgent_ns.get_mut(&address) {
            Some(flag) => *flag |= ns,
            None => {
                self.urgent_ns.insert(address, ns);
                self.urgent.push_back(address);
            }
        }
    }

    /// Queue every address a block may have changed the answer of.
    fn ingest_block(&mut self, block: u64) -> Result<(), String> {
        let diffs = self.rpc.fetch_block_tx_diffs(block)?;
        for diff in &diffs {
            let mut touches = false;
            let mut touches_ns = false;
            let mut hit: Vec<u32> = Vec::new();
            for u in &diff.storage {
                if let Some(&rid) = self.read_ids.get(&(u.contract, Some(u.slot))) {
                    hit.extend(self.readers[rid as usize].iter().copied());
                }
                touches |= self.known.contains(&u.contract) || self.contract_refs.contains_key(&u.contract);
                touches_ns |= self.ns_contracts.contains(&u.contract);
            }
            for c in &diff.code {
                if let Some(&rid) = self.read_ids.get(&(*c, None)) {
                    hit.extend(self.readers[rid as usize].iter().copied());
                }
            }
            for id in hit {
                let address = self.addresses[id as usize];
                self.push_urgent(address, false);
            }
            if touches {
                for &a in &diff.accounts {
                    if touches_ns {
                        self.ns_tracked.insert(a);
                    }
                    self.push_urgent(a, touches_ns);
                }
            }
            for &a in &diff.accounts {
                if self.seen.insert(a) {
                    self.fresh.push_back(a);
                }
            }
        }

        let events = [
            (ENS_REVERSE_REGISTRAR, REVERSE_CLAIMED, false),
            (ENS_DEFAULT_REVERSE_REGISTRAR, NAME_FOR_ADDR_CHANGED, false),
            (GNS, PRIMARY_NAME_SET, true),
            (WNS, PRIMARY_NAME_SET, true),
        ];
        for (contract, topic, ns) in events {
            for log in self.rpc.get_logs(block, contract, topic)? {
                let Some(address) = log
                    .get("topics")
                    .and_then(|t| t.get(1))
                    .and_then(Value::as_str)
                    .and_then(|t| parse_address(&format!("0x{}", &t[t.len().saturating_sub(40)..])))
                else {
                    continue;
                };
                if ns {
                    self.ns_tracked.insert(address);
                }
                self.push_urgent(address, ns);
            }
        }

        self.check_expiry()
    }

    /// GNS and WNS names lapse with time and no event, so the addresses
    /// holding one are asked every block. Only a changed answer is evaluated
    /// in full.
    fn check_expiry(&mut self) -> Result<(), String> {
        let named: Vec<Address> = self.ns_named.iter().copied().collect();
        for chunk in named.chunks(BATCH) {
            let mut reqs = Vec::with_capacity(2 * chunk.len());
            for a in chunk {
                reqs.push(call_request(GNS, &ns_reverse_call(a)));
                reqs.push(call_request(WNS, &ns_reverse_call(a)));
            }
            let mut out = self.rpc.batch_raw(&reqs)?.into_iter();
            for a in chunk {
                let gns = ns_name(parse_call(out.next().ok_or("short batch")?)?);
                let wns = ns_name(parse_call(out.next().ok_or("short batch")?)?);
                let current = self.names.get(a).cloned().unwrap_or_default();
                if entry_name(&current.gns) != gns || entry_name(&current.wns) != wns {
                    self.push_urgent(*a, true);
                }
            }
        }
        Ok(())
    }

    /// Ask about `jobs`, a batch per thread, then refresh the index for the
    /// ones it should hold and return the values that changed.
    fn evaluate(&mut self, jobs: &[Job]) -> Result<Vec<Change>, String> {
        let indexed: Vec<bool> = jobs.iter().map(|j| self.ids.contains_key(&j.address)).collect();
        let rpc = &self.rpc;
        let fetched: Vec<Fetched> = std::thread::scope(|s| {
            let handles: Vec<_> = jobs
                .chunks(BATCH)
                .zip(indexed.chunks(BATCH))
                .map(|(j, i)| s.spawn(move || fetch(rpc, j, i)))
                .collect();
            let mut all = Vec::with_capacity(jobs.len());
            for h in handles {
                all.extend(h.join().map_err(|_| "a fetch thread panicked".to_string())??);
            }
            Ok::<_, String>(all)
        })?;

        let mut changes = Vec::new();
        for (j, f) in jobs.iter().zip(fetched) {
            let a = f.answer;
            let mut names = self.names.get(&j.address).cloned().unwrap_or_default();
            let offchain = a.ens == EnsOnchain::Offchain;
            match a.ens {
                EnsOnchain::Name(n) => names.ens = Some(NameEntry::Name(n)),
                EnsOnchain::Empty | EnsOnchain::Invalid(_) => names.ens = None,
                // Unchanged until a gateway answers.
                EnsOnchain::Offchain => {}
            }
            if offchain {
                self.offchain.insert(j.address);
                if self.offchain_queued.insert(j.address) {
                    self.offchain_queue.push_front(j.address);
                }
            } else {
                self.offchain.remove(&j.address);
            }
            if j.ns {
                names.gns = a.gns.map(NameEntry::Name);
                names.wns = a.wns.map(NameEntry::Name);
                if names.gns.is_some() || names.wns.is_some() {
                    self.ns_named.insert(j.address);
                } else {
                    self.ns_named.remove(&j.address);
                }
            }
            if let Some(change) = self.set_names(j.address, names) {
                changes.push(change);
            }
            if let Some(reads) = f.reads {
                self.set_reads(j.address, reads);
            }
        }
        Ok(changes)
    }

    /// Ask the gateways for offchain answers at a steady pace: every address
    /// comes round once per poll period, and at least `MIN_OFFCHAIN_RATE` are
    /// asked per second in all.
    fn poll_offchain(&mut self) -> Result<Vec<Change>, String> {
        let period = self.cfg.offchain_poll.as_secs_f64().max(1.0);
        let rate = (self.offchain.len() as f64 / period).max(MIN_OFFCHAIN_RATE);
        let now = Instant::now();
        let earned = rate * (now - self.last_offchain_tick).as_secs_f64();
        self.offchain_credit = (self.offchain_credit + earned).min(rate * 10.0 + 1.0);
        self.last_offchain_tick = now;
        let mut work = VecDeque::new();
        while work.len() + 1 <= self.offchain_credit as usize {
            let Some(a) = self.offchain_queue.pop_front() else { break };
            self.offchain_queued.remove(&a);
            if self.offchain.contains(&a) {
                work.push_back(a);
            }
        }
        if work.is_empty() {
            return Ok(Vec::new());
        }
        self.offchain_credit -= work.len() as f64;

        let deadline = now + OFFCHAIN_BUDGET;
        let queue = Mutex::new(work);
        let (rpc, gateway) = (&self.rpc, &self.gateway);
        let results: Vec<(Address, Result<EnsOffchain, String>)> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..self.cfg.concurrency.clamp(1, OFFCHAIN_WORKERS))
                .map(|_| {
                    s.spawn(|| {
                        let mut done = Vec::new();
                        while Instant::now() < deadline {
                            let Some(address) = queue.lock().unwrap().pop_front() else { break };
                            done.push((address, resolve_offchain(rpc, gateway, &address)));
                        }
                        done
                    })
                })
                .collect();
            handles.into_iter().flat_map(|h| h.join().unwrap_or_default()).collect()
        });
        // What the budget left unasked goes first next time.
        for a in queue.into_inner().unwrap().into_iter().rev() {
            if self.offchain_queued.insert(a) {
                self.offchain_queue.push_front(a);
            }
        }

        let mut changes = Vec::new();
        let mut first_error = None;
        for (address, result) in results {
            if self.offchain.contains(&address) && self.offchain_queued.insert(address) {
                self.offchain_queue.push_back(address);
            }
            let ens = match result {
                Ok(EnsOffchain::Name(n)) => Some(NameEntry::Name(n)),
                Ok(EnsOffchain::NoName) => None,
                // Keep what the table had: a gateway that is down says
                // nothing about the name.
                Ok(EnsOffchain::GatewayFailed(why)) => {
                    self.gateway_failures += 1;
                    *self.failures_by.entry(why).or_insert(0) += 1;
                    continue;
                }
                Err(e) => {
                    first_error.get_or_insert(e);
                    continue;
                }
            };
            let mut names = self.names.get(&address).cloned().unwrap_or_default();
            names.ens = ens;
            if let Some(change) = self.set_names(address, names) {
                changes.push(change);
            }
        }
        if let Some(e) = first_error {
            eprintln!("names: offchain lookups hit node errors, retrying later ({e})");
        }
        Ok(changes)
    }

    fn set_names(&mut self, address: Address, names: Names) -> Option<Change> {
        let old = self.names.get(&address).cloned().unwrap_or_default();
        if old == names {
            return None;
        }
        let value = names.pack();
        if names.is_empty() {
            self.names.remove(&address);
        } else {
            self.names.insert(address, names);
        }
        Some(Change { address, value })
    }

    fn id_of(&mut self, address: Address) -> u32 {
        match self.ids.get(&address) {
            Some(&id) => id,
            None => {
                let id = self.addresses.len() as u32;
                self.ids.insert(address, id);
                self.addresses.push(address);
                self.reads_of.push(Vec::new());
                id
            }
        }
    }

    fn read_id(&mut self, read: Read) -> u32 {
        match self.read_ids.get(&read) {
            Some(&rid) => rid,
            None => {
                let rid = self.readers.len() as u32;
                self.read_ids.insert(read, rid);
                self.read_list.push(read);
                self.readers.push(HashSet::new());
                rid
            }
        }
    }

    fn set_reads(&mut self, address: Address, mut reads: Vec<Read>) {
        reads.sort();
        reads.dedup();
        let id = self.id_of(address);
        for rid in std::mem::take(&mut self.reads_of[id as usize]) {
            self.readers[rid as usize].remove(&id);
            self.unref(self.read_list[rid as usize].0);
        }
        let rids: Vec<u32> = reads.into_iter().map(|read| self.read_id(read)).collect();
        for &rid in &rids {
            self.readers[rid as usize].insert(id);
            *self.contract_refs.entry(self.read_list[rid as usize].0).or_insert(0) += 1;
        }
        self.reads_of[id as usize] = rids;
    }

    fn unref(&mut self, contract: Address) {
        if let Some(c) = self.contract_refs.get_mut(&contract) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.contract_refs.remove(&contract);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Saving and loading
    // ------------------------------------------------------------------------

    fn maybe_save(&mut self) {
        let Some((path, every)) = &self.cfg.state else { return };
        if self.last_save.elapsed() < *every {
            return;
        }
        let path = path.clone();
        self.save_now(&path);
    }

    /// Save now, logging the outcome. Following waits while it writes.
    pub fn save_now(&mut self, path: &Path) {
        let t0 = Instant::now();
        let partial = PathBuf::from(format!("{}.partial", path.display()));
        match self.save(&partial).and_then(|n| std::fs::rename(&partial, path).map(|_| n)) {
            Ok(bytes) => eprintln!(
                "names: state at block #{} saved to {} ({:.0} MB) in {:.1}s",
                self.synced_to,
                path.display(),
                bytes as f64 / 1e6,
                t0.elapsed().as_secs_f64()
            ),
            Err(e) => eprintln!("names: saving state to {} failed: {}", path.display(), e),
        }
        self.last_save = Instant::now();
    }

    fn save(&self, path: &Path) -> io::Result<u64> {
        let mut w = Out(BufWriter::with_capacity(1 << 20, File::create(path)?), 0);
        w.bytes(STATE_MAGIC)?;
        w.u64(self.synced_to)?;
        w.u32(self.read_list.len() as u32)?;
        for (contract, slot) in &self.read_list {
            w.bytes(contract)?;
            match slot {
                Some(slot) => {
                    w.u8(1)?;
                    w.bytes(slot)?;
                }
                None => w.u8(0)?,
            }
        }
        w.u32(self.addresses.len() as u32)?;
        for (address, rids) in self.addresses.iter().zip(&self.reads_of) {
            w.bytes(address)?;
            w.u32(rids.len() as u32)?;
            for &rid in rids {
                w.u32(rid)?;
            }
        }
        w.u32(self.names.len() as u32)?;
        for (address, names) in &self.names {
            w.bytes(address)?;
            for entry in [&names.ens, &names.gns, &names.wns] {
                match entry {
                    Some(NameEntry::Name(n)) => {
                        w.u8(1)?;
                        w.u32(n.len() as u32)?;
                        w.bytes(n.as_bytes())?;
                    }
                    _ => w.u8(0)?,
                }
            }
        }
        for set in [&self.ns_tracked, &self.ns_named, &self.offchain, &self.seen] {
            w.addresses(set.iter())?;
        }
        w.u32(self.urgent.len() as u32)?;
        for a in &self.urgent {
            w.bytes(a)?;
            w.u8(self.urgent_ns.get(a).copied().unwrap_or(false) as u8)?;
        }
        w.addresses(self.fresh.iter())?;
        w.u32(self.bulk.len() as u32)?;
        for j in &self.bulk {
            w.bytes(&j.address)?;
            w.u8(j.ns as u8)?;
        }
        w.addresses(self.offchain_queue.iter())?;
        w.0.flush()?;
        Ok(w.1)
    }

    /// The block a saved state reached, read from its header alone.
    pub fn saved_block(path: &Path) -> io::Result<u64> {
        let mut r = In(BufReader::new(File::open(path)?));
        r.magic()?;
        r.u64()
    }

    /// Load a saved state, to follow on from where it stopped.
    pub fn load(path: &Path, rpc_url: &str, cfg: TrackerConfig) -> io::Result<Self> {
        let mut r = In(BufReader::with_capacity(1 << 20, File::open(path)?));
        r.magic()?;
        let block = r.u64()?;
        let mut t = Self::empty(rpc_url, block, cfg);
        for _ in 0..r.u32()? {
            let contract = r.address()?;
            let slot = match r.u8()? {
                1 => Some(r.array::<32>()?),
                _ => None,
            };
            t.read_id((contract, slot));
        }
        for _ in 0..r.u32()? {
            let address = r.address()?;
            let id = t.id_of(address);
            let n = r.u32()? as usize;
            let mut rids = Vec::with_capacity(n);
            for _ in 0..n {
                let rid = r.u32()?;
                let read = *t
                    .read_list
                    .get(rid as usize)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "read id out of range"))?;
                t.readers[rid as usize].insert(id);
                *t.contract_refs.entry(read.0).or_insert(0) += 1;
                rids.push(rid);
            }
            t.reads_of[id as usize] = rids;
        }
        for _ in 0..r.u32()? {
            let address = r.address()?;
            let mut names = Names::default();
            for system in pir_keyword::names::NameSystem::ALL {
                if r.u8()? == 1 {
                    let len = r.u32()? as usize;
                    let mut buf = vec![0u8; len];
                    r.0.read_exact(&mut buf)?;
                    *names.get_mut(system) = Some(NameEntry::Name(String::from_utf8_lossy(&buf).into_owned()));
                }
            }
            t.names.insert(address, names);
        }
        t.ns_tracked = r.addresses()?.into_iter().collect();
        t.ns_named = r.addresses()?.into_iter().collect();
        t.offchain = r.addresses()?.into_iter().collect();
        t.seen = r.addresses()?.into_iter().collect();
        for _ in 0..r.u32()? {
            let address = r.address()?;
            let ns = r.u8()? == 1;
            t.push_urgent(address, ns);
        }
        t.fresh = r.addresses()?.into_iter().collect();
        for _ in 0..r.u32()? {
            let address = r.address()?;
            let ns = r.u8()? == 1;
            t.bulk.push_back(Job { address, ns });
        }
        for a in r.addresses()? {
            if t.offchain_queued.insert(a) {
                t.offchain_queue.push_back(a);
            }
        }
        // Every offchain address keeps coming round, in case the save caught
        // one between turns.
        let missing: Vec<Address> = t.offchain.iter().filter(|a| !t.offchain_queued.contains(*a)).copied().collect();
        for a in missing {
            t.offchain_queued.insert(a);
            t.offchain_queue.push_back(a);
        }
        Ok(t)
    }
}

/// Ask the node about `jobs`: the answers, then what they read for every
/// address the index should hold (one with an answer, or one it already
/// holds). A call that reverted lists nothing on ethrex, so its two halves
/// are read instead.
fn fetch(rpc: &EthRpc, jobs: &[Job], indexed: &[bool]) -> Result<Vec<Fetched>, String> {
    let ur = UNIVERSAL_RESOLVER;
    let mut reqs = Vec::with_capacity(3 * jobs.len());
    for j in jobs {
        reqs.push(call_request(ur, &reverse_call(&j.address)));
        if j.ns {
            reqs.push(call_request(GNS, &ns_reverse_call(&j.address)));
            reqs.push(call_request(WNS, &ns_reverse_call(&j.address)));
        }
    }
    let mut out = rpc.batch_raw(&reqs)?.into_iter();
    let mut answers = Vec::with_capacity(jobs.len());
    for j in jobs {
        let ens = classify_reverse(parse_call(out.next().ok_or("short batch")?)?);
        let (gns, wns) = if j.ns {
            (
                ns_name(parse_call(out.next().ok_or("short batch")?)?),
                ns_name(parse_call(out.next().ok_or("short batch")?)?),
            )
        } else {
            (None, None)
        };
        answers.push(Answer { ens, gns, wns });
    }

    let index: Vec<bool> = answers
        .iter()
        .zip(indexed)
        .map(|(a, &already)| already || a.ens != EnsOnchain::Empty || a.gns.is_some() || a.wns.is_some())
        .collect();
    let mut reads: Vec<Vec<Read>> = vec![Vec::new(); jobs.len()];
    let mut split: Vec<usize> = Vec::new();
    let mut reqs = Vec::new();
    let mut owners = Vec::new();
    for (i, j) in jobs.iter().enumerate() {
        if !index[i] {
            continue;
        }
        match answers[i].ens {
            EnsOnchain::Name(_) | EnsOnchain::Empty => {
                reqs.push(access_list_request(ur, &reverse_call(&j.address)));
                owners.push(i);
            }
            _ => split.push(i),
        }
        if j.ns {
            reqs.push(access_list_request(GNS, &ns_reverse_call(&j.address)));
            owners.push(i);
            reqs.push(access_list_request(WNS, &ns_reverse_call(&j.address)));
            owners.push(i);
        }
    }
    for (i, item) in owners.iter().zip(rpc.batch_raw(&reqs)?) {
        match parse_access_list(item)? {
            Some(r) => reads[*i].extend(r),
            None => {
                if !split.contains(i) {
                    split.push(*i);
                }
            }
        }
    }
    if !split.is_empty() {
        read_halves(rpc, jobs, &split, &mut reads)?;
    }

    Ok(answers
        .into_iter()
        .zip(reads)
        .zip(index)
        .map(|((answer, reads), index)| Fetched {
            answer,
            reads: index.then_some(reads),
        })
        .collect())
}

/// Read sets for answers that reverted: the reverse half, then the forward
/// half of the name it gave, each falling back to the registry walk alone
/// when it reverts as well.
fn read_halves(rpc: &EthRpc, jobs: &[Job], split: &[usize], reads: &mut [Vec<Read>]) -> Result<(), String> {
    let ur = UNIVERSAL_RESOLVER;
    let mut reqs = Vec::with_capacity(2 * split.len());
    for &i in split {
        let data = reverse_half_call(&jobs[i].address);
        reqs.push(call_request(ur, &data));
        reqs.push(access_list_request(ur, &data));
    }
    let mut out = rpc.batch_raw(&reqs)?.into_iter();
    let mut next = Vec::new();
    let mut next_owner = Vec::new();
    for &i in split {
        let name = match parse_call(out.next().ok_or("short batch")?)? {
            CallOutcome::Output(o) => decode_reverse_half(&o).ok(),
            CallOutcome::Reverted(_) => None,
        };
        match parse_access_list(out.next().ok_or("short batch")?)? {
            Some(r) => reads[i].extend(r),
            None => {
                if let Some(data) = find_resolver_call(&reverse_name(&jobs[i].address)) {
                    next.push(access_list_request(ur, &data));
                    next_owner.push((i, None));
                }
            }
        }
        if let Some(name) = name.filter(|n| !n.is_empty()) {
            if let Some(data) = forward_half_call(&name) {
                next.push(access_list_request(ur, &data));
                next_owner.push((i, Some(name)));
            }
        }
    }
    let mut last = Vec::new();
    let mut last_owner = Vec::new();
    for ((i, name), item) in next_owner.into_iter().zip(rpc.batch_raw(&next)?) {
        match parse_access_list(item)? {
            Some(r) => reads[i].extend(r),
            None => {
                if let Some(data) = name.as_deref().and_then(find_resolver_call) {
                    last.push(access_list_request(ur, &data));
                    last_owner.push(i);
                }
            }
        }
    }
    for (i, item) in last_owner.into_iter().zip(rpc.batch_raw(&last)?) {
        if let Some(r) = parse_access_list(item)? {
            reads[i].extend(r);
        }
    }
    Ok(())
}

fn ns_name(outcome: CallOutcome) -> Option<String> {
    match outcome {
        CallOutcome::Output(o) => decode_first_string(&o).ok().filter(|n| !n.is_empty()),
        CallOutcome::Reverted(_) => None,
    }
}

fn entry_name(entry: &Option<NameEntry>) -> Option<String> {
    match entry {
        Some(NameEntry::Name(n)) => Some(n.clone()),
        _ => None,
    }
}

/// A buffered writer that counts what it wrote.
struct Out(BufWriter<File>, u64);

impl Out {
    fn bytes(&mut self, b: &[u8]) -> io::Result<()> {
        self.1 += b.len() as u64;
        self.0.write_all(b)
    }
    fn u8(&mut self, v: u8) -> io::Result<()> {
        self.bytes(&[v])
    }
    fn u32(&mut self, v: u32) -> io::Result<()> {
        self.bytes(&v.to_le_bytes())
    }
    fn u64(&mut self, v: u64) -> io::Result<()> {
        self.bytes(&v.to_le_bytes())
    }
    fn addresses<'a>(&mut self, items: impl ExactSizeIterator<Item = &'a Address>) -> io::Result<()> {
        self.u32(items.len() as u32)?;
        for a in items {
            self.bytes(a)?;
        }
        Ok(())
    }
}

struct In(BufReader<File>);

impl In {
    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut b = [0u8; N];
        self.0.read_exact(&mut b)?;
        Ok(b)
    }
    fn magic(&mut self) -> io::Result<()> {
        if &self.array::<10>()? != STATE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not a names state file"));
        }
        Ok(())
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn address(&mut self) -> io::Result<Address> {
        self.array()
    }
    fn addresses(&mut self) -> io::Result<Vec<Address>> {
        let n = self.u32()? as usize;
        (0..n).map(|_| self.address()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TrackerConfig {
        TrackerConfig {
            offchain_poll: Duration::from_secs(3600),
            concurrency: 2,
            state: None,
        }
    }

    fn name(s: &str) -> Option<NameEntry> {
        Some(NameEntry::Name(s.into()))
    }

    /// A saved state loads back into the same index, names, sets and queues,
    /// and the offchain answers come back due for a fresh poll.
    #[test]
    fn state_round_trips() {
        let (a, b, c) = ([1u8; 20], [2u8; 20], [3u8; 20]);
        let mut t = NamesTracker::new("http://127.0.0.1:1", 100, vec![a, b], vec![c], cfg());
        t.offchain_queue.push_back(c);
        t.offchain_queued.insert(c);
        t.set_reads(a, vec![(c, Some([9; 32])), (c, None)]);
        t.set_reads(b, vec![(c, Some([9; 32]))]);
        t.set_names(a, Names { ens: name("alice.eth"), ..Default::default() });
        t.set_names(c, Names { gns: name("c.gwei"), wns: name("c.wei"), ..Default::default() });
        t.offchain.insert(b);
        t.ns_named.insert(c);
        t.push_urgent(b, true);
        t.fresh.push_back([4u8; 20]);

        let path = std::env::temp_dir().join(format!("names-state-test-{}", std::process::id()));
        t.save(&path).unwrap();
        assert_eq!(NamesTracker::saved_block(&path).unwrap(), 100);
        let u = NamesTracker::load(&path, "http://127.0.0.1:1", cfg()).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(u.synced_to, 100);
        assert_eq!(u.names, t.names);
        assert_eq!(u.ids, t.ids);
        assert_eq!(u.reads_of, t.reads_of);
        assert_eq!(u.read_list, t.read_list);
        assert_eq!(u.readers, t.readers);
        assert_eq!(u.contract_refs, t.contract_refs);
        assert_eq!(u.ns_tracked, t.ns_tracked);
        assert_eq!(u.ns_named, t.ns_named);
        assert_eq!(u.offchain, t.offchain);
        assert_eq!(u.seen, t.seen);
        assert_eq!(u.urgent, t.urgent);
        assert_eq!(u.urgent_ns, t.urgent_ns);
        assert_eq!(u.fresh, t.fresh);
        assert_eq!(u.bulk.len(), t.bulk.len());
        assert_eq!(u.offchain_queue, VecDeque::from([c, b]));
    }

    /// Re-indexing an address drops it from the readers of what it no longer
    /// reads, and a contract nobody reads any more stops counting as one.
    #[test]
    fn reindexing_moves_readers_and_contract_counts() {
        let (a, r1, r2) = ([1u8; 20], [7u8; 20], [8u8; 20]);
        let mut t = NamesTracker::new("http://127.0.0.1:1", 0, vec![], vec![], cfg());
        t.set_reads(a, vec![(r1, Some([1; 32])), (r1, Some([1; 32])), (r1, None)]);
        assert_eq!(t.reads_of[0].len(), 2);
        assert_eq!(t.contract_refs[&r1], 2);
        t.set_reads(a, vec![(r2, Some([2; 32]))]);
        assert!(!t.contract_refs.contains_key(&r1));
        assert_eq!(t.contract_refs[&r2], 1);
        let old = t.read_ids[&(r1, Some([1; 32]))];
        assert!(t.readers[old as usize].is_empty());
        let new = t.read_ids[&(r2, Some([2; 32]))];
        assert!(t.readers[new as usize].contains(&0));
    }

    /// A name that did not change makes no table change, and clearing every
    /// name writes the all-zero value.
    #[test]
    fn only_changes_reach_the_table() {
        let a = [1u8; 20];
        let mut t = NamesTracker::new("http://127.0.0.1:1", 0, vec![], vec![], cfg());
        let n = Names { ens: name("alice.eth"), ..Default::default() };
        assert!(t.set_names(a, n.clone()).is_some());
        assert!(t.set_names(a, n).is_none());
        let cleared = t.set_names(a, Names::default()).unwrap();
        assert_eq!(cleared.value, vec![0u8; pir_keyword::names::NAME_VALUE_SIZE]);
        assert!(t.names.is_empty());
    }

    #[test]
    fn candidates_from_a_storage_snapshot_or_a_list() {
        let dir = std::env::temp_dir();
        let snapshot = dir.join(format!("names-cands-{}.csv", std::process::id()));
        std::fs::write(
            &snapshot,
            "# block=1\n# key_derivation=storage\n# contracts=0x00\nkey,value\n\
             aa,d8da6bf26964af9d7eed9e03e53415d37aa96045\n\
             bb,1\n",
        )
        .unwrap();
        let list = dir.join(format!("names-list-{}.txt", std::process::id()));
        std::fs::write(&list, "0xd8da6bf26964af9d7eed9e03e53415d37aa96045\nnot an address\n").unwrap();
        let vitalik = parse_address("0xd8da6bf26964af9d7eed9e03e53415d37aa96045").unwrap();
        assert_eq!(read_candidates(&snapshot, true).unwrap(), vec![vitalik]);
        assert_eq!(read_candidates(&list, true).unwrap(), vec![vitalik]);
        std::fs::remove_file(snapshot).unwrap();
        std::fs::remove_file(list).unwrap();
    }
}
