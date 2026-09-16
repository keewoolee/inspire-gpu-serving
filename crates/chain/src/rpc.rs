//! Minimal Ethereum JSON-RPC client for fetching real block data.
//!
//! Used by the server's follower loop (`--eth-rpc`) and the standalone
//! `resync` tool.

use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use pir_keyword::cuckoo::CuckooTable;

/// Cap on a JSON-RPC response body. A block's state diff runs to tens of
/// megabytes, far past ureq's 10 MiB default, which would otherwise stall the
/// follower on exactly the busiest blocks.
const MAX_RESPONSE_BYTES: u64 = 128 * 1024 * 1024;

/// Whether the endpoint serves `debug_traceBlockByNumber`, learned on first use.
const DIFF_UNKNOWN: u8 = 0;
const DIFF_YES: u8 = 1;
const DIFF_NO: u8 = 2;

/// Ethereum JSON-RPC client (synchronous, uses ureq).
pub struct EthRpc {
    url: String,
    agent: ureq::Agent,
    diff_support: AtomicU8,
}

/// State change extracted from a block: an address with its new balance and nonce.
pub struct AccountUpdate {
    pub address: Vec<u8>, // 20 bytes
    pub balance: Vec<u8>, // 32 bytes, big-endian
    pub nonce: u64,
}

impl EthRpc {
    pub fn new(url: &str) -> Self {
        let agent = ureq::Agent::new_with_defaults();
        Self {
            url: url.to_string(),
            agent,
            diff_support: AtomicU8::new(DIFF_UNKNOWN),
        }
    }

    fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });
        let resp = self
            .agent
            .post(&self.url)
            .header("Content-Type", "application/json")
            .send_json(&body)
            .map_err(|e| format!("RPC request failed: {}", e))?;
        let mut body = resp.into_body();
        let json: Value = body
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_json()
            .map_err(|e| format!("RPC parse failed: {}", e))?;
        if let Some(err) = json.get("error") {
            return Err(format!("RPC error: {}", err));
        }
        json.get("result")
            .cloned()
            .ok_or_else(|| "no result field".into())
    }

    /// Batch JSON-RPC call. Returns results in order.
    fn batch_call(&self, requests: &[(String, Value)]) -> Result<Vec<Value>, String> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
        let batch: Vec<Value> = requests
            .iter()
            .enumerate()
            .map(|(i, (method, params))| {
                json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                    "id": i + 1
                })
            })
            .collect();

        let resp = self
            .agent
            .post(&self.url)
            .header("Content-Type", "application/json")
            .send_json(&batch)
            .map_err(|e| format!("batch RPC failed: {}", e))?;
        let mut body = resp.into_body();
        let json: Value = body
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_json()
            .map_err(|e| format!("batch parse failed: {}", e))?;

        let arr = json.as_array().ok_or("batch response not array")?;
        // Sort by id to maintain order
        let mut results: Vec<(usize, Value)> = arr
            .iter()
            .map(|v| {
                let id = v["id"].as_u64().unwrap_or(0) as usize;
                let result = v.get("result").cloned().unwrap_or(Value::Null);
                (id, result)
            })
            .collect();
        results.sort_by_key(|(id, _)| *id);
        Ok(results.into_iter().map(|(_, v)| v).collect())
    }

    /// Get the latest block number.
    pub fn block_number(&self) -> Result<u64, String> {
        let result = self.call("eth_blockNumber", json!([]))?;
        parse_hex_u64(&result)
    }

    /// Get a block by number with full transaction objects.
    pub fn get_block(&self, number: u64) -> Result<Value, String> {
        let hex = format!("0x{:x}", number);
        self.call("eth_getBlockByNumber", json!([hex, true]))
    }

    /// Extract all unique addresses touched by a block (from, to, miner/coinbase).
    pub fn addresses_from_block(block: &Value) -> Vec<Vec<u8>> {
        let mut seen = HashSet::new();
        let mut addrs = Vec::new();

        // Miner/coinbase
        if let Some(miner) = block.get("miner").and_then(|v| v.as_str()) {
            if let Some(bytes) = parse_hex_address(miner) {
                if seen.insert(bytes.clone()) {
                    addrs.push(bytes);
                }
            }
        }

        // Transaction senders and recipients
        if let Some(txs) = block.get("transactions").and_then(|v| v.as_array()) {
            for tx in txs {
                if let Some(from) = tx.get("from").and_then(|v| v.as_str()) {
                    if let Some(bytes) = parse_hex_address(from) {
                        if seen.insert(bytes.clone()) {
                            addrs.push(bytes);
                        }
                    }
                }
                if let Some(to) = tx.get("to").and_then(|v| v.as_str()) {
                    if let Some(bytes) = parse_hex_address(to) {
                        if seen.insert(bytes.clone()) {
                            addrs.push(bytes);
                        }
                    }
                }
            }
        }

        addrs
    }

    /// Batch-fetch balance and nonce for a list of addresses.
    /// Uses "latest" to avoid requiring an archive node.
    pub fn get_account_states(&self, addresses: &[Vec<u8>]) -> Result<Vec<AccountUpdate>, String> {
        let block_tag = "latest";
        let mut requests = Vec::with_capacity(addresses.len() * 2);

        for addr in addresses {
            let addr_hex = format!("0x{}", hex::encode(addr));
            requests.push(("eth_getBalance".to_string(), json!([&addr_hex, block_tag])));
            requests.push((
                "eth_getTransactionCount".to_string(),
                json!([&addr_hex, block_tag]),
            ));
        }

        // Split into chunks and pace them: public endpoints rate-limit by
        // calls per second, and every entry in a JSON-RPC batch counts.
        let chunk_size = 100; // 50 addresses × 2 calls each
        let mut all_results = Vec::with_capacity(requests.len());
        for (i, chunk) in requests.chunks(chunk_size).enumerate() {
            if i > 0 {
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
            let chunk_results = self.batch_call(&chunk.to_vec())?;
            all_results.extend(chunk_results);
        }

        let mut updates = Vec::with_capacity(addresses.len());
        for (i, addr) in addresses.iter().enumerate() {
            let balance_val = &all_results[i * 2];
            let nonce_val = &all_results[i * 2 + 1];

            let balance = parse_hex_u256(balance_val);
            let nonce = parse_hex_u64(nonce_val).unwrap_or(0);

            updates.push(AccountUpdate {
                address: addr.clone(),
                balance,
                nonce,
            });
        }

        Ok(updates)
    }

    /// Fetch a block's account changes from a `prestateTracer` state diff.
    ///
    /// The diff names every account the EVM changed, so it catches value moved
    /// inside a contract call and credited by a withdrawal, neither of which
    /// appears in a transaction's `from` or `to`. It also carries the new
    /// balance and nonce, so no follow-up reads are needed.
    ///
    /// `post` holds only the fields that changed, so an account whose balance
    /// moved but whose nonce did not arrives without a nonce; each field falls
    /// back to `pre`, which is that account's state entering the transaction.
    /// Later transactions overwrite earlier ones, leaving the block's end state.
    pub fn fetch_block_updates_via_diff(
        &self,
        block_num: u64,
    ) -> Result<Vec<AccountUpdate>, String> {
        let block_hex = format!("0x{:x}", block_num);
        let config = json!({"tracer": "prestateTracer", "tracerConfig": {"diffMode": true}});
        let result = self.call("debug_traceBlockByNumber", json!([block_hex, config]))?;
        let traces = result.as_array().ok_or("trace result is not an array")?;
        Ok(updates_from_prestate_diff(traces))
    }

    /// Fetch a block and return all account state changes.
    /// Returns (block_number, updates).
    ///
    /// Prefers a state diff. An endpoint without the `debug` namespace falls
    /// back to the addresses named by the block's transactions, which is
    /// incomplete: measured against mainnet diffs it misses about a quarter of
    /// the accounts a block changes, and those stay stale until their next
    /// transaction. The fallback is decided once and remembered.
    pub fn fetch_block_updates(&self, block_num: u64) -> Result<(u64, Vec<AccountUpdate>), String> {
        if self.diff_support.load(Ordering::Relaxed) != DIFF_NO {
            match self.fetch_block_updates_via_diff(block_num) {
                Ok(updates) => {
                    if self.diff_support.swap(DIFF_YES, Ordering::Relaxed) == DIFF_UNKNOWN {
                        eprintln!("chain: following state diffs (prestateTracer, diffMode)");
                    }
                    return Ok((block_num, updates));
                }
                Err(e) if is_unsupported_method(&e) => {
                    if self.diff_support.swap(DIFF_NO, Ordering::Relaxed) != DIFF_NO {
                        eprintln!(
                            "chain: endpoint has no debug_traceBlockByNumber ({}); \
                             falling back to transaction from/to, which misses accounts \
                             changed inside contract calls",
                            e
                        );
                    }
                }
                Err(e) => return Err(e),
            }
        }

        let block = self.get_block(block_num)?;
        let addresses = Self::addresses_from_block(&block);

        if addresses.is_empty() {
            return Ok((block_num, vec![]));
        }

        let updates = self.get_account_states(&addresses)?;
        Ok((block_num, updates))
    }

    /// Resync a cuckoo table from `start_block+1` to the chain head.
    ///
    /// Two-phase approach to minimize RPC calls:
    ///   Phase 1: Fetch all blocks, collect the unique set of touched addresses.
    ///   Phase 2: Batch-fetch balance/nonce for each unique address once at the latest block.
    ///
    /// Repeats automatically until fully caught up (new blocks may arrive during sync).
    /// If `target_block` is `Some`, syncs to exactly that block without repeating.
    pub fn resync(
        &self,
        table: &mut CuckooTable,
        start_block: u64,
        target_block: Option<u64>,
    ) -> Result<ResyncResult, String> {
        let fixed_target = target_block.is_some();
        let mut synced_to = start_block;
        let mut total_changes_all = 0usize;
        let mut total_blocks_all = 0u64;
        let mut prev_gap = u64::MAX;
        let t_global = Instant::now();
        let mut round = 0u32;

        loop {
            // Sleep between rounds to avoid rate limiting
            if round > 0 {
                std::thread::sleep(std::time::Duration::from_secs(2));
            }

            let target = match target_block {
                Some(t) => t,
                None => {
                    // Retry block_number with backoff
                    let mut retries = 0u32;
                    loop {
                        match self.block_number() {
                            Ok(n) => break n,
                            Err(e) => {
                                retries += 1;
                                let backoff = std::cmp::min(1u64 << retries, 30);
                                log::warn!("block_number failed: {} (retry in {}s)", e, backoff);
                                std::thread::sleep(std::time::Duration::from_secs(backoff));
                            }
                        }
                    }
                }
            };

            if target <= synced_to {
                if round == 0 {
                    eprintln!("Already at block #{}. Nothing to do.", synced_to);
                }
                break;
            }

            let total_blocks = target - synced_to;
            round += 1;
            if round > 1 {
                eprintln!(
                    "\n--- Round {}: {} new blocks arrived during sync ---",
                    round, total_blocks
                );
            }

            let t0 = Instant::now();

            // Phase 1: collect unique addresses from all blocks
            eprintln!(
                "Phase 1: scanning {} blocks for touched addresses...",
                total_blocks
            );
            let mut all_addresses: HashSet<Vec<u8>> = HashSet::new();
            let mut current = synced_to;
            let mut consecutive_errors = 0u32;

            while current < target {
                current += 1;

                let block = match self.get_block(current) {
                    Ok(b) => {
                        consecutive_errors = 0;
                        b
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        let backoff = std::cmp::min(1u64 << consecutive_errors, 30);
                        log::warn!(
                            "Failed to fetch block #{}: {} (retry in {}s)",
                            current,
                            e,
                            backoff
                        );
                        current -= 1;
                        std::thread::sleep(std::time::Duration::from_secs(backoff));
                        continue;
                    }
                };

                let addrs = Self::addresses_from_block(&block);
                all_addresses.extend(addrs);

                let done = current - synced_to;
                if done % 100 == 0 || current == target {
                    let pct = done as f64 / total_blocks as f64 * 100.0;
                    let elapsed = t0.elapsed().as_secs_f64();
                    let rate = done as f64 / elapsed;
                    let remaining = (total_blocks - done) as f64 / rate;
                    eprintln!(
                        "  Blocks: {}/{} ({:.0}%) | {} unique addresses | ~{:.0}s remaining",
                        done,
                        total_blocks,
                        pct,
                        all_addresses.len(),
                        remaining,
                    );
                }

                // Rate limit: ~50ms between block fetches (only 1 RPC call each)
                std::thread::sleep(std::time::Duration::from_millis(50));
            }

            eprintln!(
                "Phase 1 done: {} unique addresses from {} blocks ({:.1}s)",
                all_addresses.len(),
                total_blocks,
                t0.elapsed().as_secs_f64()
            );

            // Phase 2: batch-fetch state at the latest block (not target — could be newer)
            let state_block = if fixed_target {
                target
            } else {
                self.block_number().unwrap_or(target)
            };
            eprintln!(
                "Phase 2: fetching state for {} addresses at latest (head ~#{})...",
                all_addresses.len(),
                state_block
            );
            let t1 = Instant::now();
            let addr_list: Vec<Vec<u8>> = all_addresses.into_iter().collect();
            let mut round_changes = 0usize;

            let chunk_size = 100;
            for (chunk_idx, chunk) in addr_list.chunks(chunk_size).enumerate() {
                let mut retry_errors = 0u32;
                let updates = loop {
                    match self.get_account_states(chunk) {
                        Ok(u) => break u,
                        Err(e) => {
                            retry_errors += 1;
                            let backoff = std::cmp::min(1u64 << retry_errors, 30);
                            log::warn!("Batch {} failed: {} (retry in {}s)", chunk_idx, e, backoff);
                            std::thread::sleep(std::time::Duration::from_secs(backoff));
                        }
                    }
                };

                for update in &updates {
                    let value = account_update_to_value(update);
                    table.upsert(&update.address, &value);
                    round_changes += 1;
                }

                let done = (chunk_idx + 1) * chunk_size;
                let total_addrs = addr_list.len();
                if (chunk_idx + 1) % 10 == 0 || done >= total_addrs {
                    let pct = std::cmp::min(done, total_addrs) as f64 / total_addrs as f64 * 100.0;
                    let elapsed = t1.elapsed().as_secs_f64();
                    let rate = done as f64 / elapsed;
                    let remaining = (total_addrs.saturating_sub(done)) as f64 / rate;
                    eprintln!(
                        "  Addresses: {}/{} ({:.0}%) | ~{:.0}s remaining",
                        std::cmp::min(done, total_addrs),
                        total_addrs,
                        pct,
                        remaining,
                    );
                }

                std::thread::sleep(std::time::Duration::from_millis(200));
            }

            eprintln!(
                "Round {} done: {} blocks, {} addresses ({:.1}s)",
                round,
                total_blocks,
                round_changes,
                t0.elapsed().as_secs_f64(),
            );

            // Advance to state_block (latest head at Phase 2 time).
            // Addresses' balances/nonces are correct at state_block.
            synced_to = state_block;

            total_changes_all += round_changes;
            total_blocks_all += total_blocks;

            // Fixed target: single round
            if fixed_target {
                break;
            }

            // Stop when the gap stopped shrinking — we're as caught up as
            // this RPC rate limit allows. The server's live block thread handles the rest.
            if round >= 2 && total_blocks >= prev_gap {
                break;
            }
            prev_gap = total_blocks;
        }

        eprintln!(
            "Resync complete: {} total blocks, {} addresses updated, {} rounds ({:.1}s)",
            total_blocks_all,
            total_changes_all,
            round,
            t_global.elapsed().as_secs_f64(),
        );

        Ok(ResyncResult {
            blocks_processed: total_blocks_all,
            last_block: synced_to,
            total_changes: total_changes_all,
        })
    }
}

/// Result of a resync operation.
pub struct ResyncResult {
    pub blocks_processed: u64,
    pub last_block: u64,
    pub total_changes: usize,
}

/// Convert an AccountUpdate to the 40-byte value format used by the cuckoo table.
/// Format: [16B zero-pad][16B balance (last 16 bytes, BE)][8B nonce (BE)]
pub fn account_update_to_value(update: &AccountUpdate) -> Vec<u8> {
    let mut value = vec![0u8; 40];
    // Balance: take last 16 bytes (low 128 bits) — fits u128
    // The full balance is 32 bytes big-endian; copy last 16 into value[16..32]
    let bal = &update.balance;
    if bal.len() >= 16 {
        value[16..32].copy_from_slice(&bal[bal.len() - 16..]);
    } else {
        // Balance fits in fewer than 16 bytes, right-align
        let start = 32 - bal.len();
        value[start..32].copy_from_slice(bal);
    }
    // Nonce
    value[32..40].copy_from_slice(&update.nonce.to_be_bytes());
    value
}

// ============================================================================
// Hex parsing helpers
// ============================================================================

fn parse_hex_u64(val: &Value) -> Result<u64, String> {
    let s = val.as_str().ok_or("expected hex string")?;
    let s = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(s, 16).map_err(|e| format!("bad hex u64: {}", e))
}

fn parse_hex_u256(val: &Value) -> Vec<u8> {
    let s = match val.as_str() {
        Some(s) => s,
        None => return vec![0u8; 32],
    };
    let s = s.strip_prefix("0x").unwrap_or(s);
    // JSON-RPC encodes quantities as minimal hex with no leading zeros, so the
    // digit count is odd whenever the leading nibble is nonzero. hex::decode
    // rejects odd-length input, so pad it back to a whole number of bytes.
    let padded;
    let s = if s.len() % 2 == 1 {
        padded = format!("0{}", s);
        padded.as_str()
    } else {
        s
    };
    // Decode hex to bytes, left-pad to 32 bytes. A failure here means the node
    // sent something that is not a quantity, which must not pass silently.
    let raw = match hex::decode(s) {
        Ok(raw) => raw,
        Err(e) => {
            eprintln!("bad hex u256 {:?}: {} (reading as zero)", s, e);
            return vec![0u8; 32];
        }
    };
    let mut out = vec![0u8; 32];
    if raw.len() <= 32 {
        out[32 - raw.len()..].copy_from_slice(&raw);
    } else {
        // Shouldn't happen for balances, but truncate to 32 bytes
        out.copy_from_slice(&raw[raw.len() - 32..]);
    }
    out
}

/// Fold a block's per-transaction prestate diffs into one update per account.
///
/// See `fetch_block_updates_via_diff` for why each field falls back to `pre`.
fn updates_from_prestate_diff(traces: &[Value]) -> Vec<AccountUpdate> {
    let mut state: HashMap<Vec<u8>, (Vec<u8>, u64)> = HashMap::new();
    let mut order: Vec<Vec<u8>> = Vec::new();

    for trace in traces {
        // Each entry is {"txHash": ..., "result": {"pre": ..., "post": ...}},
        // but a bare {"pre": ..., "post": ...} is tolerated too.
        let inner = trace.get("result").unwrap_or(trace);
        let post = match inner.get("post").and_then(|v| v.as_object()) {
            Some(post) => post,
            None => continue,
        };
        let pre = inner.get("pre").and_then(|v| v.as_object());

        for (address_hex, changed) in post {
            let address = match parse_hex_address(address_hex) {
                Some(address) => address,
                None => continue,
            };
            let before = pre.and_then(|pre| pre.get(address_hex));
            let balance = pick_field(changed, before, "balance")
                .map(parse_hex_u256)
                .unwrap_or_else(|| vec![0u8; 32]);
            let nonce = pick_field(changed, before, "nonce")
                .and_then(parse_json_nonce)
                .unwrap_or(0);
            if state.insert(address.clone(), (balance, nonce)).is_none() {
                order.push(address);
            }
        }
    }

    order
        .into_iter()
        .filter_map(|address| {
            let (balance, nonce) = state.remove(&address)?;
            Some(AccountUpdate {
                address,
                balance,
                nonce,
            })
        })
        .collect()
}

/// A field of a prestate diff entry, taken from the post state when the
/// transaction changed it and from the pre state when it did not.
fn pick_field<'a>(post: &'a Value, pre: Option<&'a Value>, field: &str) -> Option<&'a Value> {
    post.get(field).or_else(|| pre.and_then(|pre| pre.get(field)))
}

/// A prestate nonce is a JSON number, unlike the hex quantities elsewhere in
/// this API. Hex is accepted too, for endpoints that encode it that way.
fn parse_json_nonce(val: &Value) -> Option<u64> {
    if let Some(n) = val.as_u64() {
        return Some(n);
    }
    let s = val.as_str()?;
    u64::from_str_radix(s.strip_prefix("0x").unwrap_or(s), 16).ok()
}

/// Whether an RPC error means the method is absent or its namespace is off,
/// as opposed to a real failure worth propagating.
fn is_unsupported_method(err: &str) -> bool {
    err.contains("-32601") || err.contains("Method not found")
}

fn parse_hex_address(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 40 {
        return None;
    }
    hex::decode(s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn u256(hex: &str) -> u128 {
        let v = parse_hex_u256(&json!(hex));
        u128::from_be_bytes(v[16..32].try_into().unwrap())
    }

    /// Quantities arrive as minimal hex, so half of them have an odd digit
    /// count. Decoding those must not silently yield zero.
    #[test]
    fn parses_minimal_hex_quantities() {
        assert_eq!(u256("0x0"), 0);
        assert_eq!(u256("0x1"), 1);
        assert_eq!(u256("0xff"), 255);
        assert_eq!(u256("0xdc6"), 0xdc6);
        assert_eq!(u256("0x92a5e054800d0b5"), 660_443_672_139_059_381);
        assert_eq!(u256("0x173a75fcf49e44a7c"), 26_780_479_053_084_510_844);
        assert_eq!(u256("0xde0b6b3a7640000"), 1_000_000_000_000_000_000);
    }

    #[test]
    fn parse_hex_u256_is_big_endian_right_aligned() {
        let v = parse_hex_u256(&json!("0x1"));
        assert_eq!(v.len(), 32);
        assert_eq!(v[31], 1);
        assert!(v[..31].iter().all(|&b| b == 0));
    }

    /// An account value round-trips through the layout the client decodes.
    #[test]
    fn account_value_round_trips_balance_and_nonce() {
        let update = AccountUpdate {
            address: vec![0xab; 20],
            balance: parse_hex_u256(&json!("0x92a5e054800d0b5")),
            nonce: 116_692,
        };
        let value = account_update_to_value(&update);
        assert_eq!(value.len(), 40);
        let balance = u128::from_be_bytes(value[16..32].try_into().unwrap());
        let nonce = u64::from_be_bytes(value[32..40].try_into().unwrap());
        assert_eq!(balance, 660_443_672_139_059_381);
        assert_eq!(nonce, 116_692);
    }

    const A: &str = "0x1111111111111111111111111111111111111111";
    const B: &str = "0x2222222222222222222222222222222222222222";
    const C: &str = "0x3333333333333333333333333333333333333333";

    fn balance_of(update: &AccountUpdate) -> u128 {
        u128::from_be_bytes(update.balance[16..32].try_into().unwrap())
    }

    fn find<'a>(updates: &'a [AccountUpdate], address_hex: &str) -> &'a AccountUpdate {
        let want = parse_hex_address(address_hex).unwrap();
        updates
            .iter()
            .find(|u| u.address == want)
            .unwrap_or_else(|| panic!("no update for {address_hex}"))
    }

    /// post carries only what the transaction changed, so every other field has
    /// to come from pre. Reading a missing nonce as zero would roll accounts back.
    #[test]
    fn diff_takes_unchanged_fields_from_pre() {
        let traces = [json!({
            "txHash": "0xaa",
            "result": {
                "pre":  { A: {"balance": "0x64", "nonce": 7},
                          B: {"balance": "0x5",  "nonce": 3} },
                // A spent, so only its balance moved; B only touched storage.
                "post": { A: {"balance": "0xc8"},
                          B: {"storage": {"0x00": "0x01"}} },
            }
        })];
        let updates = updates_from_prestate_diff(&traces);
        assert_eq!(updates.len(), 2);
        assert_eq!(balance_of(find(&updates, A)), 200);
        assert_eq!(find(&updates, A).nonce, 7);
        assert_eq!(balance_of(find(&updates, B)), 5);
        assert_eq!(find(&updates, B).nonce, 3);
    }

    /// An account can have its balance changed by one transaction and its nonce
    /// by a later one. Each field must end on the last value the block gave it.
    #[test]
    fn diff_folds_changes_across_transactions() {
        let traces = [
            json!({"result": {
                "pre":  { A: {"balance": "0x64", "nonce": 7} },
                "post": { A: {"balance": "0xc8", "nonce": 8} },
            }}),
            json!({"result": {
                "pre":  { A: {"balance": "0xc8", "nonce": 8} },
                "post": { A: {"nonce": 9} },
            }}),
        ];
        let updates = updates_from_prestate_diff(&traces);
        assert_eq!(updates.len(), 1, "one update per account, not per transaction");
        assert_eq!(balance_of(&updates[0]), 200);
        assert_eq!(updates[0].nonce, 9);
    }

    /// An account created by the block appears in post with no pre entry.
    #[test]
    fn diff_handles_accounts_absent_from_pre() {
        let traces = [json!({"result": {
            "pre":  {},
            "post": { C: {"balance": "0x1", "nonce": 0} },
        }})];
        let updates = updates_from_prestate_diff(&traces);
        assert_eq!(updates.len(), 1);
        assert_eq!(balance_of(&updates[0]), 1);
        assert_eq!(updates[0].nonce, 0);
    }

    /// Tolerate a trace entry that is the diff itself rather than {txHash, result}.
    #[test]
    fn diff_accepts_an_unwrapped_entry() {
        let traces = [json!({
            "pre":  { A: {"balance": "0x64", "nonce": 7} },
            "post": { A: {"balance": "0x92a5e054800d0b5"} },
        })];
        let updates = updates_from_prestate_diff(&traces);
        assert_eq!(balance_of(&updates[0]), 660_443_672_139_059_381);
        assert_eq!(updates[0].nonce, 7);
    }

    #[test]
    fn unsupported_method_is_recognized() {
        assert!(is_unsupported_method(
            "RPC error: {\"code\":-32601,\"message\":\"Method not found: debug_traceBlockByNumber\"}"
        ));
        assert!(!is_unsupported_method("RPC error: {\"code\":-32000}"));
    }
}
