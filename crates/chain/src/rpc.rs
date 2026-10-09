//! Minimal Ethereum JSON-RPC client for fetching real block data.
//!
//! Used by the server's follower loop (`--eth-rpc`) and the standalone
//! `resync` tool.

use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use pir_keyword::account::{delegate_from_code, AccountValue};
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

/// State change extracted from a block: an address with its new balance,
/// nonce and EIP-7702 delegate.
pub struct AccountUpdate {
    pub address: Vec<u8>, // 20 bytes
    pub balance: Vec<u8>, // 32 bytes, big-endian
    pub nonce: u64,
    /// The contract the account's code delegates to, if its code is an
    /// EIP-7702 designator.
    pub delegate: Option<[u8; 20]>,
}

/// A contract storage slot's value at the end of a block. Zero means the
/// block emptied the slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageUpdate {
    pub contract: [u8; 20],
    pub slot: [u8; 32],
    pub value: [u8; 32],
}

/// What one transaction changed, from its prestate diff.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxDiff {
    /// Every account the transaction changed, its sender included.
    pub accounts: Vec<[u8; 20]>,
    /// Every storage slot it changed, with the value it left there.
    pub storage: Vec<StorageUpdate>,
    /// The accounts whose code it changed.
    pub code: Vec<[u8; 20]>,
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

    /// Batch-fetch balance, nonce and delegate for a list of addresses.
    /// Uses "latest" to avoid requiring an archive node.
    pub fn get_account_states(&self, addresses: &[Vec<u8>]) -> Result<Vec<AccountUpdate>, String> {
        let block_tag = "latest";
        let mut requests = Vec::with_capacity(addresses.len() * 3);

        for addr in addresses {
            let addr_hex = format!("0x{}", hex::encode(addr));
            requests.push(("eth_getBalance".to_string(), json!([&addr_hex, block_tag])));
            requests.push((
                "eth_getTransactionCount".to_string(),
                json!([&addr_hex, block_tag]),
            ));
            requests.push(("eth_getCode".to_string(), json!([&addr_hex, block_tag])));
        }

        // Split into chunks and pace them: public endpoints rate-limit by
        // calls per second, and every entry in a JSON-RPC batch counts.
        let chunk_size = 150; // 50 addresses × 3 calls each
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
            let balance_val = &all_results[i * 3];
            let nonce_val = &all_results[i * 3 + 1];
            let code_val = &all_results[i * 3 + 2];

            let balance = parse_hex_u256(balance_val);
            let nonce = parse_hex_u64(nonce_val).unwrap_or(0);
            let delegate = code_val.as_str().and_then(parse_hex_bytes).and_then(|c| delegate_from_code(&c));

            updates.push(AccountUpdate {
                address: addr.clone(),
                balance,
                nonce,
                delegate,
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
    ///
    /// The one change a diff cannot show is a delegation being cleared: an
    /// ethrex node leaves code out of `post` when it becomes empty, just as
    /// when it stays the same. Clearing takes an authorization, which moves
    /// the account's nonce, so a delegated account whose nonce moved is
    /// checked with `eth_getCode` at the block. That is ten to twenty accounts
    /// a block on mainnet, in one batch.
    pub fn fetch_block_updates_via_diff(
        &self,
        block_num: u64,
    ) -> Result<Vec<AccountUpdate>, String> {
        let block_hex = format!("0x{:x}", block_num);
        let config = json!({"tracer": "prestateTracer", "tracerConfig": {"diffMode": true}});
        let result = self.call("debug_traceBlockByNumber", json!([block_hex, config]))?;
        let traces = result.as_array().ok_or("trace result is not an array")?;
        let (mut updates, unsure) = updates_from_prestate_diff(traces);
        if !unsure.is_empty() {
            let addresses: Vec<&[u8]> = unsure.iter().map(|&i| updates[i].address.as_slice()).collect();
            let delegates = self.delegates_at(&addresses, block_num)?;
            for (&i, delegate) in unsure.iter().zip(delegates) {
                updates[i].delegate = delegate;
            }
        }
        Ok(updates)
    }

    /// The delegate of each address at the end of `block`, from its code.
    pub fn delegates_at(&self, addresses: &[&[u8]], block: u64) -> Result<Vec<Option<[u8; 20]>>, String> {
        let block_hex = format!("0x{:x}", block);
        let requests: Vec<(&str, Value)> = addresses
            .iter()
            .map(|a| ("eth_getCode", json!([format!("0x{}", hex::encode(a)), block_hex])))
            .collect();
        self.batch_raw(&requests)?
            .into_iter()
            .zip(addresses)
            .map(|(result, a)| {
                let code = result
                    .map_err(|e| format!("eth_getCode 0x{} at block {block}: {e}", hex::encode(a)))?;
                let code = code
                    .as_str()
                    .and_then(parse_hex_bytes)
                    .ok_or_else(|| format!("eth_getCode 0x{} returned {code}", hex::encode(a)))?;
                Ok(delegate_from_code(&code))
            })
            .collect()
    }

    /// Fetch the storage changes a block made to `contracts`, from the same
    /// state diff. There is no fallback, unlike for accounts: a transaction's
    /// from and to say nothing about storage, so an endpoint without the
    /// `debug` namespace, or a block older than the node can trace, is an
    /// error for the caller to surface rather than a gap to paper over.
    pub fn fetch_block_storage_updates(
        &self,
        block_num: u64,
        contracts: &[[u8; 20]],
    ) -> Result<Vec<StorageUpdate>, String> {
        let block_hex = format!("0x{:x}", block_num);
        let config = json!({"tracer": "prestateTracer", "tracerConfig": {"diffMode": true}});
        let result = self.call("debug_traceBlockByNumber", json!([block_hex, config]))?;
        let traces = result.as_array().ok_or("trace result is not an array")?;
        storage_updates_from_prestate_diff(traces, contracts)
    }

    /// Fetch what each transaction of a block changed, from the same state
    /// diff, kept apart per transaction so a caller can tell which accounts a
    /// change came with. No fallback, as for storage.
    pub fn fetch_block_tx_diffs(&self, block_num: u64) -> Result<Vec<TxDiff>, String> {
        let block_hex = format!("0x{:x}", block_num);
        let config = json!({"tracer": "prestateTracer", "tracerConfig": {"diffMode": true}});
        let result = self.call("debug_traceBlockByNumber", json!([block_hex, config]))?;
        let traces = result.as_array().ok_or("trace result is not an array")?;
        tx_diffs_from_prestate(traces)
    }

    /// Logs of one block emitted by `address` with first topic `topic0`.
    pub fn get_logs(&self, block_num: u64, address: &str, topic0: &str) -> Result<Vec<Value>, String> {
        let block_hex = format!("0x{:x}", block_num);
        let filter = json!({
            "fromBlock": block_hex,
            "toBlock": block_hex,
            "address": address,
            "topics": [topic0],
        });
        let result = self.call("eth_getLogs", json!([filter]))?;
        result
            .as_array()
            .cloned()
            .ok_or_else(|| "logs result is not an array".into())
    }

    /// A batch call that keeps each item's error, unlike `batch_call`, since a
    /// reverted `eth_call` carries its answer in the error's data. Items come
    /// back in request order.
    pub fn batch_raw(&self, requests: &[(&str, Value)]) -> Result<Vec<Result<Value, Value>>, String> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
        let batch: Vec<Value> = requests
            .iter()
            .enumerate()
            .map(|(i, (method, params))| {
                json!({"jsonrpc": "2.0", "method": method, "params": params, "id": i})
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
        let mut out: Vec<Option<Result<Value, Value>>> = vec![None; requests.len()];
        for item in arr {
            let id = item["id"].as_u64().ok_or("batch item without id")? as usize;
            let slot = out.get_mut(id).ok_or("batch item with an unknown id")?;
            *slot = Some(match item.get("error") {
                Some(err) => Err(err.clone()),
                None => Ok(item.get("result").cloned().unwrap_or(Value::Null)),
            });
        }
        out.into_iter()
            .map(|r| r.ok_or_else(|| "batch response missing an item".to_string()))
            .collect()
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
                // A block older than the node's state history cannot be traced,
                // which is what catching up from a snapshot runs into: the
                // snapshot sits at the far edge of that window and the window
                // moves on while the snapshot loads. Take the weaker source for
                // this block rather than stalling on it, and say so, because
                // accounts changed inside a contract call go unseen until they
                // are touched again. Later blocks are tried as diffs as usual.
                Err(e) => {
                    eprintln!(
                        "chain: block #{block_num} could not be traced ({e}); \
                         using transaction from/to for it, which misses accounts \
                         changed inside contract calls"
                    );
                }
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

/// Convert an AccountUpdate to the 40-byte value an account table holds
/// (see `pir_keyword::account`).
pub fn account_update_to_value(update: &AccountUpdate) -> Vec<u8> {
    // The balance arrives as 32 bytes; every real one fits the low 16.
    let bal = &update.balance;
    let mut low = [0u8; 16];
    let take = bal.len().min(16);
    low[16 - take..].copy_from_slice(&bal[bal.len() - take..]);
    AccountValue {
        balance: u128::from_be_bytes(low),
        nonce: update.nonce,
        delegate: update.delegate,
    }
    .pack()
    .to_vec()
}

// ============================================================================
// Hex parsing helpers
// ============================================================================

fn parse_hex_u64(val: &Value) -> Result<u64, String> {
    let s = val.as_str().ok_or("expected hex string")?;
    let s = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(s, 16).map_err(|e| format!("bad hex u64: {}", e))
}

/// Bytes as `eth_getCode` and the tracer return them: 0x, then whole bytes.
fn parse_hex_bytes(s: &str) -> Option<Vec<u8>> {
    hex::decode(s.strip_prefix("0x").unwrap_or(s)).ok()
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

/// Fold a block's per-transaction prestate diffs into one update per account,
/// and say which updates' delegates the diffs could not settle (indices into
/// the updates).
///
/// See `fetch_block_updates_via_diff` for why each field falls back to `pre`,
/// and why a delegated account whose nonce moved is unsettled.
fn updates_from_prestate_diff(traces: &[Value]) -> (Vec<AccountUpdate>, Vec<usize>) {
    let mut state: HashMap<Vec<u8>, (Vec<u8>, u64, Option<[u8; 20]>, bool)> = HashMap::new();
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
            let code_of = |account: &Value| {
                account.get("code").and_then(|c| c.as_str()).and_then(parse_hex_bytes)
            };
            let (delegate, unsure) = match (code_of(changed), before.and_then(code_of)) {
                (Some(code), _) => (delegate_from_code(&code), false),
                (None, Some(code)) => {
                    let delegate = delegate_from_code(&code);
                    (delegate, delegate.is_some() && changed.get("nonce").is_some())
                }
                (None, None) => (None, false),
            };
            if state.insert(address.clone(), (balance, nonce, delegate, unsure)).is_none() {
                order.push(address);
            }
        }
    }

    let mut unsure_at = Vec::new();
    let updates = order
        .into_iter()
        .filter_map(|address| {
            let (balance, nonce, delegate, unsure) = state.remove(&address)?;
            Some((address, balance, nonce, delegate, unsure))
        })
        .enumerate()
        .map(|(i, (address, balance, nonce, delegate, unsure))| {
            if unsure {
                unsure_at.push(i);
            }
            AccountUpdate {
                address,
                balance,
                nonce,
                delegate,
            }
        })
        .collect();
    (updates, unsure_at)
}

/// Fold a block's per-transaction prestate diffs into the end-of-block value of
/// every slot of `contracts` the block changed.
///
/// In diff mode `pre` holds the old value of each slot a transaction changed
/// and `post` the new one, except that a slot set to zero is left out of
/// `post`, so a slot in `pre` but not in `post` went to zero. Checked against
/// `eth_getStorageAt` on an ethrex node: 2,770 token slot changes over 10
/// mainnet blocks, 399 of them to zero, all matched. Later transactions
/// overwrite earlier ones, leaving the block's end state.
fn storage_updates_from_prestate_diff(
    traces: &[Value],
    contracts: &[[u8; 20]],
) -> Result<Vec<StorageUpdate>, String> {
    let mut state: HashMap<([u8; 20], [u8; 32]), [u8; 32]> = HashMap::new();
    let mut order: Vec<([u8; 20], [u8; 32])> = Vec::new();
    let mut record = |contract: [u8; 20], slot: [u8; 32], value: [u8; 32]| {
        if state.insert((contract, slot), value).is_none() {
            order.push((contract, slot));
        }
    };

    for trace in traces {
        let inner = trace.get("result").unwrap_or(trace);
        let pre = inner.get("pre").and_then(|v| v.as_object());
        let post = inner.get("post").and_then(|v| v.as_object());
        let storage_of = |side: Option<&serde_json::Map<String, Value>>, address_hex: &str| {
            side.and_then(|s| s.get(address_hex))
                .and_then(|account| account.get("storage"))
                .and_then(|storage| storage.as_object())
                .cloned()
        };
        // An account whose changed slots all went to zero may appear in `pre`
        // alone, so walk both sides.
        let mut addresses: Vec<&String> = pre.into_iter().flat_map(|p| p.keys()).collect();
        addresses.extend(post.into_iter().flat_map(|p| p.keys()));
        addresses.sort();
        addresses.dedup();

        for address_hex in addresses {
            let contract: [u8; 20] = match parse_hex_address(address_hex) {
                Some(address) => address.try_into().unwrap(),
                None => continue,
            };
            if !contracts.contains(&contract) {
                continue;
            }
            let before = storage_of(pre, address_hex).unwrap_or_default();
            let after = storage_of(post, address_hex).unwrap_or_default();
            for (slot_hex, value) in &after {
                let slot = parse_hex_word(slot_hex).ok_or(format!("bad slot {slot_hex}"))?;
                let value = value
                    .as_str()
                    .and_then(parse_hex_word)
                    .ok_or(format!("bad value for slot {slot_hex}"))?;
                record(contract, slot, value);
            }
            for slot_hex in before.keys().filter(|slot| !after.contains_key(*slot)) {
                let slot = parse_hex_word(slot_hex).ok_or(format!("bad slot {slot_hex}"))?;
                record(contract, slot, [0u8; 32]);
            }
        }
    }

    Ok(order
        .into_iter()
        .map(|(contract, slot)| StorageUpdate {
            contract,
            slot,
            value: state[&(contract, slot)],
        })
        .collect())
}

/// Split a block's prestate diffs into what each transaction changed. Storage
/// follows the same rule as `storage_updates_from_prestate_diff`: a slot in
/// `pre` but not in `post` went to zero.
fn tx_diffs_from_prestate(traces: &[Value]) -> Result<Vec<TxDiff>, String> {
    let mut out = Vec::with_capacity(traces.len());
    for trace in traces {
        let inner = trace.get("result").unwrap_or(trace);
        let pre = inner.get("pre").and_then(|v| v.as_object());
        let post = inner.get("post").and_then(|v| v.as_object());
        let mut addresses: Vec<&String> = pre.into_iter().flat_map(|p| p.keys()).collect();
        addresses.extend(post.into_iter().flat_map(|p| p.keys()));
        addresses.sort();
        addresses.dedup();

        let mut diff = TxDiff::default();
        for address_hex in addresses {
            let Some(address) = parse_hex_address(address_hex) else { continue };
            let address: [u8; 20] = address.try_into().unwrap();
            diff.accounts.push(address);
            let pre_account = pre.and_then(|p| p.get(address_hex));
            let post_account = post.and_then(|p| p.get(address_hex));
            let storage_of = |account: Option<&Value>| {
                account
                    .and_then(|account| account.get("storage"))
                    .and_then(|storage| storage.as_object())
                    .cloned()
                    .unwrap_or_default()
            };
            let before = storage_of(pre_account);
            let after = storage_of(post_account);
            for (slot_hex, value) in &after {
                let slot = parse_hex_word(slot_hex).ok_or(format!("bad slot {slot_hex}"))?;
                let value = value
                    .as_str()
                    .and_then(parse_hex_word)
                    .ok_or(format!("bad value for slot {slot_hex}"))?;
                diff.storage.push(StorageUpdate { contract: address, slot, value });
            }
            for slot_hex in before.keys().filter(|slot| !after.contains_key(*slot)) {
                let slot = parse_hex_word(slot_hex).ok_or(format!("bad slot {slot_hex}"))?;
                diff.storage.push(StorageUpdate { contract: address, slot, value: [0u8; 32] });
            }
            if post_account.and_then(|account| account.get("code")).is_some() {
                diff.code.push(address);
            }
        }
        out.push(diff);
    }
    Ok(out)
}

/// A 32-byte word from hex, right-aligned, as storage slots and their values
/// arrive (usually padded to 64 digits, but minimal hex is read too).
fn parse_hex_word(s: &str) -> Option<[u8; 32]> {
    let digits = s.strip_prefix("0x").unwrap_or(s);
    if digits.len() > 64 {
        return None;
    }
    let raw = hex::decode(format!("{:0>64}", digits)).ok()?;
    raw.try_into().ok()
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
    fn account_value_round_trips_balance_nonce_and_delegate() {
        let update = AccountUpdate {
            address: vec![0xab; 20],
            balance: parse_hex_u256(&json!("0x92a5e054800d0b5")),
            nonce: 116_692,
            delegate: Some([0x63; 20]),
        };
        let value = account_update_to_value(&update);
        assert_eq!(
            AccountValue::unpack(&value),
            Some(AccountValue {
                balance: 660_443_672_139_059_381,
                nonce: 116_692,
                delegate: Some([0x63; 20]),
            })
        );
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
        let (updates, _) = updates_from_prestate_diff(&traces);
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
        let (updates, _) = updates_from_prestate_diff(&traces);
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
        let (updates, _) = updates_from_prestate_diff(&traces);
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
        let (updates, _) = updates_from_prestate_diff(&traces);
        assert_eq!(balance_of(&updates[0]), 660_443_672_139_059_381);
        assert_eq!(updates[0].nonce, 7);
    }

    const DESIGNATOR_1: &str = "0xef010063c0c19a282a1b52b07dd5a65b58948a07dae32b";
    const DESIGNATOR_2: &str = "0xef010084d05511614272694d3a9cebe896514dbde51f40";

    fn delegate(designator: &str) -> Option<[u8; 20]> {
        delegate_from_code(&parse_hex_bytes(designator).unwrap())
    }

    /// Delegation as an ethrex node traced it on mainnet (blocks 26,153,077 to
    /// 26,153,120): set on a new account, changed, kept while the balance
    /// moves, and possibly cleared, where `post` looks the same as when
    /// nothing happened to the code but the nonce moved.
    #[test]
    fn diff_reads_delegates_and_flags_what_it_cannot_settle() {
        let traces = [json!({"result": {
            "pre":  { A: {},
                      B: {"balance": "0x0", "code": DESIGNATOR_1, "nonce": 545_278},
                      C: {"balance": "0xd454fe5cb745f", "code": DESIGNATOR_1, "nonce": 56} },
            "post": { A: {"code": DESIGNATOR_1, "nonce": 1},
                      B: {"code": DESIGNATOR_2, "nonce": 545_279},
                      C: {"balance": "0xc8caf4d94590e58"} },
        }}), json!({"result": {
            "pre":  { USDT: {"balance": "0x1", "code": "0x6080604052", "nonce": 1},
                      "0x09b1e13a3eb32f1064990b1ab130492da9d1e76e":
                          {"balance": "0x1639771d0190b5d", "code": DESIGNATOR_1, "nonce": 719} },
            "post": { USDT: {"balance": "0x2"},
                      "0x09b1e13a3eb32f1064990b1ab130492da9d1e76e":
                          {"balance": "0x16350981c84169d", "nonce": 721} },
        }})];
        let (updates, unsure) = updates_from_prestate_diff(&traces);
        assert_eq!(find(&updates, A).delegate, delegate(DESIGNATOR_1));
        assert_eq!(find(&updates, B).delegate, delegate(DESIGNATOR_2));
        assert_eq!(find(&updates, C).delegate, delegate(DESIGNATOR_1));
        assert_eq!(find(&updates, USDT).delegate, None, "a contract has no delegate");
        let cleared = "0x09b1e13a3eb32f1064990b1ab130492da9d1e76e";
        let flagged: Vec<String> =
            unsure.iter().map(|&i| format!("0x{}", hex::encode(&updates[i].address))).collect();
        assert_eq!(flagged, vec![cleared.to_string()]);
    }

    /// The last transaction decides, so a delegation set and then followed by
    /// a transaction that moved the nonce is unsettled again, and one cleared
    /// to the zero address on a node that writes empty code is settled.
    #[test]
    fn diff_delegates_follow_the_last_transaction() {
        let traces = [
            json!({"result": {
                "pre":  { A: {"balance": "0x5", "nonce": 3} },
                "post": { A: {"code": DESIGNATOR_1, "nonce": 4} },
            }}),
            json!({"result": {
                "pre":  { A: {"balance": "0x5", "code": DESIGNATOR_1, "nonce": 4},
                          B: {"balance": "0x5", "code": DESIGNATOR_2, "nonce": 9} },
                "post": { A: {"nonce": 5}, B: {"code": "0x", "nonce": 10} },
            }}),
        ];
        let (updates, unsure) = updates_from_prestate_diff(&traces);
        assert_eq!(find(&updates, A).delegate, delegate(DESIGNATOR_1));
        assert_eq!(find(&updates, B).delegate, None);
        assert_eq!(unsure.len(), 1);
        assert_eq!(updates[unsure[0]].address, parse_hex_address(A).unwrap());
    }

    #[test]
    fn unsupported_method_is_recognized() {
        assert!(is_unsupported_method(
            "RPC error: {\"code\":-32601,\"message\":\"Method not found: debug_traceBlockByNumber\"}"
        ));
        assert!(!is_unsupported_method("RPC error: {\"code\":-32000}"));
    }

    const USDT: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";

    fn usdt() -> [u8; 20] {
        parse_hex_address(USDT).unwrap().try_into().unwrap()
    }

    fn word(hex: &str) -> [u8; 32] {
        parse_hex_word(hex).unwrap()
    }

    fn value_of(updates: &[StorageUpdate], slot: &str) -> [u8; 32] {
        updates
            .iter()
            .find(|u| u.slot == word(slot))
            .unwrap_or_else(|| panic!("no update for {slot}"))
            .value
    }

    /// One transaction's USDT diff exactly as an ethrex node returned it
    /// (block 26,128,349), cut to three slots: one emptied, so it is in `pre`
    /// alone, one created, so it is in `post` alone, and one changed.
    #[test]
    fn storage_diff_from_a_real_block() {
        let traces = vec![json!({
            "txHash": "0xc200840c99a5b61ef3fe86abe1cee2d917257cd59cd435aea9aecbc3530159f7",
            "result": {
                "pre": { USDT: { "storage": {
                    "0x57c6e74a94dc6c6b73e1a11d41c41f61dc139d88b2ea858fb8c7ed5be349facc": "0x000000000000000000000000000000000000000000000000000000000cb3d895",
                    "0x8fafb133c724b15b2b281e4b0cfe79f90a1b043ef0bb35747a95949ced0c8c86": "0x00000000000000000000000000000000000000000000000000000000a8ad9716"
                }}},
                "post": { USDT: { "storage": {
                    "0x7e0f773e549c4e14fe5e51f5ac84e164124a89ccbf97234649b7ff00c1923202": "0x000000000000000000000000000000000000000000000000000000000ca0abff",
                    "0x8fafb133c724b15b2b281e4b0cfe79f90a1b043ef0bb35747a95949ced0c8c86": "0x00000000000000000000000000000000000000000000000000000000a8c0c3ac"
                }}}
            }
        })];
        let updates = storage_updates_from_prestate_diff(&traces, &[usdt()]).unwrap();
        assert_eq!(updates.len(), 3);
        assert!(updates.iter().all(|u| u.contract == usdt()));
        assert_eq!(
            value_of(&updates, "0x57c6e74a94dc6c6b73e1a11d41c41f61dc139d88b2ea858fb8c7ed5be349facc"),
            [0u8; 32]
        );
        assert_eq!(
            value_of(&updates, "0x7e0f773e549c4e14fe5e51f5ac84e164124a89ccbf97234649b7ff00c1923202"),
            word("0xca0abff")
        );
        assert_eq!(
            value_of(&updates, "0x8fafb133c724b15b2b281e4b0cfe79f90a1b043ef0bb35747a95949ced0c8c86"),
            word("0xa8c0c3ac")
        );
    }

    /// The last transaction to touch a slot decides its value, including
    /// emptying a slot an earlier transaction filled, and contracts nobody
    /// asked for are left out.
    #[test]
    fn storage_diff_folds_transactions_and_filters_contracts() {
        let other = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
        let traces = vec![
            json!({"result": {
                "pre": {},
                "post": { USDT: { "storage": { "0x01": "0x05", "0x02": "0x07" } },
                          other: { "storage": { "0x01": "0x09" } } }
            }}),
            json!({"result": {
                "pre": { USDT: { "storage": { "0x01": "0x05" } } },
                "post": { USDT: { "balance": "0x0" } }
            }}),
        ];
        let updates = storage_updates_from_prestate_diff(&traces, &[usdt()]).unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(value_of(&updates, "0x01"), [0u8; 32]);
        assert_eq!(value_of(&updates, "0x02"), word("0x07"));
    }

    /// Malformed hex from the node stops the block instead of writing a wrong
    /// value into the table.
    #[test]
    fn storage_diff_refuses_malformed_values() {
        let traces = vec![json!({"result": {
            "pre": {},
            "post": { USDT: { "storage": { "0x01": "0xnot-hex" } } }
        }})];
        assert!(storage_updates_from_prestate_diff(&traces, &[usdt()]).is_err());
    }

    /// Per-transaction diffs keep each transaction's accounts, slots and code
    /// changes apart, and a slot left out of `post` went to zero.
    #[test]
    fn tx_diffs_keep_transactions_apart() {
        let traces = vec![
            json!({"txHash": "0x01", "result": {
                "pre": { A: { "nonce": 1 }, USDT: { "storage": { "0x01": "0x05" } } },
                "post": { A: { "nonce": 2 }, USDT: { "storage": { "0x02": "0x07" } } }
            }}),
            json!({"txHash": "0x02", "result": {
                "pre": { B: { "balance": "0x1" } },
                "post": { B: { "balance": "0x0" }, C: { "code": "0x6001" } }
            }}),
        ];
        let diffs = tx_diffs_from_prestate(&traces).unwrap();
        assert_eq!(diffs.len(), 2);
        let addr = |s: &str| -> [u8; 20] { parse_hex_address(s).unwrap().try_into().unwrap() };
        assert_eq!(diffs[0].accounts, vec![addr(A), usdt()]);
        let mut slots: Vec<_> = diffs[0].storage.iter().map(|u| (u.slot, u.value)).collect();
        slots.sort();
        assert_eq!(slots, vec![(word("0x01"), [0u8; 32]), (word("0x02"), word("0x07"))]);
        assert!(diffs[0].code.is_empty());
        assert_eq!(diffs[1].accounts, vec![addr(B), addr(C)]);
        assert!(diffs[1].storage.is_empty());
        assert_eq!(diffs[1].code, vec![addr(C)]);
    }
}
