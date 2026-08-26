//! Minimal Ethereum JSON-RPC client for fetching real block data.
//!
//! Used by the server's follower loop (`--eth-rpc`) and the standalone
//! `resync` tool.

use serde_json::{json, Value};
use std::collections::HashSet;
use std::time::Instant;

use pir_keyword::cuckoo::CuckooTable;

/// Ethereum JSON-RPC client (synchronous, uses ureq).
pub struct EthRpc {
    url: String,
    agent: ureq::Agent,
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
        let json: Value = resp
            .into_body()
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
        let json: Value = resp
            .into_body()
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

    /// Fetch a block and return all account state changes.
    /// Returns (block_number, updates).
    pub fn fetch_block_updates(&self, block_num: u64) -> Result<(u64, Vec<AccountUpdate>), String> {
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
    // Decode hex to bytes, left-pad to 32 bytes
    let raw = hex::decode(s).unwrap_or_default();
    let mut out = vec![0u8; 32];
    if raw.len() <= 32 {
        out[32 - raw.len()..].copy_from_slice(&raw);
    } else {
        // Shouldn't happen for balances, but truncate to 32 bytes
        out.copy_from_slice(&raw[raw.len() - 32..]);
    }
    out
}

fn parse_hex_address(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 40 {
        return None;
    }
    hex::decode(s).ok()
}
