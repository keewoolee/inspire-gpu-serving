//! Primary names: what an address resolves to in ENS, GNS and WNS, which
//! storage slots that answer read, and CCIP-Read for the ENS names whose
//! forward record lives off chain.
//!
//! The ENS answer is the Universal Resolver's `reverseWithGateways`, called
//! exactly as viem's `getEnsName` calls it, so a table built from it agrees
//! with what a wallet would have fetched itself. The resolver does both halves
//! of a primary name in one call: the reverse record gives a name, and the
//! name's forward record has to point back at the address. GNS and WNS answer
//! through `reverseResolve` on their name contracts, which checks the same
//! thing.
//!
//! What a table has to watch is whatever the answer read. `eth_createAccessList`
//! lists it, but an ethrex node returns an empty list for a call that reverts,
//! and a reverse lookup reverts whenever the forward check fails or needs a
//! gateway. Those are read in two halves that do not revert: the reverse name
//! through `resolveWithGateways(<address>.addr.reverse, name(node))`, and its
//! forward record through `resolveWithGateways(name, addr(node))`, or through
//! `findResolver(name)` when that reverts too.

use crate::rpc::EthRpc;
use pir_keyword::storage::keccak256;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const UNIVERSAL_RESOLVER: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
pub const GNS: &str = "0x9d51d507bc7264d4fe8ad1cf7fe191933a0a81d6";
pub const WNS: &str = "0x0000000000696760e15f265e828db644a0c242eb";

/// The ENS reverse registrar that owns `addr.reverse`, the only one that can
/// still create reverse records there. Its `ReverseClaimed` event names the
/// address, which matters when someone else sets the record for it.
pub const ENS_REVERSE_REGISTRAR: &str = "0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb";
/// The ENSIP-19 default reverse registrar. A name can be set here by
/// signature, from a transaction the address never sends, so only its
/// `NameForAddrChanged` event says which address changed.
pub const ENS_DEFAULT_REVERSE_REGISTRAR: &str = "0x283f227c4bd38ece252c4ae7ece650b0e913f1f9";

pub const REVERSE_CLAIMED: &str = "0x6ada868dd3058cf77a48a74489fd7963688e5464b2b0fa957ace976243270e92";
pub const NAME_FOR_ADDR_CHANGED: &str =
    "0x8af7a4c7007a33f680904f3b64733396b730fef22d79555dee29801ca2e479a9";
pub const PRIMARY_NAME_SET: &str = "0x41f2b80eda6de6f23cab2e867951d054a48d6794db479c1d252bb840a374b62c";

/// Contracts that hold primary-name state: the ENS registries (current and the
/// original one it falls back to), the reverse registrars and resolvers, the
/// public resolvers, the name wrapper and .eth registrar and controller, the
/// Universal Resolver, and GNS and WNS. A transaction that writes any of them
/// may have given one of its accounts a name.
pub const KNOWN_CONTRACTS: [&str; 20] = [
    "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e",
    "0x314159265dd8dbb310642f98f50c066173c1259b",
    "0x9062c0a6dbd6108336bcbe4593a3d1ce05512069",
    "0x084b1c3c81545d370f3634392de611caabff8148",
    ENS_REVERSE_REGISTRAR,
    ENS_DEFAULT_REVERSE_REGISTRAR,
    "0xa7d635c8de9a58a228aa69353a1699c7cc240dcf",
    "0x5fbb459c49bb06083c33109fa4f14810ec2cf358",
    "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
    "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63",
    "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41",
    "0xf29100983e058b709f3d539b0c765937b804ac15",
    "0xdaaf96c344f63131acadd0ea35170e7892d3dfba",
    "0x226159d592e2b063810a10ebf6dcbada94ed68b8",
    "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401",
    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85",
    "0x253553366da8546fc250f225fe3d25d0c782303b",
    UNIVERSAL_RESOLVER,
    GNS,
    WNS,
];

const SEL_REVERSE_WITH_GATEWAYS: [u8; 4] = [0xb7, 0xd6, 0xca, 0x64];
const SEL_RESOLVE_WITH_GATEWAYS: [u8; 4] = [0xa1, 0x47, 0x28, 0x44];
const SEL_FIND_RESOLVER: [u8; 4] = [0xa1, 0xcb, 0xcb, 0xaf];
const SEL_NAME: [u8; 4] = [0x69, 0x1f, 0x34, 0x31];
const SEL_ADDR: [u8; 4] = [0x3b, 0x3b, 0x57, 0xde];
const SEL_REVERSE_RESOLVE: [u8; 4] = [0x9a, 0xf8, 0xb7, 0xaa];
const SEL_OFFCHAIN_LOOKUP: [u8; 4] = [0x55, 0x6f, 0x18, 0x30];
const SEL_BATCH_QUERY: [u8; 4] = [0xa7, 0x80, 0xba, 0xb6];
const SEL_HTTP_ERROR: [u8; 4] = [0x01, 0x80, 0x01, 0x52];

/// Asks the Universal Resolver to hand every gateway lookup back to the
/// caller in one batch, which `call_with_ccip` answers itself. This is what
/// viem passes by default.
pub const LOCAL_BATCH_GATEWAY: &str = "x-batch-gateway:true";

/// EIP-3668 suggests a limit on chained lookups. A reverse lookup needs at most
/// two (reverse name, then forward record).
const MAX_CCIP_ROUNDS: usize = 6;

pub type Address = [u8; 20];

pub fn parse_address(s: &str) -> Option<Address> {
    let digits = s.strip_prefix("0x").unwrap_or(s);
    if digits.len() != 40 {
        return None;
    }
    hex::decode(digits).ok()?.try_into().ok()
}

fn hex0x(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

// ============================================================================
// ABI
// ============================================================================

enum Tok {
    Word([u8; 32]),
    Bytes(Vec<u8>),
    StringArray(Vec<String>),
    BytesArray(Vec<Vec<u8>>),
    BoolArray(Vec<bool>),
}

fn word(n: u64) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[24..].copy_from_slice(&n.to_be_bytes());
    w
}

fn padded(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out.resize(bytes.len().div_ceil(32) * 32, 0);
    out
}

fn encode(tokens: &[Tok]) -> Vec<u8> {
    let head_len = 32 * tokens.len();
    let mut head = Vec::with_capacity(head_len);
    let mut tail = Vec::new();
    for t in tokens {
        match t {
            Tok::Word(w) => head.extend_from_slice(w),
            dynamic => {
                head.extend_from_slice(&word((head_len + tail.len()) as u64));
                tail.extend(encode_dynamic(dynamic));
            }
        }
    }
    head.extend(tail);
    head
}

fn encode_dynamic(t: &Tok) -> Vec<u8> {
    match t {
        Tok::Word(w) => w.to_vec(),
        Tok::Bytes(b) => [word(b.len() as u64).to_vec(), padded(b)].concat(),
        Tok::StringArray(items) => {
            let inner: Vec<Tok> = items.iter().map(|s| Tok::Bytes(s.as_bytes().to_vec())).collect();
            [word(items.len() as u64).to_vec(), encode(&inner)].concat()
        }
        Tok::BytesArray(items) => {
            let inner: Vec<Tok> = items.iter().map(|b| Tok::Bytes(b.clone())).collect();
            [word(items.len() as u64).to_vec(), encode(&inner)].concat()
        }
        Tok::BoolArray(items) => {
            let mut out = word(items.len() as u64).to_vec();
            for &b in items {
                out.extend_from_slice(&word(b as u64));
            }
            out
        }
    }
}

/// Reads ABI-encoded data. Offsets are relative to the start of the slice.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn word(&self, at: usize) -> Result<&'a [u8], String> {
        self.0.get(at..at + 32).ok_or_else(|| "ABI data too short".into())
    }

    fn uint(&self, at: usize) -> Result<usize, String> {
        let w = self.word(at)?;
        if w[..24].iter().any(|&b| b != 0) {
            return Err("ABI offset or length out of range".into());
        }
        Ok(u64::from_be_bytes(w[24..].try_into().unwrap()) as usize)
    }

    fn address(&self, at: usize) -> Result<Address, String> {
        Ok(self.word(at)?[12..].try_into().unwrap())
    }

    /// The dynamic bytes whose offset sits in the head word at `at`.
    fn bytes(&self, at: usize) -> Result<&'a [u8], String> {
        let start = self.uint(at)?;
        let len = self.uint(start)?;
        self.0
            .get(start + 32..start + 32 + len)
            .ok_or_else(|| "ABI bytes run past the end".into())
    }

    fn string_array(&self, at: usize) -> Result<Vec<String>, String> {
        let start = self.uint(at)?;
        let n = self.uint(start)?;
        let items = Reader(self.0.get(start + 32..).ok_or("ABI array runs past the end")?);
        (0..n)
            .map(|i| Ok(String::from_utf8_lossy(items.bytes(32 * i)?).into_owned()))
            .collect()
    }
}

/// The first return value of a function returning `string` first, as
/// `reverseWithGateways` and `reverseResolve` do.
pub fn decode_first_string(output: &[u8]) -> Result<String, String> {
    Ok(String::from_utf8_lossy(Reader(output).bytes(0)?).into_owned())
}

// ============================================================================
// Names and calls
// ============================================================================

/// The ENSIP-19 reverse name of an address on Ethereum: lowercase hex, no 0x.
pub fn reverse_name(address: &Address) -> String {
    format!("{}.addr.reverse", hex::encode(address))
}

/// DNS-encode a name the way the Universal Resolver's NameCoder does: a label
/// longer than 255 bytes becomes its hash in brackets. An empty label cannot
/// be encoded.
pub fn dns_encode(name: &str) -> Option<Vec<u8>> {
    if name.is_empty() {
        return Some(vec![0]);
    }
    let mut out = Vec::with_capacity(name.len() + 2);
    for label in name.split('.') {
        if label.is_empty() {
            return None;
        }
        if label.len() > 255 {
            let hashed = format!("[{}]", hex::encode(keccak256(label.as_bytes())));
            out.push(hashed.len() as u8);
            out.extend_from_slice(hashed.as_bytes());
        } else {
            out.push(label.len() as u8);
            out.extend_from_slice(label.as_bytes());
        }
    }
    out.push(0);
    Some(out)
}

pub fn namehash(name: &str) -> [u8; 32] {
    let mut node = [0u8; 32];
    if name.is_empty() {
        return node;
    }
    for label in name.rsplit('.') {
        let mut input = [0u8; 64];
        input[..32].copy_from_slice(&node);
        input[32..].copy_from_slice(&keccak256(label.as_bytes()));
        node = keccak256(&input);
    }
    node
}

fn gateways() -> Tok {
    Tok::StringArray(vec![LOCAL_BATCH_GATEWAY.to_string()])
}

/// `reverseWithGateways(address, 60, ["x-batch-gateway:true"])`, as viem sends it.
pub fn reverse_call(address: &Address) -> Vec<u8> {
    let args = encode(&[Tok::Bytes(address.to_vec()), Tok::Word(word(60)), gateways()]);
    [SEL_REVERSE_WITH_GATEWAYS.to_vec(), args].concat()
}

fn resolve_call(dns_name: &[u8], inner: Vec<u8>) -> Vec<u8> {
    let args = encode(&[Tok::Bytes(dns_name.to_vec()), Tok::Bytes(inner), gateways()]);
    [SEL_RESOLVE_WITH_GATEWAYS.to_vec(), args].concat()
}

/// The reverse half alone: `name(node)` of `<address>.addr.reverse`.
pub fn reverse_half_call(address: &Address) -> Vec<u8> {
    let name = reverse_name(address);
    let inner = [SEL_NAME.to_vec(), namehash(&name).to_vec()].concat();
    resolve_call(&dns_encode(&name).unwrap(), inner)
}

/// The forward half alone: `addr(node)` of `name`.
pub fn forward_half_call(name: &str) -> Option<Vec<u8>> {
    let inner = [SEL_ADDR.to_vec(), namehash(name).to_vec()].concat();
    Some(resolve_call(&dns_encode(name)?, inner))
}

/// `findResolver(name)`: the registry walk alone, which never reverts.
pub fn find_resolver_call(name: &str) -> Option<Vec<u8>> {
    let args = encode(&[Tok::Bytes(dns_encode(name)?)]);
    Some([SEL_FIND_RESOLVER.to_vec(), args].concat())
}

/// `reverseResolve(address)` on a GNS or WNS name contract.
pub fn ns_reverse_call(address: &Address) -> Vec<u8> {
    [SEL_REVERSE_RESOLVE.to_vec(), [0u8; 12].to_vec(), address.to_vec()].concat()
}

/// The name a `resolveWithGateways(…, name(node), …)` call returned: its
/// `bytes result` is itself an ABI-encoded string.
pub fn decode_reverse_half(output: &[u8]) -> Result<String, String> {
    decode_first_string(Reader(output).bytes(0)?)
}

// ============================================================================
// Batched RPC
// ============================================================================

pub fn call_request(to: &str, data: &[u8]) -> (&'static str, Value) {
    ("eth_call", json!([{"to": to, "data": hex0x(data)}, "latest"]))
}

pub fn access_list_request(to: &str, data: &[u8]) -> (&'static str, Value) {
    ("eth_createAccessList", json!([{"to": to, "data": hex0x(data)}, "latest"]))
}

/// How an `eth_call` came back: its output, or the data it reverted with.
pub enum CallOutcome {
    Output(Vec<u8>),
    Reverted(Vec<u8>),
}

pub fn parse_call(item: Result<Value, Value>) -> Result<CallOutcome, String> {
    let decode = |v: &Value| -> Result<Vec<u8>, String> {
        let s = v.as_str().unwrap_or("0x");
        hex::decode(s.strip_prefix("0x").unwrap_or(s)).map_err(|e| e.to_string())
    };
    match item {
        Ok(out) => Ok(CallOutcome::Output(decode(&out)?)),
        Err(err) => {
            // A revert carries its data; anything else is a failure of the
            // call itself and must not read as an answer.
            let message = err.get("message").and_then(|m| m.as_str()).unwrap_or("");
            match err.get("data") {
                Some(data) if data.is_string() => Ok(CallOutcome::Reverted(decode(data)?)),
                _ if message.contains("revert") => Ok(CallOutcome::Reverted(vec![])),
                _ => Err(format!("eth_call failed: {err}")),
            }
        }
    }
}

/// One thing an answer read: a storage slot, or an account's code when `slot`
/// is `None`.
pub type Read = (Address, Option<[u8; 32]>);

/// The slots and accounts an access list names. `None` when the call
/// reverted, since an ethrex node lists nothing then.
pub fn parse_access_list(item: Result<Value, Value>) -> Result<Option<Vec<Read>>, String> {
    let result = item.map_err(|e| format!("eth_createAccessList failed: {e}"))?;
    if result.get("error").is_some_and(|e| !e.is_null()) {
        return Ok(None);
    }
    let list = result
        .get("accessList")
        .and_then(|l| l.as_array())
        .ok_or("access list missing")?;
    let mut reads = Vec::new();
    for entry in list {
        let address = entry
            .get("address")
            .and_then(|a| a.as_str())
            .and_then(parse_address)
            .ok_or("bad access-list address")?;
        reads.push((address, None));
        for key in entry.get("storageKeys").and_then(|k| k.as_array()).into_iter().flatten() {
            let digits = key.as_str().ok_or("bad storage key")?;
            let digits = digits.strip_prefix("0x").unwrap_or(digits);
            let slot = hex::decode(format!("{:0>64}", digits))
                .ok()
                .and_then(|v| v.try_into().ok())
                .ok_or("bad storage key")?;
            reads.push((address, Some(slot)));
        }
    }
    Ok(Some(reads))
}

/// What the Universal Resolver said without any gateway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnsOnchain {
    Name(String),
    /// No reverse name, or an empty one.
    Empty,
    /// A reverse name whose forward check failed on chain, with the error.
    Invalid([u8; 4]),
    /// The answer needs a gateway.
    Offchain,
}

pub fn classify_reverse(outcome: CallOutcome) -> EnsOnchain {
    match outcome {
        CallOutcome::Output(out) => match decode_first_string(&out) {
            Ok(name) if !name.is_empty() => EnsOnchain::Name(name),
            _ => EnsOnchain::Empty,
        },
        CallOutcome::Reverted(data) if data.starts_with(&SEL_OFFCHAIN_LOOKUP) => EnsOnchain::Offchain,
        CallOutcome::Reverted(data) => {
            EnsOnchain::Invalid(data.get(..4).and_then(|s| s.try_into().ok()).unwrap_or([0; 4]))
        }
    }
}

// ============================================================================
// CCIP-Read (EIP-3668)
// ============================================================================

/// What a gateway-backed lookup ended in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnsOffchain {
    Name(String),
    /// The gateways answered and there is no valid name.
    NoName,
    /// A gateway did not answer, so nothing is known. A table keeps whatever
    /// it last had.
    GatewayFailed(String),
}

pub struct Gateway {
    agent: ureq::Agent,
    /// Hosts that answered 429, and until when to leave them alone.
    cooldown: Mutex<HashMap<String, Instant>>,
}

/// How long to leave a host alone after a 429 that names no `Retry-After`.
const COOLDOWN: Duration = Duration::from_secs(60);

fn host_of(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    rest.split(['/', '?']).next().unwrap_or(rest)
}

impl Default for Gateway {
    fn default() -> Self {
        Self::new()
    }
}

impl Gateway {
    pub fn new() -> Self {
        let config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(15)))
            .http_status_as_error(false)
            .user_agent("pir-names/0.1")
            .build();
        Gateway {
            agent: config.into(),
            cooldown: Mutex::new(HashMap::new()),
        }
    }

    /// One EIP-3668 request: a URL with `{data}` is fetched with GET and the
    /// rest with a JSON POST. A 4xx answer ends the request, as the EIP says;
    /// a 5xx or a network error moves on to the next URL. A host that
    /// answered 429 is not asked again until its cooldown ends. Errors start
    /// with the host, so failures can be counted per gateway.
    fn fetch(&self, sender: &Address, urls: &[String], data: &[u8]) -> Result<Vec<u8>, (u16, String)> {
        let sender_hex = hex0x(sender);
        let data_hex = hex0x(data);
        let mut last = (500u16, "no gateway URL".to_string());
        for url in urls {
            let url = url.replace("{sender}", &sender_hex);
            let host = host_of(&url).to_string();
            if self.cooldown.lock().unwrap().get(&host).is_some_and(|until| Instant::now() < *until) {
                last = (429, format!("{host}: cooling down after 429"));
                continue;
            }
            let response = if url.contains("{data}") {
                self.agent.get(&url.replace("{data}", &data_hex)).call()
            } else {
                self.agent
                    .post(&url)
                    .send_json(json!({"data": data_hex, "sender": sender_hex}))
            };
            let mut response = match response {
                Ok(r) => r,
                Err(e) => {
                    last = (500, format!("{host}: {e}"));
                    continue;
                }
            };
            let status = response.status().as_u16();
            if status == 429 {
                let wait = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .map_or(COOLDOWN, Duration::from_secs);
                self.cooldown.lock().unwrap().insert(host.clone(), Instant::now() + wait);
            }
            if status >= 400 {
                last = (status, format!("{host}: HTTP {status}"));
                if status < 500 {
                    break;
                }
                continue;
            }
            let body: Value = match response.body_mut().read_json() {
                Ok(v) => v,
                Err(e) => {
                    last = (500, format!("{host}: bad response ({e})"));
                    continue;
                }
            };
            let Some(hex_data) = body.get("data").and_then(|d| d.as_str()) else {
                last = (500, format!("{host}: response without data"));
                continue;
            };
            return hex::decode(hex_data.strip_prefix("0x").unwrap_or(hex_data))
                .map_err(|e| (500, format!("{host}: bad hex ({e})")));
        }
        Err(last)
    }

    /// Answer a local batch request, `query((sender, urls, data)[])`, the way
    /// viem does: each lookup goes to its own gateways, and a failed one comes
    /// back flagged with an `HttpError` in place of its response. Returns the
    /// encoded answer and the first failure (HTTP status, message), if any.
    fn answer_batch(&self, call_data: &[u8]) -> Result<(Vec<u8>, Option<(u16, String)>), String> {
        let args = call_data
            .strip_prefix(&SEL_BATCH_QUERY[..])
            .ok_or("batch gateway call is not query()")?;
        let r = Reader(args);
        let start = r.uint(0)?;
        let n = r.uint(start)?;
        let items = Reader(args.get(start + 32..).ok_or("batch array runs past the end")?);
        let mut failures = Vec::with_capacity(n);
        let mut responses = Vec::with_capacity(n);
        let mut first_failure = None;
        for i in 0..n {
            let at = items.uint(32 * i)?;
            let tuple = Reader(items.0.get(at..).ok_or("batch item runs past the end")?);
            let sender = tuple.address(0)?;
            let urls = tuple.string_array(32)?;
            let data = tuple.bytes(64)?;
            match self.fetch(&sender, &urls, data) {
                Ok(response) => {
                    failures.push(false);
                    responses.push(response);
                }
                Err((status, message)) => {
                    first_failure.get_or_insert_with(|| (status, message.clone()));
                    failures.push(true);
                    let err = encode(&[Tok::Word(word(status as u64)), Tok::Bytes(message.into_bytes())]);
                    responses.push([SEL_HTTP_ERROR.to_vec(), err].concat());
                }
            }
        }
        let answer = encode(&[Tok::BoolArray(failures), Tok::BytesArray(responses)]);
        Ok((answer, first_failure))
    }
}

/// `eth_call` that follows `OffchainLookup` reverts (EIP-3668) until the call
/// returns or reverts with anything else. Returns the final outcome and the
/// first gateway failure seen on the way (HTTP status, message).
pub fn call_with_ccip(
    rpc: &EthRpc,
    gateway: &Gateway,
    to: &str,
    data: Vec<u8>,
) -> Result<(CallOutcome, Option<(u16, String)>), String> {
    let mut to = to.to_string();
    let mut data = data;
    let mut failure = None;
    for _ in 0..MAX_CCIP_ROUNDS {
        let mut items = rpc.batch_raw(&[call_request(&to, &data)])?;
        let outcome = parse_call(items.pop().ok_or("empty batch response")?)?;
        let revert = match outcome {
            CallOutcome::Reverted(d) if d.starts_with(&SEL_OFFCHAIN_LOOKUP) => d,
            other => return Ok((other, failure)),
        };
        // OffchainLookup(sender, urls, callData, callbackFunction, extraData)
        let r = Reader(&revert[4..]);
        let sender = r.address(0)?;
        let urls = r.string_array(32)?;
        let call_data = r.bytes(64)?;
        let callback: [u8; 4] = r.word(96)?[..4].try_into().unwrap();
        let extra = r.bytes(128)?;
        let response = if urls.iter().any(|u| u == LOCAL_BATCH_GATEWAY) {
            let (answer, failed) = gateway.answer_batch(call_data)?;
            if failure.is_none() {
                failure = failed;
            }
            answer
        } else {
            match gateway.fetch(&sender, &urls, call_data) {
                Ok(response) => response,
                Err(failure) => return Ok((CallOutcome::Reverted(vec![]), Some(failure))),
            }
        };
        to = hex0x(&sender);
        data = [
            callback.to_vec(),
            encode(&[Tok::Bytes(response), Tok::Bytes(extra.to_vec())]),
        ]
        .concat();
    }
    Err("too many chained offchain lookups".into())
}

/// The full ENS reverse lookup, gateways included. A gateway that answers
/// with a client error (4xx other than 429) has answered: viem reads that as
/// no name, and so does this. A 429, a 5xx or no answer at all says nothing.
pub fn resolve_offchain(rpc: &EthRpc, gateway: &Gateway, address: &Address) -> Result<EnsOffchain, String> {
    let (outcome, failure) = call_with_ccip(rpc, gateway, UNIVERSAL_RESOLVER, reverse_call(address))?;
    Ok(match (classify_reverse(outcome), failure) {
        (EnsOnchain::Name(name), _) => EnsOffchain::Name(name),
        (_, Some((status, _))) if (400..500).contains(&status) && status != 429 => EnsOffchain::NoName,
        (_, Some((_, message))) => EnsOffchain::GatewayFailed(message),
        _ => EnsOffchain::NoName,
    })
}

// ============================================================================
// Candidates
// ============================================================================

/// The addresses a storage word may hold. `strict` takes only the layouts ENS
/// contracts use: an address right-aligned (an owner or a legacy `addr`
/// record), the same packed under a small field such as a TTL, or a public
/// resolver's 20-byte `addr` record stored as short bytes. Without `strict`,
/// any word whose low 160 bits look like an address counts, which is how
/// solady's ERC-721 in GNS and WNS packs an owner with extra data, and which
/// would turn every hash in an ENS contract into a candidate.
pub fn addresses_in_word(w: &[u8; 32], strict: bool) -> Vec<Address> {
    let address_like = |a: &[u8]| a[..5].iter().any(|&b| b != 0);
    let mut out = Vec::new();
    let low: Address = w[12..].try_into().unwrap();
    if address_like(&low) && (!strict || w[..4].iter().all(|&b| b == 0)) {
        out.push(low);
    }
    if w[31] == 0x28 && w[20..31].iter().all(|&b| b == 0) && w[..20].iter().any(|&b| b != 0) {
        out.push(w[..20].try_into().unwrap());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> Address {
        parse_address(s).unwrap()
    }

    /// The bytes viem sends for vitalik.eth's address, checked against a
    /// mainnet call that returned "vitalik.eth".
    #[test]
    fn reverse_call_matches_what_viem_sends() {
        let data = reverse_call(&addr("0xd8da6bf26964af9d7eed9e03e53415d37aa96045"));
        let expected = concat!(
            "b7d6ca64",
            "0000000000000000000000000000000000000000000000000000000000000060",
            "000000000000000000000000000000000000000000000000000000000000003c",
            "00000000000000000000000000000000000000000000000000000000000000a0",
            "0000000000000000000000000000000000000000000000000000000000000014",
            "d8da6bf26964af9d7eed9e03e53415d37aa96045000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "0000000000000000000000000000000000000000000000000000000000000020",
            "0000000000000000000000000000000000000000000000000000000000000014",
            "782d62617463682d676174657761793a74727565000000000000000000000000",
        );
        assert_eq!(hex::encode(data), expected);
    }

    #[test]
    fn namehash_and_dns_encoding() {
        assert_eq!(namehash(""), [0u8; 32]);
        assert_eq!(
            hex::encode(namehash("addr.reverse")),
            "91d1777781884d03a6757a803996e38de2a42967fb37eeaca72729271025a9e2"
        );
        assert_eq!(dns_encode("vitalik.eth").unwrap(), b"\x07vitalik\x03eth\x00");
        assert_eq!(dns_encode("a..eth"), None);
        let long = "x".repeat(300) + ".eth";
        let enc = dns_encode(&long).unwrap();
        assert_eq!(enc[0], 66);
        assert_eq!(enc[1], b'[');
        assert_eq!(enc[66], b']');
    }

    #[test]
    fn decodes_a_reverse_answer() {
        // ("vitalik.eth", resolver, reverse resolver)
        let out = encode(&[
            Tok::Bytes(b"vitalik.eth".to_vec()),
            Tok::Word(word(1)),
            Tok::Word(word(2)),
        ]);
        assert_eq!(decode_first_string(&out).unwrap(), "vitalik.eth");
        assert_eq!(
            classify_reverse(CallOutcome::Output(out)),
            EnsOnchain::Name("vitalik.eth".into())
        );
        let empty = encode(&[Tok::Bytes(vec![]), Tok::Word(word(0)), Tok::Word(word(0))]);
        assert_eq!(classify_reverse(CallOutcome::Output(empty)), EnsOnchain::Empty);
        let mut lookup = SEL_OFFCHAIN_LOOKUP.to_vec();
        lookup.extend([0u8; 64]);
        assert_eq!(classify_reverse(CallOutcome::Reverted(lookup)), EnsOnchain::Offchain);
        assert_eq!(
            classify_reverse(CallOutcome::Reverted(vec![0xef, 0x9c, 0x03, 0xce, 0])),
            EnsOnchain::Invalid([0xef, 0x9c, 0x03, 0xce])
        );
    }

    #[test]
    fn decodes_the_reverse_half() {
        let inner = encode(&[Tok::Bytes(b"alice.eth".to_vec())]);
        let out = encode(&[Tok::Bytes(inner), Tok::Word(word(7))]);
        assert_eq!(decode_reverse_half(&out).unwrap(), "alice.eth");
    }

    /// An OffchainLookup and a batch query built the way the Universal
    /// Resolver builds them read back field by field.
    #[test]
    fn reads_offchain_lookups_and_batch_queries() {
        let sender = addr("0x2291053f49cd008306b92f84a61c6a1bc9b5cb65");
        let mut sender_word = [0u8; 32];
        sender_word[12..].copy_from_slice(&sender);
        let tuple = encode(&[
            Tok::Word(sender_word),
            Tok::StringArray(vec!["https://a.example/{sender}/{data}.json".into()]),
            Tok::Bytes(vec![0xde, 0xad]),
        ]);
        // query((address,string[],bytes)[]) with one tuple: the array holds an
        // offset to each tuple, since the tuple is dynamic.
        let mut array = word(1).to_vec();
        array.extend_from_slice(&word(32));
        array.extend(tuple);
        let mut call = SEL_BATCH_QUERY.to_vec();
        call.extend_from_slice(&word(32));
        call.extend(array);

        let args = Reader(&call[4..]);
        let start = args.uint(0).unwrap();
        assert_eq!(args.uint(start).unwrap(), 1);
        let items = Reader(&call[4 + start + 32..]);
        let t = Reader(&items.0[items.uint(0).unwrap()..]);
        assert_eq!(t.address(0).unwrap(), sender);
        assert_eq!(t.string_array(32).unwrap(), vec!["https://a.example/{sender}/{data}.json"]);
        assert_eq!(t.bytes(64).unwrap(), &[0xde, 0xad]);
    }

    #[test]
    fn encodes_a_batch_answer() {
        let answer = encode(&[
            Tok::BoolArray(vec![false, true]),
            Tok::BytesArray(vec![vec![1, 2, 3], vec![]]),
        ]);
        let r = Reader(&answer);
        let bools = r.uint(0).unwrap();
        assert_eq!(r.uint(bools).unwrap(), 2);
        assert_eq!(r.uint(bools + 32).unwrap(), 0);
        assert_eq!(r.uint(bools + 64).unwrap(), 1);
        let arr = r.uint(32).unwrap();
        let items = Reader(&answer[arr + 32..]);
        assert_eq!(items.bytes(0).unwrap(), &[1, 2, 3]);
        assert_eq!(items.bytes(32).unwrap(), &[] as &[u8]);
    }

    #[test]
    fn access_lists_and_reverts() {
        let ok = Ok(json!({"accessList": [
            {"address": "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e", "storageKeys": ["0x01"]},
            {"address": UNIVERSAL_RESOLVER, "storageKeys": []}
        ], "gasUsed": "0x1"}));
        let reads = parse_access_list(ok).unwrap().unwrap();
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[1].1.unwrap()[31], 1);
        assert_eq!(reads[2], (addr(UNIVERSAL_RESOLVER), None));
        let reverted = Ok(json!({"accessList": [], "error": "Transaction Reverted", "gasUsed": "0x1"}));
        assert_eq!(parse_access_list(reverted).unwrap(), None);

        let revert = Err(json!({"code": 3, "message": "execution reverted", "data": "0x556f1830"}));
        assert!(matches!(parse_call(revert), Ok(CallOutcome::Reverted(d)) if d == SEL_OFFCHAIN_LOOKUP));
        let broken = Err(json!({"code": -32000, "message": "header not found"}));
        assert!(parse_call(broken).is_err());
    }

    #[test]
    fn gateway_hosts() {
        assert_eq!(host_of("https://api.coinbase.com/api/v1/x/{data}"), "api.coinbase.com");
        assert_eq!(host_of("https://a.example?sender=1"), "a.example");
        assert_eq!(host_of("https://gateway.example"), "gateway.example");
    }

    #[test]
    fn finds_addresses_in_storage_words() {
        let a = addr("0xd8da6bf26964af9d7eed9e03e53415d37aa96045");
        let mut right = [0u8; 32];
        right[12..].copy_from_slice(&a);
        assert_eq!(addresses_in_word(&right, true), vec![a]);
        let mut ttl = right;
        ttl[11] = 1;
        assert_eq!(addresses_in_word(&ttl, true), vec![a]);
        let mut short_bytes = [0u8; 32];
        short_bytes[..20].copy_from_slice(&a);
        short_bytes[31] = 0x28;
        assert_eq!(addresses_in_word(&short_bytes, true), vec![a]);
        let hash = keccak256(b"not an address");
        assert!(addresses_in_word(&hash, true).is_empty());
        assert_eq!(addresses_in_word(&hash, false).len(), 1);
        let small = word(12345);
        assert!(addresses_in_word(&small, false).is_empty());
    }
}
