# inspire-gpu-serving

The serving layer around the
[inspire-gpu](https://github.com/keewoolee/inspire-gpu) PIR engine. The
engine answers "give me entry number `i` without learning `i`"; this repo
answers "give me the value for key `K` without learning `K`", keeps the
database tracking its source, and swaps in fresh database generations with
no downtime.

- **Scope.** Application-agnostic serving around the engine, built on two
  mechanisms:
  - **keyword PIR**[^1] — cuckoo hashing maps a key to 2 candidate
    buckets, turning "look up key `K`" into 2 positional PIR queries;
  - **the sidecar pattern**[^2] — the PIR matrix serves a preprocessed
    snapshot, and a small broadcast (the *sidecar*) carries the updates
    that arrived since, so serving stays current between the periodic
    re-preprocesses.
- **Deployment target.** Private Ethereum state retrieval (a wallet
  privately reading an account's balance and nonce, its balance of a
  token, or its primary name); the Ethereum side
  is confined to `crates/chain` (a JSON-RPC chain follower) plus a chain
  simulator for demos. Any other key-value source slots in by supplying the same two
  things: an initial key-value set, and a stream of updates.
- **Engine.** Vendored as the git submodule `third_party/inspire-gpu`
  (pinned commit; `INSPIRE_GPU_DIR` overrides it for development). The two
  repos meet at exactly one boundary: the `ipir_*` C ABI (inspire-gpu
  `src/capi.h`), whose flat query/response layouts also serve as the wire
  format.

## Disclaimer

This code is AI-written: the author supplied the high-level design
choices and has not thoroughly reviewed the implementation.

## How a lookup works

At setup — once, not per lookup — the client fetches the **manifest**
(JSON): cuckoo seed, PIR parameters, and the service's **fixed CRS seed**.
None of it changes when the database updates, so in steady state the
client never fetches it again.

Every lookup is then a **single round** with the same shape:

1. **Client → server:** derive the key from the address (the manifest
   says how), hash it to its 2 cuckoo candidate buckets, and send ONE
   request carrying 2 self-contained PIR queries.
   - **Always both, never an early exit** — PIR hides query contents, not
     query counts, so the request shape must not depend on the key.
   - The CRS is fixed, so the queries are valid against every database
     generation.
2. **Server:** answer both queries as one unit (the pair shares a GPU
   batch), and only THEN attach the **sidecar** broadcast for the
   snapshot that answered them: the snapshot block, the account changes
   since that snapshot, and the (normally empty) **stash** of
   cuckoo-overflow entries.
   - The attached broadcast does not depend on the key being looked up:
     every client gets the same bytes, and they are attached whether or
     not the answer ends up coming from the sidecar — so the response,
     like the request, reveals nothing about the key.
   - The two answers and the broadcast always describe the same snapshot,
     even if a generation flip lands mid-request — guaranteed by the
     server's processing order, not by client-side checks or retries.
3. **Client, locally:** the freshest sidecar entry wins, else the
   matching cell from either decrypted bucket, else the stash (eviction
   bound 10,000; it rides the broadcast everyone downloads, so reading it
   leaks nothing). No match anywhere is a proven non-membership at that
   snapshot.

Per lookup, at the 16 GB tier on an RTX 5090:

| | |
|---|---|
| Up | 2 × 371 KB — the 53-bit CRT-packed queries (actual wire bytes, not estimates) |
| Down | 2 × 12 KB modulus-switched responses, plus the sidecar suffix (grows ~50 KB per mainnet block; the flip cadence caps it) |
| Server throughput | up to ~57 lookups/s per card: the engine bound, half its ~115 queries/s at full batch (the HTTP server measures lower, see Validated) |
| End to end, remote | ~250 ms from a laptop over the internet, ~60 ms of it client-side query building |

### Design choice: why capacity-2 buckets

With one key-value pair per bucket, 2-hash cuckoo hashing fills only to
load factor 0.5[^3], so the matrix must hold twice the data's bytes. Of
the two standard escapes, a **third hash function** (threshold ~0.918)
buys capacity by making every lookup 3 queries forever; **capacity-2
buckets** (threshold ~0.897[^3]) buy almost the same capacity at no
cost: halving the bucket count while doubling the entry leaves the
matrix bytes unchanged, the lookup stays 2 queries, and communication
does not grow either — an InsPIRe response is a fixed-size ciphertext
with room for far more slots than one entry, so a doubled entry rides
the same response.

### Entry layout

One PIR entry is one bucket — 120 bytes, which is exactly 64 of the
engine's 15-bit plaintext slots:

```
bucket (120 B)
├─ cell 0 (60 B):  key (20 B) ‖ value (40 B)
└─ cell 1 (60 B):  key (20 B) ‖ value (40 B)
key    (20 B)  =   address, or keccak256(address) cut to 20 B
value  (40 B)  =   reserved (16 B) ‖ balance, BE (16 B) ‖ nonce, BE (8 B)
```

- **Cells carry their key** because a retrieved bucket can hold two
  different accounts (or fewer — empty cells are all-zero): the client
  compares its 20-byte key against both cells, and finding it in
  neither — nor in the sidecar or stash — is the non-membership proof.
- **The key is the address or its hash**, and the manifest's
  `key_derivation` field says which, so a client never has to be told. A
  snapshot read out of a node's state trie arrives keyed by
  keccak256(address), because the trie is keyed that way and a hash cannot
  be turned back into an address. The client hashes the address it is
  asking about, and cutting the hash to 20 bytes keeps the cell layout
  unchanged.
- **A stored key must stay collision-free** across the *entire* key set,
  not just within a bucket (the table treats equal stored keys as the same
  entry). By the birthday bound about 12 bytes suffice for 2^28 keys, so a
  shorter hash could free cell bytes for longer values.

[^1]: Asra Ali, Tancrède Lepoint, Sarvar Patel, Mariana Raykova, Phillipp
    Schoppmann, Karn Seth, and Kevin Yeo.
    [*Communication–Computation Trade-offs in PIR.*](https://www.usenix.org/conference/usenixsecurity21/presentation/ali)
    USENIX Security 2021. Introduced the cuckoo-hashing conversion from
    keyword to index PIR used here.

[^2]: Ali Atiia and Keewoo Lee.
    [*Sharded PIR Design for the Ethereum State.*](https://ethresear.ch/t/sharded-pir-design-for-the-ethereum-state/24552)
    ethresear.ch, 2026. The sidecar pattern is Section 5.3.

[^3]: Orientability thresholds of the random cuckoo (hyper)graph: 0.5 for
    2 choices × capacity 1 (the Erdős–Rényi giant-component transition),
    ≈0.897 for 2 choices × capacity 2 (Cain–Sanders–Wormald; Fernholz–
    Ramachandran, both SODA 2007), ≈0.918 for 3 choices × capacity 1.

## Validated

What has actually been demonstrated, beyond the numbers above:

- **Mainnet, live, on one H100 (80 GB).** The server serves the 206.8M
  mainnet accounts holding ETH (out of 418.8M in a full-state dump taken
  on 2026-09-16) from a 16 GB PIR matrix at 77% cell load, and follows the
  chain block by block through state diffs. It holds 25.4 GiB of VRAM in
  steady state and peaks at 53.6 GiB during a generation flip, which
  builds the next generation beside the serving one (2.11×). The HTTP
  server sustains ~44 lookups/s with 32 concurrent clients (~10 for a
  single client), and a lookup moves ~742 KB up and ~277 KB down, most of
  the download being the sidecar.
- **Every account needs the next tier.** All 418.8M accounts need 2^28
  buckets: extrapolating from the measured tier, ~50 GiB in steady state
  and ~107 GiB during a flip, beyond one H100. On a 32 GB card even the
  16 GB tier cannot flip in place, so zero-downtime updates there need the
  two-card role swap below.
- **The 16 GB tier on a 32 GB card (2x RTX 5090 pod).** ~182M synthetic
  accounts, about the number of mainnet accounts holding ETH in early
  2026, insert into a 16 GB PIR matrix (68% cell load, zero overflow) and
  serve from 25.9 GB of VRAM.
- **Zero-downtime machine swap** (`scripts/roleswap-demo.sh`). The front
  switched from one GPU's server to the other's mid-load: continuous
  lookups saw **0 failures**, and the response stamp was identical before
  and after.
- **Database updates are invisible to clients**
  (`scripts/live-sim-demo.sh`). Over an accelerated simulation the
  serving snapshot advanced 0 → 9 → 18 with **no client resync**, and an
  account updated every block was always current.
- **ERC-20 balances, live beside the accounts on the same H100.** The
  storage of USDC, USDT, DAI and WETH is 53.7M slots, which fill a
  2^25-bucket table to 80% and take 6.5 GB of VRAM next to the account
  table. For 40 holders and all four tokens, each of the 160 balances
  matched `balanceOf` on the node at the block that answered it. A restart
  from the server's own save resumed at the next block, and 96 more
  balances checked after it all matched.
- **Primary names, live beside the accounts and tokens on the same H100.**
  Asking the node about 1.45M starting addresses took 20 minutes with eight
  batches in flight and found 913,983 addresses with a name (ENS 913,841,
  GNS 151, WNS 162), in a 2^20-bucket table at 44% load and 0.23 GB of VRAM.
  A sample of 53 lookups matched the chain, and so did all 110 changes the
  server followed live over 10 minutes, checked from another IP. A restart
  from the saved state serves again within 10 seconds.
- **The live-chain path works end to end.** Against a key-less public
  endpoint, the server snapshotted the mainnet head, ingested live
  blocks, flipped, and returned the fresh balance/nonce of an account
  the chain had touched minutes earlier. (Free endpoints rate-limit the
  state fetch; the follower backs off and lags by a few blocks — a
  provisioned endpoint or local node removes the lag.)

## What a session looks like

Output from a live-simulation run, lightly trimmed (1M synthetic
accounts; blocks accelerated to 5 s, generation flip every 45 s).
Client side:

```console
$ pir-client --server http://…:18086 canary
canary: serving snapshot of block #0 (169 ms)

$ pir-client --server … synthetic 12345      # untouched since genesis
balance: 12345 wei
nonce:   0
source:  PIR (snapshot block #0), 122 ms

$ pir-client --server … synthetic 0          # updated every block
balance: 0 wei
nonce:   4                                   # nonce = last-touched block
source:  sidecar broadcast (block #4), 175 ms

$ pir-client --server … lookup 0x00…00ff     # absent address
not found (proven non-membership at snapshot block #0), 160 ms

# …after a flip, with no client action of any kind:
$ pir-client --server … canary
canary: serving snapshot of block #9 (158 ms)
$ pir-client --server … synthetic 0
nonce:   9
source:  PIR (snapshot block #9), 168 ms    # what the sidecar carried is
                                            # now inside the PIR matrix
```

All four cases (snapshot hit, sidecar hit, absent, canary) sit in the same
~120-175 ms band — the fixed request shape means even timing does not
reveal what was looked up. Server side over the same period:

```console
CRS seed: 27b163406c6c22ee…d1a81403
generation 1 built in 3.6s (snapshot block #0, 0.43 GB resident)
Serving on 127.0.0.1:18086 (32 workers), ready 4.1s after start.
block #1: +300 simulated updates (sidecar 300 entries)
   ⋮
block #9: +300 simulated updates (sidecar 2700 entries)
generation 2 built in 3.1s (snapshot block #9, 0.43 GB resident)
flipped to snapshot #9 (2700 sidecar entries retained)
block #10: +300 simulated updates (sidecar 3000 entries)
```

The flip *retains* the retired snapshot's sidecar entries: lookups
still in flight on the old generation need them; they are deleted at
the next flip.

## Crates

| Crate | Contents |
|---|---|
| [`crates/keyword`](crates/keyword) | Cuckoo hashing with capacity-2 buckets; 15-bit byte↔slot packing; the row-major slot matrix the engine ingests; the manifest and sidecar-broadcast wire types; keys and values for contract storage slots; the value a name table holds. |
| [`crates/backend-ffi`](crates/backend-ffi) | Safe Rust bindings over the `ipir_*` C ABI. The client half builds anywhere (compiles the engine's CPU sources directly — no CMake, no CUDA); the server half (`gpu` feature) links the static libraries, running the CMake build itself when needed. |
| [`crates/server`](crates/server) | The serving front: batch scheduler owning the GPU handle, generation builder + flips, sidecar store, chain source (`--eth-rpc` follower or `--simulate` simulator), the tracker that keeps a table of primary names current, HTTP API (`/manifest`, `/lookup`, `/sidecar`, `/query`, `/healthz` — wire contract documented in [`src/http.rs`](crates/server/src/http.rs)). |
| [`crates/client`](crates/client) | Client library + CLI, the reference for wallet integration: single-round fixed-shape lookups of accounts, token balances and primary names, local answer picking, reconfiguration detection. No GPU. |
| [`crates/front`](crates/front) | Thin switchable forwarder for the cross-machine role swap (`POST /admin/target`; no auth — keep it inside the deployment boundary). |
| [`crates/chain`](crates/chain) | Ethereum JSON-RPC adapter: block tracking, state-diff and touched-address extraction, batched balance/nonce fetch, snapshot resync, and primary names (the calls a wallet makes, what they read, CCIP-Read). Also `ethrex-statedump` (feature `ethrex-dump`), which writes an ethrex node's account table, or the storage of chosen contracts, as a snapshot CSV stamped at the chain head. |

## Build & run

```bash
git clone --recursive https://github.com/keewoolee/inspire-gpu-serving
# (already cloned? git submodule update --init)

# Anywhere (Rust stable; the client-only path needs just a C++ compiler) —
# keyword, chain, client, and the CPU half of backend-ffi:
cargo build && cargo test

# On a CUDA machine (nvcc under /usr/local/cuda* is found automatically;
# the CMake build of the engine runs inside cargo on first build). The
# engine builds for sm_120 (RTX 5090) unless INSPIRE_CUDA_ARCH says
# otherwise, e.g. INSPIRE_CUDA_ARCH=90 for an H100:
cargo test -p pir-server            # HTTP e2e incl. sidecar, stash, flip

# Serve 1M synthetic accounts and look one up privately:
cargo run --release -p pir-server -- --synthetic 1000000 --buckets 4194304
cargo run --release -p pir-client -- --server http://127.0.0.1:8080 synthetic 12345
cargo run --release -p pir-client -- --server http://127.0.0.1:8080 canary

# Live serving simulation: 300 random updates per 12 s block, flip every
# minute (see scripts/live-sim-demo.sh for the scripted, checked version):
cargo run --release -p pir-server -- --synthetic 1000000 --buckets 2097152 \
    --simulate 300

# Two-GPU role-swap demo (build on GPU1 while GPU0 serves, then switch;
# every machine serving the same database shares one --crs-seed):
scripts/roleswap-demo.sh
```

## Serving real Ethereum state

Everything above runs on synthetic data. Pointing the same server at
mainnet needs two things from the operator:

1. **A snapshot CSV**: one row per account, `address,nonce,balance_wei`,
   plus a `# block=N` header line recording the block it represents.
   `ethrex-statedump` produces it from an ethrex node, reading the node's
   flat account table through a RocksDB secondary instance while the node
   keeps running (all of mainnet in a few minutes). That table trails the
   node's head by 128 blocks, so the dump first replays those blocks
   through the node's state diffs and stamps the file at the head. Its
   first column holds
   keccak256(address) cut to 20 bytes rather than the address, since the
   trie stores accounts by that hash, and the file says so with a
   `# key_derivation=keccak` line that the server carries into the
   manifest. Any other export that yields address/nonce/balance works
   too.

2. **A JSON-RPC endpoint** for staying current. The follower reads each
   block's state diff through `debug_traceBlockByNumber` with
   `prestateTracer` in diff mode, which catches every balance change a
   transaction makes, internal transfers included. That needs the
   node's `debug` namespace but no archive node. A node traces only its
   recent blocks (128 on ethrex, about 25 minutes), so a server started
   from a snapshot older than that fills in the older blocks through the
   fallback below. Without the `debug` namespace
   the follower falls back to standard methods against `latest`
   (`eth_getBlockByNumber`, `eth_getBalance`, `eth_getTransactionCount`),
   paced and retried within hosted-endpoint rate limits. That feed sees
   an account only when a transaction touches it and misses about a
   quarter of balance changes.

Then:

```bash
# Dump an ethrex node's accounts, next to the node (building it needs
# libclang; --min-balance 1 keeps only accounts holding ETH, about half):
cargo run --release -p pir-chain --features ethrex-dump \
    --bin ethrex-statedump -- --datadir /path/to/ethrex/mainnet \
    --out accounts.csv --min-balance 1

# If the snapshot lags the chain head, catch it up first (repeats until
# it converges to within the endpoint's rate limit of the head):
cargo run --release -p pir-chain --bin resync -- \
    --snapshot accounts.csv --eth-rpc https://RPC-ENDPOINT/KEY \
    --buckets 134217728

# Serve it. The follower ingests each new block into the sidecar and
# flips a fresh generation every --rebuild-secs (default 60):
cargo run --release -p pir-server -- --accounts-csv accounts.csv \
    --buckets 134217728 --eth-rpc https://RPC-ENDPOINT/KEY

# Look up any address privately:
cargo run --release -p pir-client -- --server http://HOST:8080 \
    lookup 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

## Token balances

The server can also serve ERC-20 balances, from a table that holds every
storage slot of chosen contracts. A token's `balanceOf(holder)` is one slot of
its balances mapping, so a wallet reads it like any other key. Such a table
runs in a server process of its own, started with `--storage-csv` instead of
`--accounts-csv`, and its manifest says `"key_derivation": "storage"` and
lists the contracts it holds.

A node's storage table keys each slot by keccak256 of the contract and of the
slot, and neither hash can be turned back, so the PIR table is keyed the same
way:

```
slot   (32 B)  =   keccak256(pad32(holder) ‖ pad32(p))      p: slot of the balances mapping
key    (20 B)  =   keccak256(keccak256(contract) ‖ keccak256(slot)) cut to 20 B
value  (40 B)  =   zero (8 B) ‖ the slot's value, BE (32 B)
```

The client knows four tokens, the ones kohaku-cli syncs by default. Each `p`
was checked against `balanceOf` on mainnet.

| Token | Contract | `p` | Decimals |
|---|---|---|---|
| USDC | `0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48` | 9 | 6 |
| USDT | `0xdAC17F958D2ee523a2206206994597C13D831ec7` | 2 | 6 |
| DAI | `0x6B175474E89094C44Da98b954EedeAC495271d0F` | 2 | 18 |
| WETH | `0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2` | 3 | 18 |

- **USDC keeps a blacklist flag** in the top bit of a balance slot. The
  client clears it, as `balanceOf` does.
- **No match is a zero balance.** Storage holds no empty slots, so finding
  the key in neither bucket, nor in the sidecar or stash, means the slot is 0.
- **Always look up every token** for every address a wallet syncs, even one
  it expects to be empty. The server sees how many lookups arrive, so asking
  only for the tokens an address holds would tell it which ones those are.
- **Any slot of these contracts can be read**, not just balances, since the
  table holds their whole storage, allowances included. A slot of any other
  contract is not in the table, so the client refuses it rather than reading
  it as empty.
- **Each token costs one lookup**, the same as an account. Grouping a
  holder's balances under one key would need the holders' addresses, and the
  node's state has only the slot hashes.

From code, or from the command line:

```rust
let mut client = PirClient::connect("http://HOST:8081")?;
for token in pir_client::TOKENS {                    // USDC, USDT, DAI, WETH
    let b = client.token_balance(&token, &holder)?;  // b.balance: 32 B, BE
}
```

```bash
cargo run --release -p pir-client -- --server http://HOST:8081 \
    tokens 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

`lookup_storage(contract, slot)` reads any other slot of the four contracts.

Serving it:

```bash
# Dump the contracts' storage next to the node. The node's table trails the
# head by 128 blocks, so the dump replays them and stamps the file at the head:
cargo run --release -p pir-chain --features ethrex-dump \
    --bin ethrex-statedump -- --datadir /path/to/ethrex/mainnet \
    --out tokens.csv \
    --storage-of 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
    --storage-of 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    --storage-of 0x6B175474E89094C44Da98b954EedeAC495271d0F \
    --storage-of 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2

# Serve it next to the account server, saving the table over the same file
# every 10 minutes:
cargo run --release -p pir-server -- --storage-csv tokens.csv \
    --buckets 33554432 --eth-rpc http://NODE:8545 \
    --checkpoint-secs 600 --listen 0.0.0.0:8081
```

A storage table can catch up only through the node's state diffs, because a
transaction's from and to do not say which slots it changed. So the server
refuses a snapshot it could not catch up from, the follower stops the server
once a block it needs has left the node's window, and `--checkpoint-secs`
keeps a recent save to restart from. A longer outage needs a fresh dump.

The storage table can share the account table's `--crs-seed`, since the CRS
is public. Its response stamp also covers the contracts it holds, so a client
sent to the wrong table still notices.

## Primary names

The server can also serve an address's primary names in ENS, GNS and WNS,
the names a wallet shows for its own accounts. One lookup answers all three.
Such a table runs in a server process of its own, started with
`--names-ens-candidates`, and its manifest says `"content": "names"`. It is
keyed by the address itself:

```
key    (20 B)  =   the address
value  (40 B)  =   [system (1 B)][length (1 B)][UTF-8 name]  for each system with a name,
                   in the order ENS (1), GNS (2), WNS (3), then zeros
```

A system byte with its top bit set (`0x81`, `0x82`, `0x83`) and length 0 means
that system has a name too long for the value, about 0.26% of named
addresses. An address with no name in any system holds all zeros or is not in
the table, and both read as no name.

Each answer is what a wallet would have fetched itself:

| System | The call behind the answer | Contract |
|---|---|---|
| ENS | `reverseWithGateways(address, 60, ["x-batch-gateway:true"])` | Universal Resolver `0xeeeeeeee14d718c2b47d9923deab1335e144eeee` |
| GNS | `reverseResolve(address)` | `0x9D51D507BC7264d4fE8Ad1cf7Fe191933A0a81d6` |
| WNS | `reverseResolve(address)` | `0x0000000000696760E15f265e828DB644A0c242EB` |

The ENS call is the one viem's `getEnsName` makes, gateways included, so the
name is checked both ways: the address's reverse record names it, and the
name's forward record points back at the address. GNS and WNS check the same
inside `reverseResolve`.

From code, or from the command line:

```rust
let mut client = PirClient::connect("http://HOST:8092")?;
let found = client.names(&address)?;   // found.names.ens, .gns, .wns
```

```bash
cargo run --release -p pir-client -- --server http://HOST:8092 \
    names 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

### In a middleware

A wallet library asks for names through several `eth_call`s, and every one
of them carries the address or a name that identifies it just as well.
ethereum-names, which kohaku-cli uses, asks GNS, ENS and WNS in turn, and
checks each name it gets with a forward lookup. A middleware answers all of
these from one names lookup per address, the forward checks included:

| `eth_call` | Selector | Answer |
|---|---|---|
| `reverseWithGateways(bytes addr, uint256 coinType, string[] gateways)` on the Universal Resolver, `coinType` 60 | `0xb7d6ca64` | `abi.encode(string name, address, address)`, `""` for no name. viem reads only the name, so both addresses can be 0. |
| `resolveWithGateways(bytes dnsName, bytes data, string[] gateways)` on the Universal Resolver, where `data` is `addr(bytes32 node)` (`0x3b3b57de`) for a name the lookup returned | `0xa1472844` | `abi.encode(bytes result, address resolver)` with `result = abi.encode(address)`, the address looked up. viem ignores `resolver`. |
| `reverseResolve(address)` on GNS or WNS | `0x9af8b7aa` | `abi.encode(string name)`, `""` for no name |
| `computeId(string fullName)` on GNS or WNS | `0xfb021939` | the name's namehash as a `uint256`. The function is pure, so the middleware computes it: lowercase the ASCII letters, then the usual ENS namehash. |
| `resolve(uint256 tokenId)` on GNS or WNS, for that id | `0x4f896d4f` | `abi.encode(address)`, the address looked up |

- **One lookup per address, whatever was asked.** Answering every system from
  the same lookup keeps the number of lookups from saying which systems an
  address uses.
- **A name too long for the table** cannot be answered from it. Pass that call
  on to the RPC, which then sees the address, or treat it as no name.
- **Everything else passes through as before, and still says what it asks.**
  That covers resolving a name the user typed (a send to `vitalik.eth`), text
  records and avatars (the stealth meta-address record among them), and
  primary names on other chains (`coinType` other than 60).

### Freshness and coverage

- **On-chain changes are followed block by block.** For every address with
  an answer, the server keeps the storage slots that answer read
  (`eth_createAccessList`), and each block's state diff says which slots
  changed. Only the addresses that read one are asked again, so a change
  anywhere the answer depends on is caught, a resolver that emits no event
  included. GNS and WNS names expire without an event, so their holders are
  asked every block.
- **Offchain names** (`cb.id`, `base.eth` and others whose forward record sits
  behind a gateway) are resolved by the server through CCIP-Read, the way viem
  does, and asked again about once an hour, paced so that no gateway sees a
  burst. A gateway that fails keeps the last answer, and a 4xx from it reads
  as no name, as in viem. A change made at the gateway shows up within the
  hour.
- **Which addresses the table knows.** It starts from every address-like value
  in the name contracts' storage, since a primary name needs a forward record
  pointing at the address and an on-chain one stores it there. After that,
  every account a block changes is asked once, along with every account of a
  transaction that writes name-contract storage and the addresses named in
  `ReverseClaimed`, `NameForAddrChanged` and `PrimaryNameSet`. A name set long
  ago for an address that never transacts again can be missed until it does.

Serving it:

```bash
# Dump the storage of the ENS contracts that hold names and forward records
# (registries, resolvers, reverse registrar, name wrapper, .eth registrar),
# and of GNS and WNS, next to the node:
ENS="0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e 0x314159265dD8dbb310642f98f50C066173C1259b
     0x231b0Ee14048e9dCcD1d247744d114a4EB5E8E63 0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41
     0xF29100983E058B709F3D539b0c765937B804AC15 0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA
     0x226159d592E2b063810a10Ebf6dcbADA94Ed68b8 0x5FfC014343cd971B7eb70732021E26C35B744cc4
     0xA2C122BE93b0074270ebeE7f6b7292C7deB45047 0x5fBb459C49BB06083C33109fA4f14810eC2Cf358
     0x283F227c4Bd38ecE252C4Ae7ECE650B0e913f1f9 0xDa1756Bb923Af5d1a05E277CB1E54f1D0A127890
     0xB23267E7A0dEe4Dcba80c1D2fFdB0270aF76fE80 0xF58d55f06bB92F083e78bb5063A2dD3544f9B6a3
     0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401 0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"
cargo run --release -p pir-chain --features ethrex-dump \
    --bin ethrex-statedump -- --datadir /path/to/ethrex/mainnet \
    --out ens.csv $(for c in $ENS; do echo --storage-of $c; done)
cargo run --release -p pir-chain --features ethrex-dump \
    --bin ethrex-statedump -- --datadir /path/to/ethrex/mainnet \
    --out ns.csv --storage-of 0x9D51D507BC7264d4fE8Ad1cf7Fe191933A0a81d6 \
    --storage-of 0x0000000000696760E15f265e828DB644A0c242EB

# Serve it. The server asks the node about every starting address (about 20
# minutes on mainnet), then follows the chain, saving its state every 10 minutes:
cargo run --release -p pir-server -- --names-ens-candidates ens.csv \
    --names-ns-candidates ns.csv --names-state names.state \
    --buckets 1048576 --eth-rpc http://NODE:8545 --listen 0.0.0.0:8092
```

A restart within the node's trace window loads the saved state and serves
again in about 10 seconds. After a longer outage the server builds the table
from the starting set again. The table can share the account table's
`--crs-seed`, and its response stamp covers its content tag, so a client sent
to the wrong table notices.
