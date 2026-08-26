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
  privately reading an account's balance and nonce); the Ethereum side
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

1. **Client → server:** hash the address to its 2 cuckoo candidate
   buckets and send ONE request carrying 2 self-contained PIR queries.
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
| Server throughput | up to ~57 lookups/s per card (half the engine's ~115 queries/s at full batch) |
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
├─ cell 0 (60 B):  address (20 B) ‖ value (40 B)
└─ cell 1 (60 B):  address (20 B) ‖ value (40 B)
value  (40 B)  =   reserved (16 B) ‖ balance, BE (16 B) ‖ nonce, BE (8 B)
```

- **Cells carry their key** because a retrieved bucket can hold two
  different accounts (or fewer — empty cells are all-zero): the client
  compares its 20-byte address against both cells, and finding it in
  neither — nor in the sidecar or stash — is the non-membership proof.
- **The stored key can be a hash of the address**, freeing cell bytes for
  the value (relevant for sources with longer keys). It must stay
  collision-free across the *entire* key set, not just within a bucket
  (the table treats equal stored keys as the same entry): by the birthday
  bound, about 12 bytes for 2^28 keys.

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

## Validated (2x RTX 5090 pod)

What has actually been demonstrated, beyond the numbers above:

- **All of mainnet fits one card.** ~182M synthetic accounts (mainnet's
  size as of an early-2026 snapshot) insert into a 16 GB PIR matrix (68%
  cell load, zero overflow) and serve from 25.9 GB of VRAM.
- **Zero-downtime machine swap** (`scripts/roleswap-demo.sh`). The front
  switched from one GPU's server to the other's mid-load: continuous
  lookups saw **0 failures**, and the response stamp was identical before
  and after.
- **Database updates are invisible to clients**
  (`scripts/live-sim-demo.sh`). Over an accelerated simulation the
  serving snapshot advanced 0 → 9 → 18 with **no client resync**, and an
  account updated every block was always current.
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
| [`crates/keyword`](crates/keyword) | Cuckoo hashing with capacity-2 buckets; 15-bit byte↔slot packing; the row-major slot matrix the engine ingests; the manifest and sidecar-broadcast wire types. |
| [`crates/backend-ffi`](crates/backend-ffi) | Safe Rust bindings over the `ipir_*` C ABI. The client half builds anywhere (compiles the engine's CPU sources directly — no CMake, no CUDA); the server half (`gpu` feature) links the static libraries, running the CMake build itself when needed. |
| [`crates/server`](crates/server) | The serving front: batch scheduler owning the GPU handle, generation builder + flips, sidecar store, chain source (`--eth-rpc` follower or `--simulate` simulator), HTTP API (`/manifest`, `/lookup`, `/sidecar`, `/query`, `/healthz` — wire contract documented in [`src/http.rs`](crates/server/src/http.rs)). |
| [`crates/client`](crates/client) | Client library + CLI, the reference for wallet integration: single-round fixed-shape lookups, local answer picking, reconfiguration detection. No GPU. |
| [`crates/front`](crates/front) | Thin switchable forwarder for the cross-machine role swap (`POST /admin/target`; no auth — keep it inside the deployment boundary). |
| [`crates/chain`](crates/chain) | Ethereum JSON-RPC adapter: block tracking, touched-address extraction, batched balance/nonce fetch, snapshot resync. |

## Build & run

```bash
git clone --recursive https://github.com/keewoolee/inspire-gpu-serving
# (already cloned? git submodule update --init)

# Anywhere (Rust stable; the client-only path needs just a C++ compiler) —
# keyword, chain, client, and the CPU half of backend-ffi:
cargo build && cargo test

# On a CUDA machine (nvcc under /usr/local/cuda* is found automatically;
# the CMake build of the engine runs inside cargo on first build):
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

1. **A snapshot CSV** — one row per account, `address,nonce,balance_wei`,
   plus a `# block=N` header line recording the block it represents. This
   is the one input the repo does not produce: export it from a node you
   control or from an existing dataset (any dump that yields
   address/nonce/balance works).

2. **A JSON-RPC endpoint** for staying current. Only standard methods
   against `latest` are used (`eth_getBlockByNumber`, `eth_getBalance`,
   `eth_getTransactionCount`) — no archive node, no trace APIs — and the
   calls are paced and retried with backoff to live within hosted-endpoint
   rate limits. Known limit of this feed: it sees an account only when a
   transaction touches it, so a balance that changes with no transaction
   of its own (a withdrawal credit, a transfer inside a contract call)
   stays stale until that address's next touch; exact per-block fidelity
   would take a local node's state diffs.

Then:

```bash
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

