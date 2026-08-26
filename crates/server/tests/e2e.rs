//! End-to-end on a GPU machine, through the real client library:
//! synthetic accounts → first generation → HTTP serving → sidecar overlay →
//! generation flip, which the client rides with NO refetch of anything
//! (the CRS is fixed; each /lookup response is one consistent snapshot
//! view by construction).

use pir_client::{PirClient, Source};
use pir_keyword::cuckoo::*;
use pir_server::generation::{GenerationBuilder, ServingState};
use pir_server::http::serve;
use pir_server::sidecar::Sidecar;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BUCKETS: usize = 262144; // x2 cells: 150k keys = 29% cell load
const DB_ROWS: usize = 2048;
const ACCOUNTS: usize = 150_000;
const BLOCK1: u64 = 4242;
const BLOCK2: u64 = 4244;

#[test]
fn test_e2e_serving_and_flip() {
    // --- Generation 1 up (mirrors main.rs) ---
    let cparams = CuckooParams::new(BUCKETS, 20, 40, DETERMINISTIC_SEED);
    let mut table = CuckooTable::new(cparams);
    for i in 0..ACCOUNTS {
        table.insert(&address_from_index(i), &initial_value(i));
    }
    assert!(table.stash.is_empty(), "stash at this load factor");

    // Fabricate a cuckoo-overflow cell: it must be served via the sidecar
    // broadcast's stash list.
    let ghost = address_from_index(9_999_999); // never inserted
    let mut ghost_cell = vec![0u8; 60];
    ghost_cell[..20].copy_from_slice(&ghost);
    ghost_cell[20..60].copy_from_slice(&[0xABu8; 40]);
    table.stash.push(ghost_cell);

    let crs_seed = [0x42u8; 64];
    let builder = Arc::new(Mutex::new(GenerationBuilder::new(
        table,
        DB_ROWS,
        4,
        Duration::from_millis(25),
        crs_seed,
    )));
    let gen1 = builder.lock().unwrap().build(BLOCK1).unwrap();
    assert_eq!(gen1.snapshot_block, BLOCK1);
    assert_eq!(gen1.stamp, gen1.manifest.stamp_for(BLOCK1));
    let state = Arc::new(ServingState::new(gen1, Arc::new(Sidecar::new())));

    let http = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let addr = http.server_addr().to_ip().unwrap();
    let base = format!("http://{}", addr);
    {
        let state = Arc::clone(&state);
        std::thread::spawn(move || serve(http, state, 8));
    }

    // --- Phase 1: serving through the client library ---
    let mut client = PirClient::connect(&base).unwrap();
    // The manifest is static: the pinned CRS seed is what the server prints.
    assert_eq!(
        client.manifest.pir.crs_seed().unwrap(),
        crs_seed,
        "server must serve under the pinned CRS"
    );

    for i in [0usize, 777, ACCOUNTS - 1] {
        let l = client
            .lookup(&address_from_index(i))
            .unwrap()
            .unwrap_or_else(|| panic!("account {} not found", i));
        assert_eq!(l.value, initial_value(i), "value mismatch for account {}", i);
        assert_eq!(l.source, Source::Snapshot { block: BLOCK1 });
    }

    // Canary answers with the snapshot block.
    assert_eq!(client.canary_block().unwrap(), Some(BLOCK1));

    // The stash entry arrives through the sidecar broadcast (no PIR hit).
    let l = client.lookup(&ghost).unwrap().expect("stash entry missing");
    assert_eq!(l.value, vec![0xABu8; 40]);
    assert_eq!(l.source, Source::Snapshot { block: BLOCK1 });

    // Absent key: proven non-membership.
    assert!(client
        .lookup(&address_from_index(BUCKETS + 1))
        .unwrap()
        .is_none());
    assert_eq!(client.last_snapshot_block, BLOCK1);

    // Concurrent lookups exercise the batch path.
    let mut handles = Vec::new();
    for i in 10..14usize {
        let base = base.clone();
        handles.push(std::thread::spawn(move || {
            let mut c = PirClient::connect(&base).unwrap();
            let l = c.lookup(&address_from_index(i)).unwrap().unwrap();
            assert_eq!(l.value, initial_value(i));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Malformed requests are rejected per request.
    let bad = ureq::post(&format!("{}/query", base)).send(&[0u8; 16][..]);
    assert!(bad.is_err() || bad.unwrap().status() != 200);
    let bad = ureq::post(&format!("{}/lookup", base)).send(&[0u8; 16][..]);
    assert!(bad.is_err() || bad.unwrap().status() != 200);

    // --- Phase 2: a chain update lands (what the follower does per block) ---
    let addr3 = address_from_index(3);
    let fresh = vec![0xEEu8; 40];
    builder.lock().unwrap().apply_update(&addr3, &fresh);
    state.sidecar.push(&addr3, &fresh, BLOCK2);

    // The freshest view comes from the sidecar; the flip below shows the
    // same update absorbed into the PIR matrix.
    let l = client.lookup(&addr3).unwrap().unwrap();
    assert_eq!(l.value, fresh);
    assert_eq!(l.source, Source::Sidecar { block: BLOCK2 });

    // --- Phase 3: generation flip. Truncation lags one flip (through the
    // RETIRING snapshot), so the BLOCK2 entry survives — in-flight lookups
    // answered by generation 1 still need it. Fresh lookups are served at
    // BLOCK2 and filter it out server-side.
    let gen2 = builder.lock().unwrap().build(BLOCK2).unwrap();
    state.swap(gen2);
    state.sidecar.truncate_through(BLOCK1);
    assert_eq!(state.sidecar.len(), 1, "one-flip retention lag");

    // The client needs NO refetch of anything static: the same connection
    // keeps working, and the update it saw via the sidecar is now inside
    // the PIR matrix.
    let l = client.lookup(&addr3).unwrap().unwrap();
    assert_eq!(l.value, fresh, "flip must not masquerade as a stale value");
    assert_eq!(l.source, Source::Snapshot { block: BLOCK2 });

    // Untouched accounts survive the flip; the canary proves the new block.
    let l = client.lookup(&address_from_index(0)).unwrap().unwrap();
    assert_eq!(l.value, initial_value(0));
    assert_eq!(client.canary_block().unwrap(), Some(BLOCK2));

    // A COLD client built before the flip data existed would have the same
    // manifest bytes: static across generations.
    let client2 = PirClient::connect(&base).unwrap();
    assert_eq!(client2.manifest, client.manifest);
}
