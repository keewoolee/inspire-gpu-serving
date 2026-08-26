//! Small HTTP API over the batch scheduler.
//!
//!   GET  /healthz   → "ok"
//!   GET  /manifest  → the static service manifest, JSON (fetched once at
//!                     client setup; unchanged by generation flips)
//!   POST /lookup    → body: TWO packed queries back to back (a lookup's
//!                     cuckoo bucket pair, 2 x query_packed_bytes).
//!                     Response: [4B LE json length][SidecarBroadcast
//!                     JSON][compressed response 1][compressed response 2].
//!                     The server answers the queries first, then attaches
//!                     the sidecar suffix for the snapshot that ANSWERED —
//!                     one consistent view by construction, single round.
//!   GET  /sidecar   → the current broadcast alone (monitoring/debugging)
//!   POST /query     → one packed query → one compressed response
//!                     (debugging; real clients use /lookup)
//!
//! Every response carries `X-Snapshot: <snapshot block>:<config
//! fingerprint>` (see Manifest::stamp_for). Queries are valid against every
//! generation (the CRS is fixed). /lookup responses are internally
//! consistent by construction: the queries are answered by the generation
//! captured at request start (a flip does not interrupt it — the retiring
//! generation keeps serving its in-flight jobs), and the sidecar store
//! retains entries one flip past their snapshot (see follower), so the
//! suffix for that generation is still complete even if a flip landed
//! mid-request.

use crate::generation::ServingState;
use crate::scheduler::QueryJob;
use pir_keyword::manifest::SidecarBroadcast;
use std::io::Read;
use std::sync::mpsc::channel;
use std::sync::Arc;
use tiny_http::{Header, Method, Response, Server};

fn json_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap()
}

fn octet_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/octet-stream"[..]).unwrap()
}

fn stamp_header(stamp: &str) -> Header {
    Header::from_bytes(&b"X-Snapshot"[..], stamp.as_bytes()).unwrap()
}

/// Serve until the process exits. `workers` request threads share the
/// listener; each blocks on the GPU scheduler for its own query.
pub fn serve(server: Server, state: Arc<ServingState>, workers: usize) {
    let server = Arc::new(server);
    let mut handles = Vec::new();
    for _ in 0..workers.max(1) {
        let server = Arc::clone(&server);
        let state = Arc::clone(&state);
        handles.push(std::thread::spawn(move || loop {
            match server.recv() {
                Ok(req) => handle(req, &state),
                Err(_) => return,
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
}

fn handle(mut req: tiny_http::Request, state: &ServingState) {
    let url = req.url().to_string();
    let path = url.split('?').next().unwrap_or("");
    let method = req.method().clone();
    let gen = state.current();
    let stamp = stamp_header(&gen.stamp);

    let response = match (method, path) {
        (Method::Get, "/healthz") => Response::from_string("ok").with_header(stamp),

        (Method::Get, "/manifest") => Response::from_string(gen.manifest_json.clone())
            .with_header(json_header())
            .with_header(stamp),

        (Method::Get, "/sidecar") => {
            // A flip landing between capturing `gen` and reading the store
            // is harmless: truncation lags one flip (only entries at or
            // before THIS generation's snapshot are ever deleted while it
            // can still answer), so the suffix stays complete.
            let broadcast = SidecarBroadcast {
                snapshot_block: gen.snapshot_block,
                stash: gen.stash.clone(),
                entries: state.sidecar.since(gen.snapshot_block),
            };
            Response::from_string(broadcast.to_json())
                .with_header(json_header())
                .with_header(stamp)
        }

        (Method::Post, "/lookup") => {
            let qb = gen.params.query_packed_bytes();
            let mut body = Vec::with_capacity(2 * qb);
            if req.as_reader().read_to_end(&mut body).is_err() || body.len() != 2 * qb {
                respond(
                    req,
                    Response::from_string(format!("lookup body must be exactly 2x{} bytes", qb))
                        .with_status_code(400)
                        .with_header(stamp),
                );
                return;
            }
            let mut flats = Vec::with_capacity(2);
            for half in body.chunks(qb) {
                match pir_backend_ffi::unpack_query(&gen.params, half) {
                    Ok(f) => flats.push(f),
                    Err(e) => {
                        respond(
                            req,
                            Response::from_string(e).with_status_code(400).with_header(stamp),
                        );
                        return;
                    }
                }
            }

            let responses = match answer(&gen, flats) {
                Ok(r) => r,
                Err((code, msg)) => {
                    respond(
                        req,
                        Response::from_string(msg).with_status_code(code).with_header(stamp),
                    );
                    return;
                }
            };

            // Queries answered — NOW attach the suffix for the snapshot
            // that answered them. The store retains entries one flip past
            // their snapshot, so this is complete even across a flip.
            let broadcast = SidecarBroadcast {
                snapshot_block: gen.snapshot_block,
                stash: gen.stash.clone(),
                entries: state.sidecar.since(gen.snapshot_block),
            };
            let json = broadcast.to_json();
            let mut out =
                Vec::with_capacity(4 + json.len() + responses.iter().map(Vec::len).sum::<usize>());
            out.extend_from_slice(&(json.len() as u32).to_le_bytes());
            out.extend_from_slice(json.as_bytes());
            for r in &responses {
                out.extend_from_slice(r);
            }
            respond(
                req,
                Response::from_data(out).with_header(octet_header()).with_header(stamp),
            );
            return;
        }

        (Method::Post, "/query") => {
            let expected_bytes = gen.params.query_packed_bytes();
            let mut body = Vec::with_capacity(expected_bytes);
            if req.as_reader().read_to_end(&mut body).is_err() {
                respond(
                    req,
                    Response::from_string("read error").with_status_code(400).with_header(stamp),
                );
                return;
            }
            let flat = match pir_backend_ffi::unpack_query(&gen.params, &body) {
                Ok(f) => f,
                Err(e) => {
                    respond(
                        req,
                        Response::from_string(e).with_status_code(400).with_header(stamp),
                    );
                    return;
                }
            };
            match answer(&gen, vec![flat]) {
                Ok(mut responses) => {
                    respond(
                        req,
                        Response::from_data(responses.remove(0))
                            .with_header(octet_header())
                            .with_header(stamp),
                    );
                }
                Err((code, msg)) => {
                    respond(
                        req,
                        Response::from_string(msg).with_status_code(code).with_header(stamp),
                    );
                }
            }
            return;
        }

        _ => Response::from_string("not found").with_status_code(404),
    };
    respond(req, response);
}

fn respond<R: Read>(req: tiny_http::Request, resp: Response<R>) {
    let _ = req.respond(resp);
}

/// Send flat queries to the generation's scheduler as one job and compress
/// each answer. Errors as (HTTP status, message).
fn answer(
    gen: &crate::generation::Generation,
    flats: Vec<Vec<u64>>,
) -> Result<Vec<Vec<u8>>, (u16, String)> {
    let (tx, rx) = channel();
    if gen.jobs.send(QueryJob { flats, resp: tx }).is_err() {
        return Err((503, "scheduler down".into()));
    }
    let resp_u64s = match rx.recv() {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return Err((400, e)),
        Err(_) => return Err((503, "scheduler down".into())),
    };
    resp_u64s
        .iter()
        .map(|r| pir_backend_ffi::compress_response(&gen.params, r).map_err(|e| (500, e)))
        .collect()
}
