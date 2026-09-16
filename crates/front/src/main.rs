//! Thin switchable forwarder. Clients talk to one address; operators point it
//! at whichever pir-server currently serves (role swap = one POST).
//!
//!   pir-front --listen 0.0.0.0:8000 --target http://127.0.0.1:8080
//!
//!   POST /admin/target   body: http://host:port   → switch backends
//!   GET  /admin/target                            → current backend
//!
//! Everything else is forwarded verbatim (method, path, query, body) and the
//! response status, body, and X-Snapshot come back untouched, so the
//! client's consistency check works across a machine swap exactly as it
//! does across an in-process one. The admin endpoints have no auth: keep
//! them inside the pod boundary (demo scope).

use clap::Parser;
use std::sync::{Arc, OnceLock, RwLock};
use tiny_http::{Header, Method, Response, Server};

#[derive(Parser)]
#[command(name = "pir-front")]
struct Args {
    #[clap(long, default_value = "0.0.0.0:8000")]
    listen: String,

    /// Initial backend, e.g. http://127.0.0.1:8080
    #[clap(long)]
    target: String,

    #[clap(long, default_value_t = 32)]
    workers: usize,
}

/// Agent that treats non-2xx as ordinary responses, so backend errors are
/// forwarded with their bodies instead of being swallowed.
fn agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| {
        ureq::config::Config::builder()
            .http_status_as_error(false)
            .build()
            .new_agent()
    })
}

fn main() {
    let args = Args::parse();
    let target = Arc::new(RwLock::new(args.target.trim_end_matches('/').to_string()));
    let server = Arc::new(Server::http(&args.listen).expect("failed to bind"));
    eprintln!("front on {} -> {}", args.listen, target.read().unwrap());

    let mut handles = Vec::new();
    for _ in 0..args.workers.max(1) {
        let server = Arc::clone(&server);
        let target = Arc::clone(&target);
        handles.push(std::thread::spawn(move || loop {
            match server.recv() {
                Ok(req) => handle(req, &target),
                Err(_) => return,
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
}

fn handle(mut req: tiny_http::Request, target: &RwLock<String>) {
    let url = req.url().to_string();
    let method = req.method().clone();

    // Admin: read or switch the backend.
    if url == "/admin/target" {
        match method {
            Method::Get => {
                let t = target.read().unwrap().clone();
                let _ = req.respond(Response::from_string(t));
            }
            Method::Post => {
                let mut body = String::new();
                if req.as_reader().read_to_string(&mut body).is_err() || body.trim().is_empty() {
                    let _ = req.respond(Response::from_string("bad target").with_status_code(400));
                    return;
                }
                let new_target = body.trim().trim_end_matches('/').to_string();
                *target.write().unwrap() = new_target.clone();
                eprintln!("switched -> {}", new_target);
                let _ = req.respond(Response::from_string("ok"));
            }
            _ => {
                let _ = req.respond(Response::from_string("nope").with_status_code(405));
            }
        }
        return;
    }

    // Forward everything else to the current backend.
    let base = target.read().unwrap().clone();
    let full = format!("{}{}", base, url);
    let result = match method {
        Method::Get => agent().get(&full).call(),
        Method::Post => {
            let mut body = Vec::new();
            if req.as_reader().read_to_end(&mut body).is_err() {
                let _ = req.respond(Response::from_string("read error").with_status_code(400));
                return;
            }
            agent().post(&full).send(&body[..])
        }
        _ => {
            let _ = req.respond(Response::from_string("nope").with_status_code(405));
            return;
        }
    };

    match result {
        Ok(mut resp) => {
            let status = resp.status().as_u16();
            let mut headers: Vec<Header> = Vec::new();
            for (name, value) in resp.headers() {
                let n = name.as_str();
                if n.eq_ignore_ascii_case("content-type")
                    || n.eq_ignore_ascii_case("x-snapshot")
                {
                    if let Ok(v) = value.to_str() {
                        if let Ok(h) = Header::from_bytes(n.as_bytes(), v.as_bytes()) {
                            headers.push(h);
                        }
                    }
                }
            }
            let body = resp.body_mut().with_config().limit(128 * 1024 * 1024).read_to_vec().unwrap_or_default();
            let mut out = Response::from_data(body).with_status_code(status);
            for h in headers {
                out = out.with_header(h);
            }
            let _ = req.respond(out);
        }
        Err(e) => {
            let _ = req.respond(
                Response::from_string(format!("backend unreachable: {}", e)).with_status_code(502),
            );
        }
    }
}
