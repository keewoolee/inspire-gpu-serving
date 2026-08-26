//! Batch scheduler: the one thread that owns the GPU handle. Request threads
//! submit jobs of 1 or 2 flat queries through a channel; the scheduler
//! collects up to `max_batch` query slots (waiting at most `window` after
//! the first arrival) and answers them in one `ipir_answer_batch` pass.
//!
//! A lookup's bucket pair travels as ONE job, so the pair is never split
//! across batches — with an even `max_batch`, pairs always pack cleanly. A
//! job that does not fit the remaining slots carries over to the next
//! round.

use pir_backend_ffi::GpuServer;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct QueryJob {
    /// 1 or 2 flat queries, `params.query_u64s()` values each (2 = one
    /// lookup's bucket pair, answered by the same batch).
    pub flats: Vec<Vec<u64>>,
    /// Where the flat responses (or an error) are delivered, one per query,
    /// in submission order.
    pub resp: Sender<Result<Vec<Vec<u64>>, String>>,
}

/// Move the server into its scheduler thread; the returned Sender is the only
/// way to reach the GPU from then on. Dropping every Sender stops the thread.
pub fn spawn(
    srv: GpuServer,
    max_batch: usize,
    window: Duration,
) -> (Sender<QueryJob>, JoinHandle<()>) {
    let (tx, rx) = channel::<QueryJob>();
    let handle = std::thread::spawn(move || run(srv, rx, max_batch, window));
    (tx, handle)
}

fn run(mut srv: GpuServer, rx: Receiver<QueryJob>, max_batch: usize, window: Duration) {
    let expected = srv.params().query_u64s();
    let resp_u64s = srv.params().response_u64s();
    // A job received when the round was already full waits here for the
    // next round.
    let mut carry: Option<QueryJob> = None;

    loop {
        // First job of the round: the carried-over one, or block for one.
        let first = match carry.take() {
            Some(j) => j,
            None => match rx.recv() {
                Ok(j) => j,
                Err(_) => return, // all senders gone
            },
        };
        let mut slots = first.flats.len();
        let mut jobs = vec![first];

        // Fill the batch until max_batch slots or the window closes.
        let deadline = Instant::now() + window;
        while slots < max_batch {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(j) => {
                    if slots + j.flats.len() > max_batch {
                        carry = Some(j);
                        break;
                    }
                    slots += j.flats.len();
                    jobs.push(j);
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        // Reject malformed jobs individually; never let one poison a batch.
        let (good, bad): (Vec<QueryJob>, Vec<QueryJob>) = jobs.into_iter().partition(|j| {
            !j.flats.is_empty()
                && j.flats.len() <= 2
                && j.flats.iter().all(|f| f.len() == expected)
        });
        for j in bad {
            let _ = j.resp.send(Err(format!(
                "job must carry 1 or 2 queries of exactly {} u64 values each",
                expected
            )));
        }
        if good.is_empty() {
            continue;
        }

        let refs: Vec<&[u64]> = good
            .iter()
            .flat_map(|j| j.flats.iter().map(|f| f.as_slice()))
            .collect();
        match srv.answer_batch(&refs) {
            Ok(out) => {
                let mut pos = 0usize;
                for j in &good {
                    let mut resps = Vec::with_capacity(j.flats.len());
                    for _ in 0..j.flats.len() {
                        resps.push(out[pos * resp_u64s..(pos + 1) * resp_u64s].to_vec());
                        pos += 1;
                    }
                    let _ = j.resp.send(Ok(resps));
                }
            }
            Err(e) => {
                for j in &good {
                    let _ = j.resp.send(Err(e.clone()));
                }
            }
        }
    }
}
