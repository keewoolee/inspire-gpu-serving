#!/usr/bin/env bash
# Live-serving simulation: random initial accounts, a fixed number of random
# updates arriving every block, periodic generation flips. A watcher polls
# the canary (which snapshot block answered?) and account 0 (touched every
# block, so its nonce = the block that last updated it) — the lookup source
# alternates between the sidecar broadcast and the next generation.
#
# Usage: live-sim-demo.sh [ACCOUNTS] [BUCKETS] [UPB] [BLOCK_SECS] [REBUILD_SECS] [WATCH_SECS]
set -euo pipefail

ACCOUNTS="${1:-1000000}"
BUCKETS="${2:-2097152}"
UPB="${3:-300}"
BLOCK_SECS="${4:-12}"
REBUILD_SECS="${5:-60}"
WATCH="${6:-150}"
BIN=./target/release
SRV=http://127.0.0.1:18081

cleanup() { kill $(jobs -p) 2>/dev/null || true; }
trap cleanup EXIT

$BIN/pir-server --synthetic "$ACCOUNTS" --buckets "$BUCKETS" --db-rows 32768 \
  --simulate "$UPB" --block-secs "$BLOCK_SECS" --rebuild-secs "$REBUILD_SECS" \
  --listen 127.0.0.1:18081 > /tmp/sim-server.log 2>&1 &
SRVPID=$!
for _ in $(seq 1 600); do
  if curl -s -o /dev/null "$SRV/healthz"; then break; fi
  if ! kill -0 $SRVPID 2>/dev/null; then echo "server died:"; tail -5 /tmp/sim-server.log; exit 1; fi
  sleep 1
done

SNAP_FIRST=$($BIN/pir-client --server "$SRV" canary | sed -n 's/.*block #\([0-9]*\).*/\1/p')
echo "=== watching for ${WATCH}s (block ${BLOCK_SECS}s, ${UPB} updates/block, flip every ${REBUILD_SECS}s) ==="
END=$(( $(date +%s) + WATCH ))
while [ "$(date +%s)" -lt "$END" ]; do
  C=$($BIN/pir-client --server "$SRV" canary)
  A=$($BIN/pir-client --server "$SRV" synthetic 0 | grep -E "nonce|source" | tr '\n' ' ')
  echo "[t+$(( WATCH - (END - $(date +%s)) ))s] $C"
  echo "         account0: $A"
  sleep 10
done

SNAP_LAST=$($BIN/pir-client --server "$SRV" canary | sed -n 's/.*block #\([0-9]*\).*/\1/p')
echo "=== checks ==="
echo "snapshot block: $SNAP_FIRST -> $SNAP_LAST"
if [ "$SNAP_LAST" -le "$SNAP_FIRST" ]; then echo "FAIL: no generation flip observed"; exit 1; fi
NONCE=$($BIN/pir-client --server "$SRV" synthetic 0 | sed -n 's/^nonce: *//p')
if [ "$NONCE" -le 0 ]; then echo "FAIL: heartbeat account never updated"; exit 1; fi
UNTOUCHED=$($BIN/pir-client --server "$SRV" synthetic 999983 | grep -c "balance: 999983" || true)
if [ "$UNTOUCHED" -ne 1 ]; then echo "WARN: probe account 999983 was touched or missing"; fi
echo "OK: flips observed, heartbeat account fresh (nonce=block $NONCE), data intact."
