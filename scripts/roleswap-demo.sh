#!/usr/bin/env bash
# Two-GPU role-swap demo on one machine.
#
# GPU0 serves generation A behind the front while GPU1 builds generation B
# from scratch; when B is healthy the front switches, A is retired. A client
# hammers the front the whole time — the swap must produce zero failed
# lookups. Both servers run under the SAME --crs-seed, so queries stay valid
# across the swap and clients never resync anything.
#
# Usage: roleswap-demo.sh [ACCOUNTS] [BUCKETS] [DB_ROWS]
# 120 B/bucket: 16 GB tier = 134217728 buckets; mainnet-like load = 182M accounts.
set -euo pipefail

ACCOUNTS="${1:-1000000}"
BUCKETS="${2:-2097152}"
DB_ROWS="${3:-32768}"
# One CRS for both machines: in production the operator copies the seed the
# first server prints at startup.
CRS=$(python3 -c "import secrets; print(secrets.token_hex(64))")
# 18xxx: RunPod images run nginx on common low ports (8081 &c).
BIN=./target/release
FRONT=http://127.0.0.1:18000
A=http://127.0.0.1:18081
B=http://127.0.0.1:18082

cleanup() { kill $(jobs -p) 2>/dev/null || true; }
trap cleanup EXIT

wait_healthy() { # url [pid [logfile]] — fail fast if the process died
  for _ in $(seq 1 600); do
    if curl -s -o /dev/null "$1/healthz"; then return 0; fi
    if [ -n "${2:-}" ] && ! kill -0 "$2" 2>/dev/null; then
      echo "process died waiting for $1:"; tail -5 "${3:-/dev/null}"; return 1
    fi
    sleep 1
  done
  echo "timeout waiting for $1"; return 1
}

echo "=== [1/5] generation A up on GPU0 ==="
CUDA_VISIBLE_DEVICES=0 $BIN/pir-server --synthetic "$ACCOUNTS" --buckets "$BUCKETS" \
  --db-rows "$DB_ROWS" --crs-seed "$CRS" --listen 127.0.0.1:18081 > /tmp/srvA.log 2>&1 &
wait_healthy "$A" $! /tmp/srvA.log
grep "generation" /tmp/srvA.log | tail -1 || true

$BIN/pir-front --listen 127.0.0.1:18000 --target "$A" > /tmp/front.log 2>&1 &
wait_healthy "$FRONT" $! /tmp/front.log

echo "=== [2/5] continuous client load against the front ==="
( ok=0; fail=0
  while [ ! -f /tmp/stop_load ]; do
    if $BIN/pir-client --server "$FRONT" synthetic 12345 > /tmp/last_lookup.txt 2>&1; then
      ok=$((ok+1)); else fail=$((fail+1)); cp /tmp/last_lookup.txt /tmp/failed_lookup.txt; fi
  done
  echo "load done: $ok ok, $fail failed" > /tmp/load_result.txt ) &
LOAD=$!
sleep 3

echo "=== [3/5] generation B builds on GPU1 while A serves ==="
T0=$(date +%s)
CUDA_VISIBLE_DEVICES=1 $BIN/pir-server --synthetic "$ACCOUNTS" --buckets "$BUCKETS" \
  --db-rows "$DB_ROWS" --crs-seed "$CRS" --listen 127.0.0.1:18082 > /tmp/srvB.log 2>&1 &
wait_healthy "$B" $! /tmp/srvB.log
echo "B ready after $(( $(date +%s) - T0 ))s (A kept serving)"

echo "=== [4/5] switch the front, retire A ==="
STAMP_BEFORE=$(curl -s -D- -o /dev/null "$FRONT/manifest" | tr -d '\r' | sed -n 's/^X-Snapshot: //Ip')
curl -s -X POST --data "$B" "$FRONT/admin/target"; echo
sleep 2
pkill -f 'listen 127.0.0.1:18081' || true
STAMP_AFTER=$(curl -s -D- -o /dev/null "$FRONT/manifest" | tr -d '\r' | sed -n 's/^X-Snapshot: //Ip')
# Identical synthetic snapshots under one CRS give identical stamps — the
# swap is invisible to clients, which is the point.
echo "stamp: $STAMP_BEFORE -> $STAMP_AFTER"

sleep 3
echo "=== [5/5] results ==="
touch /tmp/stop_load; wait $LOAD 2>/dev/null || true
cat /tmp/load_result.txt
echo "--- last lookup through the front (now on GPU1) ---"
$BIN/pir-client --server "$FRONT" synthetic 12345
rm -f /tmp/stop_load
