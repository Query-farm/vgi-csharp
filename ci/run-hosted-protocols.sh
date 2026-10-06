#!/usr/bin/env bash
# Runs vgi-rpc's shared hosted-protocols group (`vgi-rpc-test-hosted`, MULTI_PROTOCOL_HOSTING.md
# §5) against the example fixture worker on stdio, unix and HTTP (+ --identity). The expected
# list is deliberately non-alphabetical: a server that sorts its protocol listing fails it.
#
# Required: vgi-rpc-test-hosted on PATH (pip install "vgi-rpc[http,conformance]" pytest
# pytest-timeout, from a checkout of the reference). Optional: CONFIGURATION (default Release).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
CONFIGURATION="${CONFIGURATION:-Release}"
WORKER="$REPO/fixtures/QueryFarm.Vgi.ExampleWorker/bin/$CONFIGURATION/net10.0/vgi-example-worker"
EXPECT="vgi.v2,conformance.Secondary.v1"
[ -x "$WORKER" ] || { echo "::error::missing $WORKER (run: dotnet build -c $CONFIGURATION)"; exit 1; }

# Workers run from an empty directory: the HTTP fixture's content root is its cwd.
RUN_DIR="$(mktemp -d)"
SOCK="/tmp/vgi-cs-hosted-$$.sock"   # short: AF_UNIX paths are limited to ~108 bytes
PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; rm -f "$SOCK"; }
trap cleanup EXIT

echo "=== stdio"
vgi-rpc-test-hosted --cmd "$WORKER" --expect "$EXPECT"

echo "=== unix"
( cd "$RUN_DIR" && exec "$WORKER" --unix "$SOCK" --idle-timeout 0 ) > "$RUN_DIR/unix.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 120); do [ -S "$SOCK" ] && break; sleep 0.5; done
[ -S "$SOCK" ] || { echo "::error::unix worker never bound $SOCK"; cat "$RUN_DIR/unix.log"; exit 1; }
vgi-rpc-test-hosted --unix "$SOCK" --expect "$EXPECT"

echo "=== http --identity"
( cd "$RUN_DIR" && exec "$WORKER" --http --identity ) > "$RUN_DIR/http.log" 2>&1 &
PIDS+=($!)
port=""
for _ in $(seq 1 120); do
  port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$RUN_DIR/http.log" | head -1)"
  [ -n "$port" ] && break
  sleep 0.5
done
[ -n "$port" ] || { echo "::error::http worker never reported a port"; cat "$RUN_DIR/http.log"; exit 1; }
vgi-rpc-test-hosted --url "http://127.0.0.1:$port" --expect "$EXPECT" --identity
