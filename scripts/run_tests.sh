#!/usr/bin/env bash
# Run the in-scope VGI integration suite against the C# worker(s) — ONE `unittest`
# invocation across every requested glob (not one subprocess per .test file, unlike
# `~/Development/vgi/scripts/run_tests.py`), and by default the pooled AF_UNIX
# `launch:` transport so the worker process is started once and reused across every
# ATTACH in the run, not cold-spawned per test file. Both together make a full-suite
# run minutes instead of tens of minutes. Modeled directly on vgi-rust's
# scripts/run_tests.sh — see that file for the pattern this was ported from.
#
# Usage:
#   scripts/run_tests.sh                      # full in-scope suite, launcher transport
#   scripts/run_tests.sh scalar               # one category
#   scripts/run_tests.sh "test/sql/integration/table/sequence.test"   # one file (path relative to vgi checkout)
#   scripts/run_tests.sh --no-build ...       # skip dotnet build
#   SUBPROCESS=1 scripts/run_tests.sh ...     # bare-subprocess transport instead of launch: (slower; use to isolate a launcher-specific bug)
#
# Caches output under /tmp/vgi-csharp-test-cache/:
#   run.log        full unittest stdout/stderr
#   failures       unique failing .test paths
#   summary        pass/fail context around failures

set -uo pipefail

VGI_CSHARP="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VGI_EXT="${VGI_EXT:-$HOME/Development/vgi}"
UNITTEST="$VGI_EXT/build/release/test/unittest"
DOTNET="${DOTNET:-$HOME/.dotnet/dotnet}"
export PATH="$HOME/.dotnet:$PATH"

EXAMPLE_BIN="$VGI_CSHARP/fixtures/QueryFarm.Vgi.ExampleWorker/bin/Debug/net10.0/vgi-example-worker"
WRITABLE_BIN="$VGI_CSHARP/fixtures/QueryFarm.Vgi.SimpleWritableWorker/bin/Debug/net10.0/vgi-simple-writable-worker"
BAD_PROTOCOL_BIN="$VGI_CSHARP/fixtures/QueryFarm.Vgi.BadProtocolWorker/bin/Debug/net10.0/vgi-bad-protocol-worker"
# Dedicated single-catalog binaries — attach/versioning.test and attach/versioned_tables*.test's
# vgi_catalogs() discovery queries are unfiltered and expect exactly one row, which a shared
# multi-catalog worker (like EXAMPLE_BIN) can't satisfy. attach_options/attach_options_required
# ARE catalogs inside EXAMPLE_BIN — their test files always filter `WHERE catalog = '...'`.
VERSIONED_BIN="$VGI_CSHARP/fixtures/QueryFarm.Vgi.VersionedWorker/bin/Debug/net10.0/vgi-versioned-worker"
VERSIONED_TABLES_BIN="$VGI_CSHARP/fixtures/QueryFarm.Vgi.VersionedTablesWorker/bin/Debug/net10.0/vgi-versioned-tables-worker"
# bad_enum.test — bare path always (never launch:); a single deliberately-malformed catalog, no
# pooling/launcher interaction needed.
BAD_ENUM_BIN="$VGI_CSHARP/fixtures/QueryFarm.Vgi.BadEnumWorker/bin/Debug/net10.0/vgi-bad-enum-worker"

CACHE="/tmp/vgi-csharp-test-cache"
mkdir -p "$CACHE"

BUILD=1
if [[ "${1:-}" == "--no-build" ]]; then BUILD=0; shift; fi

if [[ $BUILD == 1 ]]; then
  echo "[harness] building (Debug)..."
  ( cd "$VGI_CSHARP" && "$DOTNET" build vgi-csharp.slnx 2>&1 ) | tail -5
  if [[ ! -x "$EXAMPLE_BIN" ]]; then echo "[harness] build failed: $EXAMPLE_BIN missing"; exit 1; fi
  if [[ ! -x "$VERSIONED_BIN" ]]; then echo "[harness] build failed: $VERSIONED_BIN missing"; exit 1; fi
  if [[ ! -x "$VERSIONED_TABLES_BIN" ]]; then echo "[harness] build failed: $VERSIONED_TABLES_BIN missing"; exit 1; fi
fi

# Determine the filter set.
ARGS=()
if [[ $# -ge 1 ]]; then
  case "$1" in
    test/*) ARGS=("$1");;
    *)      ARGS=("test/sql/integration/$1/*");;
  esac
else
  ARGS=("test/sql/integration/*")
fi

ENV_ARGS=()
if [[ "${SUBPROCESS:-0}" == "1" ]]; then
  TEST_WORKER="$EXAMPLE_BIN"
  WRITABLE_WORKER="$WRITABLE_BIN"
  VERSIONED_WORKER="$VERSIONED_BIN"
  VERSIONED_TABLES_WORKER="$VERSIONED_TABLES_BIN"
else
  TEST_WORKER="launch:$EXAMPLE_BIN"
  WRITABLE_WORKER="launch:$WRITABLE_BIN"
  VERSIONED_WORKER="launch:$VERSIONED_BIN"
  VERSIONED_TABLES_WORKER="launch:$VERSIONED_TABLES_BIN"
  # Only meaningful (and only asserted on) under the launcher transport — see
  # test/sql/integration/launcher/options_validation.test.
  ENV_ARGS+=(VGI_REQUIRE_LAUNCHER_TRANSPORT=1)
fi

ENV_ARGS+=(VGI_TEST_WORKER="$TEST_WORKER" VGI_SIMPLE_WRITABLE_WORKER="$WRITABLE_WORKER")
# attach_options / attach_options_required are additional catalogs on the SAME example-worker
# binary (see fixtures/QueryFarm.Vgi.ExampleWorker/AttachOptions/) — same $TEST_WORKER value,
# just a different ATTACH catalog name per test file. versioned / versioned_tables are their own
# dedicated binaries — see the VERSIONED_BIN/VERSIONED_TABLES_BIN comment above.
ENV_ARGS+=(
  VGI_VERSIONED_WORKER="$VERSIONED_WORKER"
  VGI_VERSIONED_TABLES_WORKER="$VERSIONED_TABLES_WORKER"
  VGI_ATTACH_OPTIONS_WORKER="$TEST_WORKER"
  VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$TEST_WORKER"
)
if [[ -x "$BAD_PROTOCOL_BIN" ]]; then
  ENV_ARGS+=(VGI_BAD_PROTOCOL_WORKER="$BAD_PROTOCOL_BIN")
fi
if [[ -x "$BAD_ENUM_BIN" ]]; then
  ENV_ARGS+=(VGI_BAD_ENUM_WORKER="$BAD_ENUM_BIN")
fi

echo "[harness] running: ${ARGS[*]} (worker: $TEST_WORKER)"
env "${ENV_ARGS[@]}" "$UNITTEST" "${ARGS[@]}" > "$CACHE/run.log" 2>&1
RC=$?

grep -B1 -A20 -iE 'unexpectedly|FAILED|Mismatch|Worker Exception|Error:' "$CACHE/run.log" > "$CACHE/summary" 2>/dev/null
awk '/unexpectedly|FAILED:|Mismatch on/{print}' "$CACHE/run.log" \
  | grep -oE 'test/sql/integration/[A-Za-z0-9_/]+\.test(_slow)?' | sort -u > "$CACHE/failures" 2>/dev/null

echo "===== TAIL ====="
tail -8 "$CACHE/run.log"
echo "===== FAILURES ($(wc -l < "$CACHE/failures" | tr -d ' ')) ====="
cat "$CACHE/failures" 2>/dev/null
echo "(full log: $CACHE/run.log  summary: $CACHE/summary)"
exit $RC
