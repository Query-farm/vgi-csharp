#!/usr/bin/env bash
# Run the canonical Query-farm/vgi integration sqllogictest suite against the C#
# example worker, using a prebuilt standalone `haybarn-unittest` and the signed
# community vgi extension — no C++ build from source. See ci/README.md.
#
# Ported from vgi-go's ci/run-integration.sh. Runs ONE transport per invocation;
# both must pass. A worker's two transports are separate dispatch implementations
# (the pipe/launcher server loop vs. the HTTP endpoint handlers) and they DO
# diverge: running only the launcher lane let a scalar-stream shape bug and three
# HTTP metadata bugs reach a published release with CI reporting 333/334 green.
#
# Required environment:
#   VGI_SRC           path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST  path to the haybarn-unittest binary
# Optional:
#   TRANSPORT         launch (default) | http — see the lane setup below
#   CONFIGURATION     build configuration the worker binaries were built in (default: Release)
#   STAGE             scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
STAGE="${STAGE:-$(mktemp -d)}"
CONFIGURATION="${CONFIGURATION:-Release}"
TRANSPORT="${TRANSPORT:-launch}"
case "$TRANSPORT" in
  launch|http) ;;
  *) echo "::error::TRANSPORT must be 'launch' or 'http', got '$TRANSPORT'"; exit 1 ;;
esac
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }

# Unlike vgi-go's single shared BIN_DIR (its `make build` places every worker binary in one
# place), each fixture here is its own .csproj with .NET's standard per-project bin/ layout —
# find each worker binary in its own project's output directory.
WORKER="$REPO/fixtures/QueryFarm.Vgi.ExampleWorker/bin/$CONFIGURATION/net10.0/vgi-example-worker"
SIMPLE_WRITABLE="$REPO/fixtures/QueryFarm.Vgi.SimpleWritableWorker/bin/$CONFIGURATION/net10.0/vgi-simple-writable-worker"
BAD_PROTOCOL="$REPO/fixtures/QueryFarm.Vgi.BadProtocolWorker/bin/$CONFIGURATION/net10.0/vgi-bad-protocol-worker"
for b in "$WORKER" "$SIMPLE_WRITABLE" "$BAD_PROTOCOL"; do
  [ -x "$b" ] || { echo "::error::missing worker binary $b (run: dotnet build -c $CONFIGURATION)"; exit 1; }
done

# ---------------------------------------------------------------------------
# Stage a preprocessed copy of the suite. preprocess-require.awk rewrites each
# `require <ext>` gate into a signed INSTALL+LOAD so the standalone runner
# (which links none of these extensions) can run them.
#
# Excluded (properties of the prebuilt standalone runner / community-published
# extension build — not gaps in the C# worker's own 333/333 local suite, verified
# against a locally-built unittest; see docs/roadmap.md for how that number was
# reached). table/expression_filter.test USED to be excluded here too (this port's
# expression-filter pushdown was genuinely unimplemented — see
# Internal/ExpressionFilterEvaluator.cs's doc comment for the fix and
# SpatialFilterExampleFunction's/ExpressionFilterTestFunction's doc comments for how
# it's wired in); it's no longer excluded — verified 32/32 assertions passing against
# a real haybarn-unittest + spatial extension (this environment has no local spatial
# build, so this lane is this file's ONLY real coverage — see ci/README.md):
#   writable/                    — opt-in generic writable catalog
#                                   (VGI_WORKER_ENABLE_WRITABLE), no fixture wired here.
#   nested_type_combinations.test — segfaults the prebuilt standalone runner in
#                                   vgi-go's CI too (a property of that C++ build,
#                                   not the worker); unverified here yet — keep
#                                   this exclusion until proven otherwise.
#   cache/secret_ineligible.test,
#   macro/macros.test            — both assert exact counts of specific
#                                   duckdb_logs()/catalog-RPC events; both pass 333/333
#                                   locally against a git-HEAD-built unittest. The
#                                   community-published vgi extension this lane
#                                   installs (FORCE INSTALL vgi FROM community) is not
#                                   version-pinned to VGI_REF's test-file commit (see
#                                   ci/README.md's "Version pins" section) — these read
#                                   as the same class of extension-build-vs-test-file
#                                   skew as vgi-go's own CI hits, not a worker bug.
#                                   Revisit if a future community-extension publish
#                                   catches up.
# ---------------------------------------------------------------------------
AWK_HTTP=0
# database_worker/package.test packages $VGI_TEST_WORKER as an EXECUTABLE artifact into a DuckDB
# table and then runs it (test/support/database_worker_fixture.sh is a wrapper that execs it). On
# the http lane VGI_TEST_WORKER is a URL, so the fixture fails with
# `/bin/sh: http://localhost:NNNNN: No such file or directory`. The test's premise is a local
# worker binary; there is nothing for an HTTP worker to package. Excluded on that lane only — it
# runs, and must pass, on the launch lane.
EXTRA_EXCLUDES=()
if [ "$TRANSPORT" = http ]; then
  AWK_HTTP=1
  EXTRA_EXCLUDES=(-not -path './database_worker/package.test')
fi

echo "Staging preprocessed tests into $STAGE (transport=$TRANSPORT) ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  find . -name '*.test' \
       -not -path './writable/*' \
       -not -name 'nested_type_combinations.test' \
       -not -path './cache/secret_ineligible.test' \
       -not -path './macro/macros.test' \
       ${EXTRA_EXCLUDES[@]+"${EXTRA_EXCLUDES[@]}"} | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -v http="$AWK_HTTP" -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# The database-worker tests package this executable through a path relative to
# the staged unittest working directory. Staging only .test files leaves that
# path unmatched, so preserve the fixture and its executable bit explicitly.
DATABASE_WORKER_FIXTURE="$VGI_SRC/test/support/database_worker_fixture.sh"
if [ ! -f "$DATABASE_WORKER_FIXTURE" ]; then
  echo "::error::pinned VGI suite is missing $DATABASE_WORKER_FIXTURE" >&2
  exit 1
fi
mkdir -p "$STAGE/test/support"
install -m 0755 "$DATABASE_WORKER_FIXTURE" \
  "$STAGE/test/support/database_worker_fixture.sh"

HTTP_PID=""
if [ "$TRANSPORT" = launch ]; then
  # Pool the main worker behind DuckDB's AF_UNIX launcher. The suite opens many
  # connections and ATTACHes the same worker repeatedly; a bare path starts a new
  # .NET process for each connection, while launch: reuses one warm process.
  export VGI_TEST_WORKER="launch:$WORKER"
  export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
else
  # One long-lived HTTP server for the whole lane, on an ephemeral port it reports
  # on stdout as `PORT:<n>`. VGI_REQUIRE_LAUNCHER_TRANSPORT is deliberately NOT set.
  HTTP_LOG="$STAGE/http-worker.log"
  ( cd "$STAGE" && exec "$WORKER" --http ) > "$HTTP_LOG" 2>&1 &
  HTTP_PID=$!
  trap '[ -n "$HTTP_PID" ] && kill -TERM "$HTTP_PID" 2>/dev/null || true' EXIT
  port=""
  for _ in $(seq 1 120); do
    kill -0 "$HTTP_PID" 2>/dev/null || { echo "::error::http worker exited before reporting a port"; cat "$HTTP_LOG"; exit 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$HTTP_LOG" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker never reported a port"; cat "$HTTP_LOG"; exit 1; }
  echo "http worker pid=$HTTP_PID port=$port"
  export VGI_TEST_WORKER="http://localhost:${port}"
  # DELIBERATELY NOT SET: VGI_HTTP_TRANSPORT, which would un-gate the suite's five HTTP-only
  # files. Four of them (http/capability_probe, http/producer_turns, http/small_body_encoding,
  # cache/partition_scope_identity) were verified to pass here as-is; the fifth,
  # cache/identity_isolation.test, needs the example worker to answer as a NAMED principal —
  # the reference fixture server maps `vgi-test-alice`->alice / `vgi-test-bob`->bob as optional
  # bearer auth (vgi-python's `_test_fixtures/http_server.py`). This port's `Worker.RunHttpAsync`
  # exposes no authenticate hook at all, so there is nowhere to wire that map; adding one is a
  # product API change, not a harness change, and belongs in its own commit. Setting the variable
  # without it would leave a permanently red file, and excluding that file would be the exact
  # dishonesty the guards below exist to prevent. See ci/README.md.
fi

# Keep the small stateful and deliberately-incompatible fixtures isolated.
# They account for only a handful of tests, and process isolation prevents
# their mutable/error state from contaminating the shared main worker.
export VGI_SIMPLE_WRITABLE_WORKER="$SIMPLE_WRITABLE"
export VGI_BAD_PROTOCOL_WORKER="$BAD_PROTOCOL"

cd "$STAGE"

echo "Warming the extension cache (vgi from community, deps from core) ..."
mkdir -p "$STAGE/test"
cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
FORCE INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;

statement ok
INSTALL parquet FROM core;
EOF
"$HAYBARN_UNITTEST" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
rm -f "$STAGE/test/_warm.test"

echo "Running suite (test/sql/integration/*) ..."
log="$(mktemp)"
rc=0
"$HAYBARN_UNITTEST" "test/sql/integration/*" 2>&1 | tee "$log" && rc=0 || rc="${PIPESTATUS[0]}"

if grep -q 'No test cases matched\|No tests ran' "$log"; then
  echo "::error::the runner matched no test cases — the glob or the staging is wrong (an empty stage still exits 0)."
  rc=1
fi

# -----------------------------------------------------------------------------
# Guards against a lane that is green because it did not really run.
#
# DuckDB's sqllogictest runner defaults `ignore_error_messages` to
# {"HTTP", "Unable to connect"} — a statement whose error text contains "HTTP" is
# SKIPPED, not failed. On a lane that reaches the worker over HTTP that default is
# a live hazard rather than a convenience: a worker returning 500 produces an error
# message containing "HTTP", so a whole class of server-side crashes reads as
# "skipped" and the lane still exits 0. Observed directly: an intermediate state of
# this port turned 118 assertions into silent skips this way while the summary line
# still said 0 failed. The floor is ZERO, not a tolerance: the whole suite was re-run
# once with `set ignore_error_messages Unable to connect` injected into every staged
# file, and with the worker fixed NOTHING in it legitimately errors with an
# HTTP-containing message. Every such skip is a bug in hiding.
#
# The assertion floor catches the same failure mode from the other side — a run that
# skipped most of the suite for any reason at all.
if [ "$TRANSPORT" = http ]; then
  swallowed="$(sed -n "s/^skip on error_message matching 'HTTP': \([0-9]*\)$/\1/p" "$log" | head -1)"
  swallowed="${swallowed:-0}"
  if [ "$swallowed" -gt "${MAX_HTTP_SWALLOWED:-0}" ]; then
    echo "::error::$swallowed assertions were skipped by DuckDB's ignore_error_messages 'HTTP' default (max ${MAX_HTTP_SWALLOWED:-0}). A server-side 500 hides here — read the worker log at $HTTP_LOG."
    rc=1
  fi

  if ! kill -0 "$HTTP_PID" 2>/dev/null; then
    echo "::error::the shared http worker died during the run — every result after that point is untrustworthy."
    echo "--- http worker log tail ---"; tail -80 "$HTTP_LOG"
    rc=1
  fi
fi

# Catch2 prints two different summary shapes, and the FULLY GREEN one is the shape this guard
# most needs to read: "All tests passed (N assertions in M test cases)" when nothing failed, and
# a "assertions: N | ..." table when something did. Match both — reading only the table would
# make the floor unreachable on exactly the runs it exists to catch.
assertions="$(sed -n 's/^assertions: *\([0-9]*\) .*/\1/p' "$log" | tail -1)"
if [ -z "$assertions" ]; then
  assertions="$(sed -n 's/^All tests passed .*[( ]\([0-9][0-9]*\) assertions.*/\1/p' "$log" | tail -1)"
fi
if [ -z "$assertions" ]; then
  echo "::error::could not read an assertion count out of the runner summary — the guard below cannot do its job, so fail rather than assume."
  rc=1
elif [ "$assertions" -lt "${MIN_ASSERTIONS:-9000}" ]; then
  echo "::error::only $assertions assertions ran (floor ${MIN_ASSERTIONS:-9000}) — the suite was largely skipped, not passed."
  rc=1
fi

rm -f "$log"
exit "$rc"
