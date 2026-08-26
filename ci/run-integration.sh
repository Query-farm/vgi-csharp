#!/usr/bin/env bash
# Run the canonical Query-farm/vgi integration sqllogictest suite against the C#
# example worker, using a prebuilt standalone `haybarn-unittest` and the signed
# community vgi extension — no C++ build from source. See ci/README.md.
#
# Ported from vgi-go's ci/run-integration.sh, trimmed to a single (stdio)
# transport lane for this first version — vgi-go's version additionally covers
# launch:/shm/http lanes with a skip-reason allowlist and an executed-case
# floor to catch silent whole-suite skips; that hardening is a natural
# follow-up here once this lane is proven green in real CI (see ci/README.md).
#
# Required environment:
#   VGI_SRC           path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST  path to the haybarn-unittest binary
# Optional:
#   BIN_DIR           dir holding the built worker binaries (default: repo root)
#   STAGE             scratch dir for the preprocessed test tree (default: mktemp)
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
BIN_DIR="${BIN_DIR:-$REPO}"
STAGE="${STAGE:-$(mktemp -d)}"
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }

WORKER="$BIN_DIR/vgi-example-worker"
SIMPLE_WRITABLE="$BIN_DIR/vgi-simple-writable-worker"
BAD_PROTOCOL="$BIN_DIR/vgi-bad-protocol-worker"
for b in "$WORKER" "$SIMPLE_WRITABLE" "$BAD_PROTOCOL"; do
  [ -x "$b" ] || { echo "::error::missing worker binary $b (run: dotnet build -c Release)"; exit 1; }
done

# ---------------------------------------------------------------------------
# Stage a preprocessed copy of the suite. preprocess-require.awk rewrites each
# `require <ext>` gate into a signed INSTALL+LOAD so the standalone runner
# (which links none of these extensions) can run them.
#
# Excluded (properties of the prebuilt standalone runner / this being a
# single-worker fixture, not gaps in the C# worker's own 333/333 local suite —
# see docs/roadmap.md for how that number was reached against a
# locally-built unittest):
#   writable/                    — opt-in generic writable catalog
#                                   (VGI_WORKER_ENABLE_WRITABLE), no fixture wired here.
#   nested_type_combinations.test — segfaults the prebuilt standalone runner in
#                                   vgi-go's CI too (a property of that C++ build,
#                                   not the worker); unverified here yet — keep
#                                   this exclusion until proven otherwise.
# ---------------------------------------------------------------------------
echo "Staging preprocessed tests into $STAGE ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  find . -name '*.test' \
       -not -path './writable/*' \
       -not -name 'nested_type_combinations.test' | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# Matches scripts/run_tests.sh's SUBPROCESS=1 lane — the default DuckDB
# `LOCATION` subprocess transport, no launcher/AF_UNIX pooling.
export VGI_TEST_WORKER="$WORKER"
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

rm -f "$log"
exit "$rc"
