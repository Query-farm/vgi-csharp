#!/usr/bin/env bash
# Run the extension's blob-vendor externalization files (test/sql/external_storage/, including the
# protocol 3.4 argument-reference files) against the C# example worker over HTTP, with vgi-rpc's
# fake blob vendor (vgi docs/testing/blob-test-catalog.md).
#
# Each file gets its own worker (`--server-per-test`), so a file's `# server-env:` switches
# (VGI_TEST_DISABLE_ARGUMENTS_REF, VGI_RPC_OBJECT_CACHE_BYTES) reach it; files run one at a time
# because they compare the vendor's lane-wide counters.
#
# Required environment:
#   VGI_SRC           a Query-farm/vgi checkout (test/, scripts/)
#   HAYBARN_UNITTEST  the matching native unittest (links vgi/httpfs/json/parquet)
# Optional:
#   CONFIGURATION     build configuration of the worker binaries (default: Release)
#
# The one expected skip:
#   arguments_ref_state_ref.test   require-env VGI_WORKER_INIT_STATE_REF: C# keeps HTTP streams in
#                                  process memory (single-replica in protocol 3.4), so there is no
#                                  stored `init` StateRef to observe.
# The fetch_*.test files test the extension's own download client against vgi-python's probe worker
# and are not run here.
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the native unittest}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
CONFIGURATION="${CONFIGURATION:-Release}"
WORKER="$REPO/fixtures/QueryFarm.Vgi.ExampleWorker/bin/$CONFIGURATION/net10.0/vgi-example-worker"
[ -x "$WORKER" ] || { echo "::error::missing worker binary $WORKER (run: dotnet build -c $CONFIGURATION)"; exit 1; }

STAGE="${STAGE:-$(mktemp -d)}"
mkdir -p "$STAGE/build/release/test"
cp -R "$VGI_SRC/test" "$VGI_SRC/scripts" "$STAGE/"
# scripts/run_tests.py runs build/release/test/unittest; a wrapper rather than a symlink pins
# --test-dir to the stage (the binary's default is the source tree it was built in).
printf '#!/bin/bash\nexec %q --test-dir %q "$@"\n' "$HAYBARN_UNITTEST" "$STAGE" > "$STAGE/build/release/test/unittest"
chmod +x "$STAGE/build/release/test/unittest"
cd "$STAGE"

# shellcheck source=/dev/null
source test/start_fake_blob_vendor.sh
trap stop_fake_blob_vendor EXIT

export VGI_WORKER_ARGUMENTS_REF=1 VGI_FAKE_VENDOR_DELETE_ROUTE=1
rc=0
python3 scripts/run_tests.py --build release --root "$STAGE" --server-per-test "$WORKER --http" \
    "test/sql/external_storage/*" "~test/sql/external_storage/fetch_*" -j 1 \
    --allow-skip 'require-env VGI_WORKER_INIT_STATE_REF' || rc=$?
echo "fake blob vendor counters: $(curl -s "$VGI_BLOB_VENDOR_URL/__stats__" | tr -s ' ')"
exit "$rc"
