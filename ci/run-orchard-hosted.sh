#!/usr/bin/env bash
# Run vgi's SDK-neutral Orchard hosted-mode conformance harness (test/hosted/) against the C#
# example worker: VGI_ORCHARD_HOSTED=1 over HTTP, with every non-HTTP transport this SDK has
# (stdio, --unix, --iroh-raw-upstream) as a --refuse-cmd. See vgi's test/hosted/README.md.
#
# Required environment:
#   VGI_SRC        path to a Query-farm/vgi checkout containing test/hosted/
# Optional:
#   CONFIGURATION  build configuration the worker was built in (default: Release)
# Extra arguments go to the harness (e.g. --slow, --only 'jwks.*').
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
CONFIGURATION="${CONFIGURATION:-Release}"
HARNESS="$VGI_SRC/test/hosted/run_hosted_conformance.py"
[ -f "$HARNESS" ] || { echo "::error::no test/hosted/run_hosted_conformance.py under VGI_SRC=$VGI_SRC"; exit 1; }
WORKER="$REPO/fixtures/QueryFarm.Vgi.ExampleWorker/bin/$CONFIGURATION/net10.0/vgi-example-worker"
[ -x "$WORKER" ] || { echo "::error::missing worker binary $WORKER (run: dotnet build -c $CONFIGURATION)"; exit 1; }

# No skips: the SDK has every feature the startup rules refuse. The C# worker has no log-level
# flag; ASP.NET's default console logging is what the log-leak rules read.
exec uv run --script "$HARNESS" \
  --worker-cwd "$REPO" \
  --worker-cmd "$WORKER --http --host 127.0.0.1 --port 0" \
  --refuse-cmd "$WORKER" \
  --refuse-cmd "$WORKER --unix /tmp/vgi-hosted-refuse-{port}.sock" \
  --refuse-cmd "$WORKER --iroh-raw-upstream 127.0.0.1:{port} --iroh-issuer refuse-test" \
  --streaming-body \
  "$@"
