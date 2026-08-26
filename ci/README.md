# CI: the vgi integration suite

[`.github/workflows/integration.yml`](../.github/workflows/integration.yml) runs the canonical
[Query-farm/vgi](https://github.com/Query-farm/vgi) integration sqllogictest suite against this
repo's C# example worker on every push/PR. The same `.test` files run against the Python, Go,
Rust, and Java ports, so a green run here is real wire-compatibility evidence.

(The separate [`ci.yml`](../.github/workflows/ci.yml) covers build/test/format.)

## How it works (no C++ build)

Rather than building the vgi DuckDB extension from source, CI drives a **prebuilt** standalone
`haybarn-unittest` (the DuckDB/Haybarn sqllogictest runner, published in Haybarn's releases) and
installs the **signed** vgi extension from the Haybarn community channel — the same approach
`vgi-go`'s CI uses (see its `ci/README.md` for the fuller, multi-lane version this was ported
from):

1. **Build the workers** — `dotnet build -c Release` produces `vgi-example-worker`,
   `vgi-simple-writable-worker`, and `vgi-bad-protocol-worker`.
2. **Checkout the test suite** — `Query-farm/vgi` at a pinned commit; its
   `test/sql/integration/*.test` files are the suite.
3. **Download the runner** — `haybarn_unittest-linux-amd64.zip` from the latest Haybarn release.
4. **Preprocess** — the standalone runner links none of the extensions the tests gate on, so
   [`preprocess-require.awk`](preprocess-require.awk) rewrites each `require <ext>` into an
   explicit signed `INSTALL <ext> FROM {community,core}; LOAD <ext>;`. `require-env` and
   everything else pass through.
5. **Run** — [`run-integration.sh`](run-integration.sh) stages the preprocessed tree, wires
   `VGI_TEST_WORKER`/`VGI_SIMPLE_WRITABLE_WORKER`/`VGI_BAD_PROTOCOL_WORKER` at the three built
   binaries, `FORCE INSTALL`s the vgi extension (so the run uses what users can install today),
   then runs the suite in a single `haybarn-unittest` invocation.

## Scope of this version

This is deliberately a **single lane** (the default subprocess transport, matching
`scripts/run_tests.sh`'s `SUBPROCESS=1` mode) with no coverage collection, no skip-reason
allowlist, and no executed-case floor — unlike `vgi-go`'s CI, which covers stdio/launch/shm/http
lanes and guards against a whole-suite silent skip (a failed `require`/`require-env` is a *skip*,
not a failure, so "all tests passed" alone isn't proof anything ran). That hardening is a natural
follow-up if this lane ever needs it.

**This lane has real value beyond the local suite**: this environment doesn't have the DuckDB
`spatial` extension built, so `require spatial`-gated files (e.g. `table/expression_filter.test`)
have zero local coverage — they always skip. The haybarn runner *does* have `spatial` built, and
running for real here caught a genuine crash the local suite structurally could not: the initial
`spatial_filter_example` fixture used a native GeoArrow `geoarrow.point` struct encoding that
crashed DuckDB itself (`INTERNAL Error: dereference unique_ptr that is NULL`) on the simplest
possible query — a real worker bug, fixed by switching to the `geoarrow.wkb` binary encoding
`~/Development/vgi-python`'s reference fixture already uses successfully (see the fixture's own
doc comment and the fixing commit for the full story). Three known gaps remain, documented in
`run-integration.sh`'s exclusion comments and re-run there to confirm before excluding: one
already-documented worker limitation (no expression-filter pushdown, so one residual-`FILTER`
EXPLAIN assertion in `expression_filter.test` fails even though results are correct), and two
`duckdb_logs()`/RPC-count assertions (`cache/secret_ineligible.test`, `macro/macros.test`) that
read as community-extension-build-vs-`main`-branch-test-file skew — both pass 333/333 against a
locally-built unittest, so they're not worker bugs.

The local, fully-verified conformance gate remains `scripts/run_tests.sh` against a
locally-built `~/Development/vgi` checkout — see the root `README.md` and `docs/roadmap.md`. This
CI job is a lighter-weight, no-C++-build check for every push/PR, and it now genuinely passes; it
supplements (catches spatial-path gaps the local suite structurally can't) rather than replaces
that local verification when actually changing worker behavior.

## Run it locally

```bash
dotnet build -c Release
VGI_SRC=~/Development/vgi \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
  ci/run-integration.sh
```

Download `haybarn-unittest` for your platform from the latest Haybarn release:
`gh release download --repo Query-farm-haybarn/haybarn --pattern 'haybarn_unittest-*.zip'`.
