# CI: the vgi integration suite

The `integration` job in [`ci.yml`](../.github/workflows/ci.yml) runs the canonical
[Query-farm/vgi](https://github.com/Query-farm/vgi) integration sqllogictest suite against this
repo's C# example worker on every push/PR, once per transport. The same `.test` files run against
the Python, Go, Rust, and Java ports, so a green run here is real wire-compatibility evidence.

(The same workflow's `build-test` and `lint` jobs cover build/test/format.)

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
5. **Run** — [`run-integration.sh`](run-integration.sh) stages the preprocessed tree and runs the
   suite in a single `haybarn-unittest` invocation, after `FORCE INSTALL`ing the vgi extension (so
   the run uses what users can install today). The small stateful and incompatible-protocol
   fixture workers remain isolated subprocesses on both lanes.

## Two transport lanes

`TRANSPORT=launch` (the default) puts the main worker behind DuckDB's `launch:` AF_UNIX pool, so
repeated ATTACHes reuse one warm .NET process. `TRANSPORT=http` boots the same worker as
`--http` on an ephemeral port and ATTACHes `http://localhost:<port>`. CI runs both as a matrix.

**Both are required, because they are two separate dispatch implementations.** A worker's pipe
path and its HTTP path share the function code but not the server loop, and they have diverged in
production: with only the launcher lane, this repo published a release whose CI said 333/334 while
**81 of 334 files were red over HTTP** — a scalar stream declaring no input schema (so HTTP
classified it as a producer and drove it with a zero-column tick), plus three metadata bugs in
`vgi-rpc-csharp`'s HTTP dispatch. Nothing about those was visible from the launcher lane, by
construction: the pipe transport never synthesizes a turn and never re-encodes batch metadata.

Two things differ by lane, both for reasons intrinsic to the test rather than to the worker:

- `database_worker/package.test` is excluded on **http**. It packages `$VGI_TEST_WORKER` as an
  executable artifact into a DuckDB table and runs it; a URL is not an executable. It runs, and
  must pass, on the launch lane.
- `VGI_REQUIRE_LAUNCHER_TRANSPORT` is set only on **launch**.

`VGI_HTTP_TRANSPORT` is **not** set on either lane yet, which leaves five HTTP-only files
skipped. Four of them — `http/capability_probe`, `http/producer_turns`,
`http/small_body_encoding`, `cache/partition_scope_identity` — were verified to pass on this
lane as-is. The fifth, `cache/identity_isolation.test`, needs the example worker to answer as a
*named* principal: the reference fixture server maps `vgi-test-alice`→alice and
`vgi-test-bob`→bob as optional bearer auth (see vgi-python's `_test_fixtures/http_server.py`).
`Worker.RunHttpAsync` here exposes no `authenticate` hook at all — a worker written against this
port cannot authenticate an HTTP caller — so there is nowhere to wire that map. Adding the hook
is a product API change and belongs in its own commit; when it lands, set `VGI_HTTP_TRANSPORT=1`
on the http lane and all five run.

## Guards against a lane that is green without running

A failed `require`/`require-env` is a *skip*, not a failure, so "all tests passed" alone is not
proof anything ran. `run-integration.sh` therefore fails the lane on:

- **no test cases matched** — an empty stage still exits 0;
- **fewer than `MIN_ASSERTIONS` (9000) assertions executed** — the blunt floor against a
  largely-skipped run;
- **the http worker dying mid-run** — every result after that point is untrustworthy;
- **any assertion skipped by DuckDB's `ignore_error_messages` default** (`MAX_HTTP_SWALLOWED`,
  default 0).

That last one deserves its own note. DuckDB's sqllogictest runner defaults
`ignore_error_messages` to `{"HTTP", "Unable to connect"}`: a statement whose error text contains
`HTTP` is **skipped, not failed**. On a lane that reaches the worker over HTTP, that is a live
hazard rather than a convenience — a worker answering 500 produces an error message containing
`HTTP`, so a whole class of server-side crashes reads as "skipped" and the lane exits 0. This was
not theoretical: during the work that added this lane, an intermediate state of the worker turned
**118 assertions into silent skips** while the summary line still read `0 failed`, and two files
(`table_in_out/echo/all_types`, `echo/union_tags`) had been red on this lane for as long as it
existed while reading as "2 skipped". The suite's own files narrow the default where it matters to
them (`bearer_auth/bearer_token.test` sets `ignore_error_messages Unable to connect`;
`http/no_compression.test` clears it), which is upstream agreeing that the HTTP entry is wrong for
VGI tests — but it cannot be cleared from outside a `.test` file, so the count guard stands in for
it here.

The threshold is **zero**, not a tolerance. That was established rather than assumed: the whole
suite was re-run once with `set ignore_error_messages Unable to connect` injected into every
staged file, turning each swallowed skip into a visible failure. With the worker fixed, nothing in
the suite legitimately errors with an HTTP-containing message on this lane.

**This lane has real value beyond the local suite**: this environment doesn't have the DuckDB
`spatial` extension built, so `require spatial`-gated files (e.g. `table/expression_filter.test`)
have zero local coverage — they always skip. The haybarn runner *does* have `spatial` built, and
running for real here caught a genuine crash the local suite structurally could not: the initial
`spatial_filter_example` fixture used a native GeoArrow `geoarrow.point` struct encoding that
crashed DuckDB itself (`INTERNAL Error: dereference unique_ptr that is NULL`) on the simplest
possible query — a real worker bug, fixed by switching to the `geoarrow.wkb` binary encoding
`~/Development/vgi-python`'s reference fixture already uses successfully (see the fixture's own
doc comment and the fixing commit for the full story). It also stayed this lane's only real
coverage for genuine expression-filter (function-call/spatial) pushdown until that was implemented
(see `Internal/ExpressionFilterEvaluator.cs`'s doc comment) — `table/expression_filter.test` now
passes 32/32 assertions here, verified against a real `haybarn-unittest` + `spatial` extension
before removing its exclusion from `run-integration.sh`. Two known gaps remain, documented in
`run-integration.sh`'s exclusion comments and re-run there to confirm before excluding:
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
  ci/run-integration.sh                       # launch lane (default)

VGI_SRC=~/Development/vgi \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
TRANSPORT=http \
  ci/run-integration.sh                       # http lane
```

Download `haybarn-unittest` for your platform from the latest Haybarn release:
`gh release download --repo Query-farm-haybarn/haybarn --pattern 'haybarn_unittest-*.zip'`.
