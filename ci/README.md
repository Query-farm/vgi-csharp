# CI: the vgi integration suite

CI uses the VGI source revision pinned by `VGI_REF` for both the test suite and a native runner linked with that VGI extension. `scripts/ci_fetch_native_runner.py` verifies the runner provenance and sets `RUNNER_LINKS_EXTENSIONS=1`; this lets wire-contract changes run before a separate public extension release. The local driver still defaults to the signed community extension when that flag is omitted. Existing test exclusions and coverage floors apply in both modes.

The `integration` job in [`ci.yml`](../.github/workflows/ci.yml) runs the canonical
[Query-farm/vgi](https://github.com/Query-farm/vgi) integration sqllogictest suite against this
repo's C# example worker on every push/PR, once per transport. The same `.test` files run against
the Python, Go, Rust, and Java ports, so a green run here is real wire-compatibility evidence.

(The same workflow's `build-test` and `lint` jobs cover build/test/format.)

## How it works (no C++ build)

CI downloads the native integration artifact built by the pinned VGI revision.
The SDK repository builds only the C# workers:

1. **Build the workers** — `dotnet build -c Release` produces the example,
   simple-writable and bad-protocol workers.
2. **Checkout the test suite** — `Query-farm/vgi` at `VGI_REF` supplies the
   canonical test files and artifact-fetch helper.
3. **Download and verify the runner** — the helper selects that exact commit's
   artifact and validates its manifest, provenance and checksums.
4. **Stage and run** — `RUNNER_LINKS_EXTENSIONS=1` preserves extension gates and
   runs the staged suite against the linked native build. Local runs with an
   independent standalone runner still preprocess signed community installs.
   Coverage floors and error-skip guards apply to both paths.

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

- `VGI_DATABASE_PACKAGE_WORKER` is set on **http** to the example worker's command.
  `database_worker/package.test` packages a worker that the `database://` resolver execs
  locally, whatever transport the lane tests; the suite's fixture wrapper runs
  `${VGI_DATABASE_PACKAGE_WORKER:-${VGI_TEST_WORKER#launch:}}`, and a URL is not a command.
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
VGI tests. It can now be cleared from outside a `.test` file too: the extension ships
`test/configs/no_error_skip.json` (vgi main >= `f5aa489`), and `run-integration.sh` passes it as
`--test-config` on both lanes, taken from the same `VGI_SRC` checkout whose suite it runs. (An empty
`skip_error_messages` list crashes the runner, so the file lists one string no error contains.)
`scripts/run_tests.sh` and the Makefile's `test_integration*` targets pass the same file. The count
guard stays as a tripwire for a config that stops applying.

The threshold is **zero**, not a tolerance. That was established rather than assumed: the whole
suite was re-run once with `set ignore_error_messages Unable to connect` injected into every
staged file, turning each swallowed skip into a visible failure. With the worker fixed, nothing in
the suite legitimately errors with an HTTP-containing message on this lane.

The matching native artifact does not link `spatial`, so that optional path is
skipped in this mode. Independent standalone-runner checks with `spatial`
installed remain useful for spatial expression coverage. Existing exclusions
in `run-integration.sh` remain unchanged; this runner switch does not waive
worker failures or lower assertion floors.

## Run it locally

```bash
dotnet build -c Release
VGI_SRC="$PWD/../vgi" \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
  ci/run-integration.sh                       # launch lane (default)

VGI_SRC="$PWD/../vgi" \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
TRANSPORT=http \
  ci/run-integration.sh                       # http lane
```

Download `haybarn-unittest` for your platform from the latest Haybarn release:
`gh release download --repo Query-farm-haybarn/haybarn --pattern 'haybarn_unittest-*.zip'`.
