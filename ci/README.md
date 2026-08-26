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

## Scope of this first version

This is deliberately a **single lane** (the default subprocess transport, matching
`scripts/run_tests.sh`'s `SUBPROCESS=1` mode) with no coverage collection, no skip-reason
allowlist, and no executed-case floor — unlike `vgi-go`'s CI, which covers stdio/launch/shm/http
lanes and guards against a whole-suite silent skip (a failed `require`/`require-env` is a *skip*,
not a failure, so "all tests passed" alone isn't proof anything ran). That hardening is a natural
follow-up once this lane has run green for real on GitHub's infrastructure — it was written and
locally sanity-checked (the awk rewrite, the staging logic) but **not run end-to-end against a
real `haybarn-unittest` binary**, since none was available in the environment this was authored
in. Treat the first few CI runs of this job as the actual verification pass; expect to iterate on
worker-arg wiring or excluded files (see `run-integration.sh`'s comments on
`nested_type_combinations.test`/`writable/`, both carried over from vgi-go's own findings without
independent confirmation here).

The local, fully-verified conformance gate remains `scripts/run_tests.sh` against a
locally-built `~/Development/vgi` checkout — see the root `README.md` and `docs/roadmap.md`. This
CI job is a lighter-weight, no-C++-build check for every push/PR; it is not a replacement for that
local verification when actually changing worker behavior.

## Run it locally

```bash
dotnet build -c Release
VGI_SRC=~/Development/vgi \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
  ci/run-integration.sh
```

Download `haybarn-unittest` for your platform from the latest Haybarn release:
`gh release download --repo Query-farm-haybarn/haybarn --pattern 'haybarn_unittest-*.zip'`.
