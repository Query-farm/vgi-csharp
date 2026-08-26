# CLAUDE.md

Guidance for working in this repository.

## What this is

A from-scratch C# port of [VGI](https://query.farm) ("Vector Gateway Interface"), Query.Farm's
application-level protocol for DuckDB worker processes — the fifth port, alongside the canonical
Python implementation (`~/Development/vgi-python`) and the Go/Rust/Java/TypeScript ports
(`~/Development/vgi-{go,rust,java,typescript}`). It's layered on
[`vgi-rpc-csharp`](../vgi-rpc-csharp) (a sibling repo), which implements the lower-level `vgi-rpc`
transport/RPC framework everything here rides on — Arrow IPC streaming, method dispatch via
`vgi_rpc.*` custom metadata, no IDL/codegen. `vgi-rpc-csharp` is a separate, already-published
project; when working on transport/RPC-layer behavior rather than VGI's own application semantics,
that's the repo to change, not this one.

The C++ DuckDB extension that drives the acceptance suite lives at `~/Development/vgi` (read-only
reference — **never modify it or its `test/sql/integration/**` sqllogictest files**; that suite is
the shared, unmodified oracle every language port is graded against, and it's what makes a green
run here real cross-language wire-compatibility evidence, not a self-graded exercise).

**Status: full parity.** All 333 sqllogictests in `~/Development/vgi/test/sql/integration/**`
pass. See [`docs/roadmap.md`](docs/roadmap.md) for the milestone history and how the handful of
tricky failures along the way got resolved (including one genuine, real bug this port helped
surface and fix upstream, and a standing lesson about not trusting "it must be the C++ side"
conclusions without empirically checking against the canonical Python worker first).

## Build & test

```bash
dotnet restore
dotnet build vgi-csharp.slnx
dotnet test test/QueryFarm.Vgi.Tests
dotnet format vgi-csharp.slnx --verify-no-changes   # or: make format_check
```

Or via `make` — see the `Makefile` for the full target list (`build`, `test`, `smoke`, `format`,
`format_check`, `test_integration`, `test_integration_subprocess`).

SDK version is pinned in `global.json`. Package versions are centrally managed in
`Directory.Packages.props`. Shared MSBuild settings are in `Directory.Build.props` (root) plus a
nested one per `src/`, `test/`, `fixtures/`, `examples/` folder that sets `IsPackable` (and other
per-kind settings) appropriately.

**Critical rule, everywhere in this codebase**: stdout is the wire channel for stdio-transport
workers. Every diagnostic/log line — in `Worker`, in any fixture — must go to `Console.Error`,
never `Console.WriteLine`/plain stdout. A stray stdout write corrupts the Arrow IPC stream and
manifests as a bewildering downstream deserialization failure, not an obvious "why is there text
here" error.

## Solution layout

- `src/QueryFarm.Vgi/` — the published package: protocol DTOs, function-kind interfaces/base
  classes (`Scalar/`, `Table/`, `TableInOut/`, `Buffering/`, `Aggregate/`), catalog registry
  (`Catalog/`), the `IVgiService` dispatcher (`Internal/VgiServiceImpl.cs` — the largest file,
  touched by nearly every feature), and the `Worker` builder/CLI.
- `fixtures/QueryFarm.Vgi.ExampleWorker/` — the ~170-function conformance-driving fixture worker
  the sqllogictest suite runs against. One subdirectory per function kind, plus `Cache/`,
  `CopyFormats/`, `Splits/`, `Accumulate/`, `NarrowBind/`, `ProjectionRepro/` for specific test
  clusters. Not published.
- `fixtures/QueryFarm.Vgi.SimpleWritableWorker/`, `fixtures/QueryFarm.Vgi.BadProtocolWorker/` —
  secondary fixtures (writable-catalog write paths; deliberately-malformed-protocol negative
  tests). Not published.
- `examples/01-minimal-scalar-worker/` — quickstart: one scalar function, stdio transport. Not
  published.
- `test/QueryFarm.Vgi.Tests/` — xUnit unit tests (schema derivation, dispatch, codecs, storage).
- `scripts/run_tests.sh` — the fast local sqllogictest runner (see below).
- `ci/` — GitHub Actions integration-test harness (prebuilt `haybarn-unittest`, no C++ build from
  source) — see `ci/README.md`.

## Wire-protocol conventions (read before touching `Protocol/`)

- **No IDL/codegen** — RPC method dispatch/versioning rides as `vgi_rpc.*` custom metadata on
  Arrow IPC batches, not a schema-defined wire format.
- **Two-tier dataclass rule**: a method's own top-level parameter/return type embeds as IPC inside
  a `binary` field; a property nested inside *another* dataclass is a native Arrow `struct`. A
  doubly-nested case (a binary wrapping ANOTHER embedded IPC batch — e.g. `InitRequest.BindCall`
  wrapping a serialized `BindRequest`) needs manual `Internal.EmbeddedIpc.Encode<T>`/`Decode<T>`,
  since the two-tier rule only covers one level.
- **Positional vs. name-based decoding, this is the one that bites people**: REQUEST types
  (C++ → worker) decode *positionally* — C# property declaration order must exactly match the
  C++/generated-schema field order, verified against
  `~/Development/vgi/src/generated/vgi_protocol_schemas.hpp`. RESPONSE types (worker → C++) are
  validated with a *strict* `arrow::Schema::Equals` (field count/order/name/type/nullability)
  against that same generated header's schema factories — not a tolerant name-based read. Get
  either direction's field order wrong and it fails at runtime, not at compile time.
- **Packed vs. flat RPC methods**: packed = single `request: binary` embedded-IPC param; flat =
  params map 1:1 by name to method parameters.

## Testing against the canonical sqllogictest suite

The acceptance gate for this port is the same one every other port uses: DuckDB's sqllogictest
framework, run against a real worker, using the unmodified `.test` files in
`~/Development/vgi/test/sql/integration/**`.

**Locally — use `scripts/run_tests.sh`, not a naive per-file loop.** It runs the whole requested
scope as ONE `unittest` invocation over the pooled `launch:` (AF_UNIX) transport, so the worker
process starts once and is reused across every `ATTACH` in the run rather than being cold-spawned
per `.test` file — this is the difference between a ~3 minute full-suite run and 15-20+ minutes.

```bash
scripts/run_tests.sh                      # full suite, launcher transport
scripts/run_tests.sh scalar               # one category
scripts/run_tests.sh "test/sql/integration/table/sequence.test"   # one file
scripts/run_tests.sh --no-build ...       # skip the dotnet build step
SUBPROCESS=1 scripts/run_tests.sh ...     # bare-subprocess transport (slower; needed for the
                                           # handful of tests asserting on DuckDB's own
                                           # subprocess-pool/PID-reuse behavior, which the
                                           # launcher transport bypasses by design)
```

Output is cached under `/tmp/vgi-csharp-test-cache/` (`run.log`, `failures`, `summary`) —
`cat`/`grep` those to investigate a failure rather than re-running to see it again.

**Critical gotcha with the `launch:` transport**: pooled workers persist *across test runs*,
keyed by `(argv, cwd, VGI_RPC_*-env)` — an env var outside that prefix is invisible to the
launcher's reuse-identity hash, so a code change (or an env-var-dependent behavior change that
isn't itself a `VGI_RPC_*` var) can silently get served by a stale, already-running worker even
after you rebuild. Before re-testing after any change:

```bash
for pid in $(pgrep -f "vgi-example-worker|vgi-simple-writable-worker|vgi-bad-protocol-worker"); do
  kill -9 "$pid" 2>/dev/null
done
sleep 1
rm -rf /tmp/vgi-rpc-501   # adjust the UID suffix to match your user
```

**In CI**: `ci/run-integration.sh` drives a prebuilt standalone `haybarn-unittest` binary against
the signed community-published vgi extension — no C++ build from source. See `ci/README.md`,
including its "Scope of this first version" section — it's deliberately a single lane, not yet
run end-to-end against a real `haybarn-unittest` at the time it was written, unlike
`scripts/run_tests.sh`'s local suite which has been exhaustively verified.

**Never trust "it must be the C++ side" without checking.** This port hit exactly that mistake
mid-development: two failures were confidently diagnosed as C++-extension-side gaps, with detailed
cited source evidence, across multiple independent investigation passes — and the diagnosis was
wrong, because none of those passes actually ran the failing test against the canonical Python
reference worker to check. It passed there outright; the real bug was a straightforward,
fixable gap in this port's own fixture code (a missing `ProjectionPushdown` declaration). Before
concluding a gap is out of this repo's control, reproduce it against `~/Development/vgi-python`:

```bash
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
  ~/Development/vgi/build/release/test/unittest "test/sql/integration/<path>"
```

If it fails there too, it's real reference-suite drift or a genuine C++-side issue — safe to
document as out of scope. If it passes there, the bug is here.

## Sibling-repo dependency

`QueryFarm.Vgi.csproj` conditionally references `vgi-rpc-csharp`'s `QueryFarm.VgiRpc` project
directly (source, via `ProjectReference`) when a `vgi-rpc-csharp` checkout sits next to this repo
— the expected local-dev layout — falling back to the published `QueryFarm.VgiRpc` NuGet package
otherwise (see `Directory.Build.props`'s `VgiRpcCSharpRoot`/`VgiRpcCSharpUseSource`, and
`Directory.Packages.props` for the pinned fallback version). **The pinned version matters**: it
must be a `vgi-rpc-csharp` release where `QueryFarm.VgiRpc`'s own dependency chain actually
resolves correctly from nuget.org without a local sibling checkout — verify this with a clean
restore (`VgiRpcCSharpRoot=/nonexistent dotnet build`) before bumping it, not just by trusting that
the sibling repo's own local build passes (which only ever exercises the `ProjectReference` path,
never the published-package path a real external consumer of `QueryFarm.Vgi` would hit).
