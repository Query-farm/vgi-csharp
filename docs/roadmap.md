# vgi-csharp roadmap

A living log of milestone progress, in the style of `vgi-rpc-csharp`'s own `docs/roadmap.md`.
Milestone numbers below are directional (matching the original implementation plan's intent), not
literal — actual scope grew considerably as the sqllogictest suite's real requirements surfaced.

## Current state: 333/333 sqllogictests passing — full parity

Verified via a clean rebuild, full `test/QueryFarm.Vgi.Tests` unit suite (146/146),
`make format_check` clean, and a full `scripts/run_tests.sh` run against
`~/Development/vgi/test/sql/integration/**`, reproduced independently twice from a clean slate.
See the README's Status section for how the last few failures resolved.

## Milestone history

- **M0 — Scaffold.** Solution/project layout, sibling-repo reference to `vgi-rpc-csharp`
  (source when checked out locally, NuGet package otherwise), smoke-tested embedded-IPC round
  trip of a hand-written protocol type.

- **M1 — Minimal worker.** `bind` → `init` → `exchange` for one scalar function
  (`upper_case`) over real stdio RPCs, plus the minimum catalog RPC surface DuckDB's `ATTACH`
  walks before ever calling the function (`catalog_catalogs`, `catalog_attach`,
  `catalog_schemas`, `catalog_schema_contents_functions`, `catalog_detach`).

- **M2 — Full scalar surface.** `ScalarFn`'s attribute-driven parameter binding
  (`[Param]`/`[ConstParam]`/`[Setting]`/`[OutputLength]`), numeric type-promotion rules, varargs,
  nullable handling, secrets, caching opt-in. `fixtures/QueryFarm.Vgi.ExampleWorker/Scalar/`.

- **M3 — Table functions.** `ITableFunction`/producer streaming, filter/projection pushdown,
  cardinality reporting (including the inlined-cardinality fast path), partitioned/batch-indexed
  scans, dynamic filters, split scans. `Table/`, `Splits/`.

- **M4 — Table-in-out and table-buffering.** `ITableInOutFunction` (exchange-shaped: one
  request batch in, one response batch out per turn) and `ITableBufferingFunction`
  (sink-then-source: every input batch must be seen before any output, via a Combine phase that
  merges per-batch state before the Source/finalize phase runs). Table-buffering's defining
  requirement — that `Process`/`Combine`/finalize are each independently worker-pool-acquired
  unary RPCs that may each land on a *different OS process* — drove `IFunctionStorage`, the
  durable cross-process log storage abstraction both table-buffering and (later) per-transaction
  state build on. `TableInOut/`, `Buffering/`.

- **M5 — Aggregate functions.** `IAggregateFunction<TState>`, group-keyed state, multi-worker
  `Combine` merge semantics. `Aggregate/`.

- **M6 — Full catalog surface.** DDL, constraints (NOT NULL/PK/UNIQUE/CHECK/FK), column
  statistics (including GEOMETRY/WKB columns), copy-from/to, view/macro catalog entries,
  multi-branch scans, time travel, per-transaction cross-process storage (`SupportsTransactions`
  scoped per catalog identity, `TransactionOpaqueData` threaded through bind/init, reusing
  `IFunctionStorage` keyed by transaction id instead of execution id).

- **M7 — Non-stdio transports + launcher pooling.** `Worker.RunUnixSocketAsync`, idle-timeout
  watchdog, `RunFromArgsAsync`'s `--unix`/`--idle-timeout` CLI surface, and the AF_UNIX
  `launch:<argv>` pooling contract (a worker is reused across DuckDB processes/connections
  sharing the same `(argv, cwd, VGI_RPC_*-env)` identity) — this is what makes
  `scripts/run_tests.sh` fast (worker started once, reused across the whole suite, instead of
  cold-spawned per `.test` file).

- **Fixture-parity milestones (the long tail).** Once the framework itself was feature-complete,
  the remaining gap to "all sqllogictests pass" was almost entirely about the *fixture worker*
  faithfully reproducing the exact functions, names, comments, and edge-case behaviors the
  canonical Python reference worker's fixtures define — not new framework capability. Notable
  fixes in this phase:
  - Two deep vendored-Arrow bugs in the `vgi-rpc-csharp` dependency (a zero-field struct crash in
    `MessageSerializer`, and a dictionary-in-struct ID mismatch in `ArrowStreamWriter`'s
    `DictionaryCollector`), both root-caused with reproducible regression tests.
  - A framework bug in `InitTableBuffering`'s FINALIZE branch: it declared the *full* output
    schema instead of the projection-narrowed one (unlike `InitTableInOut`, which already did
    this correctly) — never exercised until a table-buffering fixture first declared
    `ProjectionPushdown`.
  - The same projection-pushdown data-narrowing gap on the streaming table-in-out path
    (`EchoFunction`): declaring `ProjectionPushdown=true` alone isn't enough — `Process` must
    itself narrow the batch it emits to match the wire-declared narrowed schema, not just rely on
    the framework narrowing the schema declaration.
  - A function-instance-sharing refactor: several catalog tables (`numbers`, `volatile_numbers`,
    `ten_thousand_table`, `cardinality_inlined_table`, and the `cacheable_numbers`/
    `cache_revalidatable`/`cache_filtered` family) needed to scan via a *shared* function instance
    with an explicit `Columns` override, rather than each having its own dedicated backing
    function — proving (and matching) that the C++ extension resolves a table's columns
    positionally against the declared `CatalogTable.Columns`, not by name-matching against
    whatever the scan function's own schema says.

## Known gaps

None — 333/333. Three failures were encountered and resolved along the way, worth recording since
two carried a real lesson:

- `splits/dynamic_filters.test` and `table/value_prune.test` were initially misdiagnosed as
  C++-extension gaps on the strength of code reading alone — detailed, cited, plausible-sounding,
  and wrong, because that investigation never ran the failing test against the actual reference
  worker to check. The moment it was (prompted to), both passed outright (18/18, 26/26), and the
  real cause turned out to be a straightforward worker-side bug: both fixtures had two output
  columns but no `ProjectionPushdown` declaration, so DuckDB inserted an extra `PROJECTION`
  operator above the scan whenever a query needed fewer than all columns, which silently defeated
  DuckDB's join-filter-pushdown optimizer entirely. Declaring the flag and properly narrowing
  emitted columns to `TableInitParams.ProjectionIds` fixed both.
- `scalar/function_registration.test` was confirmed via direct reproduction against the canonical
  Python worker to be genuine drift in the reference suite's roster-count assertion (both workers
  reported 52, the test wanted an exact 55). It resolved on its own when
  `~/Development/vgi` picked up an upstream commit relaxing the assertion to `BETWEEN 52 AND 55`.
  A follow-up name-by-name diff confirmed the C# and Python workers register the identical 41
  scalar function names / 52 overloads (a handful of cosmetic parameter-name differences aside) —
  nothing was ever actually missing.

**Standing lesson for this project**: never conclude "C++-extension-side gap" from code reading
alone, however well-cited. Always reproduce the failure against `~/Development/vgi-python`'s
fixture worker first — `VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-fixture-worker"`
— before trusting a code-reading-only theory.

## Possible future work (not required for sqllogictest parity)

- `M8+` from the original plan: HTTP transport, access-log sink, bearer/JWT auth, S3/GCS
  externalization — mostly wiring onto `QueryFarm.VgiRpc.Http`/`.S3`/`.Gcs` capabilities that
  already exist in `vgi-rpc-csharp`, not new protocol design.
- `ArrayDataConcatenator` has no `DictionaryType` visitor in the vendored Arrow fork (silently
  drops dictionary values on concat) — currently only worked around at one call site
  (`ConstantColumnsFunction`), not root-cause-fixed, because no currently-scoped test exercises
  the gap directly.
- The naming-convention/`late_mat`-variant-reuse items noted in `Program.cs`'s
  `function_registration.test` roadmap comment were deliberately left as-is: not needed by any
  currently-scoped test, and carry real regression risk against currently-passing tests for
  cosmetic parity with the Python reference's internal naming.
