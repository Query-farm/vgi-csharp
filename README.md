<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-csharp/main/docs/vgi-logo.png?v=2" alt="Vector Gateway Interface logo" width="320">
</p>

<h1 align="center">VGI for .NET</h1>

<p align="center">
  Add your own functions and tables to DuckDB with C# and Apache Arrow.<br>
  Built by <a href="https://query.farm">🚜 Query.Farm</a>
</p>

<p align="center">
  <a href="https://github.com/Query-farm/vgi-csharp/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/vgi-csharp/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://www.nuget.org/packages/QueryFarm.Vgi"><img src="https://img.shields.io/nuget/v/QueryFarm.Vgi" alt="NuGet"></a>
  <a href="https://www.nuget.org/packages/QueryFarm.Vgi"><img src="https://img.shields.io/nuget/dt/QueryFarm.Vgi" alt="NuGet downloads"></a>
</p>

A **VGI worker** is a small .NET program that DuckDB talks to over Apache Arrow IPC. It can expose
scalar / table / table-in-out / table-buffering / aggregate functions and whole catalogs (schemas,
tables, views, macros) that behave like native DuckDB objects. DuckDB launches your worker for you
when a query needs it — you never run a server by hand.

This repo is the **C#** worker SDK ([`QueryFarm.Vgi`](https://www.nuget.org/packages/QueryFarm.Vgi)).
It is wire-compatible with the canonical [Python](https://github.com/Query-farm/vgi-python) SDK and
the Go/Rust/Java/TypeScript ports, so a C# worker drops in behind the same `ATTACH ... (TYPE vgi)`.
Built on [`vgi-rpc-csharp`](https://github.com/Query-farm/vgi-rpc-csharp); targets **.NET 10**.

> **Status: full parity.** All 333 sqllogictests in the canonical
> `~/Development/vgi/test/sql/integration/**` suite pass — the same unmodified suite the
> Python/Go/Rust/Java ports are graded against. See [`docs/roadmap.md`](docs/roadmap.md) for the
> milestone history.

## Why a worker instead of a C++ extension?

| Traditional DuckDB extension | VGI worker |
|------------------------------|------------|
| Written in C/C++, compiled and linked against DuckDB | Written in C#, one standalone worker process |
| Must be rebuilt for each DuckDB version | Version independent |
| Complex build / signing / release cycle | `dotnet build`, ship the executable |
| Runs in-process | Process isolation |

**Reach for it when you want to:** call REST APIs or external services from SQL, run ML inference
(ML.NET, ONNX Runtime, etc.), expose an external database/API/filesystem as a queryable catalog, or
ship domain-specific functions to your team as one binary.

## Your first worker

**1. Add the package:**

```bash
dotnet add package QueryFarm.Vgi
```

**2. Write a function and serve it:**

```csharp
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

var worker = new Worker()
    .CatalogName("example")
    .DefaultSchema("main")
    .RegisterScalar(new UpperCaseFunction());

await worker.RunFromArgsAsync(args);

public sealed class UpperCaseFunction : ScalarFn
{
    public override string Name => "upper_case";

    private void Compute([Param] StringArray value, StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i)) { result.AppendNull(); continue; }
            result.Append(value.GetString(i).ToUpperInvariant());
        }
    }
}
```

`ScalarFn` reflects `Compute`'s parameters once per subclass and dispatches per batch — no manual
Arrow-schema bookkeeping needed for the common case.

**3. Build it (`dotnet build -c Release`), then call it from a DuckDB engine that has the `vgi`
extension.** The `vgi` extension currently ships with Query Farm's
[Haybarn](https://github.com/Query-farm-haybarn/haybarn) DuckDB distribution, which starts with no
install via `uvx haybarn-cli`. Stock `duckdb` works too via `INSTALL vgi FROM community`.

```sql
INSTALL vgi FROM community;
LOAD vgi;

-- LOCATION is the command DuckDB runs to launch the worker; the first ATTACH argument names
-- the catalog it appears under (independent of what the worker itself calls itself).
ATTACH 'example' AS example (TYPE vgi, LOCATION './my-worker');

SELECT example.upper_case('hello'); -- => 'HELLO'
```

### Troubleshooting

- **`ATTACH` can't find the worker** — `LOCATION` is resolved relative to DuckDB's working
  directory, not your project. Use an absolute path if in doubt.
- **`Catalog Error: ... does not exist`** — qualify with the attach alias
  (`example.upper_case`) or run `USE example;`.
- **Runtime / type errors** — exceptions thrown from `Bind`/`Compute` (and bind-time type-bound
  checks) surface directly in DuckDB's error message.

## Function shapes

| Shape | Interface | Base class | Use case |
|---|---|---|---|
| Scalar | `IScalarFunction` | `ScalarFn` | 1:1 row mapping |
| Table (producer) | `ITableFunction` | — | row generator, no streamed input |
| Table-in-out | `ITableInOutFunction` | — | stream input rows → output rows, one turn at a time |
| Table-buffering | `ITableBufferingFunction` | — | sort/aggregate/join-style: see every input row before producing any output |
| Aggregate | `IAggregateFunction<TState>` | — | cumulative state + final emit |

Each raw interface is a small, direct implementation surface (see any fixture under
`fixtures/QueryFarm.Vgi.ExampleWorker/` for real examples); `ScalarFn` is the one convenience base
class with attribute-driven parameter binding (`[Param]`, `[ConstParam]`, `[Setting]`,
`[OutputLength]` — see "Your first worker" above). Projection/filter pushdown (including genuine
expression/spatial-predicate pushdown, evaluated via an embedded DuckDB engine — see
`Internal/ExpressionFilterEvaluator.cs`), ORDER BY/TABLESAMPLE hints, settings, secrets, splits, and
cross-process state storage are all handled by the framework, not something each function
reimplements.

## Beyond functions: full catalogs

A worker can expose more than bare functions — a complete catalog of schemas, function-backed
**tables**, **views**, and **macros** that behave like native DuckDB objects:

```csharp
var worker = new Worker()
    .CatalogName("example")
    .DefaultSchema("main")
    .RegisterScalar(new UpperCaseFunction())      // ScalarFn, as above
    .RegisterTable(new MyGeneratorFunction())     // ITableFunction — see Function shapes above
    .RegisterSchema("data", comment: "Reference tables")
    .RegisterCatalogTable(myTable, identity: "data");
```

```sql
ATTACH 'external_db' (TYPE vgi, LOCATION './my-catalog-worker');

SELECT * FROM external_db.data.users;            -- a catalog table
SELECT * FROM external_db.main.upper_case(name)  -- a function
FROM (VALUES ('alice')) t(name);
```

`identity` scopes a registration to a specific catalog identity when a worker serves more than one
logical catalog from the same process (see `Worker.RegisterCatalog`); most workers only need the
default.

## Transports

```csharp
await worker.RunStdioAsync();                          // default — DuckDB's plain LOCATION
await worker.RunUnixSocketAsync("/tmp/my-worker.sock"); // AF_UNIX, for the launch: pool
await worker.RunFromArgsAsync(args);                    // parses --unix/--idle-timeout/etc. from argv
```

`LOCATION` also accepts `http://…`/`https://…` for an HTTP worker, or a `launch:<argv>` prefix for
the pooled AF_UNIX launcher transport (a worker process reused across every DuckDB connection that
shares the same `(argv, cwd, VGI_RPC_*-env)` identity, rather than cold-spawned per `ATTACH`).

**Critical rule**: stdout is the wire channel for stdio-transport workers. Every diagnostic/log
line must go to `Console.Error`, never plain `Console.WriteLine` — a stray stdout write corrupts
the Arrow IPC stream.

## Protocol overview

VGI uses `vgi_rpc`, an Apache Arrow IPC-based RPC framework, for all client-worker communication —
you don't write to this directly (`Worker`/`ScalarFn`/the function-kind interfaces handle it), but
here's what happens per query:

```
DuckDB (client)                      VGI worker
  │──── bind(request) ─────────────▶ │  function name, args, input schema
  │◀─── BindResponse ───────────────  │  output schema (your Bind/ResolveOutputSchema)
  │──── init(request) ─────────────▶ │  start the processing stream
  │◀─── stream header ──────────────  │  execution_id, max_workers
  │──── exchange/tick(batch) ──────▶ │
  │◀─── output batch ───────────────  │  your Compute/Produce
  │──── [stream close] ────────────▶ │
```

See [`docs/roadmap.md`](docs/roadmap.md) and inline doc comments in `Internal/VgiServiceImpl.cs`
for the full RPC surface (catalog DDL, transactions, splits, secrets, etc.) beyond this per-query
happy path.

## Repo layout

```
src/QueryFarm.Vgi/                    the published package
  Attributes/                         [Param]/[ConstParam]/[Setting]/[OutputLength]
  Scalar/ Table/ TableInOut/          per-function-kind interfaces + ScalarFn
  Buffering/ Aggregate/
  Catalog/                            CatalogTable/CatalogView/CatalogMacro
  Protocol/                           wire DTOs, one per RPC request/response type
  Internal/                           VgiServiceImpl (the IVgiService dispatcher), pushdown
                                       filter codec/evaluator, argument codecs, storage
fixtures/QueryFarm.Vgi.ExampleWorker/ the ~170-function conformance-driving fixture worker
fixtures/QueryFarm.Vgi.SimpleWritableWorker/  writable-catalog write-path fixture
fixtures/QueryFarm.Vgi.BadProtocolWorker/     malformed-protocol negative-test fixture
examples/01-minimal-scalar-worker/    "Your first worker" above, as a buildable project
test/QueryFarm.Vgi.Tests/             xUnit unit tests
scripts/run_tests.sh                  fast local sqllogictest runner (see CLAUDE.md)
ci/                                   GitHub Actions integration-test harness
```

Read `fixtures/QueryFarm.Vgi.ExampleWorker/` for a working example of every function kind and
catalog feature — it's the fixture the full sqllogictest suite is graded against.

## Testing your own worker

The fastest check is to call your function from a DuckDB session (see "Your first worker" above).
For automated tests, drive the worker directly with `QueryFarm.VgiRpc`'s client, or shell out to a
DuckDB session from your test harness. `test/QueryFarm.Vgi.Tests/` shows the former pattern for
this SDK's own unit tests (schema derivation, dispatch, codecs, storage).

## Build & test

```bash
make build                # dotnet build vgi-csharp.slnx
make test                 # unit tests (test/QueryFarm.Vgi.Tests)
make format_check         # dotnet format --verify-no-changes
make test_integration      # full sqllogictest suite against ~/Development/vgi (launcher transport)
```

See [`CLAUDE.md`](CLAUDE.md) for the full local-development workflow, including the fast
sqllogictest iteration loop and the wire-protocol conventions worth knowing before touching
`Protocol/`.

## Architecture notes

- **No IDL/codegen** — RPC method dispatch and versioning ride as `vgi_rpc.*` custom metadata on
  Arrow IPC batches, not a schema-defined wire format.
- **Two-tier dataclass rule**: a method's own top-level parameter/return type embeds as IPC inside
  a `binary` field; a property nested inside another dataclass is a native Arrow `struct`.
- **Positional vs. name-based decoding**: request types (C++ → worker) decode *positionally* —
  property declaration order must exactly match the C++ generated schema's field order. Response
  types (worker → C++) are validated with a strict `arrow::Schema::Equals` against the C++
  extension's generated schema factories.
- **Cross-process storage**: table-buffering and per-transaction state must survive landing on a
  *different* worker process than the call that wrote it (the worker-pool/launcher owns process
  lifetime, not the caller) — see `IFunctionStorage`'s doc comment for the durable,
  execution-id/transaction-id-scoped storage contract this requires.

See inline doc comments throughout `src/QueryFarm.Vgi/` and `fixtures/QueryFarm.Vgi.ExampleWorker/`
for the deeper "why" behind specific design choices — most non-obvious decisions are documented at
the point of use, cross-referencing the specific sqllogictest file(s) they exist to satisfy.

## License

Copyright 2025, 2026 Query Farm LLC.

Licensed under the **Query Farm Source-Available License, Version 1.0** — see
[`LICENSE`](./LICENSE) for the full terms. In brief, you may use, modify, and redistribute the
software freely for non-production use, and for production use except where it would constitute a
Competing Offering or a Commercial Marketplace as defined in the license. Each version converts to
the Apache License, Version 2.0 on the tenth anniversary of its public release.

For uses not permitted under this license, contact
[hello@query.farm](mailto:hello@query.farm) for a commercial license.
