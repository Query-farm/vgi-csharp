<!-- markdownlint-disable MD041 -->
# vgi-csharp

[![CI](https://github.com/Query-farm/vgi-csharp/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-csharp/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/QueryFarm.Vgi.svg)](https://www.nuget.org/packages/QueryFarm.Vgi)

C# SDK for **VGI** ("Vector Gateway Interface"), Query.Farm's application-level protocol for
DuckDB worker processes. VGI lets DuckDB `ATTACH` a worker — a plain executable, in any language —
that serves catalogs, schemas, and scalar/table/table-in-out/table-buffering/aggregate functions
over an Arrow-IPC-streaming wire protocol, with no IDL/codegen step. This is the fifth port,
alongside the canonical Python implementation and the Go/Rust/Java/TypeScript ports.

- Sibling reference port (Python): [`vgi-python`](https://github.com/Query-farm/vgi-python)
- DuckDB extension: [`vgi`](https://github.com/Query-farm/vgi)
- Lower-level RPC framework this is built on: [`vgi-rpc-csharp`](https://github.com/Query-farm/vgi-rpc-csharp)

> **Status: full parity.** All 333 sqllogictests in the canonical
> `~/Development/vgi/test/sql/integration/**` suite pass — the same unmodified suite the
> Python/Go/Rust/Java ports are graded against. See [`docs/roadmap.md`](docs/roadmap.md) for the
> milestone history.

## Install

```bash
dotnet add package QueryFarm.Vgi
```

Requires the .NET 10 SDK.

## Quickstart

A minimal worker — one scalar function, served over stdio:

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
Arrow-schema bookkeeping needed for the common case. Build it (`dotnet build -c Release`), then
point DuckDB at the compiled executable:

```sql
INSTALL vgi FROM community;
LOAD vgi;

-- LOCATION is the command DuckDB runs to launch the worker; the first ATTACH argument names
-- the catalog it appears under (independent of what the worker itself calls itself).
ATTACH 'example' AS example (TYPE vgi, LOCATION './my-worker');

SELECT example.upper_case('hello'); -- => 'HELLO'
```

`LOCATION` also accepts `http://…`/`https://…` for an HTTP worker, or a `launch:<argv>` prefix for
the pooled AF_UNIX launcher transport (a worker process reused across every DuckDB connection that
shares the same `(argv, cwd, VGI_RPC_*-env)` identity, rather than cold-spawned per `ATTACH`).

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
`[OutputLength]` — see the Quickstart above).

### Catalogs, schemas, and registration

A worker declares its own catalog name (`CatalogName`) and can register catalog tables/views/
macros, settings, secret types, and copy-from/to formats alongside functions:

```csharp
var worker = new Worker()
    .CatalogName("example")
    .DefaultSchema("main")
    .RegisterScalar(new UpperCaseFunction())      // ScalarFn, as above
    .RegisterTable(new MyGeneratorFunction())     // ITableFunction — see Function shapes below
    .RegisterSchema("data", comment: "Reference tables")
    .RegisterCatalogTable(myTable, identity: "data");
```

`identity` scopes a registration to a specific catalog identity when a worker serves more than
one logical catalog from the same process (see `Worker.RegisterCatalog`); most workers only need
the default.

### Transports

```csharp
await worker.RunStdioAsync();                          // default — DuckDB's plain LOCATION
await worker.RunUnixSocketAsync("/tmp/my-worker.sock"); // AF_UNIX, for the launch: pool
await worker.RunFromArgsAsync(args);                    // parses --unix/--idle-timeout/etc. from argv
```

**Critical rule**: stdout is the wire channel for stdio-transport workers. Every diagnostic/log
line must go to `Console.Error`, never plain `Console.WriteLine` — a stray stdout write corrupts
the Arrow IPC stream.

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
examples/01-minimal-scalar-worker/    the Quickstart above, as a buildable project
test/QueryFarm.Vgi.Tests/             xUnit unit tests
scripts/run_tests.sh                  fast local sqllogictest runner (see CLAUDE.md)
ci/                                   GitHub Actions integration-test harness
```

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
