using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary>
/// The <c>data</c>-schema, bare-table (no-parens-callable) result-cache fixtures — backs the bulk of
/// <c>test/sql/integration/cache/*.test</c> (M8). Each is a real <see cref="CatalogTable"/> (M6's
/// function-backed-table pattern, same as <see cref="ExampleWorker.Table.DataSchemaTables"/>) whose
/// backing <see cref="ITableFunction"/> attaches <c>vgi.cache.*</c> custom_metadata to its emitted
/// batch(es) via <c>output.Emit(batch, metadata)</c> — there is no static
/// <c>CacheControlMetadata</c>-style convenience property for table functions the way
/// <see cref="Scalar.IScalarFunction"/> has one, so every fixture below builds the dict itself
/// (see <see cref="CacheMetadata"/>).
/// </summary>
public static class CacheDataTables
{
    private const string SchemaName = "data";

    public static CatalogTable CacheNoStore { get; } = Build(
        "cache_no_store", "Advertises vgi.cache.no_store — must never be cached", new CacheNoStoreFunction());

    public static CatalogTable CacheBig { get; } = Build(
        "cache_big", "Large multi-batch cacheable result (advertises vgi.cache.ttl)", new CacheBigFunction());

    public static CatalogTable CacheNonce { get; } = Build(
        "cache_nonce",
        "One-row cacheable result whose value changes per real invocation",
        new MonotonicNonceFunction(SchemaName, "cache_nonce", CacheMetadata.Ttl(300)));

    public static CatalogTable CacheMulticol { get; } = Build(
        "cache_multicol", "Multi-column cacheable result (projection-coverage reuse)", new CacheMulticolFunction());

    public static CatalogTable CacheProjection { get; } = Build(
        "cache_projection", "Projection-pushdown cacheable result (SELECT a vs b are distinct keys)", new CacheProjectionFunction());

    public static CatalogTable CacheScopedTxn { get; } = Build(
        "cache_scoped_txn",
        "Advertises vgi.cache.scope=transaction",
        new MonotonicNonceFunction(SchemaName, "cache_scoped_txn", CacheMetadata.TransactionScoped(3600)));

    public static CatalogTable CacheOrdered { get; } = Build(
        "cache_ordered",
        "Multi-worker order-sensitive cacheable result (batch_index; parallel capture, ordered serve)",
        new CacheOrderedFunction());

    public static CatalogTable CachePoison { get; } = Build(
        "cache_poison", "Cacheable first batch then a mid-stream error (never-partial check)", new CachePoisonFunction());

    public static CatalogTable CacheWhoami { get; } = Build(
        "cache_whoami", "Cacheable result echoing the caller's auth principal (identity-scoped)", new CacheWhoamiFunction());

    public static CatalogTable CacheExternalFail { get; } = Build(
        "cache_external_fail",
        "Cacheable first batch then an unresolvable external-location pointer",
        new CacheExternalFailFunction());

    /// <summary><paramref name="cacheableNumbersFunction"/>/<paramref name="cacheRevalidatableFunction"/>/
    /// <paramref name="cacheFilteredFunction"/> are the SAME instances registered as ordinary
    /// <c>main</c>-schema callable functions in <c>Program.cs</c> (threaded in here rather than each
    /// getting its own dedicated instance) — mirrors vgi-python's <c>worker.py</c>, which references
    /// the identical <c>CacheableNumbersFunction</c>/<c>CacheRevalidatableFunction</c>/
    /// <c>CacheFilteredFunction</c> class in both its <c>main</c> schema's <c>functions=[...]</c>
    /// list and its <c>data</c> schema's <c>Table(function=...)</c> entry, registering each backing
    /// function only ONCE (see <c>CatalogRegistry.RegisterCatalogTable</c>'s dedup-by-reference doc
    /// comment and <c>DataSchemaTables.BuildNumbers</c>'s doc comment for the general pattern) —
    /// part of <c>table/function_registration.test</c>'s 166→162 roadmap, item (d).</summary>
    public static IReadOnlyList<CatalogTable> All(
        ITableFunction cacheableNumbersFunction, ITableFunction cacheRevalidatableFunction, ITableFunction cacheFilteredFunction) =>
    [
        Build("cacheable_numbers", "Cacheable 10-row result advertising vgi.cache.ttl", cacheableNumbersFunction),
        CacheNoStore,
        Build("cache_filtered", "Cacheable sequence with static filter pushdown (filter_bytes keying)", cacheFilteredFunction),
        CacheBig, CacheNonce, CacheMulticol, CacheProjection,
        Build("cache_revalidatable", "Always-revalidate result (304 not_modified reuses stored bytes)", cacheRevalidatableFunction),
        CacheScopedTxn, CacheOrdered, CachePoison, CacheWhoami, CacheExternalFail,
    ];

    private static CatalogTable Build(string name, string comment, ITableFunction scanFunction) => new()
    {
        Name = name,
        SchemaName = SchemaName,
        Comment = comment,
        ScanFunction = scanFunction,
    };
}

/// <summary>Backs <c>ex.data.cacheable_numbers</c> (bare, always 10 rows) AND
/// <c>ex.main.cacheable_numbers(n := ...)</c>/unqualified <c>ex.cacheable_numbers(n := ...)</c>
/// (<c>prepared_reset.test</c>) — same class, parameterized by schema, with an OPTIONAL named
/// <c>n</c> argument (default 10) so both calling conventions share one implementation. Column
/// <c>n</c> = <c>0..count-1</c>. Deliberately does NOT advertise <see cref="ITableFunction.FilterPushdown"/>
/// (<c>pushdown.test</c>'s comment: "the simple generator does not push filters, so DuckDB filters
/// above the scan" — the WHOLE unfiltered result is what gets cached, once, regardless of WHERE
/// clause).</summary>
public sealed class CacheableNumbersFunction(string schemaName, long defaultCount) : ITableFunction
{
    public string Name => "cacheable_numbers";

    public string SchemaName => schemaName;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Named("n", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64Named("n", defaultCount);
        return new Producer(count, initParams.OutputSchema);
    }

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var builder = new Int64Array.Builder();
                for (var i = 0L; i < count; i++)
                {
                    builder.Append(i);
                }

                output.Emit(new RecordBatch(outputSchema, [builder.Build()], (int)count), CacheMetadata.Ttl(300));
            }

            output.Finish();
        }
    }
}

/// <summary>10 rows, advertises ONLY <c>vgi.cache.no_store</c> — <c>Cacheable()</c> is false
/// regardless of any TTL, so this is deliberately never stored (<c>basic.test</c>,
/// <c>diagnostics.test</c>).</summary>
public sealed class CacheNoStoreFunction : ITableFunction
{
    public string Name => "cache_no_store";

    public string SchemaName => "data";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var builder = new Int64Array.Builder();
                for (var i = 0; i < 10; i++)
                {
                    builder.Append(i);
                }

                output.Emit(new RecordBatch(outputSchema, [builder.Build()], 10), CacheMetadata.NoStore());
            }

            output.Finish();
        }
    }
}

/// <summary><c>ex.data.cache_big</c> — 5000 rows (<c>n = 0..4999</c>), genuinely emitted across
/// MULTIPLE batches (1000 rows/tick) so replay/GROUP BY/LIMIT-OFFSET tests exercise real batch
/// boundaries (<c>query_shapes.test</c>, <c>multi_batch_threads.test</c>, <c>replay_shapes.test</c>).</summary>
public sealed class CacheBigFunction : ITableFunction
{
    private const long TotalRows = 5000;
    private const long BatchSize = 1000;

    public string Name => "cache_big";

    public string SchemaName => "data";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= TotalRows)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, TotalRows - _next);
            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(_next + i);
            }

            _next += rows;
            var metadata = _next <= BatchSize ? CacheMetadata.Ttl(300) : null;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows), metadata);

            if (_next >= TotalRows)
            {
                output.Finish();
            }
        }
    }
}

/// <summary>Shared "monotonic counter, bumped once per REAL invocation" fixture — backs
/// <c>cache_nonce</c> (plain TTL cache — behavioral proof a served scan never re-invokes the worker,
/// <c>basic.test</c>/<c>http_symmetry.test</c>) and <c>cache_scoped_txn</c>
/// (<c>vgi.cache.scope=transaction</c> — the worker needs ZERO transaction-awareness of its own; the
/// scope string alone tells the C++ side to fold the transaction id into the key, so two scans in
/// ONE transaction still share an entry (same nonce) while a NEW transaction is a genuine miss (new
/// nonce), <c>transaction_scope.test</c>). Deliberately a CROSS-PROCESS counter
/// (<see cref="CrossProcessWorkQueue"/>), not a simple in-memory <c>static long</c>: a pooled worker
/// isn't guaranteed to be the SAME OS process across two scans separated by a COMMIT/new BEGIN
/// (discovered empirically building <c>transaction_scope.test</c> — an in-memory counter reset to 0
/// on the second transaction's scan when the pool happened to round-robin to a different already-warm
/// subprocess, making both transactions observe nonce=1). Reusing <see cref="CrossProcessWorkQueue.ClaimChunk"/>
/// with <c>chunkSize=1</c> and an effectively-unbounded <c>total</c> turns it into exactly this: an
/// atomic "claim the next value" counter, file-backed so it's visible across every worker process in
/// the pool.</summary>
public sealed class MonotonicNonceFunction(string schemaName, string name, IReadOnlyDictionary<string, string> metadata) : ITableFunction
{
    public string Name => name;

    public string SchemaName => schemaName;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("nonce", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(name, initParams.OutputSchema, metadata);

    private sealed class Producer(string counterKey, Schema outputSchema, IReadOnlyDictionary<string, string> metadata)
        : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                CrossProcessWorkQueue.ClaimChunk(counterKey, chunkSize: 1, total: long.MaxValue, out var value);
                var builder = new Int64Array.Builder();
                builder.Append(value + 1);
                output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1), metadata);
            }

            output.Finish();
        }
    }
}

/// <summary><c>ex.data.cache_multicol</c> — 4 rows, <c>(a,b,c) = (i, 10i, 100i)</c> for <c>i=0..3</c>.
/// Deliberately does NOT advertise <see cref="ITableFunction.ProjectionPushdown"/> (the inverse of
/// <see cref="CacheProjectionFunction"/>) — a narrower <c>SELECT b</c> reuses the SAME full-width
/// cached entry, re-projected by DuckDB locally (<c>coverage.test</c>, <c>replay_shapes.test</c>).</summary>
public sealed class CacheMulticolFunction : ITableFunction
{
    public string Name => "cache_multicol";

    public string SchemaName => "data";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("a", Int64Type.Default, nullable: false),
            new Field("b", Int64Type.Default, nullable: false),
            new Field("c", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var a = new Int64Array.Builder();
                var b = new Int64Array.Builder();
                var c = new Int64Array.Builder();
                for (var i = 0L; i < 4; i++)
                {
                    a.Append(i);
                    b.Append(i * 10);
                    c.Append(i * 100);
                }

                output.Emit(new RecordBatch(outputSchema, [a.Build(), b.Build(), c.Build()], 4), CacheMetadata.Ttl(300));
            }

            output.Finish();
        }
    }
}

/// <summary><c>ex.data.cache_projection</c> — 3 rows, <c>a=[1,2,3], b=[10,20,30], c=[100,200,300]</c>.
/// Advertises <see cref="ITableFunction.ProjectionPushdown"/> so <c>projection_ids</c> enter the
/// cache key — <c>SELECT a</c>/<c>SELECT b</c>/<c>SELECT a,b</c> are distinct entries
/// (<c>projection_pushdown.test</c>, <c>partition_scope_shapes.test</c>'s projection section).</summary>
public sealed class CacheProjectionFunction : ITableFunction
{
    public string Name => "cache_projection";

    public string SchemaName => "data";

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("a", Int64Type.Default, nullable: false),
            new Field("b", Int64Type.Default, nullable: false),
            new Field("c", Int64Type.Default, nullable: false),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.ProjectedSchema, initParams.ProjectionIds);

    private sealed class Producer(Schema projectedSchema, IReadOnlyList<long>? projectionIds) : ITableFunctionProducer
    {
        private static readonly long[] Values = [1, 2, 3];

        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var indices = projectionIds ?? [0, 1, 2];
                var columns = indices.Select(BuildColumn).ToList();
                output.Emit(new RecordBatch(projectedSchema, columns, 3), CacheMetadata.Ttl(300));
            }

            output.Finish();
        }

        private static IArrowArray BuildColumn(long fullIndex)
        {
            var multiplier = fullIndex switch { 0 => 1, 1 => 10, _ => 100 };
            var builder = new Int64Array.Builder();
            foreach (var v in Values)
            {
                builder.Append(v * multiplier);
            }

            return builder.Build();
        }
    }
}

/// <summary>The "always-revalidate" fixture: 1 row <c>nonce</c> (a fixed constant — the point isn't
/// the value itself, only that it's STABLE across genuinely re-invoked calls, since the data it
/// represents never changes), <c>ttl=0 + etag + revalidatable=1</c>. Reused under BOTH
/// <c>SchemaName="data"</c> (bare <c>ex.data.cache_revalidatable</c>, <c>revalidate.test</c>/
/// <c>cleanup.test</c>) and <c>SchemaName="main"</c> (<c>ex.main.cache_revalidatable()</c>,
/// <c>spill_lifecycle.test</c>) — same class, two registrations.</summary>
public sealed class CacheRevalidatableFunction(string schemaName) : ITableFunction
{
    private const string Etag = "cache-revalidatable-etag-v1";
    private const long NonceValue = 777;

    public string Name => "cache_revalidatable";

    public string SchemaName => schemaName;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("nonce", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private bool _done;

        public void Produce(OutputCollector output)
        {
            if (_done)
            {
                output.Finish();
                return;
            }

            _done = true;

            if (RevalidationHelper.IsNotModified(output.InputMetadata, Etag))
            {
                var empty = new Int64Array.Builder().Build();
                output.Emit(new RecordBatch(outputSchema, [empty], 0), CacheMetadata.NotModified());
                output.Finish();
                return;
            }

            var builder = new Int64Array.Builder();
            builder.Append(NonceValue);
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1), CacheMetadata.Revalidatable(Etag));
            output.Finish();
        }
    }
}

/// <summary><c>ex.data.cache_ordered</c> — 200,000 rows, strictly monotonic <c>n = 0..199999</c>,
/// genuinely multi-batch (20,000 rows/tick). Advertises <see cref="ITableFunction.OrderPreservation"/>
/// = FixedOrder + <see cref="ITableFunction.SupportsBatchIndex"/> so a HIT must stable-sort-replay in
/// source order, not just the same row multiset (<c>ordered_serve.test</c>).</summary>
public sealed class CacheOrderedFunction : ITableFunction
{
    private const long TotalRows = 200_000;
    private const long BatchSize = 20_000;

    public string Name => "cache_ordered";

    public string SchemaName => "data";

    public Protocol.VgiOrderPreservation? OrderPreservation => Protocol.VgiOrderPreservation.FixedOrder;

    public bool SupportsBatchIndex => true;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;
        private long _batchIndex;

        public void Produce(OutputCollector output)
        {
            if (_next >= TotalRows)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, TotalRows - _next);
            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(_next + i);
            }

            _next += rows;

            var metadata = new Dictionary<string, string>(CacheMetadata.Ttl(300))
            {
                ["vgi_batch_index"] = (_batchIndex++).ToString(),
            };
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows), metadata);

            if (_next >= TotalRows)
            {
                output.Finish();
            }
        }
    }
}

/// <summary><c>ex.data.cache_poison</c> — emits one cacheable (<c>ttl</c>-tagged) row on the first
/// tick, then THROWS with a message containing <c>"intentional mid-stream failure"</c> on the next —
/// proves the never-partial capture invariant: a producer that fails before reaching EOS must never
/// leave a partial entry committed (<c>poison.test</c>, <c>spill_poison.test</c>).</summary>
public sealed class CachePoisonFunction : ITableFunction
{
    public string Name => "cache_poison";

    public string SchemaName => "data";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private int _tick;

        public void Produce(OutputCollector output)
        {
            _tick++;
            if (_tick > 1)
            {
                throw new InvalidOperationException("intentional mid-stream failure");
            }

            var builder = new Int64Array.Builder();
            builder.Append(1);
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1), CacheMetadata.Ttl(300));
        }
    }
}

/// <summary><c>ex.data.cache_whoami</c> — 1 row, echoing the caller's auth principal
/// (<c>cache/identity_isolation.test</c>'s HTTP-only bearer-token fixture: alice/bob attach with
/// different bearer tokens and must never share a cache entry). Over this worker's subprocess
/// transport there is no bearer/JWT identity to surface, so <c>who</c> is always
/// <c>"anonymous"</c> — same answer <see cref="ExampleWorker.Scalar.WhoAmIFunction"/> gives for the
/// scalar case. Cacheable (<c>vgi.cache.ttl</c>) so the C++ side's auth-fingerprint-in-cache-key
/// behavior has something to isolate.</summary>
public sealed class CacheWhoamiFunction : ITableFunction
{
    public string Name => "cache_whoami";

    public string SchemaName => "data";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("who", StringType.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var builder = new StringArray.Builder();
                builder.Append("anonymous");
                output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1), CacheMetadata.Ttl(300));
            }

            output.Finish();
        }
    }
}

/// <summary><c>ex.data.cache_external_fail</c> — backs <c>cache/poison_external.test</c>'s second
/// never-partial check: emits a cacheable first batch (1 row), then on the SECOND tick emits a
/// 0-row EXTERNAL_LOCATION pointer batch (<see cref="MetadataKeys.Location"/> metadata, per
/// vgi-rpc's <c>ExternalLocation.IsExternalLocationBatch</c> convention — see
/// <c>QueryFarm.VgiRpc.Http.ExternalLocation</c> in vgi-rpc-csharp) whose URL is deliberately
/// unreachable (a closed localhost port). External-location resolution happens CLIENT-side (the
/// C++ extension's <c>ResolveExternalLocation</c>, <c>vgi_http_client.cpp</c>) and is
/// transport-independent — this worker only needs to emit the pointer batch itself, not perform
/// any real externalization/upload. The client's fetch fails (connection refused), aborting the
/// scan mid-stream exactly like <see cref="CachePoisonFunction"/>'s in-process exception — proving
/// the never-partial capture invariant holds for a RESOLUTION failure too, not just a worker-thrown
/// one.</summary>
public sealed class CacheExternalFailFunction : ITableFunction
{
    public string Name => "cache_external_fail";

    public string SchemaName => "data";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private int _tick;

        public void Produce(OutputCollector output)
        {
            _tick++;
            if (_tick == 1)
            {
                var builder = new Int64Array.Builder();
                builder.Append(1);
                output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1), CacheMetadata.Ttl(300));
                return;
            }

            var empty = new Int64Array.Builder().Build();
            var pointerMetadata = new Dictionary<string, string>
            {
                [MetadataKeys.Location] = "http://127.0.0.1:1/unreachable-cache-external-fail",
            };
            output.Emit(new RecordBatch(outputSchema, [empty], 0), pointerMetadata);
            output.Finish();
        }
    }
}
