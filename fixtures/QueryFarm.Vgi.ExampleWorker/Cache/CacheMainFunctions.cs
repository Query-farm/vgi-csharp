using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Cache;

/// <summary><c>ex.main.cache_bench(count)</c> — single-connection, strictly monotonic
/// <c>v = 0..count-1</c> across as many <see cref="ITableFunctionProducer.Produce"/> ticks as needed
/// (deliberately NOT parallel — <c>compression.test</c>'s content check pins <c>v = row_number()-1</c>
/// exactly, which only holds for a single, sequential producer). Backs the disk-tier/compression/
/// entry-cap/prepared-reset family (<c>disk_streaming.test</c>, <c>compression.test</c>,
/// <c>entry_cap.test</c>, <c>prepared_reset.test</c>, ...).</summary>
public sealed class CacheBenchFunction : ITableFunction
{
    private const long BatchSize = 20_000;

    public string Name => "cache_bench";

    public string SchemaName => "main";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("v", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        return new Producer(count, initParams.OutputSchema);
    }

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= count)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, count - _next);
            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(_next + i);
            }

            var metadata = _next == 0 ? CacheMetadata.Ttl(300) : null;
            _next += rows;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows), metadata);

            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}

/// <summary><c>ex.main.cache_parallel(count)</c> — a real MULTI-WORKER generator: up to
/// <see cref="MaxWorkers"/> parallel connections each claim disjoint chunks of <c>0..count-1</c> from
/// a shared cross-process work queue (same pattern as
/// <see cref="ExampleWorker.Table.PartitionedSequenceFunction"/>), so capture genuinely spans
/// <c>num_substreams &gt; 1</c> (<c>parallel_capture.test</c>) while the union stays exactly correct
/// regardless of how many readers DuckDB actually opens (<c>spill*.test</c>, <c>cap_matrix.test</c>).</summary>
public sealed class CacheParallelFunction : ITableFunction
{
    private const long ChunkSize = 25_000;

    public string Name => "cache_parallel";

    public string SchemaName => "main";

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("v", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, count, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long count, Schema outputSchema) : ITableFunctionProducer
    {
        private bool _first = true;

        public void Produce(OutputCollector output)
        {
            var rows = CrossProcessWorkQueue.ClaimChunk(key, ChunkSize, count, out var start);
            if (rows == 0)
            {
                output.Finish();
                return;
            }

            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(start + i);
            }

            var metadata = _first ? CacheMetadata.Ttl(300) : null;
            _first = false;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], (int)rows), metadata);
        }
    }
}

/// <summary><c>ex.cache_filtered(rows := N)</c> (unqualified → <c>main</c> schema) — a REAL
/// filter-pushdown-capable generator, backing <c>ineligible_reasons.test</c>'s
/// <c>reason=dynamic_filter</c> case (an <c>ORDER BY ... LIMIT</c> pushes a tightening Top-N filter
/// that has no stable key at <c>InitGlobal</c> — the C++ side detects this shape itself; this
/// fixture just needs to be a plausible, correctly filter-pushdown-honoring, cacheable generator).
/// The SAME instance also backs the bare <c>data.cache_filtered</c> table (see
/// <see cref="ExampleWorker.Cache.CacheDataTables.All"/>'s doc comment) — its <c>rows</c> default
/// of 100 is what that no-args call site relies on; mirrors vgi-python's single
/// <c>CacheFilteredFunction</c> class serving both roles (see
/// <c>table/function_registration.test</c>'s 166→162 roadmap, item (d)).</summary>
public sealed class CacheFilteredMainFunction : ITableFunction
{
    public string Name => "cache_filtered";

    public string SchemaName => "main";

    public bool? FilterPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Named("rows", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: false)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        // Default 100 (matches vgi-python's CacheFilteredFunction) — lets this SAME instance also
        // back the bare data.cache_filtered table (called with no args at all; see
        // CacheDataTables.All's doc comment on the instance-sharing pattern).
        var rows = initParams.Arguments.Int64Named("rows", 100);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new Producer(rows, decoded, initParams.OutputSchema);
    }

    private sealed class Producer(long totalRows, DecodedFilters? decoded, Schema outputSchema) : ITableFunctionProducer
    {
        private const long BatchSize = 2000;

        private long _next;
        private bool _first = true;

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var row = new Dictionary<string, object?>();

            while (ns.Count == 0 && _next < totalRows)
            {
                var candidateRows = (int)Math.Min(BatchSize, totalRows - _next);
                var start = _next;
                _next += candidateRows;

                for (var i = 0; i < candidateRows; i++)
                {
                    var n = start + i;
                    row["n"] = n;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        ns.Add(n);
                    }
                }
            }

            if (ns.Count == 0)
            {
                output.Finish();
                return;
            }

            var builder = new Int64Array.Builder();
            foreach (var n in ns)
            {
                builder.Append(n);
            }

            var metadata = _first ? CacheMetadata.Ttl(300) : null;
            _first = false;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], ns.Count), metadata);

            if (_next >= totalRows)
            {
                output.Finish();
            }
        }
    }
}
