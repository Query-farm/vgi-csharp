using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.ProjectionRepro;

/// <summary>
/// Shared 12-column "WIDE_SCHEMA" (a Kafka-consume-shaped row: topic/partition/offset/... plus two
/// always-NULL <c>*_schema_id</c> columns) and row-building logic for the
/// <c>test/sql/integration/projection_pushdown_repro.test</c> fixture set — a reproducer for a
/// vgi-kafka bug where projecting down to a single always-NULL column returned non-NULL data,
/// tracing to the C++ extension's column-id mapping when a function emits a full-width batch and
/// DOESN'T itself narrow to the requested projection (<c>arrow_scan_is_projected=false</c>).
/// <c>value_schema_id</c> is at index 10, <c>key_schema_id</c> at index 7, <c>partition</c> at
/// index 1 — the exact positions the test file's comments name.
/// </summary>
internal static class WideSchemaData
{
    public static readonly Schema Schema = new(
        [
            new Field("topic", StringType.Default, nullable: true),
            new Field("partition", Int32Type.Default, nullable: true),
            new Field("offset", Int64Type.Default, nullable: true),
            new Field("timestamp", Int64Type.Default, nullable: true),
            new Field("timestamp_type", StringType.Default, nullable: true),
            new Field("key_bytes", StringType.Default, nullable: true),
            new Field("key_string", StringType.Default, nullable: true),
            new Field("key_schema_id", Int32Type.Default, nullable: true),
            new Field("value_bytes", StringType.Default, nullable: true),
            new Field("value_string", StringType.Default, nullable: true),
            new Field("value_schema_id", Int32Type.Default, nullable: true),
            new Field("headers", StringType.Default, nullable: true),
        ],
        metadata: null);

    /// <summary>Builds ONE column's worth of data for rows <c>[start, start+rows)</c>, keyed by
    /// field NAME (so it works whether the caller wants the full 12-column schema or a narrowed
    /// <see cref="TableInitParams.ProjectedSchema"/>). <c>value_schema_id</c>/<c>key_schema_id</c>
    /// are unconditionally NULL for every row — the whole point of the reproducer.</summary>
    public static IArrowArray BuildColumn(string fieldName, long start, int rows)
    {
        switch (fieldName)
        {
            case "partition":
                {
                    var b = new Int32Array.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        b.Append((int)((start + i) % 4));
                    }

                    return b.Build();
                }
            case "offset":
                {
                    var b = new Int64Array.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        b.Append(start + i);
                    }

                    return b.Build();
                }
            case "timestamp":
                {
                    var b = new Int64Array.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        b.Append((start + i) * 1000);
                    }

                    return b.Build();
                }
            case "timestamp_type":
                {
                    var b = new StringArray.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        b.Append("create_time");
                    }

                    return b.Build();
                }
            case "topic":
                {
                    var b = new StringArray.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        b.Append("proj_repro_topic");
                    }

                    return b.Build();
                }
            case "key_string":
                {
                    var b = new StringArray.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        b.Append($"key_{start + i}");
                    }

                    return b.Build();
                }
            case "value_string":
                {
                    var b = new StringArray.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        b.Append($"value_{start + i}");
                    }

                    return b.Build();
                }
            // key_schema_id/value_schema_id: unconditionally NULL — the reproducer's whole point.
            // key_bytes/value_bytes/headers: unused by the test, also left NULL for simplicity.
            case "key_schema_id":
            case "value_schema_id":
            case "key_bytes":
            case "value_bytes":
            case "headers":
            default:
                {
                    if (fieldName is "key_schema_id" or "value_schema_id")
                    {
                        var b = new Int32Array.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            b.AppendNull();
                        }

                        return b.Build();
                    }

                    var sb = new StringArray.Builder();
                    for (var i = 0; i < rows; i++)
                    {
                        sb.AppendNull();
                    }

                    return sb.Build();
                }
        }
    }

    public static RecordBatch BuildBatch(Schema schema, long start, int rows) =>
        new(schema, schema.FieldsList.Select(f => BuildColumn(f.Name, start, rows)).ToList(), rows);
}

/// <summary>
/// <c>proj_repro_full_schema(count)</c> — does NOT declare <see cref="ITableFunction.ProjectionPushdown"/>,
/// so it always emits the full 12-column <see cref="WideSchemaData.Schema"/> in ONE batch,
/// regardless of what DuckDB actually projects; the C++ extension narrows down to the requested
/// column(s) on the client side (<c>arrow_scan_is_projected=false</c>).
/// </summary>
public sealed class ProjReproFullSchemaFunction : ITableFunction
{
    public string Name => "proj_repro_full_schema";

    public string Description => "Emits the full WIDE_SCHEMA per row regardless of projection — reproduces the vgi-kafka column-mapping bug";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema => WideSchemaData.Schema;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        return new Producer(count, initParams.OutputSchema);
    }

    private sealed class Producer(long count, Schema outputSchema) : ITableFunctionProducer
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
            output.Emit(WideSchemaData.BuildBatch(outputSchema, 0, (int)count));
            output.Finish();
        }
    }
}

/// <summary>
/// <c>proj_repro_chunked(count)</c> — same full-width, projection-unaware shape as
/// <see cref="ProjReproFullSchemaFunction"/>, but emits ONE tiny (2-row) batch per <c>process()</c>
/// tick instead of a single batch — mirrors <c>kafka_consume</c>'s shard-queue pattern, where the
/// original bug was first observed across MULTIPLE small batches rather than one large one.
/// </summary>
public sealed class ProjReproChunkedFunction : ITableFunction
{
    private const int ChunkSize = 2;

    public string Name => "proj_repro_chunked";

    public string Description => "Like proj_repro_full_schema, but emits 2-row batches across multiple ticks";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema => WideSchemaData.Schema;

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

            var rows = (int)Math.Min(ChunkSize, count - _next);
            output.Emit(WideSchemaData.BuildBatch(outputSchema, _next, rows));
            _next += rows;
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}

/// <summary>
/// <c>proj_repro_multi_worker(count)</c> — the same chunked, projection-unaware shape as
/// <see cref="ProjReproChunkedFunction"/>, but additionally <see cref="ITableFunction.MaxWorkers"/>-
/// capable: up to 4 parallel readers (potentially separate OS processes under the subprocess
/// transport) each claim successive 2-row chunks from a shared cross-process work queue keyed by
/// <see cref="TableInitParams.ExecutionId"/> (see <see cref="CrossProcessWorkQueue"/>, and
/// <see cref="Table.PartitionedSequenceFunction"/>'s doc comment for why an in-process counter
/// doesn't suffice) — the closest analogue to <c>kafka_consume</c>'s 4-partition × shard-queue
/// layout where the original bug surfaced.
/// </summary>
public sealed class ProjReproMultiWorkerFunction : ITableFunction
{
    private const long ChunkSize = 2;

    public string Name => "proj_repro_multi_worker";

    public string Description => "Like proj_repro_chunked, but claims chunks across up to 4 parallel workers";

    public int? MaxWorkers => 4;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema => WideSchemaData.Schema;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var key = Convert.ToHexString(initParams.ExecutionId ?? []);
        return new Producer(key, count, initParams.OutputSchema);
    }

    private sealed class Producer(string key, long count, Schema outputSchema) : ITableFunctionProducer
    {
        public void Produce(OutputCollector output)
        {
            var rows = CrossProcessWorkQueue.ClaimChunk(key, ChunkSize, count, out var start);
            if (rows == 0)
            {
                output.Finish();
                return;
            }

            output.Emit(WideSchemaData.BuildBatch(outputSchema, start, (int)rows));
        }
    }
}

/// <summary>
/// <c>proj_repro_strict(count)</c> — the canonical, projection-AWARE counterpart: declares
/// <see cref="ITableFunction.ProjectionPushdown"/> and emits ONLY the columns DuckDB actually
/// requested (<see cref="TableInitParams.ProjectedSchema"/>), matching the recommended pattern
/// every other projection-pushdown fixture in this worker follows. Included as the "should match"
/// cross-check the test file's final section runs.
/// </summary>
public sealed class ProjReproStrictFunction : ITableFunction
{
    public string Name => "proj_repro_strict";

    public string Description => "Projection-aware counterpart of proj_repro_full_schema — emits only the requested columns";

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema => WideSchemaData.Schema;

    public bool? ProjectionPushdown => true;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        return new Producer(count, initParams.ProjectedSchema);
    }

    private sealed class Producer(long count, Schema projectedSchema) : ITableFunctionProducer
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
            output.Emit(WideSchemaData.BuildBatch(projectedSchema, 0, (int)count));
            output.Finish();
        }
    }
}
