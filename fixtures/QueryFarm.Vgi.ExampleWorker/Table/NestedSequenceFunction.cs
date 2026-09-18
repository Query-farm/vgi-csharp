using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>nested_sequence(count [, batch_size, history_size])</c> — a struct/list-output generator.
/// Row <c>n</c>'s <c>history</c> is the last <c>history_size</c> (default 20, must be &gt;= 1)
/// sequence values ending at <c>n</c>, as in the reference Python fixture. Backs the
/// nested-column section of <c>filter_pushdown.test</c>. Deliberately does NOT
/// advertise <see cref="ITableFunction.FilterPushdown"/>: DuckDB fully trusts (never re-checks) a
/// pushdown-capable function's output, so a function happy to just emit everything and let DuckDB
/// filter client-side (the normal, simpler case) must leave that capability unset.
/// </summary>
public sealed class NestedSequenceFunction : ITableFunction
{
    public string Name => "nested_sequence";

    public string Description => "Generates a sequence with nested struct and list columns";

    private static readonly StructType MetadataType = new(
    [
        new Field("index", Int64Type.Default, nullable: true),
        new Field("label", StringType.Default, nullable: true),
    ]);

    private static readonly ListType HistoryType = new(new Field("item", Int64Type.Default, nullable: true));

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("batch_size", Int64Type.Default),
            TableArgFields.Named("history_size", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("metadata", MetadataType, nullable: true),
            new Field("history", HistoryType, nullable: true),
        ],
        metadata: null);

    /// <summary>The reference fixture's default; also what bounds a row's history, which used to
    /// run from 0 to <c>n</c> — 50 million list values for a 10,000-row call.</summary>
    private const long DefaultHistorySize = 20;

    public void Bind(TableBindParams bindParams)
    {
        var historySize = bindParams.Arguments.NamedArray("history_size");
        if (historySize is null)
        {
            return;
        }

        if (historySize.IsNull(0))
        {
            throw new InvalidOperationException("Argument 'history_size' cannot be NULL");
        }

        if (bindParams.Arguments.Int64Named("history_size", DefaultHistorySize) < 1)
        {
            throw new InvalidOperationException("Argument 'history_size' must be >= 1");
        }
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var batchSize = initParams.Arguments.Int64Named("batch_size", 1000);
        var historySize = initParams.Arguments.Int64Named("history_size", DefaultHistorySize);
        return new Producer(count, Math.Max(1, batchSize), Math.Max(1, historySize), initParams.OutputSchema);
    }

    private sealed class Producer(long count, long batchSize, long historySize, Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= count)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(batchSize, count - _next);
            var start = _next;
            _next += rows;

            var nBuilder = new Int64Array.Builder();
            var indexBuilder = new Int64Array.Builder();
            var labelBuilder = new StringArray.Builder();
            var historyBuilder = new ListArray.Builder(Int64Type.Default);
            var historyValues = (Int64Array.Builder)historyBuilder.ValueBuilder;

            for (var i = 0; i < rows; i++)
            {
                var n = start + i;
                nBuilder.Append(n);
                indexBuilder.Append(n);
                labelBuilder.Append($"row_{n}");

                historyBuilder.Append();
                for (var h = Math.Max(0, n - historySize + 1); h <= n; h++)
                {
                    historyValues.Append(h);
                }
            }

            var metadata = new StructArray(
                MetadataType, rows, [indexBuilder.Build(), labelBuilder.Build()], ArrowBuffer.Empty, nullCount: 0);

            output.Emit(new RecordBatch(outputSchema, [nBuilder.Build(), metadata, historyBuilder.Build()], rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
