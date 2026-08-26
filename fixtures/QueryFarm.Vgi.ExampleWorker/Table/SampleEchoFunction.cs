using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>sample_echo(count)</c> — echoes whatever <c>TABLESAMPLE SYSTEM(...)</c> hint DuckDB's
/// SamplingPushdown optimizer sent on <c>init</c> (only <c>SYSTEM</c> with a percentage is ever
/// pushed down — Bernoulli/Reservoir are always handled by DuckDB's own physical operators, never
/// reaching the worker). Backs <c>tablesample.test</c>.
/// </summary>
public sealed class SampleEchoFunction : ITableFunction
{
    public string Name => "sample_echo";

    public string Description => "Echoes TABLESAMPLE pushdown hints";

    public bool? SamplingPushdown => true;

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("s", StringType.Default, nullable: true),
            new Field("sample_percentage", DoubleType.Default, nullable: true),
            new Field("sample_seed", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        return new Producer(
            count,
            initParams.TablesamplePercentage ?? -1.0,
            initParams.TablesampleSeed ?? -1,
            initParams.ProjectedSchema,
            initParams.ProjectionIds);
    }

    private sealed class Producer(long count, double samplePct, long sampleSeed, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
        : ITableFunctionProducer
    {
        private const int BatchSize = 2048;
        private long _next;
        private readonly IReadOnlyList<long> _indices = projectionIds ?? [0, 1, 2, 3];

        public void Produce(OutputCollector output)
        {
            if (_next >= count)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, count - _next);
            var start = _next;
            _next += rows;

            IArrowArray BuildColumn(long fullIndex)
            {
                switch (fullIndex)
                {
                    case 0:
                        var nBuilder = new Int64Array.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            nBuilder.Append(start + i);
                        }

                        return nBuilder.Build();
                    case 1:
                        var sBuilder = new StringArray.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            sBuilder.Append($"row_{start + i}");
                        }

                        return sBuilder.Build();
                    case 2:
                        var pctBuilder = new DoubleArray.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            pctBuilder.Append(samplePct);
                        }

                        return pctBuilder.Build();
                    default:
                        var seedBuilder = new Int64Array.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            seedBuilder.Append(sampleSeed);
                        }

                        return seedBuilder.Build();
                }
            }

            var columns = _indices.Select(BuildColumn).ToList();
            output.Emit(new RecordBatch(projectedSchema, columns, rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
