using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>projected_data(count)</c> — 4-column generator (<c>id, name, value, extra</c>) that honors
/// projection pushdown: when DuckDB only needs a subset of columns (in any order),
/// <see cref="TableInitParams.ProjectionIds"/> names which ones, and this only computes/emits
/// those. Backs <c>projected_data.test</c>/<c>projection_info.test</c>.
/// </summary>
public sealed class ProjectedDataFunction : ITableFunction
{
    public string Name => "projected_data";

    public string Description => "Generates data with 4 columns, supporting projection pushdown";

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("name", StringType.Default, nullable: true),
            new Field("value", DoubleType.Default, nullable: true),
            new Field("extra", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        return new Producer(count, initParams.ProjectedSchema, initParams.ProjectionIds);
    }

    private sealed class Producer(long count, Schema projectedSchema, IReadOnlyList<long>? projectionIds) : ITableFunctionProducer
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
                        var idBuilder = new Int64Array.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            idBuilder.Append(start + i);
                        }

                        return idBuilder.Build();
                    case 1:
                        var nameBuilder = new StringArray.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            nameBuilder.Append($"item_{start + i}");
                        }

                        return nameBuilder.Build();
                    case 2:
                        var valueBuilder = new DoubleArray.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            valueBuilder.Append((start + i) * 1.5);
                        }

                        return valueBuilder.Build();
                    default:
                        var extraBuilder = new Int64Array.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            var id = start + i;
                            extraBuilder.Append(id * id);
                        }

                        return extraBuilder.Build();
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
