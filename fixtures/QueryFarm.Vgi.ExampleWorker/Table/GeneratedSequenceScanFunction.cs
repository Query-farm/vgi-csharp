using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Physical scan for data.generated_sequence. Only n crosses Arrow; DuckDB binds
/// and evaluates the two canonical VIRTUAL expressions from the table descriptor.
/// </summary>
public sealed class GeneratedSequenceScanFunction : ITableFunction
{
    public string Name => "generated_sequence_scan";

    public string SchemaName => "main";

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.ProjectionIds ?? [0], initParams.ProjectedSchema);

    private sealed class Producer(IReadOnlyList<long> projectionIds, Schema projectedSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var columns = new List<IArrowArray>();
                foreach (var columnIndex in projectionIds)
                {
                    columns.Add(BuildColumn(columnIndex));
                }

                output.Emit(new RecordBatch(projectedSchema, columns, 10));
            }

            output.Finish();
        }

        private static IArrowArray BuildColumn(long columnIndex)
        {
            if (columnIndex != 0) throw new ArgumentOutOfRangeException(nameof(columnIndex));
            var builder = new Int64Array.Builder();
            for (var i = 0; i < 10; i++)
            {
                builder.Append(i);
            }

            return builder.Build();
        }
    }
}
