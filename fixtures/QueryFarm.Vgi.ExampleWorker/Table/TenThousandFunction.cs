using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>ten_thousand()</c> — a zero-argument, fixed-output twin of <c>sequence(10000)</c>. Backs
/// <c>table/function_registration.test</c>'s pinned no-arg-fixed-output table function shape
/// (distinct from the <see cref="StaticRowsFunction"/>-backed <c>ten_thousand_table</c> catalog
/// table, which is reachable only as a real table, not a bare function call).
/// </summary>
public sealed class TenThousandFunction : ITableFunction
{
    private const long RowCount = 10_000;
    private const int BatchSize = 2048;

    public string Name => "ten_thousand";

    public string Description => "Generates 10000 integers from 0 to 9999";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public long? Cardinality(TableBindParams bindParams) => RowCount;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= RowCount)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, RowCount - _next);
            var builder = new Int64Array.Builder();
            builder.Reserve(rows);
            for (var i = 0; i < rows; i++)
            {
                builder.Append(_next + i);
            }

            _next += rows;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], rows));
            if (_next >= RowCount)
            {
                output.Finish();
            }
        }
    }
}
