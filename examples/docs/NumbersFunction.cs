using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.DocsExamples;

public sealed class NumbersFunction : ITableFunction
{
    public string Name => "numbers";

    public string Description => "Generate integers from zero through count - 1";

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.PositionalWithRange("count", Int64Type.Default, ge: 0)],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("n", Int64Type.Default, nullable: false)],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.Arguments.Int64(0), initParams.ProjectedSchema);

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

            var rows = (int)Math.Min(4096, count - _next);
            var values = new Int64Array.Builder();
            values.Reserve(rows);
            for (var row = 0; row < rows; row++)
            {
                values.Append(_next++);
            }

            output.Emit(new RecordBatch(outputSchema, [values.Build()], rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
