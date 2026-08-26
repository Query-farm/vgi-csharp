using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>generator_exception(fail_after)</c> — emits one single-row batch per producer turn, then
/// throws once <paramref name="fail_after"/> turns have completed (turn 0 = throws on the very
/// first turn, before any data is emitted at all). Backs <c>generator_exception.test</c>'s error-
/// propagation coverage — an exception thrown from <see cref="ITableFunctionProducer.Produce"/>
/// propagates through the RPC layer as a stream/query failure DuckDB surfaces to the client.
/// </summary>
public sealed class GeneratorExceptionFunction : ITableFunction
{
    public string Name => "generator_exception";

    public string Description => "Raises an exception after N batches for testing";

    public IReadOnlyList<string> Categories => ["testing"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("fail_after", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var failAfter = initParams.Arguments.Int64(0);
        return new Producer(failAfter, initParams.OutputSchema);
    }

    private sealed class Producer(long failAfter, Schema outputSchema) : ITableFunctionProducer
    {
        private long _batchesProduced;

        public void Produce(OutputCollector output)
        {
            if (_batchesProduced >= failAfter)
            {
                throw new InvalidOperationException($"Intentional failure after {failAfter} batches");
            }

            var builder = new Int64Array.Builder();
            builder.Append(_batchesProduced);
            _batchesProduced++;
            output.Emit(new RecordBatch(outputSchema, [builder.Build()], 1));
        }
    }
}
