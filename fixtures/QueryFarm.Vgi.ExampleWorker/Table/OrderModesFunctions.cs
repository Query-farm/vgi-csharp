using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>partitioned_fixed_order/preserves_order/no_order_guarantee(count)</c> — three otherwise-
/// identical <see cref="PartitionedSequenceFunction"/> twins (same <see cref="CrossProcessWorkQueue"/>
/// claim-a-chunk parallel-generator shape) differing ONLY in their declared
/// <see cref="ITableFunction.OrderPreservation"/>, to prove <c>Meta.preserves_order</c> flows
/// end-to-end onto DuckDB's <c>TableFunction::order_preservation_type</c>: <c>FixedOrder</c> forces
/// <c>Pipeline::IsOrderDependent()</c> to collapse the whole scan onto a single worker no matter how
/// many threads are available, while the other two remain free to parallelize. Backs
/// <c>table/order_preservation_modes.test</c>.
/// </summary>
public sealed class OrderModesFunction : ITableFunction
{
    private const long ChunkSize = 10000;

    public required string Name { get; init; }

    public required VgiOrderPreservation? OrderPreservation { get; init; }

    public string Description => "Partitioned sequence generator pinning a specific order-preservation mode";

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var key = Name + ":" + Convert.ToHexString(initParams.ExecutionId ?? []);
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

            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(start + i);
            }

            output.Emit(new RecordBatch(outputSchema, [builder.Build()], (int)rows));
        }
    }
}
