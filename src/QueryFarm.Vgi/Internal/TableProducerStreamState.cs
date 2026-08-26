using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The per-call streaming state for a table function's <c>init</c>-opened producer stream: the
/// client (DuckDB) sends empty "tick" batches, and this pulls one batch at a time from the
/// <see cref="ITableFunctionProducer"/> the function's <c>init</c> created, emitting it and calling
/// <see cref="OutputCollector.Finish"/> once the producer signals it has nothing left.
/// </summary>
public sealed class TableProducerStreamState(ITableFunctionProducer producer) : ProducerState
{
    public override Task ProduceAsync(OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
    {
        producer.Produce(output);
        return Task.CompletedTask;
    }
}
