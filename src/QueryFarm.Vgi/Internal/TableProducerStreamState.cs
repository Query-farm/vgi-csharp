using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The per-call streaming state for a table function's <c>init</c>-opened producer stream: the
/// client (DuckDB) sends empty "tick" batches, and this pulls one batch at a time from the
/// <see cref="ITableFunctionProducer"/> the function's <c>init</c> created (awaiting
/// <see cref="IAsyncTableFunctionProducer.ProduceAsync"/> for an asynchronous one), emitting it and
/// calling <see cref="OutputCollector.Finish"/> once the producer signals it has nothing left.
/// </summary>
public sealed class TableProducerStreamState(ITableFunctionProducer producer) : ProducerState
{
    public override async Task ProduceAsync(OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
    {
        if (producer is IAsyncTableFunctionProducer asyncProducer)
        {
            await asyncProducer.ProduceAsync(output, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            producer.Produce(output);
        }

        ContinuedOutput.RefuseOutsideExchange(producer.GetType().Name, output.EmittedMetadata);
    }
}
