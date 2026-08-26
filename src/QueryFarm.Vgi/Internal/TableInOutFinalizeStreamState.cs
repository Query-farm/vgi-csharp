using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The per-substream streaming state for a table-in-out function's <c>init(phase=FINALIZE)</c>-opened
/// producer stream (run on the SAME connection right after the INPUT phase's EOS) — the client sends
/// empty "tick" batches, this pulls one turn at a time from <see cref="ITableInOutProcessor.Finalize"/>.
/// Trivial wrapper — see <see cref="TableProducerStreamState"/> for the analogous table-function
/// producer version.
/// </summary>
public sealed class TableInOutFinalizeStreamState(ITableInOutProcessor processor) : ProducerState
{
    public override Task ProduceAsync(OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
    {
        processor.Finalize(output);
        return Task.CompletedTask;
    }
}
