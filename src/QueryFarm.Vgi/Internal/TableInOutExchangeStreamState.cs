using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The per-substream streaming state for a table-in-out function's <c>init(phase=INPUT)</c>-opened
/// exchange: one input batch in, at most one output batch out, per turn. Trivial wrapper — see
/// <see cref="ScalarStreamState"/> for the analogous scalar-function version.
/// </summary>
public sealed class TableInOutExchangeStreamState(ITableInOutProcessor processor) : ExchangeState
{
    public override Task ExchangeAsync(AnnotatedBatch input, OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
    {
        // ITableInOutProcessor.Process only receives the raw RecordBatch (not the AnnotatedBatch),
        // so a processor that needs this turn's incoming custom_metadata (e.g. a revalidation-aware
        // function reading vgi.cache.if_none_match off the input batch — see
        // OutputCollector.InputMetadata's doc comment) has no other way to see it.
        output.InputMetadata = input.Metadata;
        processor.Process(input.Batch, output);
        return Task.CompletedTask;
    }
}
