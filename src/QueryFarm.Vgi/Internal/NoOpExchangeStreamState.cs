using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;
using QueryFarm.VgiRpc.Wire;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// The stream state for table-buffering's <c>init(phase=TABLE_BUFFERING)</c> Sink-init connection.
/// Per the C++ extension's own comment on this exact call ("the init RPC opens a Stream on the wire;
/// for TABLE_BUFFERING we don't use it — all subsequent traffic is unary RPCs"), the client opens its
/// input writer and immediately closes it (0 batches) purely so the exchange completes and hands
/// stdin/stdout back for the standalone <c>table_buffering_process</c>/<c>_combine</c>/<c>_destructor</c>
/// unary RPCs. <see cref="ExchangeAsync"/> should therefore never actually be invoked in practice —
/// it exists only so this phase has a concrete, harmless <see cref="ExchangeState"/> to open.
/// </summary>
public sealed class NoOpExchangeStreamState : ExchangeState
{
    public override Task ExchangeAsync(AnnotatedBatch input, OutputCollector output, ICallContext? ctx, CancellationToken cancellationToken)
    {
        // Should never actually run (see class doc comment) — echo the input back rather than throw,
        // so a client that somehow does write a batch here gets a well-formed (if useless) reply
        // instead of tearing the connection down.
        output.Emit(input.Batch);
        return Task.CompletedTask;
    }
}
