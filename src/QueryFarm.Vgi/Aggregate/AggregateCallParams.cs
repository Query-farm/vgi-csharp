using QueryFarm.Vgi.Internal;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Aggregate;

/// <summary>Parameters an <see cref="IAggregateFunction"/> sees on every <c>aggregate_update</c>/
/// <c>_combine</c>/<c>_finalize</c> call. Unlike <see cref="AggregateBindParams"/>, these three RPCs
/// are each independently worker-pool-acquired unary calls (may land on a different process than
/// the one that ran <c>aggregate_bind</c>) — so <see cref="Arguments"/> is recovered from durable
/// storage (stashed at bind time) rather than riding this call's own wire request, mirroring
/// <see cref="Buffering.TableBufferingProcessParams"/>'s doc comment for the same reason.</summary>
public sealed class AggregateCallParams
{
    public required string FunctionName { get; init; }

    public required byte[] ExecutionId { get; init; }

    /// <summary>The SAME bind-time const-argument values <see cref="AggregateBindParams.Arguments"/>
    /// carried — reloaded from storage, since update/combine/finalize don't ride the original bind
    /// request.</summary>
    public required TableArguments Arguments { get; init; }

    /// <summary>In-band log sink (surfaces as a <c>duckdb_logs()</c> row with <c>type='VGI'</c>) —
    /// <see langword="null"/> only in a unit test that constructs this params object directly.</summary>
    public ICallContext? Ctx { get; init; }
}
