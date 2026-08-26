using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Buffering;

/// <summary>Parameters an <see cref="ITableBufferingFunction"/> sees on each Sink-phase
/// <see cref="ITableBufferingFunction.Process"/> call. NOTE: unlike <see cref="TableInOut.TableInOutInitParams"/>,
/// <see cref="Arguments"/>/<see cref="Settings"/> here are recovered from <see cref="IFunctionStorage"/>
/// (stashed at <c>init(phase=TABLE_BUFFERING)</c> time) rather than riding this call's own wire
/// request — <c>table_buffering_process</c>'s wire shape carries neither, since it's a standalone
/// unary RPC that may land on a worker process that never itself ran this query's bind.</summary>
public sealed class TableBufferingProcessParams
{
    public required string FunctionName { get; init; }

    public required byte[] ExecutionId { get; init; }

    public required TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    /// <summary>Already-RESOLVED secrets (any two-phase dynamic-scope retry completed back at
    /// <c>bind</c> time, before this Sink phase ever started) — decode with
    /// <see cref="SecretArgCodec.Decode"/> then <see cref="SecretArgCodec.FindByType"/>/
    /// <see cref="SecretArgCodec.ForScopeOfType"/>. <see langword="null"/> when none were resolved.
    /// Recovered from the persisted bind context, like <see cref="Arguments"/>/<see cref="Settings"/>.</summary>
    public byte[]? Secrets { get; init; }

    public byte[]? AttachOpaqueData { get; init; }

    public byte[]? TransactionId { get; init; }

    /// <summary>Non-<see langword="null"/> only for a <c>COPY ... TO</c> sink — see
    /// <see cref="TableInOut.TableInOutBindParams.CopyTo"/>'s doc comment. Recovered (like
    /// <see cref="Arguments"/>/<see cref="Settings"/>) from the persisted bind context, since this
    /// call's own wire request carries neither.</summary>
    public Protocol.CopyToContext? CopyTo { get; init; }

    /// <summary>DuckDB's globally-unique batch index — populated only when this function advertised
    /// <see cref="ITableBufferingFunction.RequiresInputBatchIndex"/>.</summary>
    public long? BatchIndex { get; init; }

    /// <summary>Cross-process durable storage scoped to this execution — see
    /// <see cref="IFunctionStorage"/>'s doc comment. The canonical pattern: <c>Append</c> this
    /// batch's data (or a derived summary) under a namespace/key of your choosing, then return a
    /// <c>state_id</c> that <see cref="ITableBufferingFunction.Combine"/> and/or the FINALIZE
    /// producer knows how to read back.</summary>
    public required IFunctionStorage Storage { get; init; }

    /// <summary>In-band log sink (surfaces as a <c>duckdb_logs()</c> row with <c>type='VGI'</c>) —
    /// <see langword="null"/> only in a unit test that constructs this params object directly.</summary>
    public ICallContext? Ctx { get; init; }
}
