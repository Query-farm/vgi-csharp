using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Buffering;

/// <summary>Parameters an <see cref="ITableBufferingFunction"/> sees on its single Combine-phase
/// call. See <see cref="TableBufferingProcessParams"/>'s doc comment for why
/// <see cref="Arguments"/>/<see cref="Settings"/> come from <see cref="IFunctionStorage"/>.</summary>
public sealed class TableBufferingCombineParams
{
    public required string FunctionName { get; init; }

    public required byte[] ExecutionId { get; init; }

    public required TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    /// <summary>Already-resolved secrets — see <see cref="TableBufferingProcessParams.Secrets"/>'s
    /// doc comment.</summary>
    public byte[]? Secrets { get; init; }

    public byte[]? AttachOpaqueData { get; init; }

    public byte[]? TransactionId { get; init; }

    /// <summary>Non-<see langword="null"/> only for a <c>COPY ... TO</c> sink — see
    /// <see cref="TableBufferingProcessParams.CopyTo"/>'s doc comment.</summary>
    public Protocol.CopyToContext? CopyTo { get; init; }

    /// <summary>The bind-time TABLE argument's column schema (recovered, like <see cref="Arguments"/>,
    /// from the persisted bind context) — useful for a function that needs the source's column
    /// names/types at Combine time even when zero batches were ever <c>Process</c>ed (e.g. writing
    /// a header-only file for a genuinely empty COPY TO source).</summary>
    public Apache.Arrow.Schema? InputSchema { get; init; }

    public required IFunctionStorage Storage { get; init; }

    /// <summary>In-band log sink — see <see cref="TableBufferingProcessParams.Ctx"/>.</summary>
    public ICallContext? Ctx { get; init; }
}
