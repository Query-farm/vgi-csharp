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

    public required QueryFarm.Vgi.Arguments.TableArguments Arguments { get; init; }

    public byte[]? Settings { get; init; }

    /// <summary>Already-resolved secrets — see <see cref="TableBufferingProcessParams.Secrets"/>'s
    /// doc comment.</summary>
    public byte[]? Secrets { get; init; }

    private QueryFarm.Vgi.Arguments.SecretValues? _secretValues;

    /// <summary>Typed, read-only view of <see cref="Secrets"/> — look secrets up with
    /// <see cref="QueryFarm.Vgi.Arguments.SecretValues.FindByType"/>/<see cref="QueryFarm.Vgi.Arguments.SecretValues.Get"/>
    /// and read fields with <see cref="QueryFarm.Vgi.Arguments.Secret.GetString"/>.</summary>
    public QueryFarm.Vgi.Arguments.SecretValues SecretValues =>
        _secretValues ??= QueryFarm.Vgi.Arguments.SecretValues.FromResolved(Secrets);

    public byte[]? AttachOpaqueData { get; init; }

    private Catalog.AttachOptions? _attachOptions;

    /// <summary>This attach's ATTACH options, <c>secret</c> ones included (decrypted on first
    /// access) — <see cref="Catalog.AttachOptions.FromAttachData"/> over <see cref="AttachOpaqueData"/>.</summary>
    public Catalog.AttachOptions AttachOptions => _attachOptions ??= Catalog.AttachOptions.FromAttachData(AttachOpaqueData);

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
