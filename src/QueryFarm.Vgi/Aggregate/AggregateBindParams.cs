using Apache.Arrow;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.Aggregate;

/// <summary>Parameters an <see cref="IAggregateFunction"/> sees on its <c>aggregate_bind</c> call —
/// once per bound SQL call site (shared by every parallel worker connection DuckDB spawns for that
/// one query).</summary>
public sealed class AggregateBindParams
{
    public required string FunctionName { get; init; }

    /// <summary>Bind-time constant (<c>ConstParam</c>) values, keyed by their own sequential index
    /// among JUST the const positions — see <see cref="Protocol.AggregateBindRequest.Arguments"/>'s
    /// doc comment.</summary>
    public required QueryFarm.Vgi.Arguments.TableArguments Arguments { get; init; }

    /// <summary>Schema of the NON-const ("Param") input columns — <see langword="null"/> only for a
    /// truly nullary aggregate with zero declared Param columns (e.g. <c>vgi_count()</c>).</summary>
    public Schema? InputSchema { get; init; }

    public IReadOnlyList<string?>? ArgumentNames { get; init; }

    public byte[]? Settings { get; init; }

    public byte[]? Secrets { get; init; }

    /// <summary>Raw <c>attach_opaque_data</c> of the attach this call belongs to (empty when the call
    /// carries none) — see <see cref="Table.TableBindParams.AttachOpaqueData"/>.</summary>
    public byte[] AttachOpaqueData { get; init; } = [];

    private Catalog.AttachOptions? _attachOptions;

    /// <summary>This attach's ATTACH options, <c>secret</c> ones included (decrypted on first
    /// access) — <see cref="Catalog.AttachOptions.FromAttachData"/> over <see cref="AttachOpaqueData"/>.</summary>
    public Catalog.AttachOptions AttachOptions => _attachOptions ??= Catalog.AttachOptions.FromAttachData(AttachOpaqueData);

    private QueryFarm.Vgi.Arguments.SecretValues? _secretValues;

    /// <summary>Typed, read-only view of <see cref="Secrets"/> — look secrets up with
    /// <see cref="QueryFarm.Vgi.Arguments.SecretValues.FindByType"/>/<see cref="QueryFarm.Vgi.Arguments.SecretValues.Get"/>
    /// and read fields with <see cref="QueryFarm.Vgi.Arguments.Secret.GetString"/>.</summary>
    public QueryFarm.Vgi.Arguments.SecretValues SecretValues =>
        _secretValues ??= QueryFarm.Vgi.Arguments.SecretValues.FromResolved(Secrets);
}
