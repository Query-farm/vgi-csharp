using Apache.Arrow;

namespace QueryFarm.Vgi.Scalar;

/// <summary>Parameters an <see cref="IScalarFunction"/> sees at bind time.</summary>
public sealed class ScalarBindParams
{
    public required string FunctionName { get; init; }

    /// <summary>Opaque, not-yet-decoded serialized argument-schema/const-value bytes from
    /// <see cref="Protocol.BindRequest.Arguments"/> — an embedded IPC batch with a single column
    /// named <c>args</c> whose type is <c>struct(positional_0: T0, positional_1: T1, ...)</c>,
    /// one row, indexed 0.. over CONST parameters only (in <c>Compute</c> declaration order) —
    /// <see cref="ScalarFn"/> decodes this itself for <c>[ConstParam]</c> parameters; a
    /// hand-rolled <see cref="IScalarFunction"/> needing const values directly (e.g. a nested
    /// struct/binary const) reads <see cref="Constants"/>.</summary>
    public byte[] Arguments { get; init; } = [];

    private QueryFarm.Vgi.Arguments.ConstantArguments? _constants;

    /// <summary>The decoded constant (<c>[ConstParam]</c>/<c>vgi_const</c>) argument values of
    /// <see cref="Arguments"/> — <c>Constants.Get&lt;long&gt;(0)</c> or
    /// <c>Constants.Get&lt;long&gt;("precision")</c>. Decoded on first access.</summary>
    public QueryFarm.Vgi.Arguments.ConstantArguments Constants =>
        _constants ??= QueryFarm.Vgi.Arguments.ConstantArguments.Decode(Arguments, DeclaredArgumentsSchema);

    /// <summary>The function's declared arguments schema, used to resolve constants by name.</summary>
    internal Schema? DeclaredArgumentsSchema { get; init; }

    /// <summary>Opaque, not-yet-decoded serialized <see cref="Protocol.BindRequest.Settings"/>
    /// bytes — an embedded IPC batch, one row, columns named literally by DuckDB setting key.
    /// <c>null</c> when this function declared no <see cref="IScalarFunction.RequiredSettings"/>.</summary>
    public byte[]? Settings { get; init; }

    /// <summary>Opaque, not-yet-decoded serialized <see cref="Protocol.BindRequest.Secrets"/> bytes
    /// — an embedded IPC batch, one row, one column per resolved secret. Read it through <see cref="SecretValues"/>. <c>null</c> when this function declared no
    /// <see cref="IScalarFunction.RequiredSecrets"/> (or none matched) — a scalar function only sees a
    /// STATICALLY pre-resolved secret here (see <see cref="Attributes.SecretAttribute"/>), never a
    /// dynamic scope-based one.</summary>
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

    /// <summary>The concrete per-call argument schema DuckDB resolved (decoded from
    /// <see cref="Protocol.BindRequest.InputSchema"/>) — populated for a normal call; <c>null</c>
    /// for a zero-argument function or a pre-bind catalog probe. Needed by an ANY-typed function to
    /// derive its actual (promoted) output type — see <see cref="IScalarFunction.ResolveOutputSchema"/>.</summary>
    public Schema? InputSchema { get; init; }

    public IReadOnlyList<string?>? ArgumentNames { get; init; }
}
