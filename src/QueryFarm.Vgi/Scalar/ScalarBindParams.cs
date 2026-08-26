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
    /// struct/binary const) can decode it the same way.</summary>
    public byte[] Arguments { get; init; } = [];

    /// <summary>Opaque, not-yet-decoded serialized <see cref="Protocol.BindRequest.Settings"/>
    /// bytes — an embedded IPC batch, one row, columns named literally by DuckDB setting key.
    /// <c>null</c> when this function declared no <see cref="IScalarFunction.RequiredSettings"/>.</summary>
    public byte[]? Settings { get; init; }

    /// <summary>Opaque, not-yet-decoded serialized <see cref="Protocol.BindRequest.Secrets"/> bytes
    /// — an embedded IPC batch, one row, one column per resolved secret. Decode with
    /// <see cref="Internal.SecretArgCodec.Decode"/>. <c>null</c> when this function declared no
    /// <see cref="IScalarFunction.RequiredSecrets"/> (or none matched) — a scalar function only sees a
    /// STATICALLY pre-resolved secret here (see <see cref="Attributes.SecretAttribute"/>), never a
    /// dynamic scope-based one.</summary>
    public byte[]? Secrets { get; init; }

    /// <summary>The concrete per-call argument schema DuckDB resolved (decoded from
    /// <see cref="Protocol.BindRequest.InputSchema"/>) — populated for a normal call; <c>null</c>
    /// for a zero-argument function or a pre-bind catalog probe. Needed by an ANY-typed function to
    /// derive its actual (promoted) output type — see <see cref="IScalarFunction.ResolveOutputSchema"/>.</summary>
    public Schema? InputSchema { get; init; }
}
