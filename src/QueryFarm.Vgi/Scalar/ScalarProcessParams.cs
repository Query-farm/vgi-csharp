using Apache.Arrow;

namespace QueryFarm.Vgi.Scalar;

/// <summary>Parameters an <see cref="IScalarFunction"/> sees per exchange turn.</summary>
public sealed class ScalarProcessParams
{
    /// <summary>One column per positional argument, in declaration order. Column NAMES on the
    /// wire are DuckDB's own synthetic "col_0", "col_1", ... — not <see cref="IScalarFunction.ArgumentsSchema"/>'s
    /// (cosmetic) names — so implementations should index by position, not by name.</summary>
    public required RecordBatch Input { get; init; }

    /// <summary>The RESOLVED per-call output schema (from <see cref="IScalarFunction.ResolveOutputSchema"/>)
    /// — the returned <see cref="RecordBatch"/> must use exactly this schema.</summary>
    public required Schema OutputSchema { get; init; }

    /// <summary>Same shape/meaning as <see cref="ScalarBindParams.Arguments"/> — carried again on
    /// every batch (rather than cached as instance state on the shared, potentially
    /// concurrently-invoked <see cref="IScalarFunction"/> singleton) so const-argument values stay
    /// correct across concurrent calls of the same function with different const arguments.</summary>
    public byte[] Arguments { get; init; } = [];

    private QueryFarm.Vgi.Arguments.ConstantArguments? _constants;

    /// <summary>The decoded constant (<c>[ConstParam]</c>/<c>vgi_const</c>) argument values of
    /// <see cref="Arguments"/> — <c>Constants.Get&lt;long&gt;(0)</c> or
    /// <c>Constants.Get&lt;long&gt;("precision")</c>. Decoded on first access.</summary>
    public QueryFarm.Vgi.Arguments.ConstantArguments Constants =>
        _constants ??= QueryFarm.Vgi.Arguments.ConstantArguments.Decode(Arguments, DeclaredArgumentsSchema);

    /// <summary>The function's declared arguments schema, used to resolve constants by name.</summary>
    internal Schema? DeclaredArgumentsSchema { get; init; }

    /// <summary>Same shape/meaning as <see cref="ScalarBindParams.Settings"/>, threaded through
    /// per-batch for the same reason as <see cref="Arguments"/>.</summary>
    public byte[]? Settings { get; init; }

    /// <summary>Same shape/meaning as <see cref="ScalarBindParams.Secrets"/>, threaded through
    /// per-batch for the same reason as <see cref="Arguments"/>.</summary>
    public byte[]? Secrets { get; init; }

    private QueryFarm.Vgi.Arguments.SecretValues? _secretValues;

    /// <summary>Typed, read-only view of <see cref="Secrets"/> — look secrets up with
    /// <see cref="QueryFarm.Vgi.Arguments.SecretValues.FindByType"/>/<see cref="QueryFarm.Vgi.Arguments.SecretValues.Get"/>
    /// and read fields with <see cref="QueryFarm.Vgi.Arguments.Secret.GetString"/>.</summary>
    public QueryFarm.Vgi.Arguments.SecretValues SecretValues =>
        _secretValues ??= QueryFarm.Vgi.Arguments.SecretValues.FromResolved(Secrets);
}
