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

    /// <summary>Same shape/meaning as <see cref="ScalarBindParams.Settings"/>, threaded through
    /// per-batch for the same reason as <see cref="Arguments"/>.</summary>
    public byte[]? Settings { get; init; }

    /// <summary>Same shape/meaning as <see cref="ScalarBindParams.Secrets"/>, threaded through
    /// per-batch for the same reason as <see cref="Arguments"/>.</summary>
    public byte[]? Secrets { get; init; }
}
