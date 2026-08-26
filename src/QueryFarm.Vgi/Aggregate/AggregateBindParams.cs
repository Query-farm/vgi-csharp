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
    public required TableArguments Arguments { get; init; }

    /// <summary>Schema of the NON-const ("Param") input columns — <see langword="null"/> only for a
    /// truly nullary aggregate with zero declared Param columns (e.g. <c>vgi_count()</c>).</summary>
    public Schema? InputSchema { get; init; }

    public byte[]? Settings { get; init; }

    public byte[]? Secrets { get; init; }
}
