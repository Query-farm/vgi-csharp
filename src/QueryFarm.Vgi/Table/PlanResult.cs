namespace QueryFarm.Vgi.Table;

/// <summary>
/// What an <see cref="ITableFunction.Plan"/> call produces: the splits, plus the few plan-level
/// facts an author can meaningfully set. Deliberately NOT the wire
/// <see cref="Protocol.TableFunctionPlanResult"/>, which carries several fields the framework
/// fills in or that no author should have to think about.
///
/// An EMPTY <see cref="Splits"/> is legal and means "no work": a fully-pruned scan reaches it, and
/// the client produces an empty result rather than an error — this is distinct from a function
/// that never overrides <see cref="ITableFunction.Plan"/> at all, which
/// <see cref="ITableFunction.SupportsSplits"/> (checked BEFORE <c>Plan</c> is ever called) already
/// gates.
/// </summary>
public sealed class PlanResult
{
    /// <summary>One entry per unit of work. Empty is legal (see this type's doc comment).</summary>
    public IReadOnlyList<ScanSplit> Splits { get; init; } = [];

    /// <summary>Continued enumeration. More than one MUST partition the remaining enumeration
    /// disjointly and exhaustively — nothing on the client (or here) checks this.</summary>
    public IReadOnlyList<byte[]>? NextCursors { get; init; }

    /// <summary>The snapshot this plan is pinned to, or <see langword="null"/> to use the live
    /// catalog version. It is the anchor every token in this plan is stamped with and checked
    /// against at redemption — naming a version the catalog will not agree with is how a plan is
    /// made to expire (see <c>expired_token.test</c>).</summary>
    public long? CatalogVersion { get; init; }

    public long? EstimatedTotalSplits { get; init; }

    public long? EstimatedTotalRows { get; init; }

    /// <summary>Normative cap on redemption concurrency, or <see langword="null"/> for none.</summary>
    public long? MaxWorkers { get; init; }

    /// <summary>An empty plan: no splits, no continuation.</summary>
    public static readonly PlanResult Empty = new();

    /// <summary>A finished plan: these splits and no continuation.</summary>
    public static PlanResult Of(IReadOnlyList<ScanSplit> splits) =>
        new() { Splits = splits, EstimatedTotalSplits = splits.Count };
}
