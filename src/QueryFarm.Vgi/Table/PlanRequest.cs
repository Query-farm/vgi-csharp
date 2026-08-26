namespace QueryFarm.Vgi.Table;

/// <summary>
/// The inputs an <see cref="ITableFunction.Plan"/> call receives beyond its bind parameters: the
/// pushdown it may use to emit fewer splits, and the place in the enumeration it is resuming from.
/// Only invoked when <see cref="ITableFunction.SupportsSplits"/> is <see langword="true"/>.
/// </summary>
public sealed class PlanRequest
{
    /// <summary>Raw embedded-IPC pushdown-filter bytes — STATIC filters only (join-key values
    /// aren't known at plan time; they arrive later, per split init, via
    /// <see cref="TableInitParams.JoinKeys"/>). On a continuation call this already includes
    /// <c>refined_filters</c>' narrowing merged in. Decode with <see cref="Internal.PushdownFilterCodec.Decode"/>.
    /// <see langword="null"/> when DuckDB pushed no filters down.</summary>
    public byte[]? PushdownFilters { get; init; }

    /// <summary>Columns the scan actually reads, or <see langword="null"/> for all.</summary>
    public IReadOnlyList<long>? ProjectionIds { get; init; }

    /// <summary>A place in the ENUMERATION of splits — NOT a place in the data. Empty on the
    /// first call for a scan.</summary>
    public byte[]? Cursor { get; init; }

    /// <summary>The parallelism FLOOR (the client's own thread count) — a small but expensive
    /// table still needs one split per thread. <see langword="null"/> when the client has none.</summary>
    public long? MinSplits { get; init; }

    /// <summary>The primary sizing lever: emit splits of roughly this many bytes each, since the
    /// client cannot see per-split cost and claims them greedily as interchangeable units.
    /// <see langword="null"/> when the client has no opinion.</summary>
    public long? TargetSplitBytes { get; init; }

    /// <summary>Pagination cap for THIS call — not a sizing hint. A function that ignores it may
    /// return more splits than asked; nothing here truncates on its behalf.</summary>
    public long? MaxSplitsPerResponse { get; init; }

    /// <summary><see langword="false"/> means the client may still narrow the filter set further
    /// on a later continuation call; <see langword="true"/> (the common case) means what this call
    /// carries is final.</summary>
    public bool FiltersComplete { get; init; } = true;
}
