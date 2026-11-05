namespace QueryFarm.Vgi.Table;

/// <summary>
/// One named, independently redeemable unit of scan work — the author-facing counterpart of
/// <see cref="Protocol.ScanSplitWire"/> (which additionally carries the framework-stamped
/// <c>token</c>).
///
/// A split NAMES work rather than describing it: "these three files at version 47" survives a
/// retry; "rows 0-999 of whatever this returns now" does not — and a distributed engine WILL
/// retry, so the difference is correctness, not tidiness. The same split may be redeemed more
/// than once (recursive CTEs, retried tasks) and may be abandoned mid-stream (LIMIT, an empty
/// join build side); neither is an error — see the redeeming <see cref="TableInitParams.SplitPayloads"/>.
///
/// Set only <see cref="Payload"/> (and, optionally, the estimate fields) — the framework stamps
/// the consistency anchor and the bind fingerprint into the token, so an author never writes any
/// of that bookkeeping.
/// </summary>
public sealed class ScanSplit
{
    /// <summary>The worker's own opaque bytes naming this unit of work — round-tripped verbatim
    /// through the token and handed back on <see cref="TableInitParams.SplitPayloads"/> when this
    /// split is redeemed.</summary>
    public required byte[] Payload { get; init; }

    /// <summary>Row estimate for this split, or <see langword="null"/> if unknown.</summary>
    public long? EstimatedRows { get; init; }

    /// <summary>Whether <see cref="EstimatedRows"/> is exact rather than an estimate.</summary>
    public bool RowsExact { get; init; }

    /// <summary>Byte estimate — load-bearing for an engine that bin-packs splits by weight;
    /// <see langword="null"/> degrades it to round-robin by count.</summary>
    public long? EstimatedBytes { get; init; }

    /// <summary>One-row Arrow batch aligned with <see cref="PlanResult.KeyGrouping"/>. Every row
    /// emitted by this split must evaluate to this exact transformed tuple under null-safe
    /// equality.</summary>
    public byte[]? KeyValue { get; init; }

    /// <summary>Zero-based range owned by this split when
    /// <see cref="PlanResult.RangePartitioning"/> is present.</summary>
    public long? RangeIndex { get; init; }

    /// <summary>2-row (min, max) Arrow batch used for containment and pruning. This is not an
    /// exact grouping key or a range assignment.</summary>
    public byte[]? ColumnBounds { get; init; }

    /// <summary>2-row (min, max) batch in the <c>vgi_partition_values</c> encoding, one column
    /// per partition column — see <see cref="Internal.PartitionValuesCodec"/>. Deprecated
    /// protocol-2 compatibility spelling; new producers should set <see cref="ColumnBounds"/>.</summary>
    [Obsolete("Use ColumnBounds; PartitionBounds is the protocol-2 compatibility spelling.")]
    public byte[]? PartitionBounds { get; init; }

    public byte[]? ColumnStatistics { get; init; }

    /// <summary>Indices into <see cref="PlanResult.Locations"/> naming hosts where this split is
    /// cheap to read.</summary>
    public IReadOnlyList<long>? LocationIds { get; init; }

    public byte[]? StartPosition { get; init; }

    /// <summary>Inclusive upper bound; <see langword="null"/> means UNBOUNDED.</summary>
    public byte[]? EndPosition { get; init; }

    /// <summary>A split naming the given work, with no estimates.</summary>
    public static ScanSplit Of(byte[] payload) => new() { Payload = payload };

    /// <summary>A split naming the given work, with an exact row count and a byte estimate.</summary>
    public static ScanSplit Of(byte[] payload, long rows, long bytes) =>
        new() { Payload = payload, EstimatedRows = rows, RowsExact = true, EstimatedBytes = bytes };
}
