namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_function_plan</c> RPC's unary result — the C++ extension validates this type's
/// embedded-IPC schema with STRICT <c>arrow::Schema::Equals</c> against its generated
/// <c>TableFunctionPlanResultSchema()</c> (16 fields, order load-bearing — see
/// <see cref="BindResponse"/>'s doc comment for the same rule).
///
/// An EMPTY <see cref="Splits"/> is legal and means "no work": a fully-pruned split-capable scan
/// reaches it, and the client produces an empty result rather than an error. Built by
/// <see cref="Internal.VgiServiceImpl"/> from an <see cref="ITableFunction"/>'s author-facing
/// <c>Table.PlanResult</c> — see that type's doc comment for the split-vs-"not split-capable"
/// distinction.
/// </summary>
public sealed class TableFunctionPlanResult
{
    /// <summary>One serialized <see cref="ScanSplitWire"/> per unit of work, in emission order —
    /// what the client actually reads out of each entry is just its stamped <c>token</c>.</summary>
    public List<byte[]> Splits { get; set; } = [];

    /// <summary>Continuation cursors for a paginated enumeration; normally 0 or 1 entries.</summary>
    public List<byte[]>? NextCursors { get; set; }

    public byte[]? ExecutionId { get; set; }

    public byte[]? InitOpaqueData { get; set; }

    /// <summary>Normative cap on splits in flight at once, or <see langword="null"/> for none.</summary>
    public long? MaxWorkers { get; set; }

    public long? EstimatedTotalSplits { get; set; }

    public long? EstimatedTotalRows { get; set; }

    public long? EstimatedTotalBytes { get; set; }

    /// <summary>The catalog counter this plan is pinned to — every split's token anchor.</summary>
    public long? CatalogVersion { get; set; }

    /// <summary>Which consistency anchor every token in this plan binds: <c>"catalog"</c> or
    /// <c>"transaction"</c>. This worker always plans at catalog scope (see
    /// <see cref="Internal.VgiServiceImpl"/>'s <c>CatalogVersion</c> constant's doc comment).</summary>
    public string Scope { get; set; } = "catalog";

    public List<string>? Locations { get; set; }

    public List<byte[]>? Partitioning { get; set; }

    public List<byte[]>? SortOrder { get; set; }

    public long? CacheMaxAgeSeconds { get; set; }

    public byte[]? StartPosition { get; set; }

    public byte[]? EndPosition { get; set; }
}
