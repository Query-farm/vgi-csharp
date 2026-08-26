namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_function_plan</c> RPC's packed request — the scan-planning phase that precedes
/// per-split <c>init</c> (see <c>Table.PlanRequest</c>/<c>Table.PlanResult</c> for the author-facing
/// view, and <see cref="Internal.SplitToken"/> for the envelope every split's token gets stamped
/// with). <c>plan()</c> runs once with the STATIC pushdown filters known at that point and returns
/// named splits; each split is then redeemed by <c>init</c> — possibly from a different process —
/// which is what makes a retried/re-parallelized scan sound.
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING (see <see cref="BindRequest"/>'s doc comment for
/// why) — matches the C++ extension's generated <c>TableFunctionPlanRequestSchema</c> field order
/// exactly, 19 fields. <see cref="BindCall"/> is a SECOND level of embedded IPC (a serialized
/// <see cref="BindRequest"/>) — decode with <see cref="Internal.EmbeddedIpc.Decode{T}"/>, exactly
/// like <see cref="InitRequest.BindCall"/>.
/// </summary>
public sealed class TableFunctionPlanRequest
{
    public byte[] BindCall { get; set; } = [];

    public byte[]? BindOpaqueData { get; set; }

    public List<long>? ProjectionIds { get; set; }

    public byte[]? PushdownFilters { get; set; }

    public List<byte[]>? JoinKeys { get; set; }

    public long? RowLimit { get; set; }

    public long? TargetSplitBytes { get; set; }

    public long? MinSplits { get; set; }

    public long? MaxSplitsPerResponse { get; set; }

    public byte[]? Cursor { get; set; }

    public byte[]? RefinedFilters { get; set; }

    public bool FiltersComplete { get; set; }

    public byte[]? StartPosition { get; set; }

    public byte[]? EndPosition { get; set; }

    public string? OrderByColumnName { get; set; }

    public VgiOrderByDirection? OrderByDirection { get; set; }

    public VgiNullOrder? OrderByNullOrder { get; set; }

    public long? OrderByLimit { get; set; }

    public double? TablesamplePercentage { get; set; }

    public long? TablesampleSeed { get; set; }
}
