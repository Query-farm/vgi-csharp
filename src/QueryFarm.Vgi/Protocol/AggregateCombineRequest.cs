using Apache.Arrow;

namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>aggregate_combine</c> RPC's packed request — merges parallel-worker (or window
/// segment-tree) partial state. <see cref="MergeBatch"/>'s schema is always
/// <c>(source_group_id: int64, target_group_id: int64)</c>: for each row, the accumulator state
/// under <c>source_group_id</c> should be folded INTO the one under <c>target_group_id</c>. A
/// <c>source_group_id</c> may repeat (one leaf state feeding several targets, e.g. a window
/// segment tree) — it is NEVER implicitly deleted by combine; only <c>aggregate_destructor</c>
/// frees state.
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING — matches <c>AggregateCombineRequestSchema</c>
/// exactly: function_name, execution_id, merge_batch, attach_opaque_data, schema_name.
/// </summary>
public sealed class AggregateCombineRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] ExecutionId { get; set; } = [];

    public RecordBatch MergeBatch { get; set; } = null!;

    public byte[]? AttachOpaqueData { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>aggregate_combine</c> RPC's packed result — no fields (matches
/// <c>AggregateCombineResultSchema</c>). See <see cref="AggregateUpdateResult"/>'s doc comment.</summary>
public sealed class AggregateCombineResult
{
}
