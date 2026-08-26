using Apache.Arrow;

namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>aggregate_finalize</c> RPC's packed request — produces one output row per requested
/// group id. <see cref="GroupIdsBatch"/>'s schema is always <c>(group_id: int64)</c>. A group id
/// that never appeared in any <c>aggregate_update</c>/<c>_combine</c> call for this execution (e.g.
/// an empty input table, or a group whose only rows were all-NULL under DEFAULT null handling) is
/// legitimate — the resolved function must decide what "no accumulated state" means for its own
/// result (NULL for SUM, 0 for COUNT, etc.).
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING — matches <c>AggregateFinalizeRequestSchema</c>
/// exactly: function_name, execution_id, group_ids_batch, output_schema, attach_opaque_data,
/// schema_name.
/// </summary>
public sealed class AggregateFinalizeRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] ExecutionId { get; set; } = [];

    public RecordBatch GroupIdsBatch { get; set; } = null!;

    /// <summary>Schema-only IPC bytes for the single-field result column — the SAME resolved schema
    /// <c>aggregate_bind</c> returned (echoed back rather than re-derived, since a dynamic/ANY
    /// return type was only resolvable once, at bind time).</summary>
    public byte[] OutputSchema { get; set; } = [];

    public byte[]? AttachOpaqueData { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>aggregate_finalize</c> RPC's packed result — property order matches the C++
/// extension's generated <c>AggregateFinalizeResultSchema</c>: result_batch (its only field).</summary>
public sealed class AggregateFinalizeResult
{
    /// <summary>One column (matching <see cref="AggregateFinalizeRequest.OutputSchema"/>), one row
    /// per <see cref="AggregateFinalizeRequest.GroupIdsBatch"/> row, same order.</summary>
    public RecordBatch ResultBatch { get; set; } = null!;
}
