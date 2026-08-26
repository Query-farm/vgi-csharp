using Apache.Arrow;

namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>aggregate_destructor</c> RPC's packed request — best-effort cleanup fired once the C++
/// side has determined every DuckDB aggregate state it ever created for this bind has been torn
/// down (see <c>VgiAggregateDestroy</c>'s <c>destroy_counter</c>/<c>group_id_counter</c>
/// bookkeeping). <see cref="GroupIdsBatch"/> carries a single PLACEHOLDER row (<c>group_id=0</c>,
/// not a real group) — this is a signal to free EVERYTHING this <see cref="ExecutionId"/> ever
/// stored, not a per-group request; <see cref="Internal.FunctionStorage.DeleteAll"/> is the
/// correct (and only correct) response.
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING — matches <c>AggregateDestructorRequestSchema</c>
/// exactly: function_name, execution_id, group_ids_batch, attach_opaque_data, schema_name.
/// </summary>
public sealed class AggregateDestructorRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] ExecutionId { get; set; } = [];

    public RecordBatch GroupIdsBatch { get; set; } = null!;

    public byte[]? AttachOpaqueData { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>aggregate_destructor</c> RPC's packed result — no fields (matches
/// <c>AggregateDestructorResultSchema</c>). See <see cref="AggregateUpdateResult"/>'s doc comment.</summary>
public sealed class AggregateDestructorResult
{
}
