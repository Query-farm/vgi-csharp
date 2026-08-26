using Apache.Arrow;

namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_buffering_process</c> RPC's packed request — the Sink phase's per-batch unary call.
/// Unlike the streaming exchange path, this (and its two siblings below) is a completely standalone
/// unary RPC: the C++ extension's <c>InvokePooledUnaryRpc</c> acquires SOME worker matching this
/// worker's pool key (not necessarily the same process/connection that minted <see cref="ExecutionId"/>
/// via <c>init(phase=TABLE_BUFFERING)</c>) — so any state this call needs to hand off to
/// <c>table_buffering_combine</c>/the FINALIZE producer must be durable, cross-PROCESS storage keyed
/// by <see cref="ExecutionId"/>, never in-memory worker state (see <c>Internal.FunctionStorage</c>).
///
/// PROPERTY DECLARATION ORDER IS LOAD-BEARING — matches <c>TableBufferingProcessRequestSchema</c>
/// exactly: function_name, execution_id, input_batch, attach_opaque_data, transaction_id, batch_index,
/// schema_name.
/// </summary>
public sealed class TableBufferingProcessRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] ExecutionId { get; set; } = [];

    /// <summary>The one input batch to ingest — decodes automatically via <c>ValueCodec</c>'s
    /// <c>RecordBatch</c>-typed-property special case (an embedded-IPC <c>binary</c> field whose
    /// bytes are themselves a self-contained schema+batch IPC stream).</summary>
    public RecordBatch InputBatch { get; set; } = null!;

    public byte[]? AttachOpaqueData { get; set; }

    public byte[]? TransactionId { get; set; }

    public long? BatchIndex { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>table_buffering_process</c> RPC's packed result: one field, <c>state_id</c> —
/// opaque bytes this worker chose to name where it stashed <see cref="TableBufferingProcessRequest.InputBatch"/>.</summary>
public sealed class TableBufferingProcessResult
{
    public byte[] StateId { get; set; } = [];
}
