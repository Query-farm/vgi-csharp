namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_buffering_combine</c> RPC's packed request — called once, on whatever worker the
/// C++ extension's coordinator-election picks, after every Sink <c>table_buffering_process</c> call
/// has completed. PROPERTY DECLARATION ORDER IS LOAD-BEARING — matches
/// <c>TableBufferingCombineRequestSchema</c> exactly: function_name, execution_id, state_ids,
/// attach_opaque_data, transaction_id, schema_name.
/// </summary>
public sealed class TableBufferingCombineRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] ExecutionId { get; set; } = [];

    /// <summary>Every <c>state_id</c> returned by every <c>process()</c> call across every worker,
    /// in arbitrary order — duplicates are NOT deduplicated by the framework.</summary>
    public List<byte[]> StateIds { get; set; } = [];

    public byte[]? AttachOpaqueData { get; set; }

    public byte[]? TransactionId { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>table_buffering_combine</c> RPC's packed result: <c>finalize_state_ids</c> — the
/// keys the Source phase will iterate, one <c>init(phase=TABLE_BUFFERING_FINALIZE)</c> stream per id.</summary>
public sealed class TableBufferingCombineResult
{
    public List<byte[]> FinalizeStateIds { get; set; } = [];
}
