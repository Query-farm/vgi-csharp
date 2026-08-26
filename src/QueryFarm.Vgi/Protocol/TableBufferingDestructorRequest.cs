namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_buffering_destructor</c> RPC's packed request — a best-effort call after the Source
/// phase completes, giving the worker a chance to wipe any durable state it stashed for
/// <see cref="ExecutionId"/> (see <c>Internal.FunctionStorage</c>). PROPERTY DECLARATION ORDER IS
/// LOAD-BEARING — matches <c>TableBufferingDestructorRequestSchema</c> exactly: function_name,
/// execution_id, attach_opaque_data, transaction_id, schema_name.
/// </summary>
public sealed class TableBufferingDestructorRequest
{
    public string FunctionName { get; set; } = "";

    public byte[] ExecutionId { get; set; } = [];

    public byte[]? AttachOpaqueData { get; set; }

    public byte[]? TransactionId { get; set; }

    public string? SchemaName { get; set; }
}

/// <summary>The <c>table_buffering_destructor</c> RPC's packed result — no fields.</summary>
public sealed class TableBufferingDestructorResult
{
}
