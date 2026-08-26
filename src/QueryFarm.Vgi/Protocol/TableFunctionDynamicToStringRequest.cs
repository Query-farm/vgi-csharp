namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_function_dynamic_to_string</c> RPC's packed request — the bind call plus the
/// scan's <c>global_execution_id</c> (the correlation key a <see cref="Table.ITableFunction.DynamicToString"/>
/// implementation uses to retrieve whatever diagnostics it persisted while producing rows). Matches
/// the generated <c>TableFunctionDynamicToStringRequestSchema</c>, 3 fields.
/// </summary>
public sealed class TableFunctionDynamicToStringRequest
{
    public byte[] BindCall { get; set; } = [];

    public byte[]? BindOpaqueData { get; set; }

    public byte[] GlobalExecutionId { get; set; } = [];
}
