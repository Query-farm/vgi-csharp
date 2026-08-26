namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_function_statistics</c> RPC's packed request — same 2-field shape as
/// <see cref="TableFunctionCardinalityRequest"/> (a full copy of the bind call, since a table
/// function's per-column statistics are a pure function of its bind-time arguments). Matches the
/// generated <c>TableFunctionStatisticsRequestSchema</c>.
/// </summary>
public sealed class TableFunctionStatisticsRequest
{
    public byte[] BindCall { get; set; } = [];

    public byte[]? BindOpaqueData { get; set; }
}
