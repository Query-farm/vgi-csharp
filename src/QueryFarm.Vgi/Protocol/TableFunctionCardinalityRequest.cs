namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_function_cardinality</c> RPC's packed request — a lazy, best-effort call the C++
/// extension makes at most once per bound call site and treats as non-critical (a failure/timeout
/// just leaves the cardinality "unknown"; see <c>VgiTableFunctionCardinality</c>'s try/catch).
/// Matches the generated <c>TableFunctionCardinalityRequestSchema</c>, 2 fields.
/// </summary>
public sealed class TableFunctionCardinalityRequest
{
    public byte[] BindCall { get; set; } = [];

    public byte[]? BindOpaqueData { get; set; }
}
