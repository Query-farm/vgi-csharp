namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_function_dynamic_to_string</c> RPC's unary result. Matches the generated
/// <c>TableFunctionDynamicToStringResultSchema</c>, 2 fields — <see cref="Keys"/>[i]/<see cref="Values"/>[i]
/// are paired positionally (same length). Empty means no extra diagnostics.
/// </summary>
public sealed class TableFunctionDynamicToStringResult
{
    public List<string> Keys { get; set; } = [];

    public List<string> Values { get; set; } = [];
}
