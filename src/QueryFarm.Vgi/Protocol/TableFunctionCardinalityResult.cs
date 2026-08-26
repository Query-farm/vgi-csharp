namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The <c>table_function_cardinality</c> RPC's unary result. Matches the generated
/// <c>TableFunctionCardinalityResultSchema</c>, 2 fields — both nullable ("unknown").
/// </summary>
public sealed class TableFunctionCardinalityResult
{
    public long? Estimate { get; set; }

    public long? Max { get; set; }
}
