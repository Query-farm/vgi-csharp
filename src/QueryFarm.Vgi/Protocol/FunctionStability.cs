namespace QueryFarm.Vgi.Protocol;

/// <summary>Wire values: CONSISTENT, VOLATILE, CONSISTENT_WITHIN_QUERY (matches C++'s ParseFunctionStability).</summary>
public enum FunctionStability
{
    Consistent,
    Volatile,
    ConsistentWithinQuery,
}
