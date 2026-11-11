namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// Protocol 3.3 <c>FunctionInfo.visibility</c> values. The SDK sets it automatically on every
/// function listing (see <see cref="CatalogTable.ExposeScan"/>); authors never set it by hand.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><see cref="Public"/> (also what a null/absent value means): an ordinary catalog function,
/// visible in <c>duckdb_functions()</c> and callable from SQL.</item>
/// <item><see cref="TableOnly"/>: listed only so the tables naming it (as scan, insert, update or
/// delete function) get its capabilities; DuckDB keeps its description but does not expose it to
/// SQL, not even fully qualified.</item>
/// </list>
/// The rule: every function a <see cref="CatalogTable"/> names is listed; write functions are
/// always table-only; a table's scan is table-only unless a table naming it sets
/// <see cref="CatalogTable.ExposeScan"/> or the same instance is also registered as an ordinary
/// function. Every other function is public.
/// </remarks>
public static class FunctionVisibility
{
    /// <summary>An ordinary SQL-visible function.</summary>
    public const string Public = "public";

    /// <summary>A function only a table's scan or write path resolves.</summary>
    public const string TableOnly = "table_only";
}
