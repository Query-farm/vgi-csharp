namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The catalog object kind requested by <c>catalog_schema_contents_functions</c>/<c>_macros</c>.
/// Wire-encoded as <c>dictionary(int16, utf8)</c> by member name, producing exactly the strings
/// the C++ extension sends: TABLE, VIEW, SCALAR_FUNCTION, TABLE_FUNCTION, AGGREGATE_FUNCTION,
/// SCALAR_MACRO, TABLE_MACRO, INDEX.
/// </summary>
public enum SchemaObjectType
{
    Table,
    View,
    ScalarFunction,
    TableFunction,
    AggregateFunction,
    ScalarMacro,
    TableMacro,
    Index,
}
