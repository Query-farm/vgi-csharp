namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// Tells the C++ extension which VGI table function to call to obtain a table's data (or to
/// perform an INSERT/UPDATE/DELETE against it) — the same wire shape serves
/// <c>catalog_table_scan_function_get</c>'s result, <c>catalog_table_{insert,update,delete}_function_get</c>'s
/// result, AND the four <c>TableInfo.{scan,insert,update,delete}_function</c> inline fields (parsed
/// with the identical <c>ParseScanFunctionResult</c> on the C++ side either way — see
/// <c>vgi_catalog_api.cpp</c>). Property order matches the generated <c>ScanFunctionResultSchema()</c>:
/// function_name, arguments, required_extensions, schema_name.
///
/// <see cref="Arguments"/> is the SAME wire shape <see cref="Internal.TableArgCodec"/> decodes for a
/// normal bind call, EXCEPT the positional-argument field prefix is <c>arg_&lt;N&gt;</c> (not
/// <c>positional_&lt;N&gt;</c>) and a named argument's field name carries NO prefix at all — see
/// <c>DecodeScanArguments</c> in <c>vgi_catalog_api.cpp</c>. An empty/zero-length array (NOT a
/// zero-field embedded IPC struct) means "no arguments" — <c>DecodeScanArguments</c> returns
/// immediately when <c>arguments_bytes.empty()</c>, so a function-backed table/write-function with no
/// extra arguments should just leave this as <see cref="Array.Empty{T}"/>&lt;byte&gt;() rather than
/// constructing a degenerate embedded struct batch.
/// </summary>
public sealed class ScanFunctionResult
{
    public string FunctionName { get; set; } = "";

    public byte[] Arguments { get; set; } = [];

    public List<string> RequiredExtensions { get; set; } = [];

    /// <summary>The catalog schema <see cref="FunctionName"/> is registered in (protocol 1.5.0). A
    /// function name is unique only WITHIN a schema, so a client that doesn't know this can't tell
    /// which of two same-named implementations this result means — <c>VgiTableEntry::GetScanFunctionImpl</c>
    /// takes a schema named here that is neither the table's own schema nor the catalog's
    /// <c>default_schema</c> as authoritative, and only falls back to its old
    /// table's-schema-then-default-schema guess otherwise. <see langword="null"/> means "no VGI-side
    /// schema to report" — a pre-1.5.0 peer, or a <see cref="FunctionName"/> that is a NATIVE DuckDB
    /// function (<c>read_parquet</c>, <c>iceberg_scan</c>, ...) this worker never registered. That
    /// second case is permanent, not transitional, which is why the field stays optional rather than
    /// becoming mandatory.</summary>
    public string? SchemaName { get; set; }
}
