namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// One arm of a multi-branch table scan (or the sole, synthesized arm of an ordinary
/// single-function table answering the diagnostic <c>vgi_table_branches()</c> function) — see
/// <see cref="ScanBranchesResult"/>. Three mutually exclusive kinds, selected by which of
/// <see cref="FunctionName"/>/<see cref="SourceTable"/>/<see cref="FormatName"/> is non-empty (the
/// C++ parser rejects zero or more than one set):
/// <list type="bullet">
/// <item><b>Function branch</b> — <see cref="FunctionName"/> names a VGI OR a native DuckDB table
/// function (e.g. <c>read_parquet</c>, <c>iceberg_scan</c> — resolved directly against DuckDB's own
/// catalog, never tunneled through the worker pipe) to call with <see cref="Arguments"/> (the same
/// flat <c>arg_&lt;N&gt;</c>/bare-name wire shape as <see cref="ScanFunctionResult.Arguments"/>).</item>
/// <item><b>Catalog-table branch</b> — <see cref="SourceTable"/> (+ optional
/// <see cref="SourceCatalog"/>/<see cref="SourceSchema"/>) names a table in a companion catalog to
/// scan directly. No in-scope fixture uses this kind yet.</item>
/// <item><b>Format branch</b> — <see cref="FormatName"/> (<c>csv</c>/<c>parquet</c>/...) plus
/// <see cref="FormatLocations"/> let the C++ client pick the matching reader function itself,
/// without the worker needing to know the reader's exact spelling; <see cref="FormatOptions"/>
/// (same flat wire shape as <see cref="Arguments"/>, but every field must be a NAMED option — no
/// <c>arg_&lt;N&gt;</c> positional entries) become that reader's named arguments.</item>
/// </list>
/// <see cref="BranchFilter"/> is a raw SQL boolean expression (parsed, never bound, worker-side —
/// binding happens in the C++ optimizer rewriter once a real column list is in hand) the optimizer
/// uses to prune whole branches that can't match a query's WHERE clause. Property order matches the
/// generated <c>ScanBranchSchema()</c>: function_name, arguments, branch_filter, writable,
/// source_catalog, source_schema, source_table, format_name, format_locations, format_options.
/// </summary>
public sealed class ScanBranch
{
    public string FunctionName { get; set; } = "";

    public byte[] Arguments { get; set; } = [];

    public string? BranchFilter { get; set; }

    /// <summary>Declares this branch the INSERT target for a multi-branch table. At most one
    /// branch across a whole table's <see cref="ScanBranchesResult.Branches"/> may set this — the
    /// C++ parser (<c>ParseScanBranchesResult</c>) rejects two or more at bind time.</summary>
    public bool Writable { get; set; }

    public string? SourceCatalog { get; set; }

    public string? SourceSchema { get; set; }

    public string? SourceTable { get; set; }

    public string? FormatName { get; set; }

    public List<string>? FormatLocations { get; set; }

    public byte[]? FormatOptions { get; set; }
}
