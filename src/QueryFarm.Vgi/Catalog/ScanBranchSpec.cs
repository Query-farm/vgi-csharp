namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// A declarative, worker-side description of one arm of a <see cref="CatalogTable.Branches"/>
/// multi-branch table — the catalog-registration-time analog of the wire
/// <see cref="Protocol.ScanBranch"/> DTO (which <see cref="Internal.VgiServiceImpl"/> builds from
/// this at <c>catalog_table_scan_branches_get</c> time). See <see cref="Protocol.ScanBranch"/>'s
/// doc comment for the three mutually exclusive branch kinds this maps onto. A record (not a plain
/// class) so callers can derive a variant with <c>with</c> — e.g. <c>Seq(100) with { BranchFilter =
/// "n &lt; 50" }</c> — without repeating every other property.
/// </summary>
public sealed record ScanBranchSpec
{
    /// <summary>Function-branch kind — a VGI table function this worker itself serves, OR a
    /// native DuckDB function (<c>read_parquet</c>, <c>read_csv_auto</c>, <c>iceberg_scan</c>, ...)
    /// resolved directly against DuckDB's own catalog without ever reaching this worker.</summary>
    public string? FunctionName { get; init; }

    public IReadOnlyList<object?> PositionalArguments { get; init; } = [];

    public IReadOnlyDictionary<string, object?> NamedArguments { get; init; } =
        new Dictionary<string, object?>();

    /// <summary>Catalog-table-branch kind — scans <c>SourceCatalog.SourceSchema.SourceTable</c> in
    /// a companion catalog directly. No in-scope fixture exercises this kind yet.</summary>
    public string? SourceCatalog { get; init; }

    public string? SourceSchema { get; init; }

    public string? SourceTable { get; init; }

    /// <summary>Format-branch kind — names WHAT the data is (<c>csv</c>/<c>parquet</c>/...) and
    /// WHERE (<see cref="FormatLocations"/>), letting the C++ client pick the matching reader
    /// function itself. <see cref="FormatOptions"/> become that reader's named arguments.</summary>
    public string? FormatName { get; init; }

    public IReadOnlyList<string>? FormatLocations { get; init; }

    public IReadOnlyDictionary<string, object?>? FormatOptions { get; init; }

    /// <summary>Raw SQL boolean expression (e.g. <c>"n &lt; 50"</c>) the C++ optimizer can use to
    /// prune this whole branch when it provably can't match a query's WHERE clause. Null/empty
    /// means unconstrained.</summary>
    public string? BranchFilter { get; init; }

    /// <summary>Declares this the INSERT target among a table's branches. At most one branch of a
    /// given <see cref="CatalogTable.Branches"/> list may set this.</summary>
    public bool Writable { get; init; }
}
