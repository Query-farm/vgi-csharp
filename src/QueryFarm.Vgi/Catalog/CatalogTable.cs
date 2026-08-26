using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;

namespace QueryFarm.Vgi.Catalog;

/// <summary>
/// A declaratively-registered catalog table — the real, queryable-as-a-plain-table analog of an
/// <see cref="ITableFunction"/> (which is only reachable as <c>schema.function_name(...)</c>).
/// Mirrors vgi-python's declarative <c>vgi.catalog.Table</c> (see <c>docs/catalog-interface.md</c>'s
/// "Function-Backed Tables (Recommended)" section): the recommended shape backs a table entirely by
/// an already-registered <see cref="ScanFunction"/> and lets <see cref="Columns"/> default to that
/// function's own <see cref="ITableFunction.OutputSchema"/>, so the column list is declared exactly
/// once.
///
/// A writable table (see <c>test/sql/integration/simple_writable/*.test</c>) additionally names an
/// <see cref="ITableInOutFunction"/> per write operation — the C++ extension resolves
/// INSERT/UPDATE/DELETE by looking up a VGI table-in-out function by name (see
/// <see cref="Protocol.ScanFunctionResult"/>'s doc comment) and feeding it the affected rows over the
/// SAME exchange-stream protocol an ordinary table-in-out function like <c>echo</c> already uses —
/// there is no separate "write function" kind on the wire.
/// </summary>
public sealed class CatalogTable
{
    public required string Name { get; init; }

    public string SchemaName { get; init; } = "main";

    public string? Comment { get; init; }

    public Dictionary<string, string> Tags { get; init; } = [];

    /// <summary>The table's column schema. Defaults to <see cref="ScanFunction"/>'s
    /// <see cref="ITableFunction.OutputSchema"/> when null (the recommended function-backed
    /// pattern) — set explicitly only for a table with no backing scan function.</summary>
    public Schema? Columns { get; init; }

    /// <summary>Column NAMES (resolved to <see cref="Protocol.TableInfo.NotNullConstraints"/> indices
    /// against <see cref="Columns"/> at registration time).</summary>
    public IReadOnlyList<string> NotNullColumns { get; init; } = [];

    public IReadOnlyList<IReadOnlyList<string>> UniqueColumns { get; init; } = [];

    public IReadOnlyList<string> PrimaryKeyColumns { get; init; } = [];

    /// <summary>Raw SQL boolean expressions, e.g. <c>"(budget >= 0)"</c> — surfaced verbatim as
    /// <c>duckdb_constraints().constraint_text</c>'s <c>CHECK(...)</c> wrapper.</summary>
    public IReadOnlyList<string> CheckConstraints { get; init; } = [];

    public IReadOnlyList<CatalogForeignKey> ForeignKeys { get; init; } = [];

    /// <summary>The row-identity column for UPDATE/DELETE (<see cref="Internal.VgiRowIdMetadata"/>).
    /// Required when <see cref="SupportsUpdate"/> or <see cref="SupportsDelete"/> is set.</summary>
    public string? RowIdColumn { get; init; }

    /// <summary>The read path — a normal table function (also independently registered under this
    /// table's <see cref="SchemaName"/>/<see cref="ITableFunction.Name"/> so the C++ side can call it
    /// by name once it resolves this table's inline <see cref="Protocol.TableInfo.ScanFunction"/>).</summary>
    public ITableFunction? ScanFunction { get; init; }

    /// <summary>Fixed positional call arguments baked into <see cref="ScanFunction"/>'s inline
    /// <see cref="Protocol.ScanFunctionResult"/> — e.g. a table backed by a function whose FIRST
    /// argument is a required row count (<c>Table(function=SequenceFunction,
    /// arguments=Arguments(positional=(pa.scalar(1_000_000),)))</c> in vgi-python's terms) declares
    /// that constant here so every scan of this table binds with it. Empty (the default) means the
    /// scan function takes no arguments from the table descriptor.</summary>
    public IReadOnlyList<object?> ScanArguments { get; init; } = [];

    /// <summary>Fixed NAMED call arguments, same purpose as <see cref="ScanArguments"/>.</summary>
    public IReadOnlyDictionary<string, object?> ScanNamedArguments { get; init; } =
        new Dictionary<string, object?>();

    /// <summary>Whether <see cref="ScanFunction"/> is inlined onto <see cref="Protocol.TableInfo.ScanFunction"/>
    /// (the default, and the only behavior before this flag existed) — the C++ extension then uses
    /// it directly and never calls <c>catalog_table_scan_branches_get</c>/logs
    /// <c>vgi.scan_function.inlined</c> (see <c>table/inlined_scan_function.test</c>). Set
    /// <see langword="false"/> for a table that should exercise the legacy per-bind RPC lookup path
    /// instead — <see cref="ScanFunction"/> stays registered as an ordinary callable function and
    /// <see cref="Protocol.IVgiService.CatalogTableScanBranchesGetAsync"/> still answers for it (a
    /// single synthesized branch wrapping the same function), it's just no longer inlined onto this
    /// table's own <see cref="Protocol.TableInfo"/>.</summary>
    public bool InlineScanFunction { get; init; } = true;

    /// <summary>Per-column comments, keyed by column NAME (resolved against <see cref="Columns"/>/
    /// <see cref="ScanFunction"/>'s output schema at registration time) — applied as <c>"comment"</c>
    /// Arrow field metadata (see <c>vgi_catalog_api.cpp</c>'s column-metadata reader), surfaced via
    /// <c>duckdb_columns().comment</c>. A column with no entry here reports a NULL comment.</summary>
    public IReadOnlyDictionary<string, string> ColumnComments { get; init; } = new Dictionary<string, string>();

    /// <summary>Per-column DEFAULT value expressions (raw SQL, e.g. <c>"9.99"</c>/<c>"'unknown'"</c>),
    /// keyed by column NAME — applied as <c>"default"</c> Arrow field metadata, surfaced via
    /// <c>duckdb_columns().column_default</c>. Mutually exclusive with a generated-expression column
    /// (not yet surfaced by this builder). A column with no entry here reports a NULL default.</summary>
    public IReadOnlyDictionary<string, string> ColumnDefaults { get; init; } = new Dictionary<string, string>();

    /// <summary>Per-column GENERATED ALWAYS AS expressions (raw SQL, e.g. <c>"n * 2"</c>), keyed by
    /// column NAME — applied as <c>"generated_expression"</c> Arrow field metadata
    /// (<c>VGI_GENERATED_EXPRESSION_METADATA_KEY</c>), surfaced via <c>duckdb_columns().column_default</c>
    /// as DuckDB's own <c>CAST((&lt;expr&gt;) AS &lt;column_type&gt;)</c> rendering. Mutually exclusive
    /// per-column with <see cref="ColumnDefaults"/> (a column is either stored-with-a-default or
    /// computed, never both). Empty (the default) means no generated columns.</summary>
    public IReadOnlyDictionary<string, string> GeneratedColumns { get; init; } = new Dictionary<string, string>();

    /// <summary>Per-column statistics, keyed by column NAME — served via the
    /// <c>catalog_table_column_statistics_get</c> RPC (<see cref="Internal.ColumnStatisticsCodec"/>
    /// builds the wire batch) whenever non-empty, which also implies
    /// <see cref="Protocol.TableInfo.SupportsColumnStatistics"/>. Empty (the default) means the
    /// table declares no statistics — the optimizer gets no filter-elimination help.</summary>
    public IReadOnlyDictionary<string, ColumnStatisticsInput> Statistics { get; init; } =
        new Dictionary<string, ColumnStatisticsInput>();

    /// <summary>How long the C++ extension may cache a fetched statistics response before
    /// re-issuing <c>catalog_table_column_statistics_get</c> — <see langword="null"/> means no TTL
    /// cap is advertised (worker/C++-side default applies). <c>0</c> forces a re-fetch on every
    /// query.</summary>
    public long? StatisticsCacheMaxAgeSeconds { get; init; }

    public bool SupportsInsert { get; init; }

    public bool SupportsUpdate { get; init; }

    public bool SupportsDelete { get; init; }

    /// <summary>Whether INSERT/UPDATE/DELETE ... RETURNING is allowed — independent per-operation
    /// support flags aren't part of the wire protocol, so this applies to whichever of
    /// insert/update/delete IS supported.</summary>
    public bool SupportsReturning { get; init; }

    public ITableInOutFunction? InsertFunction { get; init; }

    public ITableInOutFunction? UpdateFunction { get; init; }

    public ITableInOutFunction? DeleteFunction { get; init; }

    public long? CardinalityEstimate { get; init; }

    public long? CardinalityMax { get; init; }

    /// <summary>Required WHERE-filter groups in conjunctive normal form (CNF): an AND (outer list)
    /// of OR-groups (inner lists) of dotted-path column references (e.g. <c>"s.a"</c> for a struct
    /// subfield). A group is satisfied when the query's WHERE clause carries a filter on ANY one of
    /// its member paths (or a filter on a PREFIX of that path — a filter on the whole struct 's'
    /// satisfies a required 's.a'); every group must be satisfied or the C++ optimizer refuses the
    /// query with a <c>BinderException</c> before it runs. A single-path group is a plain mandatory
    /// filter; a multi-path group like <c>["a", "b"]</c> means "one of a, b". Empty (the default)
    /// means no enforcement. Surfaced on <see cref="Protocol.TableInfo.RequiredFilters"/> and,
    /// pre-error, via the extension-injected <c>vgi_required_filters</c>
    /// <c>duckdb_tables().tags</c> entry.</summary>
    public IReadOnlyList<IReadOnlyList<string>> RequiredFilters { get; init; } = [];

    /// <summary>Explicit multi-branch declaration (<c>catalog_table_scan_branches_get</c>) — when
    /// non-null (even an EMPTY list, to exercise the C++ loud-fail-on-zero-branches contract; see
    /// <c>catalog/multi_branch_empty_branches.test</c>), this is the table's scan in full and
    /// <see cref="ScanFunction"/> should be left null: an inline <see cref="ScanFunction"/> always
    /// wins on the C++ side (<c>VgiTableEntry::GetScanFunctionImpl</c> never even calls
    /// <c>catalog_table_scan_branches_get</c> for the actual scan when it's set), so a table
    /// declaring both would have this list consulted ONLY by the <c>vgi_table_branches()</c>
    /// diagnostic, never by a real query. A table with neither this NOR <see cref="ScanFunction"/>
    /// set (rare — only makes sense alongside an explicit <see cref="Columns"/>) is a worker bug,
    /// caught at <c>catalog_table_scan_branches_get</c> time.</summary>
    public IReadOnlyList<ScanBranchSpec>? Branches { get; init; }

    /// <summary>DuckDB extensions required to scan any of <see cref="Branches"/> (e.g.
    /// <c>["iceberg"]</c>) — surfaced on <see cref="Protocol.ScanBranchesResult.RequiredExtensions"/>.
    /// Ignored (a plain <see cref="ScanFunction"/>-backed table has none) unless <see cref="Branches"/>
    /// is also set.</summary>
    public IReadOnlyList<string> RequiredExtensions { get; init; } = [];

    /// <summary>Whether this table honours <c>AT (VERSION =&gt; ...)</c>/<c>AT (TIMESTAMP =&gt; ...)</c>
    /// time-travel clauses. <see langword="false"/> (the default) makes
    /// <see cref="Internal.VgiServiceImpl.CatalogTableGetAsync"/> refuse any AT clause against this
    /// table with a clear error — see <c>table/time_travel.test</c>'s "AT clause on non-time-travel
    /// table" case. Multi-branch tables (<see cref="Branches"/> non-null) are exempt from this check
    /// (they're passed through to the C++ extension's OWN multi-branch-specific AT refusal instead —
    /// see <c>catalog/multi_branch_scan.test</c>).</summary>
    public bool SupportsTimeTravel { get; init; }

    /// <summary>Resolves this table's declared shape (typically <see cref="Columns"/> +
    /// <see cref="ScanArguments"/>/<see cref="ScanNamedArguments"/>) for a specific AT clause —
    /// called from <c>catalog_table_get</c> ONLY when <see cref="SupportsTimeTravel"/> is set and
    /// the query actually carries an AT clause. Takes the raw <c>(atUnit, atValue)</c> pair (never
    /// null/empty when called) and returns the <see cref="CatalogTable"/> variant to serve — e.g. a
    /// different <see cref="Columns"/> schema for a table with per-version schema evolution
    /// (<c>versioned_data</c>/<c>versioned_constraints</c>). Throw (with a message DuckDB should
    /// surface verbatim, e.g. <c>"Unknown version: ..."</c>) for an out-of-range/unsupported value.
    /// <see langword="null"/> (the default) means "serve this table unchanged for any AT clause" —
    /// the right choice for a table whose SCAN FUNCTION (not its catalog schema) resolves the
    /// version itself, either by reading <see cref="Table.TableBindParams.AtUnit"/>/
    /// <see cref="Table.TableBindParams.AtValue"/> directly (<c>tt_pushdown_fn</c>) or via
    /// <see cref="ResolveScanArguments"/> (<c>tt_pushdown_cols</c>, <c>cache_versioned</c>).</summary>
    public Func<string, string, CatalogTable>? ResolveAtClause { get; init; }

    /// <summary>Resolves this table's SCAN CALL ARGUMENTS (overriding <see cref="ScanArguments"/>/
    /// <see cref="ScanNamedArguments"/>) for a given AT clause — consulted by
    /// <see cref="Internal.VgiServiceImpl.CatalogTableScanBranchesGetAsync"/> for a
    /// <see cref="ScanFunction"/>-backed (non-<see cref="Branches"/>) table with
    /// <see cref="InlineScanFunction"/> <see langword="false"/>, on EVERY bind (both AT and
    /// no-AT — <c>atUnit</c>/<c>atValue</c> are empty strings for "no AT clause", not null, so a
    /// default/current-version answer is still required). This is the native "columns-based"
    /// time-travel mechanism: the catalog resolves AT into a scan-function ARGUMENT (e.g. a
    /// resolved <c>version</c>) rather than swapping the table's declared schema — see
    /// <c>tt_pushdown_cols</c>/<c>cache_versioned</c>. <see langword="null"/> (the default) means
    /// "use <see cref="ScanArguments"/>/<see cref="ScanNamedArguments"/> unchanged, ignore AT".</summary>
    public Func<string, string, (IReadOnlyList<object?> Positional, IReadOnlyDictionary<string, object?> Named)>? ResolveScanArguments
    { get; init; }

    /// <summary>Resolves <see cref="Columns"/>, falling back to <see cref="ScanFunction"/>'s output
    /// schema — throws if neither is available.</summary>
    public Schema ResolveColumns() => Columns
        ?? ScanFunction?.OutputSchema
        ?? throw new InvalidOperationException(
            $"Catalog table '{SchemaName}.{Name}' declares neither explicit Columns nor a ScanFunction to derive them from.");
}

/// <summary>One <see cref="CatalogTable.ForeignKeys"/> entry — column-NAME form (resolved against
/// <see cref="CatalogTable.Columns"/> at registration time, mirroring <see cref="Protocol.ForeignKeyInfo"/>
/// which travels column names directly, not indices).</summary>
public sealed class CatalogForeignKey
{
    public required IReadOnlyList<string> Columns { get; init; }

    public required string ReferencedTable { get; init; }

    public required IReadOnlyList<string> ReferencedColumns { get; init; }

    public string? ReferencedSchema { get; init; }
}
