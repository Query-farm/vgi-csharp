using QueryFarm.Vgi.Internal;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.Protocol;

/// <summary>
/// The VGI RPC surface a worker serves. Scoped down for M1 to just the scalar-function execution
/// path plus the catalog surface a plain <c>ATTACH ... (TYPE vgi, ...)</c> walks before it can
/// resolve a scalar function call — matching the file's role in the M1 milestone plan. The full
/// ~35-method surface (table/aggregate/table-in-out functions, DDL, transactions, time travel,
/// etc.) is deferred to later milestones (M2+); a client calling a method not declared here gets
/// a normal <c>MethodNotImplementedException</c>, not a crash.
///
/// Two wire-shape conventions coexist (mirrors vgi-java's own <c>VgiService</c> doc comment):
/// <list type="bullet">
/// <item><b>Packed</b> — a single dataclass-equivalent parameter, auto-embedded as IPC-in-
/// <c>binary</c> by <c>SchemaDerivation</c>/<c>ValueCodec</c> (<see cref="BindAsync"/>,
/// <see cref="InitAsync"/>, <see cref="CatalogAttachAsync"/>).</item>
/// <item><b>Flat</b> — parameters map 1:1 to wire columns by snake_case name (everything else
/// here).</item>
/// </list>
///
/// Every default-interface-method body below returns a safe, do-nothing/empty answer — a worker
/// that registers no table/view/macro content is unaffected by them ever being invoked.
/// </summary>
public interface IVgiService
{
    Task<BindResponse> BindAsync(BindRequest request, ICallContext? ctx = null);

    Task<RpcStream<StreamState>> InitAsync(InitRequest request, ICallContext? ctx = null);

    /// <summary>Scan planning (splits) — see <see cref="Table.ITableFunction.Plan"/>. Only ever
    /// called by the C++ client when a table function's catalog metadata advertises
    /// <c>supports_splits=true</c>; every other function's scan path never issues this RPC at all,
    /// so this is purely additive over the M3 table-function surface.</summary>
    Task<TableFunctionPlanResult> TableFunctionPlanAsync(TableFunctionPlanRequest request, ICallContext? ctx = null);

    /// <summary>Best-effort cardinality estimate — see <see cref="Table.ITableFunction.Cardinality"/>.
    /// The C++ client treats any failure here as non-critical and continues with "unknown".</summary>
    Task<TableFunctionCardinalityResult> TableFunctionCardinalityAsync(TableFunctionCardinalityRequest request, ICallContext? ctx = null);

    /// <summary>Per-output-column statistics for a table function call — see
    /// <see cref="Table.ITableFunction.Statistics"/>. Only consulted when the scanned catalog table
    /// (if any) declined to answer <see cref="CatalogTableColumnStatisticsGetAsync"/> for a given
    /// column (see <c>VgiTableFunctionStatistics</c>'s fallback). "Dynamic" response — no fixed
    /// wire schema; <see langword="null"/> means "unknown", same as returning no rows.</summary>
    Task<byte[]?> TableFunctionStatisticsAsync(TableFunctionStatisticsRequest request, ICallContext? ctx = null) =>
        Task.FromResult<byte[]?>(null);

    /// <summary>Per-parallel-scan-thread diagnostics surfaced under <c>EXPLAIN ANALYZE</c> — see
    /// <see cref="Table.ITableFunction.DynamicToString"/>. Fired once per thread at end-of-stream;
    /// the C++ client swallows any failure so a broken implementation never aborts the query.</summary>
    Task<TableFunctionDynamicToStringResult> TableFunctionDynamicToStringAsync(
        TableFunctionDynamicToStringRequest request, ICallContext? ctx = null) =>
        Task.FromResult(new TableFunctionDynamicToStringResult());

    /// <summary>Per-column statistics for a real catalog table — see
    /// <see cref="Catalog.CatalogTable.Statistics"/>. Only called when
    /// <see cref="TableInfo.SupportsColumnStatistics"/> is true and the table's
    /// <see cref="TableInfo.ColumnStatistics"/> wasn't inlined. "Dynamic" response (same raw
    /// <see cref="Internal.ColumnStatisticsCodec"/> wire shape as <see cref="TableFunctionStatisticsAsync"/>)
    /// — attach a <c>cache_max_age_seconds</c> custom_metadata entry on the returned batch to cap
    /// how long the C++ side may cache the answer; absent means no TTL cap.</summary>
    Task<byte[]?> CatalogTableColumnStatisticsGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult<byte[]?>(null);

    /// <summary>Table-buffering Sink phase, per input batch — see <see cref="VgiInitPhase.TableBuffering"/>'s
    /// doc comment for why this (and its two siblings below) is a standalone unary RPC rather than
    /// part of the streaming exchange the connection's own <c>init(phase=TABLE_BUFFERING)</c> opened.</summary>
    Task<TableBufferingProcessResult> TableBufferingProcessAsync(TableBufferingProcessRequest request, ICallContext? ctx = null);

    /// <summary>Table-buffering Combine phase — once per query, after every Sink call completes.</summary>
    Task<TableBufferingCombineResult> TableBufferingCombineAsync(TableBufferingCombineRequest request, ICallContext? ctx = null);

    /// <summary>Best-effort table-buffering cleanup after the Source phase completes.</summary>
    Task<TableBufferingDestructorResult> TableBufferingDestructorAsync(TableBufferingDestructorRequest request, ICallContext? ctx = null);

    /// <summary>Aggregate bind — once per bound call site; see <see cref="Aggregate.IAggregateFunction"/>.</summary>
    Task<AggregateBindResult> AggregateBindAsync(AggregateBindRequest request, ICallContext? ctx = null);

    /// <summary>Aggregate update — once per input batch.</summary>
    Task<AggregateUpdateResult> AggregateUpdateAsync(AggregateUpdateRequest request, ICallContext? ctx = null);

    /// <summary>Aggregate combine — merges two groups' partial state.</summary>
    Task<AggregateCombineResult> AggregateCombineAsync(AggregateCombineRequest request, ICallContext? ctx = null);

    /// <summary>Aggregate finalize — produces one output row per requested group id.</summary>
    Task<AggregateFinalizeResult> AggregateFinalizeAsync(AggregateFinalizeRequest request, ICallContext? ctx = null);

    /// <summary>Best-effort aggregate cleanup — frees ALL state this execution ever stored.</summary>
    Task<AggregateDestructorResult> AggregateDestructorAsync(AggregateDestructorRequest request, ICallContext? ctx = null);

    Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request, ICallContext? ctx = null);

    Task CatalogDetachAsync(byte[] attachOpaqueData, ICallContext? ctx = null);

    Task<ItemsResponse> CatalogSchemasAsync(byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null);

    Task<ItemsResponse> CatalogSchemaContentsFunctionsAsync(
        byte[] attachOpaqueData, string name, SchemaObjectType type, byte[]? transactionOpaqueData, ICallContext? ctx = null);

    Task<ItemsResponse> CatalogCatalogsAsync(ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    Task<ItemsResponse> CatalogSchemaContentsTablesAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    Task<ItemsResponse> CatalogSchemaContentsViewsAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    /// <summary>Zero-or-one-item lookup (same <see cref="ItemsResponse"/> shape as every other
    /// catalog-discovery RPC — "not found" is an empty list, not an error) for a single schema by
    /// name.</summary>
    Task<ItemsResponse> CatalogSchemaGetAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    /// <summary>Zero-or-one-item lookup for a single table by <c>(schemaName, name)</c>. <c>atUnit</c>/
    /// <c>atValue</c> are the time-travel <c>AT (VERSION =&gt; ...)</c>/<c>AT (TIMESTAMP =&gt; ...)</c>
    /// clause — a worker with no time-travel support (every table this worker registers) ignores
    /// them and returns the current/only version.</summary>
    Task<ItemsResponse> CatalogTableGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? atUnit, string? atValue,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    /// <summary>Resolves a table's scan as a list of independent <see cref="ScanBranch"/>es (see
    /// <see cref="ScanBranchesResult"/>'s doc comment) — called for EVERY VGI table (not only
    /// multi-branch ones), both to actually resolve a multi-branch scan and by the
    /// <c>vgi_table_branches()</c> diagnostic against a plain single-function table. Same
    /// <c>atUnit</c>/<c>atValue</c> time-travel parameters as <see cref="CatalogTableGetAsync"/>.</summary>
    Task<ScanBranchesResult> CatalogTableScanBranchesGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? atUnit, string? atValue,
        byte[]? transactionOpaqueData, ICallContext? ctx = null);

    /// <summary>Zero-or-one-item lookup for a single view by <c>(schemaName, name)</c>.</summary>
    Task<ItemsResponse> CatalogViewGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    Task<ItemsResponse> CatalogSchemaContentsMacrosAsync(
        byte[] attachOpaqueData, string name, SchemaObjectType type, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    /// <summary>Zero-or-one-item lookup for a single macro by <c>(schemaName, name)</c>.</summary>
    Task<ItemsResponse> CatalogMacroGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    Task<ItemsResponse> CatalogCopyFromFormatsAsync(
        byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new ItemsResponse());

    Task<CatalogVersionResponse> CatalogVersionAsync(
        byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new CatalogVersionResponse { Version = 1 });

    Task<TransactionBeginResponse> CatalogTransactionBeginAsync(byte[] attachOpaqueData, ICallContext? ctx = null) =>
        Task.FromResult(new TransactionBeginResponse());

    Task CatalogTransactionCommitAsync(byte[] attachOpaqueData, byte[] transactionOpaqueData, ICallContext? ctx = null) =>
        Task.CompletedTask;

    Task CatalogTransactionRollbackAsync(byte[] attachOpaqueData, byte[] transactionOpaqueData, ICallContext? ctx = null) =>
        Task.CompletedTask;

    // ------------------------------------------------------------------------------------------
    // Catalog DDL. None of this worker's catalogs support runtime schema/table/view mutation — a
    // declarative Worker.RegisterCatalogTable/RegisterView call at startup is the only way to
    // populate one — so every DDL RPC's default body throws CatalogReadOnlyException, matching a
    // real read-only vgi-python CatalogReadOnlyError. attach/ddl_wire_contract.test pins this
    // exact behavior (and, more importantly, pins the WIRE SHAPE below byte-for-byte against the
    // generated Catalog*ParamsSchema()s — see that test's own doc comment for why a hand-coded C++
    // params builder and this interface's derived schema can drift independently of each other).
    // ------------------------------------------------------------------------------------------

    Task CatalogSchemaCreateAsync(
        byte[] attachOpaqueData, string name, OnConflict onConflict, string? comment, Dictionary<string, string>? tags,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_schema_create");

    Task CatalogSchemaDropAsync(
        byte[] attachOpaqueData, string name, bool ignoreNotFound, bool cascade, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_schema_drop");

    Task CatalogTableCreateAsync(TableCreateRequest request, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_create");

    Task CatalogTableDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, bool ignoreNotFound, bool cascade,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_drop");

    Task CatalogTableRenameAsync(
        byte[] attachOpaqueData, string schemaName, string name, string newName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_rename");

    Task CatalogTableCommentSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? comment, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_comment_set");

    Task CatalogTableColumnAddAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[] columnDefinition, bool ignoreNotFound,
        bool ifColumnNotExists, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_column_add");

    Task CatalogTableColumnDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        bool ifColumnExists, bool cascade, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_column_drop");

    Task CatalogTableColumnRenameAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, string newColumnName,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_column_rename");

    Task CatalogTableColumnCommentSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, string? comment,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_column_comment_set");

    Task CatalogTableColumnDefaultSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, string expression,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_column_default_set");

    Task CatalogTableColumnDefaultDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_column_default_drop");

    Task CatalogTableColumnTypeChangeAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[] columnDefinition, string? expression,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_column_type_change");

    Task CatalogTableNotNullSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_not_null_set");

    Task CatalogTableNotNullDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_table_not_null_drop");

    Task CatalogViewCreateAsync(
        byte[] attachOpaqueData, string schemaName, string name, string definition, OnConflict onConflict,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_view_create");

    Task CatalogViewDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, bool ignoreNotFound, bool cascade,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_view_drop");

    Task CatalogViewRenameAsync(
        byte[] attachOpaqueData, string schemaName, string name, string newName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_view_rename");

    Task CatalogViewCommentSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? comment, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        throw new CatalogReadOnlyException("catalog_view_comment_set");
}
