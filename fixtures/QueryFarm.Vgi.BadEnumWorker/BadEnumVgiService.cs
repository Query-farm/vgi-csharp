using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Server;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.BadEnumWorker;

/// <summary>
/// Wraps a real <see cref="VgiServiceImpl"/> and forwards every <see cref="IVgiService"/> member to
/// it UNCHANGED except <see cref="CatalogSchemaContentsFunctionsAsync"/> — this is the seam
/// <c>bad_enum.test</c> needs: <c>VgiServiceImpl</c> is sealed (can't subclass) and
/// <c>Worker</c>'s hosting is hardcoded to construct one directly (no override hook), so a fixture
/// that needs to corrupt exactly one discovery response bypasses <c>Worker</c> entirely and hosts
/// this decorator instead (see <c>Program.cs</c>).
///
/// Every member below MUST forward explicitly, even the ones with a default interface-method body:
/// <c>RpcServer</c> dispatches by reflecting <c>typeof(IVgiService)</c>'s methods against
/// whatever instance it's given, so a member this class does NOT declare falls through to
/// <c>IVgiService</c>'s OWN default (an empty/no-op stub) — NOT to <c>_real</c>'s override of it —
/// silently losing <c>VgiServiceImpl</c>'s real behavior for that RPC.
/// </summary>
internal sealed class BadEnumVgiService(IVgiService real) : IVgiService
{
    public Task<BindResponse> BindAsync(BindRequest request, ICallContext? ctx = null) =>
        real.BindAsync(request, ctx);

    public Task<RpcStream<StreamState>> InitAsync(InitRequest request, ICallContext? ctx = null) =>
        real.InitAsync(request, ctx);

    public Task<TableFunctionPlanResult> TableFunctionPlanAsync(TableFunctionPlanRequest request, ICallContext? ctx = null) =>
        real.TableFunctionPlanAsync(request, ctx);

    public Task<TableFunctionCardinalityResult> TableFunctionCardinalityAsync(TableFunctionCardinalityRequest request, ICallContext? ctx = null) =>
        real.TableFunctionCardinalityAsync(request, ctx);

    public Task<byte[]?> TableFunctionStatisticsAsync(TableFunctionStatisticsRequest request, ICallContext? ctx = null) =>
        real.TableFunctionStatisticsAsync(request, ctx);

    public Task<TableFunctionDynamicToStringResult> TableFunctionDynamicToStringAsync(
        TableFunctionDynamicToStringRequest request, ICallContext? ctx = null) =>
        real.TableFunctionDynamicToStringAsync(request, ctx);

    public Task<byte[]?> CatalogTableColumnStatisticsGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnStatisticsGetAsync(attachOpaqueData, schemaName, name, transactionOpaqueData, ctx);

    public Task<TableBufferingProcessResult> TableBufferingProcessAsync(TableBufferingProcessRequest request, ICallContext? ctx = null) =>
        real.TableBufferingProcessAsync(request, ctx);

    public Task<TableBufferingCombineResult> TableBufferingCombineAsync(TableBufferingCombineRequest request, ICallContext? ctx = null) =>
        real.TableBufferingCombineAsync(request, ctx);

    public Task<TableBufferingDestructorResult> TableBufferingDestructorAsync(TableBufferingDestructorRequest request, ICallContext? ctx = null) =>
        real.TableBufferingDestructorAsync(request, ctx);

    public Task<AggregateBindResult> AggregateBindAsync(AggregateBindRequest request, ICallContext? ctx = null) =>
        real.AggregateBindAsync(request, ctx);

    public Task<AggregateUpdateResult> AggregateUpdateAsync(AggregateUpdateRequest request, ICallContext? ctx = null) =>
        real.AggregateUpdateAsync(request, ctx);

    public Task<AggregateCombineResult> AggregateCombineAsync(AggregateCombineRequest request, ICallContext? ctx = null) =>
        real.AggregateCombineAsync(request, ctx);

    public Task<AggregateFinalizeResult> AggregateFinalizeAsync(AggregateFinalizeRequest request, ICallContext? ctx = null) =>
        real.AggregateFinalizeAsync(request, ctx);

    public Task<AggregateDestructorResult> AggregateDestructorAsync(AggregateDestructorRequest request, ICallContext? ctx = null) =>
        real.AggregateDestructorAsync(request, ctx);

    public Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request, ICallContext? ctx = null) =>
        real.CatalogAttachAsync(request, ctx);

    public Task CatalogDetachAsync(byte[] attachOpaqueData, ICallContext? ctx = null) =>
        real.CatalogDetachAsync(attachOpaqueData, ctx);

    public Task<ItemsResponse> CatalogSchemasAsync(byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogSchemasAsync(attachOpaqueData, transactionOpaqueData, ctx);

    /// <summary>The one override: builds the REAL response (so every field byte-for-byte matches
    /// what <c>VgiServiceImpl</c> would have sent), then re-encodes the <c>double</c> function's
    /// item with a corrupted <c>null_handling</c> — see <see cref="BadEnumFunctionInfoEncoder"/>.</summary>
    public async Task<ItemsResponse> CatalogSchemaContentsFunctionsAsync(
        byte[] attachOpaqueData, string name, SchemaObjectType type, byte[]? transactionOpaqueData, ICallContext? ctx = null)
    {
        var response = await real.CatalogSchemaContentsFunctionsAsync(attachOpaqueData, name, type, transactionOpaqueData, ctx);
        var patched = response.Items.Select(itemBytes =>
        {
            var info = EmbeddedIpc.Decode<FunctionInfo>(itemBytes);
            return info.Name == "double" ? BadEnumFunctionInfoEncoder.Encode(info) : itemBytes;
        }).ToList();
        return new ItemsResponse { Items = patched };
    }

    public Task<ItemsResponse> CatalogCatalogsAsync(ICallContext? ctx = null) =>
        real.CatalogCatalogsAsync(ctx);

    public Task<ItemsResponse> CatalogSchemaContentsTablesAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogSchemaContentsTablesAsync(attachOpaqueData, name, transactionOpaqueData, ctx);

    public Task<ItemsResponse> CatalogSchemaContentsViewsAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogSchemaContentsViewsAsync(attachOpaqueData, name, transactionOpaqueData, ctx);

    public Task<ItemsResponse> CatalogSchemaGetAsync(
        byte[] attachOpaqueData, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogSchemaGetAsync(attachOpaqueData, name, transactionOpaqueData, ctx);

    public Task<ItemsResponse> CatalogTableGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? atUnit, string? atValue,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableGetAsync(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData, ctx);

    public Task<ScanBranchesResult> CatalogTableScanBranchesGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? atUnit, string? atValue,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableScanBranchesGetAsync(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData, ctx);

    public Task<ItemsResponse> CatalogViewGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogViewGetAsync(attachOpaqueData, schemaName, name, transactionOpaqueData, ctx);

    public Task<ItemsResponse> CatalogSchemaContentsMacrosAsync(
        byte[] attachOpaqueData, string name, SchemaObjectType type, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogSchemaContentsMacrosAsync(attachOpaqueData, name, type, transactionOpaqueData, ctx);

    public Task<ItemsResponse> CatalogMacroGetAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogMacroGetAsync(attachOpaqueData, schemaName, name, transactionOpaqueData, ctx);

    public Task<ItemsResponse> CatalogCopyFromFormatsAsync(
        byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogCopyFromFormatsAsync(attachOpaqueData, transactionOpaqueData, ctx);

    public Task<CatalogVersionResponse> CatalogVersionAsync(
        byte[] attachOpaqueData, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogVersionAsync(attachOpaqueData, transactionOpaqueData, ctx);

    public Task<TransactionBeginResponse> CatalogTransactionBeginAsync(byte[] attachOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTransactionBeginAsync(attachOpaqueData, ctx);

    public Task CatalogTransactionCommitAsync(byte[] attachOpaqueData, byte[] transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTransactionCommitAsync(attachOpaqueData, transactionOpaqueData, ctx);

    public Task CatalogTransactionRollbackAsync(byte[] attachOpaqueData, byte[] transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTransactionRollbackAsync(attachOpaqueData, transactionOpaqueData, ctx);

    // -------------------------------------------------------------------------------------------
    // Catalog DDL — this fixture's catalog is as read-only as ExampleWorker's, so `real` never
    // overrides any of these either; they still forward explicitly (see the class doc comment for
    // why relying on the interface default here would happen to also be correct, but forwarding is
    // what stays correct if `real` ever does start overriding one).
    // -------------------------------------------------------------------------------------------

    public Task CatalogSchemaCreateAsync(
        byte[] attachOpaqueData, string name, OnConflict onConflict, string? comment, Dictionary<string, string>? tags,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogSchemaCreateAsync(attachOpaqueData, name, onConflict, comment, tags, transactionOpaqueData, ctx);

    public Task CatalogSchemaDropAsync(
        byte[] attachOpaqueData, string name, bool ignoreNotFound, bool cascade, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogSchemaDropAsync(attachOpaqueData, name, ignoreNotFound, cascade, transactionOpaqueData, ctx);

    public Task CatalogTableCreateAsync(TableCreateRequest request, ICallContext? ctx = null) =>
        real.CatalogTableCreateAsync(request, ctx);

    public Task CatalogTableDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, bool ignoreNotFound, bool cascade,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableDropAsync(attachOpaqueData, schemaName, name, ignoreNotFound, cascade, transactionOpaqueData, ctx);

    public Task CatalogTableRenameAsync(
        byte[] attachOpaqueData, string schemaName, string name, string newName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableRenameAsync(attachOpaqueData, schemaName, name, newName, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableCommentSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? comment, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableCommentSetAsync(attachOpaqueData, schemaName, name, comment, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableColumnAddAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[] columnDefinition, bool ignoreNotFound,
        bool ifColumnNotExists, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnAddAsync(attachOpaqueData, schemaName, name, columnDefinition, ignoreNotFound, ifColumnNotExists, transactionOpaqueData, ctx);

    public Task CatalogTableColumnDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        bool ifColumnExists, bool cascade, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnDropAsync(attachOpaqueData, schemaName, name, columnName, ignoreNotFound, ifColumnExists, cascade, transactionOpaqueData, ctx);

    public Task CatalogTableColumnRenameAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, string newColumnName,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnRenameAsync(attachOpaqueData, schemaName, name, columnName, newColumnName, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableColumnCommentSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, string? comment,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnCommentSetAsync(attachOpaqueData, schemaName, name, columnName, comment, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableColumnDefaultSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, string expression,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnDefaultSetAsync(attachOpaqueData, schemaName, name, columnName, expression, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableColumnDefaultDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnDefaultDropAsync(attachOpaqueData, schemaName, name, columnName, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableColumnTypeChangeAsync(
        byte[] attachOpaqueData, string schemaName, string name, byte[] columnDefinition, string? expression,
        bool ignoreNotFound, byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableColumnTypeChangeAsync(attachOpaqueData, schemaName, name, columnDefinition, expression, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableNotNullSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableNotNullSetAsync(attachOpaqueData, schemaName, name, columnName, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogTableNotNullDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, string columnName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogTableNotNullDropAsync(attachOpaqueData, schemaName, name, columnName, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogViewCreateAsync(
        byte[] attachOpaqueData, string schemaName, string name, string definition, OnConflict onConflict,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogViewCreateAsync(attachOpaqueData, schemaName, name, definition, onConflict, transactionOpaqueData, ctx);

    public Task CatalogViewDropAsync(
        byte[] attachOpaqueData, string schemaName, string name, bool ignoreNotFound, bool cascade,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogViewDropAsync(attachOpaqueData, schemaName, name, ignoreNotFound, cascade, transactionOpaqueData, ctx);

    public Task CatalogViewRenameAsync(
        byte[] attachOpaqueData, string schemaName, string name, string newName, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogViewRenameAsync(attachOpaqueData, schemaName, name, newName, ignoreNotFound, transactionOpaqueData, ctx);

    public Task CatalogViewCommentSetAsync(
        byte[] attachOpaqueData, string schemaName, string name, string? comment, bool ignoreNotFound,
        byte[]? transactionOpaqueData, ICallContext? ctx = null) =>
        real.CatalogViewCommentSetAsync(attachOpaqueData, schemaName, name, comment, ignoreNotFound, transactionOpaqueData, ctx);
}
