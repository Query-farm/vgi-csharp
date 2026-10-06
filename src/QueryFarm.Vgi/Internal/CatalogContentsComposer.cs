using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Server;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Builds a <c>catalog_contents</c> answer out of a service's own per-schema catalog RPCs — the
/// body of <see cref="IVgiService.CatalogContentsAsync"/>'s default, mirroring vgi-python's
/// <c>CatalogInterface.catalog_contents</c> default.
/// </summary>
/// <remarks>
/// <para>Every item is taken verbatim from the <see cref="ItemsResponse.Items"/> the matching
/// per-schema RPC returns on the SAME service instance — <see cref="IVgiService.CatalogSchemasAsync"/>,
/// <see cref="IVgiService.CatalogSchemaContentsTablesAsync"/>, <c>…ViewsAsync</c>,
/// <c>…FunctionsAsync</c> and <c>…MacrosAsync</c> per kind — so a <c>catalog_contents</c> item is
/// byte-for-byte what the client would otherwise fetch one schema and kind at a time, and a
/// decorating service (one that rewrites a single listing) is reflected in both paths alike.</para>
/// <para>A kind the schema's <see cref="SchemaInfo.EstimatedObjectCount"/> reports as exactly
/// <c>0</c> is not fetched (the count is a hard guarantee — the client already skips those RPCs);
/// it is still sent, as an empty list, since each kind in a <see cref="SchemaContents"/> is
/// complete. A missing count means "unknown" and is fetched. This port has no
/// <c>catalog_schema_contents_indexes</c> RPC (no catalog here registers an index), so
/// <see cref="SchemaContents.Indexes"/> is always empty.</para>
/// <para>No transaction is passed: the client caches the answer for the whole attach, so it is the
/// committed catalog at <see cref="CatalogContentsResponse.CatalogVersion"/>.</para>
/// </remarks>
public static class CatalogContentsComposer
{
    /// <summary>The whole default RPC: the version, the composed snapshot, no etag (so
    /// <paramref name="ifNoneMatch"/> is ignored), shaped by <see cref="CatalogContentsResponder"/>.</summary>
    public static async Task<CatalogContentsResponse> ServeAsync(
        IVgiService service, byte[] attachOpaqueData, string? ifNoneMatch, ICallContext? ctx = null)
    {
        var version = (await service.CatalogVersionAsync(attachOpaqueData, null, ctx).ConfigureAwait(false)).Version;
        var result = await ComposeAsync(service, attachOpaqueData, ctx).ConfigureAwait(false);
        return CatalogContentsResponder.Respond(version, result, ifNoneMatch, CatalogContentsEtagMode.None);
    }

    /// <summary>Every schema and its contents, in <c>catalog_schemas</c> order, with no etag.</summary>
    public static async Task<CatalogContentsResult> ComposeAsync(
        IVgiService service, byte[] attachOpaqueData, ICallContext? ctx = null)
    {
        var schemaItems = (await service.CatalogSchemasAsync(attachOpaqueData, null, ctx).ConfigureAwait(false)).Items;

        var entries = new List<SchemaContents>(schemaItems.Count);
        foreach (var item in schemaItems)
        {
            var info = EmbeddedIpc.Decode<SchemaInfo>(item);
            var path = info.Path;
            var counts = info.EstimatedObjectCount;

            async Task<List<byte[]>> Kind(string countKey, Func<Task<ItemsResponse>> fetch) =>
                counts is not null && counts.TryGetValue(countKey, out var count) && count == 0
                    ? []
                    : (await fetch().ConfigureAwait(false)).Items;

            entries.Add(new SchemaContents
            {
                Path = [.. path],
                Schema = item,
                Tables = await Kind("table", () =>
                    service.CatalogSchemaContentsTablesAsync(attachOpaqueData, path, null, ctx)).ConfigureAwait(false),
                Views = await Kind("view", () =>
                    service.CatalogSchemaContentsViewsAsync(attachOpaqueData, path, null, ctx)).ConfigureAwait(false),
                ScalarFunctions = await Kind("scalar_function", () =>
                    service.CatalogSchemaContentsFunctionsAsync(attachOpaqueData, path, SchemaObjectType.ScalarFunction, null, ctx)).ConfigureAwait(false),
                AggregateFunctions = await Kind("aggregate_function", () =>
                    service.CatalogSchemaContentsFunctionsAsync(attachOpaqueData, path, SchemaObjectType.AggregateFunction, null, ctx)).ConfigureAwait(false),
                TableFunctions = await Kind("table_function", () =>
                    service.CatalogSchemaContentsFunctionsAsync(attachOpaqueData, path, SchemaObjectType.TableFunction, null, ctx)).ConfigureAwait(false),
                ScalarMacros = await Kind("macro", () =>
                    service.CatalogSchemaContentsMacrosAsync(attachOpaqueData, path, SchemaObjectType.ScalarMacro, null, ctx)).ConfigureAwait(false),
                TableMacros = await Kind("macro", () =>
                    service.CatalogSchemaContentsMacrosAsync(attachOpaqueData, path, SchemaObjectType.TableMacro, null, ctx)).ConfigureAwait(false),
                Indexes = [],
            });
        }

        return new CatalogContentsResult { Schemas = entries };
    }
}
