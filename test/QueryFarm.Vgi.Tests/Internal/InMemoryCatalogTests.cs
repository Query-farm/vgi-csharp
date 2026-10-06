using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>DDL-capable in-memory catalogs (<see cref="Worker.RegisterInMemoryCatalog"/>): private
/// per attach, versioned by DDL, and served through every catalog RPC including
/// <c>catalog_contents</c> — the C# counterpart of vgi-python's <c>InMemoryCatalog</c>.</summary>
public class InMemoryCatalogTests
{
    private static readonly byte[] TwoColumns = SchemaIpc.WriteSchemaOnly(new Schema(
        [new Field("a", Int32Type.Default, nullable: true), new Field("b", StringType.Default, nullable: true)], metadata: null));

    private static (IVgiService Service, CatalogRegistry Registry) Service(InMemoryCatalogOptions? options = null)
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new AddValuesFunction());
        registry.RegisterInMemoryCatalog("mem", options);
        return (new VgiServiceImpl(registry), registry);
    }

    private static async Task<(byte[] Attach, CatalogAttachResult Result)> AttachAsync(IVgiService service, string name = "mem")
    {
        var result = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = name });
        return (result.AttachOpaqueData, result);
    }

    private static TableCreateRequest CreateTable(byte[] attach, string name, OnConflict onConflict = OnConflict.Error) => new()
    {
        AttachOpaqueData = attach,
        SchemaPath = ["main"],
        Name = name,
        Columns = TwoColumns,
        OnConflict = onConflict,
        NotNullConstraints = [0],
    };

    private static async Task<List<string>> TableNames(IVgiService service, byte[] attach, List<string>? path = null) =>
        (await service.CatalogSchemaContentsTablesAsync(attach, path ?? ["main"], null)).Items
            .Select(item => EmbeddedIpc.Decode<TableInfo>(item).Name).ToList();

    private static async Task<List<string>> ViewNames(IVgiService service, byte[] attach) =>
        (await service.CatalogSchemaContentsViewsAsync(attach, ["main"], null)).Items
            .Select(item => EmbeddedIpc.Decode<ViewInfo>(item).Name).ToList();

    private static async Task<List<string>> SchemaKeys(IVgiService service, byte[] attach) =>
        (await service.CatalogSchemasAsync(attach, null)).Items
            .Select(item => string.Join('.', EmbeddedIpc.Decode<SchemaInfo>(item).Path)).ToList();

    [Fact]
    public async Task Attach_IsNotFrozen_AdvertisesContents_AndStartsWithAnEmptyDefaultSchema()
    {
        var (service, registry) = Service();
        var (attach, result) = await AttachAsync(service);

        Assert.False(result.CatalogVersionFrozen);
        Assert.Equal(1, result.CatalogVersion);
        Assert.True(result.SupportsCatalogContents);
        Assert.False(result.SupportsTimeTravel);
        Assert.Contains(registry.Catalogs, info => info.Name == "mem");
        Assert.Equal(["main"], await SchemaKeys(service, attach));
        Assert.Empty(await TableNames(service, attach));
        // Exclusive: the default bucket's functions are not inherited.
        Assert.Empty((await service.CatalogSchemaContentsFunctionsAsync(attach, ["main"], SchemaObjectType.ScalarFunction, null)).Items);
    }

    [Fact]
    public async Task EveryAttach_IsPrivate_AndDetachDropsIt()
    {
        var (service, registry) = Service();
        var (first, _) = await AttachAsync(service);
        var (second, _) = await AttachAsync(service);

        await service.CatalogTableCreateAsync(CreateTable(first, "t1"));
        Assert.Equal(["t1"], await TableNames(service, first));
        Assert.Empty(await TableNames(service, second));
        Assert.Equal(2, (await service.CatalogVersionAsync(first, null)).Version);
        Assert.Equal(1, (await service.CatalogVersionAsync(second, null)).Version);

        Assert.Equal(2, registry.InMemory.AttachCount);
        await service.CatalogDetachAsync(first);
        Assert.Equal(1, registry.InMemory.AttachCount);
    }

    [Fact]
    public async Task Ddl_ChangesListings_AndMovesTheVersion()
    {
        var (service, _) = Service();
        var (attach, _) = await AttachAsync(service);

        await service.CatalogTableCreateAsync(CreateTable(attach, "t1"));
        await service.CatalogViewCreateAsync(attach, ["main"], "v1", "SELECT 1 AS x", OnConflict.Error, null);
        await service.CatalogSchemaCreateAsync(attach, ["s2"], OnConflict.Error, "second", null, null);
        Assert.Equal(4, (await service.CatalogVersionAsync(attach, null)).Version);

        var table = EmbeddedIpc.Decode<TableInfo>(Assert.Single((await service.CatalogTableGetAsync(attach, ["main"], "t1", null, null, null)).Items));
        Assert.Equal(["a", "b"], SchemaIpc.ReadSchemaOnly(table.Columns).FieldsList.Select(f => f.Name));
        Assert.Equal([0], table.NotNullConstraints);
        Assert.Equal(["main"], table.SchemaPath);
        var view = EmbeddedIpc.Decode<ViewInfo>(Assert.Single((await service.CatalogViewGetAsync(attach, ["main"], "v1", null)).Items));
        Assert.Equal("SELECT 1 AS x", view.Definition);
        Assert.Equal(["main", "s2"], await SchemaKeys(service, attach));
        var schema = EmbeddedIpc.Decode<SchemaInfo>(Assert.Single((await service.CatalogSchemaGetAsync(attach, ["s2"], null)).Items));
        Assert.Equal("second", schema.Comment);
        var main = EmbeddedIpc.Decode<SchemaInfo>(Assert.Single((await service.CatalogSchemaGetAsync(attach, ["main"], null)).Items));
        Assert.Equal(1, main.EstimatedObjectCount!["table"]);
        Assert.Equal(1, main.EstimatedObjectCount["view"]);

        await service.CatalogViewDropAsync(attach, ["main"], "v1", false, false, null);
        await service.CatalogTableDropAsync(attach, ["main"], "t1", false, false, null);
        await service.CatalogSchemaDropAsync(attach, ["s2"], false, false, null);
        Assert.Empty(await TableNames(service, attach));
        Assert.Empty(await ViewNames(service, attach));
        Assert.Equal(["main"], await SchemaKeys(service, attach));
        Assert.Equal(7, (await service.CatalogVersionAsync(attach, null)).Version);
    }

    [Fact]
    public async Task OnConflict_And_IfExists_FollowDdlSemantics()
    {
        var (service, _) = Service();
        var (attach, _) = await AttachAsync(service);
        await service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 1", OnConflict.Error, null);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 2", OnConflict.Error, null));

        // IF NOT EXISTS keeps the old definition and the version.
        await service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 2", OnConflict.Ignore, null);
        Assert.Equal(2, (await service.CatalogVersionAsync(attach, null)).Version);

        await service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 3", OnConflict.Replace, null);
        var view = EmbeddedIpc.Decode<ViewInfo>(Assert.Single((await service.CatalogViewGetAsync(attach, ["main"], "v", null)).Items));
        Assert.Equal("SELECT 3", view.Definition);
        Assert.Equal(3, (await service.CatalogVersionAsync(attach, null)).Version);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogTableDropAsync(attach, ["main"], "missing", false, false, null));
        await service.CatalogTableDropAsync(attach, ["main"], "missing", true, false, null);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            var request = CreateTable(attach, "t");
            request.SchemaPath = ["nope"];
            return service.CatalogTableCreateAsync(request);
        });

        // A non-empty schema needs CASCADE.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogSchemaDropAsync(attach, ["main"], false, false, null));
        await service.CatalogSchemaDropAsync(attach, ["main"], false, true, null);
        Assert.Empty(await SchemaKeys(service, attach));
    }

    [Fact]
    public async Task ReportsVersionFalse_AlwaysReportsZero()
    {
        var (service, _) = Service(new InMemoryCatalogOptions { ReportsVersion = false });
        var (attach, result) = await AttachAsync(service);
        Assert.Equal(0, result.CatalogVersion);

        await service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 1", OnConflict.Error, null);
        Assert.Equal(0, (await service.CatalogVersionAsync(attach, null)).Version);
        var contents = await service.CatalogContentsAsync(attach);
        Assert.Equal(0, contents.CatalogVersion);
        Assert.Null(contents.Etag);
    }

    [Fact]
    public async Task DeclarativeCatalogs_StayReadOnly()
    {
        var (service, _) = Service();
        var (attach, result) = await AttachAsync(service, "example");
        Assert.True(result.CatalogVersionFrozen);

        var error = await Assert.ThrowsAsync<CatalogReadOnlyException>(() => service.CatalogTableCreateAsync(CreateTable(attach, "t")));
        Assert.Contains("catalog is read-only", error.Message);
        await Assert.ThrowsAsync<CatalogReadOnlyException>(() =>
            service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 1", OnConflict.Error, null));
        await Assert.ThrowsAsync<CatalogReadOnlyException>(() =>
            service.CatalogSchemaCreateAsync(attach, ["s"], OnConflict.Error, null, null, null));
    }

    /// <summary>The snapshot is composed per call, so it always reflects the attach's DDL (a cached
    /// per-identity snapshot would leak one attach's objects into another's).</summary>
    [Fact]
    public async Task CatalogContents_ReflectsDdl_AndIsPerAttach()
    {
        var (service, _) = Service();
        var (first, _) = await AttachAsync(service);
        var (second, _) = await AttachAsync(service);

        Assert.Empty(Assert.Single((await service.CatalogContentsAsync(first)).Schemas).Tables);
        await service.CatalogTableCreateAsync(CreateTable(first, "t1"));

        var contents = await service.CatalogContentsAsync(first);
        Assert.Equal(2, contents.CatalogVersion);
        var entry = Assert.Single(contents.Schemas);
        Assert.Equal(["main"], entry.Path);
        Assert.Equal((await service.CatalogSchemaContentsTablesAsync(first, ["main"], null)).Items.Single(), Assert.Single(entry.Tables));
        Assert.Empty(Assert.Single((await service.CatalogContentsAsync(second)).Schemas).Tables);
    }

    [Fact]
    public async Task ContentHash_IsStableWhileUnchanged_AndMovesWithDdl()
    {
        var (service, registry) = Service();
        registry.SetCatalogContentsEtag("mem", CatalogContentsEtagMode.ContentHash);
        var (attach, _) = await AttachAsync(service);

        var before = await service.CatalogContentsAsync(attach);
        Assert.Matches("^[0-9a-f]{64}$", before.Etag!);
        Assert.True((await service.CatalogContentsAsync(attach, ifNoneMatch: before.Etag)).NotModified);

        await service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 1", OnConflict.Error, null);
        var after = await service.CatalogContentsAsync(attach, ifNoneMatch: before.Etag);
        Assert.False(after.NotModified);
        Assert.NotEqual(before.Etag, after.Etag);
        Assert.Single(Assert.Single(after.Schemas).Views);
    }

    /// <summary>The <c>gen-&lt;version&gt;</c> revalidation pattern: the handler sees the attach's
    /// live version.</summary>
    [Fact]
    public async Task Handler_SeesTheGenerationCounter()
    {
        var (service, registry) = Service();
        var builds = 0;
        registry.SetCatalogContentsHandler("mem", async request =>
        {
            var etag = $"gen-{request.CatalogVersion}";
            if (request.IfNoneMatch == etag)
            {
                return CatalogContentsResult.Unchanged(etag);
            }

            builds++;
            return await request.BuildAsync() with { Etag = etag };
        });
        var (attach, _) = await AttachAsync(service);

        var first = await service.CatalogContentsAsync(attach);
        Assert.Equal("gen-1", first.Etag);
        Assert.True((await service.CatalogContentsAsync(attach, ifNoneMatch: "gen-1")).NotModified);
        Assert.Equal(1, builds);

        await service.CatalogSchemaCreateAsync(attach, ["s2"], OnConflict.Error, null, null, null);
        var changed = await service.CatalogContentsAsync(attach, ifNoneMatch: "gen-1");
        Assert.Equal("gen-2", changed.Etag);
        Assert.Equal(2, changed.Schemas.Count);
        Assert.Equal(2, builds);
    }
}
