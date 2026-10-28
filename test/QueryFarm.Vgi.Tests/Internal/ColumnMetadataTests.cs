// Copyright 2025, 2026 Query Farm LLC - https://query.farm
using System.Text.Json;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;
namespace QueryFarm.Vgi.Tests.Internal;

public class ColumnMetadataTests
{
    [Fact]
    public async Task CreateAndDiscoveryPreserveMetadataAndRejectMalformedValuesBeforeEffects()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null &&
               !File.Exists(Path.Combine(directory.FullName, "conformance", "catalog-contracts.json")))
            directory = directory.Parent;
        Assert.NotNull(directory);
        using var corpus = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(directory.FullName, "conformance", "catalog-contracts.json")));
        var vectors = corpus.RootElement.GetProperty("column_metadata_schemas");
        var registry = new CatalogRegistry();
        registry.RegisterInMemoryCatalog("mem");
        IVgiService service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "mem" })).AttachOpaqueData;
        foreach (var group in vectors.EnumerateObject())
            foreach (var item in group.Value.EnumerateObject())
            {
                var raw = Convert.FromBase64String(item.Value.GetString()!);
                var request = new TableCreateRequest
                {
                    AttachOpaqueData = attach,
                    SchemaPath = ["main"],
                    Name = item.Name,
                    Columns = raw,
                    ConstraintDefinitions = []
                };
                if (group.Name == "invalid")
                {
                    Assert.ThrowsAny<Exception>(() => ColumnMetadata.ValidateIpc(raw));
                    await Assert.ThrowsAnyAsync<Exception>(() => service.CatalogTableCreateAsync(request));
                    continue;
                }
                await service.CatalogTableCreateAsync(request);
                var info = EmbeddedIpc.Decode<TableInfo>(Assert.Single(
                    (await service.CatalogTableGetAsync(attach, ["main"], item.Name, null, null, null)).Items));
                Assert.Equal(raw, info.Columns);
                ColumnMetadata.ValidateIpc(info.Columns);
                var staticRegistry = new CatalogRegistry();
                staticRegistry.RegisterCatalogTable(
                    new CatalogTable { Name = item.Name, Columns = SchemaIpc.ReadSchemaOnly(raw) });
                IVgiService read = new VgiServiceImpl(staticRegistry);
                var readAttach =
                    (await read.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
                var staticInfo = EmbeddedIpc.Decode<TableInfo>(Assert.Single(
                    (await read.CatalogTableGetAsync(readAttach, ["main"], item.Name, null, null, null)).Items));
                Assert.Equal(SchemaIpc.WriteSchemaOnly(SchemaIpc.ReadSchemaOnly(raw)), staticInfo.Columns);
            }
        Assert.Equal(4, (await service.CatalogSchemaContentsTablesAsync(attach, ["main"], null)).Items.Count);
    }
}
