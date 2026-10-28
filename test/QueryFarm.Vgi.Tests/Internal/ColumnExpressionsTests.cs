// Copyright 2025, 2026 Query Farm LLC - https://query.farm
using System.Text.Json;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;
namespace QueryFarm.Vgi.Tests.Internal;

public class ColumnExpressionsTests
{
    [Fact]
    public async Task GeneratedDiscoveryPreservesCanonicalPayloadAndUnsupportedCreateHasNoEffects()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null &&
               !File.Exists(Path.Combine(directory.FullName, "conformance", "catalog-contracts.json")))
            directory = directory.Parent;
        Assert.NotNull(directory);
        using var corpus = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(directory.FullName, "conformance", "catalog-contracts.json")));
        var vectors = corpus.RootElement.GetProperty("generated_column_schemas");
        var registry = new CatalogRegistry();
        registry.RegisterInMemoryCatalog("mem");
        IVgiService service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "mem" })).AttachOpaqueData;
        foreach (var group in vectors.EnumerateObject().Where(group => group.Name is "valid" or "invalid"))
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
                    Assert.ThrowsAny<Exception>(() => ColumnExpressions.ValidateIpc(raw, ["main"]));
                    await Assert.ThrowsAnyAsync<Exception>(() => service.CatalogTableCreateAsync(request));
                    continue;
                }
                if (item.Name != "ordinary")
                    await Assert.ThrowsAnyAsync<Exception>(() => service.CatalogTableCreateAsync(request));
                else
                {
                    await service.CatalogTableCreateAsync(request);
                    var info = EmbeddedIpc.Decode<TableInfo>(Assert.Single(
                        (await service.CatalogTableGetAsync(attach, ["main"], item.Name, null, null, null)).Items));
                    Assert.Equal(raw, info.Columns);
                    ColumnExpressions.ValidateIpc(info.Columns, ["main"]);
                }
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
        Assert.Single((await service.CatalogSchemaContentsTablesAsync(attach, ["main"], null)).Items);
    }
    [Fact]
    public async Task GeneratedConveniencePreservesMetadataAndLiteralOwner()
    {
        var metadata = new Dictionary<string, string> { ["comment"] = "", ["tags"] = "{}", ["provider.note"] = "keep" };
        var columns = new Apache.Arrow.Schema([
            new Apache.Arrow.Field("n", Apache.Arrow.Types.Int64Type.Default, true),
            new Apache.Arrow.Field("doubled", Apache.Arrow.Types.Int64Type.Default, true, metadata)],
            new Dictionary<string, string> { ["provider"] = "retained" });
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "t",
            SchemaName = "raw.schema",
            Columns = columns,
            GeneratedColumns = new Dictionary<string, string> { ["doubled"] = "n * 2" }
        });
        IVgiService service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        var info = EmbeddedIpc.Decode<TableInfo>(Assert.Single((await service.CatalogTableGetAsync(attach, ["raw.schema"], "t", null, null, null)).Items));
        var readback = SchemaIpc.ReadSchemaOnly(info.Columns);
        var actual = readback.GetFieldByIndex(1).Metadata;
        Assert.Equal("", actual["comment"]); Assert.Equal("{}", actual["tags"]); Assert.Equal("keep", actual["provider.note"]);
        Assert.Equal("retained", readback.Metadata["provider"]);
        Assert.False(actual.ContainsKey("generated_expression"));
        var expression = EmbeddedIpc.Decode<SqlExpression>(Convert.FromBase64String(actual[ColumnExpressions.GeneratedKey]));
        Assert.Equal(["raw.schema"], expression.SchemaPath);
        Assert.Equal("n * 2", expression.Sql); Assert.Equal("UTC", expression.EvaluationContext.Timezone);
    }

}
