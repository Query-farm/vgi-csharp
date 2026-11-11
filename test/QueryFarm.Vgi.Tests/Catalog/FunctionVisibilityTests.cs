using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Catalog;

/// <summary>Protocol 3.3 <c>FunctionInfo.visibility</c>: every function a table names is listed,
/// write functions are always table-only, a scan is table-only unless
/// <see cref="CatalogTable.ExposeScan"/> or registered as an ordinary function.</summary>
public sealed class FunctionVisibilityTests
{
    private static async Task<Dictionary<string, FunctionInfo>> ListTableFunctions(CatalogRegistry registry, string attach = "")
    {
        var service = new VgiServiceImpl(registry);
        var attached = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = attach });
        var response = await service.CatalogSchemaContentsFunctionsAsync(
            attached.AttachOpaqueData, ["main"], SchemaObjectType.TableFunction, null);
        var infos = response.Items.Select(EmbeddedIpc.Decode<FunctionInfo>).ToList();
        Assert.Equal(infos.Count, infos.Select(info => info.Name).Distinct().Count()); // each listed once
        return infos.ToDictionary(info => info.Name);
    }

    private static CatalogTable Table(string name, ITableFunction? scan, ITableInOutFunction? update = null, bool exposeScan = false) => new()
    {
        Name = name,
        Columns = VisibilityScan.Columns,
        ScanFunction = scan,
        UpdateFunction = update,
        ExposeScan = exposeScan,
        RowIdColumn = update is null ? null : "rowid",
        WriteResultModes = update is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string> { [WriteOperation.Update] = WriteResultMode.Changes },
    };

    [Fact]
    public async Task TableScanAndWriteFunctionsAreListedTableOnlyByDefault()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(Table("items", new VisibilityScan("items_scan"), new VisibilityUpdate("items_update")));
        registry.RegisterTable(new VisibilityScan("plain_function"));

        var listed = await ListTableFunctions(registry);

        Assert.Equal(FunctionVisibility.TableOnly, listed["items_scan"].Visibility);
        Assert.Equal(FunctionVisibility.TableOnly, listed["items_update"].Visibility);
        Assert.Null(listed["plain_function"].Visibility);
    }

    [Fact]
    public async Task TableOnlyEntriesKeepTheirCapabilities()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(Table("items", new VisibilityScan("items_scan"), new VisibilityUpdate("items_update")));

        var listed = await ListTableFunctions(registry);

        Assert.True(listed["items_scan"].ProjectionPushdown);
        Assert.True(listed["items_update"].HasFinalize);
    }

    [Fact]
    public async Task ExposeScanListsTheScanPublic()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(Table("labels", new VisibilityScan("labels_scan"), exposeScan: true));

        Assert.Null((await ListTableFunctions(registry))["labels_scan"].Visibility);
    }

    [Fact]
    public async Task OneTableExposingASharedScanMakesItPublic()
    {
        var registry = new CatalogRegistry();
        var scan = new VisibilityScan("shared_scan");
        registry.RegisterCatalogTable(Table("a", scan));
        registry.RegisterCatalogTable(Table("b", scan, exposeScan: true));

        Assert.Null((await ListTableFunctions(registry))["shared_scan"].Visibility);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AScanAlsoRegisteredAsAnOrdinaryFunctionIsPublic(bool registerFirst)
    {
        var registry = new CatalogRegistry();
        var scan = new VisibilityScan("items_scan");
        if (registerFirst)
        {
            registry.RegisterTable(scan);
        }

        registry.RegisterCatalogTable(Table("items", scan));
        if (!registerFirst)
        {
            registry.RegisterTable(scan);
        }

        Assert.Null((await ListTableFunctions(registry))["items_scan"].Visibility);
    }

    [Fact]
    public async Task AWriteFunctionIsTableOnlyEvenWhenAlsoRegistered()
    {
        var registry = new CatalogRegistry();
        var update = new VisibilityUpdate("items_update");
        registry.RegisterTableInOut(update);
        registry.RegisterCatalogTable(Table("items", new VisibilityScan("items_scan"), update));

        Assert.Equal(FunctionVisibility.TableOnly, (await ListTableFunctions(registry))["items_update"].Visibility);
    }

    [Fact]
    public async Task ATableRegisteredAfterAListingChangesTheNextListing()
    {
        var registry = new CatalogRegistry();
        var scan = new VisibilityScan("late_scan");
        var service = new VgiServiceImpl(registry);
        var attached = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "" });
        Assert.Empty((await service.CatalogSchemaContentsFunctionsAsync(
            attached.AttachOpaqueData, ["main"], SchemaObjectType.TableFunction, null)).Items);

        registry.RegisterCatalogTable(Table("late", scan));

        var info = EmbeddedIpc.Decode<FunctionInfo>(Assert.Single((await service.CatalogSchemaContentsFunctionsAsync(
            attached.AttachOpaqueData, ["main"], SchemaObjectType.TableFunction, null)).Items));
        Assert.Equal(FunctionVisibility.TableOnly, info.Visibility);
    }

    [Fact]
    public async Task OnlyTheAttachServingTheTableHidesItsScan()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalog(new CatalogInfo { Name = "other" }, exclusive: true);
        var scan = new VisibilityScan("bucket_scan");
        registry.RegisterTable(scan, "other");
        registry.RegisterCatalogTable(Table("t", scan)); // default bucket: auto-listed, table-only

        Assert.Equal(FunctionVisibility.TableOnly, (await ListTableFunctions(registry))["bucket_scan"].Visibility);
        Assert.Null((await ListTableFunctions(registry, "other"))["bucket_scan"].Visibility);
    }

    [Fact]
    public void ExposeScanWithoutAScanFunctionIsRefused()
    {
        var registry = new CatalogRegistry();
        var table = new CatalogTable { Name = "no_scan", Columns = VisibilityScan.Columns, ExposeScan = true };

        var error = Assert.Throws<InvalidOperationException>(() => registry.RegisterCatalogTable(table));
        Assert.Contains("ExposeScan", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void VisibilityIsANullableUtf8LastField()
    {
        var schema = QueryFarm.Vgi.Tests.Generated.VgiProtocolSchemas.Records["FunctionInfo"];
        var field = schema.FieldsList[^1];
        Assert.Equal("visibility", field.Name);
        Assert.IsType<StringType>(field.DataType);
        Assert.True(field.IsNullable);
    }
}

file sealed class VisibilityScan(string name) : ITableFunction
{
    public static readonly Schema Columns = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            WriteResults.RowIdField("rowid"),
        ],
        metadata: null);

    public string Name => name;

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema => Columns;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new NullProducer();

    private sealed class NullProducer : ITableFunctionProducer
    {
        public void Produce(OutputCollector output) => output.Finish();
    }
}

file sealed class VisibilityUpdate(string name) : ITableInOutFunction
{
    public string Name => name;

    public bool HasFinalize => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new NullProcessor();

    private sealed class NullProcessor : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
        }

        public void Finalize(OutputCollector output) => output.Finish();
    }
}
