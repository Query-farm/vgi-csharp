using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.ExampleWorker.Aggregate;
using QueryFarm.Vgi.ExampleWorker.Scalar;
using QueryFarm.Vgi.ExampleWorker.Table;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Client;
using QueryFarm.VgiRpc.Transport;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary><c>catalog_contents</c> (protocol 2.1.0): the whole catalog in one RPC, each item
/// byte-for-byte what the per-schema RPC returns, and the attach flag that advertises it.</summary>
public class CatalogContentsTests
{
    private static readonly Schema OneColumn = new([new Field("x", Int64Type.Default, nullable: true)], metadata: null);

    /// <summary>One of every kind this port serves, spread over a parent schema, a child schema
    /// holding the only table (the registry lists table schemas before view/macro/declared ones,
    /// so <c>catalog_schemas</c> lists the child first and the composer has to reorder), and a
    /// schema with nothing but functions.</summary>
    private static CatalogRegistry PopulatedRegistry()
    {
        var registry = new CatalogRegistry();
        registry.RegisterView(new CatalogView
        {
            Name = "nested_view",
            SchemaPath = ["data", "nested"],
            Definition = "SELECT 1 AS one",
        });
        registry.RegisterScalar(new AddValuesFunction());
        registry.RegisterTable(new SequenceFunction());
        registry.RegisterAggregate(new SumFunction("contents_sum"));
        registry.RegisterSchema("data", "Data schema", new Dictionary<string, string> { ["k"] = "v" });
        registry.RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaPath = ["data", "nested"], Columns = OneColumn });
        registry.RegisterView(new CatalogView { Name = "data_view", SchemaName = "data", Definition = "SELECT 2 AS two" });
        registry.RegisterMacro(new CatalogMacro
        {
            Name = "twice",
            SchemaName = "data",
            MacroType = MacroType.Scalar,
            Parameters = ["v"],
            Definition = "v * 2",
        });
        registry.RegisterMacro(new CatalogMacro
        {
            Name = "rows_of",
            SchemaName = "data",
            MacroType = MacroType.Table,
            Definition = "SELECT 42 AS answer",
        });
        return registry;
    }

    private static async Task<byte[]> AttachAsync(IVgiService service, string name = "example") =>
        (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = name })).AttachOpaqueData;

    private static string Key(List<string> path) => string.Join(".", path);

    private static List<SchemaContents> Decode(CatalogContentsResponse response) =>
        response.Schemas.Select(EmbeddedIpc.Decode<SchemaContents>).ToList();

    [Fact]
    public async Task OneEntryPerSchema_ParentsBeforeChildren()
    {
        IVgiService service = new VgiServiceImpl(PopulatedRegistry());
        var attach = await AttachAsync(service);

        var listed = (await service.CatalogSchemasAsync(attach, null)).Items
            .Select(item => EmbeddedIpc.Decode<SchemaInfo>(item).Path).ToList();
        var contents = Decode(await service.CatalogContentsAsync(attach));
        var paths = contents.Select(c => EmbeddedIpc.Decode<SchemaInfo>(c.Schema).Path).ToList();

        // Same set as catalog_schemas, one entry each...
        Assert.Equal(listed.Count, paths.Count);
        Assert.Equal(
            listed.Select(Key).Order(StringComparer.Ordinal),
            paths.Select(Key).Order(StringComparer.Ordinal));
        // ...the fixture really does list the child first, and the answer puts it after its parent.
        Assert.True(
            listed.FindIndex(p => p.SequenceEqual(["data", "nested"])) < listed.FindIndex(p => p.SequenceEqual(["data"])),
            "fixture should make catalog_schemas list data.nested before data");
        Assert.True(
            paths.FindIndex(p => p.SequenceEqual(["data"])) < paths.FindIndex(p => p.SequenceEqual(["data", "nested"])),
            "catalog_contents must list a parent schema before its child");
        Assert.Equal(paths.Select(p => p.Count).Order(), paths.Select(p => p.Count));
    }

    [Fact]
    public async Task EveryItem_IsByteIdenticalToThePerSchemaRpc_ForEveryKind()
    {
        IVgiService service = new VgiServiceImpl(PopulatedRegistry());
        var attach = await AttachAsync(service);

        var response = await service.CatalogContentsAsync(attach);
        Assert.Equal((await service.CatalogVersionAsync(attach, null)).Version, response.CatalogVersion);

        var schemaItems = (await service.CatalogSchemasAsync(attach, null)).Items;
        var contents = Decode(response);
        var kindsSeen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in contents)
        {
            Assert.Contains(schemaItems, item => item.SequenceEqual(entry.Schema));
            var path = EmbeddedIpc.Decode<SchemaInfo>(entry.Schema).Path;

            void Same(string kind, List<byte[]> expected, List<byte[]> actual)
            {
                Assert.Equal(expected.Count, actual.Count);
                for (var i = 0; i < expected.Count; i++)
                {
                    Assert.True(expected[i].SequenceEqual(actual[i]), $"{Key(path)} {kind}[{i}] differs");
                }

                if (actual.Count > 0)
                {
                    kindsSeen.Add(kind);
                }
            }

            Same("tables", (await service.CatalogSchemaContentsTablesAsync(attach, path, null)).Items, entry.Tables);
            Same("views", (await service.CatalogSchemaContentsViewsAsync(attach, path, null)).Items, entry.Views);
            Same("scalar_functions",
                (await service.CatalogSchemaContentsFunctionsAsync(attach, path, SchemaObjectType.ScalarFunction, null)).Items,
                entry.ScalarFunctions);
            Same("aggregate_functions",
                (await service.CatalogSchemaContentsFunctionsAsync(attach, path, SchemaObjectType.AggregateFunction, null)).Items,
                entry.AggregateFunctions);
            Same("table_functions",
                (await service.CatalogSchemaContentsFunctionsAsync(attach, path, SchemaObjectType.TableFunction, null)).Items,
                entry.TableFunctions);
            Same("scalar_macros",
                (await service.CatalogSchemaContentsMacrosAsync(attach, path, SchemaObjectType.ScalarMacro, null)).Items,
                entry.ScalarMacros);
            Same("table_macros",
                (await service.CatalogSchemaContentsMacrosAsync(attach, path, SchemaObjectType.TableMacro, null)).Items,
                entry.TableMacros);
            Assert.Empty(entry.Indexes);
        }

        // The comparison is only as good as its coverage: every kind must actually have items.
        Assert.Equal(
            ["aggregate_functions", "scalar_functions", "scalar_macros", "table_functions", "table_macros", "tables", "views"],
            kindsSeen.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task KindsCountedAsZero_AreEmptyAndNotFetched()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new AddValuesFunction());
        var counting = new CountingService(new VgiServiceImpl(registry));
        var attach = await AttachAsync(counting);

        var entry = Assert.Single(Decode(await ((IVgiService)counting).CatalogContentsAsync(attach)));

        Assert.Single(entry.ScalarFunctions);
        Assert.Empty(entry.Tables);
        Assert.Empty(entry.Views);
        Assert.Empty(entry.AggregateFunctions);
        Assert.Empty(entry.TableFunctions);
        Assert.Empty(entry.ScalarMacros);
        Assert.Empty(entry.TableMacros);
        Assert.Equal([SchemaObjectType.ScalarFunction], counting.FunctionKindsFetched);
        Assert.Equal(0, counting.OtherKindFetches);
    }

    [Fact]
    public async Task Attach_AdvertisesCatalogContents_ByDefault_AndNotWhenDisabled()
    {
        var on = new CatalogRegistry();
        Assert.True((await new VgiServiceImpl(on).CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).SupportsCatalogContents);
        Assert.True((await new VgiServiceImpl(on).CatalogAttachAsync(new CatalogAttachRequest { Name = "other" })).SupportsCatalogContents);

        var off = new CatalogRegistry { SupportsCatalogContents = false };
        Assert.False((await new VgiServiceImpl(off).CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).SupportsCatalogContents);
    }

    [Fact]
    public async Task Attach_PerAttachOverride_WinsOverTheWorkerSetting()
    {
        var registry = new CatalogRegistry
        {
            OnAttach = request => request.Name == "legacy" ? new AttachContext { SupportsCatalogContents = false } : null,
        };
        var service = new VgiServiceImpl(registry);

        Assert.False((await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "legacy" })).SupportsCatalogContents);
        Assert.True((await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "probe" })).SupportsCatalogContents);
    }

    [Fact]
    public async Task OnCatalogContents_SeesTheAttachIdentity_AndCanRefuseTheCall()
    {
        var registry = PopulatedRegistry();
        var seen = new List<string>();
        registry.OnCatalogContents = identity =>
        {
            seen.Add(identity);
            if (identity == "broken")
            {
                throw new InvalidOperationException("deliberately fails");
            }
        };
        IVgiService service = new VgiServiceImpl(registry);

        Assert.NotEmpty((await service.CatalogContentsAsync(await AttachAsync(service, "probe"))).Schemas);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await service.CatalogContentsAsync(await AttachAsync(service, "broken")));
        Assert.Equal("deliberately fails", error.Message);
        Assert.Equal(["probe", "broken"], seen);
    }

    [Fact]
    public async Task RoundTripsThroughARealClient_OverTheWorkersSocket()
    {
        var worker = new Worker()
            .CatalogName("example")
            .DefaultSchema("main")
            .RegisterScalar(new AddValuesFunction())
            .RegisterTable(new SequenceFunction())
            .RegisterSchema("data", "Data schema")
            .RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaName = "data", Columns = OneColumn });
        var path = Path.Combine(Path.GetTempPath(), $"vgi-csharp-test-{Guid.NewGuid():N}.sock");
        using var cts = new CancellationTokenSource();
        var serveTask = worker.RunUnixSocketAsync(path, idleTimeoutSeconds: 30, cts.Token);
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!File.Exists(path) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }

            using var transport = (SocketTransport)await SocketTransport.ConnectUnixAsync(path);
            var client = new RpcConnection<IVgiService>(
                transport, new RpcClientOptions { ProtocolVersion = Worker.DefaultProtocolVersion }).CreateProxy();

            var attached = await client.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
            Assert.True(attached.SupportsCatalogContents);

            var response = await client.CatalogContentsAsync(attached.AttachOpaqueData);
            var contents = Decode(response);
            var byPath = contents.ToDictionary(
                c => Key(EmbeddedIpc.Decode<SchemaInfo>(c.Schema).Path), StringComparer.Ordinal);
            Assert.Equal(["data", "main"], byPath.Keys.Order(StringComparer.Ordinal));

            var main = byPath["main"];
            Assert.Equal(
                (await client.CatalogSchemaContentsFunctionsAsync(attached.AttachOpaqueData, ["main"], SchemaObjectType.ScalarFunction, null)).Items,
                main.ScalarFunctions);
            Assert.Equal("add_values", EmbeddedIpc.Decode<FunctionInfo>(Assert.Single(main.ScalarFunctions)).Name);
            Assert.Equal("plain", EmbeddedIpc.Decode<TableInfo>(Assert.Single(byPath["data"].Tables)).Name);
        }
        finally
        {
            cts.Cancel();
            await serveTask;
        }
    }

    /// <summary>Forwards to a real service and records which per-kind RPCs the default
    /// <see cref="IVgiService.CatalogContentsAsync"/> composition issues through it.</summary>
    private sealed class CountingService(IVgiService real) : IVgiService
    {
        public List<SchemaObjectType> FunctionKindsFetched { get; } = [];

        public int OtherKindFetches { get; private set; }

        public Task<BindResponse> BindAsync(BindRequest request, VgiRpc.Server.ICallContext? ctx = null) => real.BindAsync(request, ctx);

        public Task<VgiRpc.Streaming.RpcStream<VgiRpc.Streaming.StreamState>> InitAsync(InitRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.InitAsync(request, ctx);

        public Task<TableFunctionPlanResult> TableFunctionPlanAsync(TableFunctionPlanRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableFunctionPlanAsync(request, ctx);

        public Task<TableFunctionCardinalityResult> TableFunctionCardinalityAsync(TableFunctionCardinalityRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableFunctionCardinalityAsync(request, ctx);

        public Task<AggregateUpdateResult> AggregateUpdateAsync(AggregateUpdateRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateUpdateAsync(request, ctx);

        public Task<AggregateCombineResult> AggregateCombineAsync(AggregateCombineRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateCombineAsync(request, ctx);

        public Task<AggregateFinalizeResult> AggregateFinalizeAsync(AggregateFinalizeRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateFinalizeAsync(request, ctx);

        public Task<AggregateDestructorResult> AggregateDestructorAsync(AggregateDestructorRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateDestructorAsync(request, ctx);

        public Task<AggregateBindResult> AggregateBindAsync(AggregateBindRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.AggregateBindAsync(request, ctx);

        public Task<TableBufferingProcessResult> TableBufferingProcessAsync(TableBufferingProcessRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableBufferingProcessAsync(request, ctx);

        public Task<TableBufferingCombineResult> TableBufferingCombineAsync(TableBufferingCombineRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableBufferingCombineAsync(request, ctx);

        public Task<TableBufferingDestructorResult> TableBufferingDestructorAsync(TableBufferingDestructorRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.TableBufferingDestructorAsync(request, ctx);

        public Task<CatalogAttachResult> CatalogAttachAsync(CatalogAttachRequest request, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogAttachAsync(request, ctx);

        public Task CatalogDetachAsync(byte[] attachOpaqueData, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogDetachAsync(attachOpaqueData, ctx);

        public Task<ItemsResponse> CatalogSchemasAsync(byte[] attachOpaqueData, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogSchemasAsync(attachOpaqueData, transactionOpaqueData, ctx);

        public Task<ItemsResponse> CatalogSchemaContentsFunctionsAsync(
            byte[] attachOpaqueData, List<string> path, SchemaObjectType type, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            FunctionKindsFetched.Add(type);
            return real.CatalogSchemaContentsFunctionsAsync(attachOpaqueData, path, type, transactionOpaqueData, ctx);
        }

        public Task<ItemsResponse> CatalogSchemaContentsTablesAsync(
            byte[] attachOpaqueData, List<string> path, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            OtherKindFetches++;
            return real.CatalogSchemaContentsTablesAsync(attachOpaqueData, path, transactionOpaqueData, ctx);
        }

        public Task<ItemsResponse> CatalogSchemaContentsViewsAsync(
            byte[] attachOpaqueData, List<string> path, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            OtherKindFetches++;
            return real.CatalogSchemaContentsViewsAsync(attachOpaqueData, path, transactionOpaqueData, ctx);
        }

        public Task<ItemsResponse> CatalogSchemaContentsMacrosAsync(
            byte[] attachOpaqueData, List<string> path, SchemaObjectType type, byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null)
        {
            OtherKindFetches++;
            return real.CatalogSchemaContentsMacrosAsync(attachOpaqueData, path, type, transactionOpaqueData, ctx);
        }

        public Task<ScanBranchesResult> CatalogTableScanBranchesGetAsync(
            byte[] attachOpaqueData, List<string> schemaPath, string name, string? atUnit, string? atValue,
            byte[]? transactionOpaqueData, VgiRpc.Server.ICallContext? ctx = null) =>
            real.CatalogTableScanBranchesGetAsync(attachOpaqueData, schemaPath, name, atUnit, atValue, transactionOpaqueData, ctx);
    }
}
