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

/// <summary>Run-time tables (<see cref="Worker.AttachTables"/>) and protocol 3.3's
/// <c>catalog_table_create</c> reply (<c>created</c>): created tables are listed and looked up
/// next to declared ones, per attach; their functions are listed table-only; the inline write
/// descriptors carry <see cref="CatalogTable.WriteArguments"/>.</summary>
public sealed class AttachTablesTests
{
    private static readonly Schema Columns = new([new Field("id", Int64Type.Default, nullable: true)], metadata: null);

    private static TableCreateRequest Create(byte[] attach, string name, OnConflict onConflict = OnConflict.Error) => new()
    {
        AttachOpaqueData = attach,
        SchemaPath = ["main"],
        Name = name,
        Columns = SchemaIpc.WriteSchemaOnly(Columns),
        OnConflict = onConflict,
        ConstraintDefinitions = [],
    };

    private static async Task<(VgiServiceImpl Service, MemoryTables Tables, byte[] Attach)> SetUp(CatalogRegistry? registry = null)
    {
        registry ??= new CatalogRegistry();
        var tables = new MemoryTables();
        registry.RegisterAttachTables(tables);
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "w" })).AttachOpaqueData;
        return (service, tables, attach);
    }

    private static async Task<List<string>> TableNames(IVgiService service, byte[] attach) =>
        (await service.CatalogSchemaContentsTablesAsync(attach, ["main"], null)).Items
            .Select(item => EmbeddedIpc.Decode<TableInfo>(item).Name).ToList();

    [Fact]
    public async Task CreateReportsWhetherThisCallCreatedTheTable()
    {
        var (service, _, attach) = await SetUp();

        Assert.True((await service.CatalogTableCreateAsync(Create(attach, "t1"))).Created);
        Assert.False((await service.CatalogTableCreateAsync(Create(attach, "t1", OnConflict.Ignore))).Created);
        Assert.True((await service.CatalogTableCreateAsync(Create(attach, "t1", OnConflict.Replace))).Created);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => service.CatalogTableCreateAsync(Create(attach, "t1")));
        Assert.Contains("already exists", error.Message);
        Assert.Equal(["t1"], await TableNames(service, attach));
    }

    [Fact]
    public async Task CreatedTablesAreServedLikeDeclaredOnes()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable { Name = "fixed", ScanFunction = new AttachTablesNamedScan("fixed_scan") });
        var (service, _, attach) = await SetUp(registry);
        await service.CatalogTableCreateAsync(Create(attach, "made"));

        Assert.Equal(["fixed", "made"], await TableNames(service, attach));
        var info = EmbeddedIpc.Decode<TableInfo>(Assert.Single(
            (await service.CatalogTableGetAsync(attach, ["main"], "made", null, null, null)).Items));
        Assert.Equal(new Dictionary<string, string> { ["insert"] = "rows" }, info.WriteResultModes);

        // One shared scan / insert function per created table, told which table by its arguments.
        var scan = EmbeddedIpc.Decode<ScanFunctionResult>(info.ScanFunction!);
        Assert.Equal(("made_scan", "made"), (scan.FunctionName, FirstArgument(scan.Arguments)));
        var insert = EmbeddedIpc.Decode<ScanFunctionResult>(info.InsertFunction!);
        Assert.Equal(("made_insert", "made"), (insert.FunctionName, FirstArgument(insert.Arguments)));

        var branches = await service.CatalogTableScanBranchesGetAsync(attach, ["main"], "made", null, null, null);
        Assert.Equal("made_scan", EmbeddedIpc.Decode<ScanBranch>(Assert.Single(branches.Branches)).FunctionName);
    }

    [Fact]
    public async Task CreatedTablesArePerAttach()
    {
        var (service, _, first) = await SetUp();
        var second = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "w" })).AttachOpaqueData;
        await service.CatalogTableCreateAsync(Create(first, "only_first"));

        Assert.Equal(["only_first"], await TableNames(service, first));
        Assert.Empty(await TableNames(service, second));
        Assert.Empty((await service.CatalogTableGetAsync(second, ["main"], "only_first", null, null, null)).Items);
    }

    [Fact]
    public async Task DropRemovesACreatedTable()
    {
        var (service, _, attach) = await SetUp();
        await service.CatalogTableCreateAsync(Create(attach, "t"));

        await service.CatalogTableDropAsync(attach, ["main"], "t", ignoreNotFound: false, cascade: false, null);
        Assert.Empty(await TableNames(service, attach));
        await service.CatalogTableDropAsync(attach, ["main"], "t", ignoreNotFound: true, cascade: false, null);
        await Assert.ThrowsAsync<VgiNotFoundException>(() =>
            service.CatalogTableDropAsync(attach, ["main"], "t", ignoreNotFound: false, cascade: false, null));
    }

    [Fact]
    public async Task ADeclaredNameIsNeverHandedToTheAttachTables()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable { Name = "fixed", ScanFunction = new AttachTablesNamedScan("fixed_scan") });
        var (service, tables, attach) = await SetUp(registry);

        Assert.False((await service.CatalogTableCreateAsync(Create(attach, "fixed", OnConflict.Ignore))).Created);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => service.CatalogTableCreateAsync(Create(attach, "fixed")));
        Assert.Contains("already exists", error.Message);
        await Assert.ThrowsAsync<CatalogReadOnlyException>(() =>
            service.CatalogTableDropAsync(attach, ["main"], "fixed", ignoreNotFound: false, cascade: false, null));
        Assert.Equal(0, tables.CreateCalls);
    }

    [Fact]
    public async Task TheirFunctionsAreListedTableOnly()
    {
        var (service, _, attach) = await SetUp();
        await service.CatalogTableCreateAsync(Create(attach, "made"));

        var listed = (await service.CatalogSchemaContentsFunctionsAsync(attach, ["main"], SchemaObjectType.TableFunction, null)).Items
            .Select(EmbeddedIpc.Decode<FunctionInfo>).ToDictionary(info => info.Name);

        Assert.Equal(FunctionVisibility.TableOnly, listed["made_scan"].Visibility);
        Assert.Equal(FunctionVisibility.TableOnly, listed["made_insert"].Visibility);
    }

    [Fact]
    public async Task ATableNamingAnUndeclaredFunctionIsRefused()
    {
        var (service, tables, attach) = await SetUp();
        tables.Stray = new CatalogTable { Name = "stray", ScanFunction = new AttachTablesNamedScan("not_declared") };

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => service.CatalogSchemaContentsTablesAsync(attach, ["main"], null));
        Assert.Contains("not_declared", error.Message);
    }

    [Fact]
    public async Task WithoutAttachTablesADeclarativeCatalogIsReadOnly()
    {
        var service = new VgiServiceImpl(new CatalogRegistry());
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "w" })).AttachOpaqueData;

        await Assert.ThrowsAsync<CatalogReadOnlyException>(() => service.CatalogTableCreateAsync(Create(attach, "t")));
        await Assert.ThrowsAsync<CatalogReadOnlyException>(() =>
            service.CatalogTableDropAsync(attach, ["main"], "t", ignoreNotFound: true, cascade: false, null));
    }

    [Fact]
    public async Task InMemoryCatalogsReportCreatedToo()
    {
        var registry = new CatalogRegistry();
        registry.RegisterInMemoryCatalog("mem");
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "mem" })).AttachOpaqueData;

        Assert.True((await service.CatalogTableCreateAsync(Create(attach, "t"))).Created);
        Assert.False((await service.CatalogTableCreateAsync(Create(attach, "t", OnConflict.Ignore))).Created);
        Assert.True((await service.CatalogTableCreateAsync(Create(attach, "t", OnConflict.Replace))).Created);
    }

    private static string? FirstArgument(byte[] arguments)
    {
        var batch = RecordBatchIpc.Read(arguments);
        Assert.Equal("arg_0", batch.Schema.GetFieldByIndex(0).Name);
        return ((StringArray)batch.Column(0)).GetString(0);
    }

    /// <summary>Created tables kept in memory per attach id, each served by one shared scan and
    /// one shared insert function.</summary>
    private sealed class MemoryTables : AttachTables
    {
        private readonly Dictionary<string, SortedDictionary<string, Schema>> _byAttach = [];
        private readonly AttachTablesNamedScan _scan = new("made_scan");
        private readonly AttachTablesNamedInsert _insert = new("made_insert");

        public int CreateCalls { get; private set; }

        public CatalogTable? Stray { get; set; }

        public override IReadOnlyList<ITableFunction> ScanFunctions => [_scan];

        public override IReadOnlyList<ITableInOutFunction> WriteFunctions => [_insert];

        public override IEnumerable<CatalogTable> Tables(AttachTablesContext context)
        {
            var tables = For(context.AttachOpaqueData).Select(entry => new CatalogTable
            {
                Name = entry.Key,
                Columns = entry.Value,
                ScanFunction = _scan,
                ScanArguments = [entry.Key],
                InsertFunction = _insert,
                WriteArguments = [entry.Key],
                WriteResultModes = new Dictionary<string, string> { [WriteOperation.Insert] = WriteResultMode.Rows },
            }).ToList();
            if (Stray is not null)
            {
                tables.Add(Stray);
            }

            return tables;
        }

        public override bool CreateTable(TableCreateContext context)
        {
            CreateCalls++;
            var tables = For(context.AttachOpaqueData);
            if (tables.ContainsKey(context.Name))
            {
                switch (context.OnConflict)
                {
                    case OnConflict.Ignore:
                        return false;
                    case OnConflict.Error:
                        throw new InvalidOperationException($"Table with name {context.Name} already exists!");
                }
            }

            tables[context.Name] = context.Columns;
            return true;
        }

        public override void DropTable(TableDropContext context)
        {
            if (!For(context.AttachOpaqueData).Remove(context.Name) && !context.IgnoreNotFound)
            {
                throw new VgiNotFoundException($"Table with name {context.Name} does not exist!");
            }
        }

        private SortedDictionary<string, Schema> For(byte[] attachOpaqueData)
        {
            var key = Convert.ToHexString(AttachData.Parse(attachOpaqueData).AttachId);
            return _byAttach.TryGetValue(key, out var tables) ? tables : _byAttach[key] = new(StringComparer.Ordinal);
        }
    }
}

internal sealed class AttachTablesNamedScan(string name) : ITableFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("table_name", StringType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("id", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Done();

    private sealed class Done : ITableFunctionProducer
    {
        public void Produce(OutputCollector output) => output.Finish();
    }
}

internal sealed class AttachTablesNamedInsert(string name) : ITableInOutFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Positional("table_name", StringType.Default), TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("id", Int64Type.Default, nullable: true)], metadata: null);

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => throw new NotSupportedException();
}
