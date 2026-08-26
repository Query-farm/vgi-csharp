using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// Covers this milestone's time-travel additions: <see cref="VgiServiceImpl.CatalogAttachAsync"/>
/// now advertises <see cref="CatalogAttachResult.SupportsTimeTravel"/>; <see cref="VgiServiceImpl.CatalogTableGetAsync"/>
/// refuses an <c>AT (...)</c> clause for a table that doesn't opt in
/// (<see cref="CatalogTable.SupportsTimeTravel"/>), passes multi-branch tables through unchanged
/// (leaving the C++ extension's own multi-branch-specific refusal to fire), and applies
/// <see cref="CatalogTable.ResolveAtClause"/> when present; <see cref="VgiServiceImpl.CatalogTableScanBranchesGetAsync"/>
/// applies <see cref="CatalogTable.ResolveScanArguments"/> only when an AT clause is present, and
/// otherwise ships <see cref="CatalogTable.ScanArguments"/>/<see cref="CatalogTable.ScanNamedArguments"/>
/// unchanged (both for the "no AT" case and for a table that declares neither hook).
/// </summary>
public class TimeTravelCatalogTests
{
    private static VgiServiceImpl NewService(CatalogRegistry registry) => new(registry);

    private static readonly Schema OneColumnSchema = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    [Fact]
    public async Task CatalogAttach_AdvertisesSupportsTimeTravel()
    {
        var service = NewService(new CatalogRegistry());

        var result = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        Assert.True(result.SupportsTimeTravel);
    }

    [Fact]
    public async Task CatalogTableGet_NoAtClause_ReturnsTheTableUnchanged_EvenWhenTimeTravelIsUnsupported()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaName = "data", Columns = OneColumnSchema });
        var service = NewService(registry);

        var result = await service.CatalogTableGetAsync([], "data", "plain", null, null, null);

        Assert.Single(result.Items);
    }

    [Fact]
    public async Task CatalogTableGet_AtClause_RefusesATableThatDoesNotSupportTimeTravel()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaName = "data", Columns = OneColumnSchema });
        var service = NewService(registry);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.CatalogTableGetAsync([], "data", "plain", "VERSION", "1", null));

        Assert.Contains("does not support time travel", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CatalogTableGet_AtClause_MultiBranchTable_PassesThroughUnchanged_NoRefusal()
    {
        // Multi-branch tables never opt into SupportsTimeTravel, but catalog_table_get must NOT
        // refuse them itself — the C++ extension's own multi-branch-specific AT refusal (a
        // different, more precise error message) fires once it resolves the scan via
        // catalog_table_scan_branches_get. This worker refusing first would surface the wrong
        // error entirely (see catalog/multi_branch_scan.test).
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "multi",
            SchemaName = "data",
            Columns = OneColumnSchema,
            Branches = [new ScanBranchSpec { FunctionName = "sequence", PositionalArguments = [50L] }],
        });
        var service = NewService(registry);

        var result = await service.CatalogTableGetAsync([], "data", "multi", "VERSION", "1", null);

        Assert.Single(result.Items);
        var table = EmbeddedIpc.Decode<TableInfo>(result.Items[0]);
        Assert.Equal("multi", table.Name);
    }

    [Fact]
    public async Task CatalogTableGet_AtClause_TableSupportsTimeTravel_NoResolver_ReturnsUnchanged()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "tt",
            SchemaName = "data",
            Columns = OneColumnSchema,
            SupportsTimeTravel = true,
        });
        var service = NewService(registry);

        var result = await service.CatalogTableGetAsync([], "data", "tt", "VERSION", "1", null);

        Assert.Single(result.Items);
        var table = EmbeddedIpc.Decode<TableInfo>(result.Items[0]);
        Assert.Equal("tt", table.Name);
    }

    [Fact]
    public async Task CatalogTableGet_AtClause_AppliesResolveAtClause_ReturningAVariantWithDifferentColumns()
    {
        var v1Schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], metadata: null);
        var v2Schema = new Schema(
            [
                new Field("id", Int64Type.Default, nullable: false),
                new Field("score", DoubleType.Default, nullable: true),
            ],
            metadata: null);

        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "versioned",
            SchemaName = "data",
            Columns = v2Schema,
            SupportsTimeTravel = true,
            ResolveAtClause = (atUnit, atValue) =>
            {
                Assert.Equal("VERSION", atUnit);
                var version = int.Parse(atValue);
                return version switch
                {
                    1 => new CatalogTable { Name = "versioned", SchemaName = "data", Columns = v1Schema },
                    2 => new CatalogTable { Name = "versioned", SchemaName = "data", Columns = v2Schema },
                    _ => throw new InvalidOperationException($"Unknown version: {version}"),
                };
            },
        });
        var service = NewService(registry);

        var v1Result = await service.CatalogTableGetAsync([], "data", "versioned", "VERSION", "1", null);
        var v1Table = EmbeddedIpc.Decode<TableInfo>(v1Result.Items[0]);
        Assert.Single(SchemaIpc.ReadSchemaOnly(v1Table.Columns).FieldsList);

        var v2Result = await service.CatalogTableGetAsync([], "data", "versioned", "VERSION", "2", null);
        var v2Table = EmbeddedIpc.Decode<TableInfo>(v2Result.Items[0]);
        Assert.Equal(2, SchemaIpc.ReadSchemaOnly(v2Table.Columns).FieldsList.Count);
    }

    [Fact]
    public async Task CatalogTableGet_AtClause_ResolveAtClauseThrows_PropagatesAsTheRaisedException()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "versioned",
            SchemaName = "data",
            Columns = OneColumnSchema,
            SupportsTimeTravel = true,
            ResolveAtClause = (_, atValue) => throw new InvalidOperationException($"Unknown version: {atValue}"),
        });
        var service = NewService(registry);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.CatalogTableGetAsync([], "data", "versioned", "VERSION", "99", null));

        Assert.Contains("Unknown version: 99", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CatalogTableScanBranchesGet_NoAtClause_ShipsTheTablesOwnScanArgumentsUnchanged()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "cols",
            SchemaName = "data",
            Columns = OneColumnSchema,
            ScanFunction = new StubScanFunction("cols_scan"),
            ScanArguments = [3L],
            InlineScanFunction = false,
            SupportsTimeTravel = true,
            ResolveScanArguments = (_, _) => throw new InvalidOperationException("must not be called without an AT clause"),
        });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], "data", "cols", null, null, null);

        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal("cols_scan", branch.FunctionName);
        Assert.NotEmpty(branch.Arguments);
    }

    [Fact]
    public async Task CatalogTableScanBranchesGet_AtClause_AppliesResolveScanArguments()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "cols",
            SchemaName = "data",
            Columns = OneColumnSchema,
            ScanFunction = new StubScanFunction("cols_scan"),
            ScanArguments = [3L],
            InlineScanFunction = false,
            SupportsTimeTravel = true,
            ResolveScanArguments = (atUnit, atValue) =>
            {
                Assert.Equal("VERSION", atUnit);
                IReadOnlyList<object?> positional = [(long)int.Parse(atValue)];
                return (positional, new Dictionary<string, object?>());
            },
        });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], "data", "cols", "VERSION", "1", null);

        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal("cols_scan", branch.FunctionName);
        Assert.NotEmpty(branch.Arguments);
    }
}

file sealed class StubScanFunction(string name) : ITableFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("version", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        throw new NotSupportedException("Not exercised by these tests.");
}
