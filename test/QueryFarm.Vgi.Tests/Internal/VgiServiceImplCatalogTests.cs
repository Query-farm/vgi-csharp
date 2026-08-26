using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Exercises M6's catalog-surface additions to <see cref="VgiServiceImpl"/>: per-<c>ATTACH</c>
/// identity uniqueness (needed so a writable fixture's cross-process row store can scope storage per
/// attach SESSION, not just per catalog NAME — see <c>Table.TableBindParams.AttachOpaqueData</c>'s doc
/// comment) and the DDL surface's uniform read-only behavior.</summary>
public class VgiServiceImplCatalogTests
{
    private static VgiServiceImpl NewService() => new(new CatalogRegistry());

    [Fact]
    public async Task CatalogAttach_MintsADifferentAttachOpaqueData_ForEveryCall_EvenWithTheSameName()
    {
        var service = NewService();

        var first = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        var second = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        Assert.NotEqual(first.AttachOpaqueData, second.AttachOpaqueData);
    }

    [Fact]
    public async Task CatalogAttach_DifferentNames_AlsoMintDifferentAttachOpaqueData()
    {
        var service = NewService();

        var a = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "twin_a" });
        var b = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "twin_b" });

        Assert.NotEqual(a.AttachOpaqueData, b.AttachOpaqueData);
    }

    [Fact]
    public async Task CatalogAttach_TwoAttachesOfTheSameName_StillRouteToTheSameRegisteredSchemas()
    {
        // The per-attach random suffix EncodeIdentity mints must not break DecodeIdentity's ability
        // to recover the catalog name for ordinary (non-multi-identity) function/schema routing —
        // both attach calls below must see the SAME "data" schema despite minting distinct
        // AttachOpaqueData values.
        var registry = new CatalogRegistry();
        registry.RegisterSchema("data", comment: "shared across attaches of this same catalog name");
        var service = new VgiServiceImpl(registry);

        var first = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });
        var second = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        var firstSchemas = await service.CatalogSchemasAsync(first.AttachOpaqueData, null);
        var secondSchemas = await service.CatalogSchemasAsync(second.AttachOpaqueData, null);

        Assert.NotEmpty(firstSchemas.Items);
        Assert.Equal(firstSchemas.Items.Count, secondSchemas.Items.Count);
    }

    [Theory]
    [InlineData("catalog_schema_create")]
    [InlineData("catalog_table_column_add")]
    [InlineData("catalog_table_column_drop")]
    [InlineData("catalog_table_drop")]
    [InlineData("catalog_view_create")]
    public async Task DdlDefaults_AllThrowCatalogReadOnly_WithThePinnedMessageSubstring(string which)
    {
        IVgiService service = NewService();
        byte[] attach = [];

        Task Call() => which switch
        {
            "catalog_schema_create" => service.CatalogSchemaCreateAsync(attach, "s", OnConflict.Error, null, null, null),
            "catalog_table_column_add" => service.CatalogTableColumnAddAsync(attach, "main", "t", [], false, false, null),
            "catalog_table_column_drop" => service.CatalogTableColumnDropAsync(attach, "main", "t", "c", false, false, false, null),
            "catalog_table_drop" => service.CatalogTableDropAsync(attach, "main", "t", false, false, null),
            "catalog_view_create" => service.CatalogViewCreateAsync(attach, "main", "v", "SELECT 1", OnConflict.Error, null),
            _ => throw new InvalidOperationException(which),
        };

        var exception = await Assert.ThrowsAsync<CatalogReadOnlyException>(Call);
        Assert.Contains("catalog is read-only", exception.Message, StringComparison.Ordinal);
    }
}

file sealed class StubTableFunction(string name) : ITableFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        throw new NotSupportedException("Not exercised by these tests.");
}

/// <summary>Regression coverage for a table-buffering-specific projection-narrowing bug: unlike
/// <see cref="VgiServiceImpl"/>'s table-in-out FINALIZE branch (which already declared the
/// projection-NARROWED schema on its <see cref="RpcStream{TState}"/>), the table-buffering FINALIZE
/// branch declared the FULL <c>output_schema</c> even when the client only requested a column
/// subset — a projection-pushdown-aware producer that correctly emits only the requested columns
/// (per <see cref="TableBufferingFinalizeParams.ProjectedSchema"/>) then mismatched the stream's own
/// declared wire schema. Caught by <c>table_buffering_projection_filters.test</c>'s very first
/// (no-WHERE-clause) assertion returning zero rows instead of one.</summary>
public class TableBufferingFinalizeInitSchemaTests
{
    [Fact]
    public async Task InitAsync_TableBufferingFinalize_DeclaresTheProjectedSchema_NotTheFullOutputSchema()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTableBuffering(new StubBufferingFunction());
        var service = new VgiServiceImpl(registry);

        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        var inputSchema = new Schema(
            [
                new Field("a", Int32Type.Default, nullable: true),
                new Field("b", Int32Type.Default, nullable: true),
                new Field("c", Int32Type.Default, nullable: true),
            ],
            metadata: null);

        var bindRequest = new BindRequest
        {
            FunctionName = "stub_buffering",
            FunctionType = FunctionType.Table,
            Arguments = [],
            InputSchema = SchemaIpc.WriteSchemaOnly(inputSchema),
            AttachOpaqueData = attach.AttachOpaqueData,
        };

        var initRequest = new InitRequest
        {
            BindCall = EmbeddedIpc.Encode(bindRequest),
            Phase = VgiInitPhase.TableBufferingFinalize,
            ProjectionIds = [0],
            ExecutionId = Guid.NewGuid().ToByteArray(),
            FinalizeStateId = [1],
        };

        var stream = await service.InitAsync(initRequest);

        var field = Assert.Single(stream.OutputSchema.FieldsList);
        Assert.Equal("a", field.Name);
    }

    [Fact]
    public async Task InitAsync_TableBufferingFinalize_WithNoProjection_DeclaresTheFullOutputSchema()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTableBuffering(new StubBufferingFunction());
        var service = new VgiServiceImpl(registry);

        var attach = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        var inputSchema = new Schema(
            [
                new Field("a", Int32Type.Default, nullable: true),
                new Field("b", Int32Type.Default, nullable: true),
                new Field("c", Int32Type.Default, nullable: true),
            ],
            metadata: null);

        var bindRequest = new BindRequest
        {
            FunctionName = "stub_buffering",
            FunctionType = FunctionType.Table,
            Arguments = [],
            InputSchema = SchemaIpc.WriteSchemaOnly(inputSchema),
            AttachOpaqueData = attach.AttachOpaqueData,
        };

        var initRequest = new InitRequest
        {
            BindCall = EmbeddedIpc.Encode(bindRequest),
            Phase = VgiInitPhase.TableBufferingFinalize,
            ProjectionIds = null,
            ExecutionId = Guid.NewGuid().ToByteArray(),
            FinalizeStateId = [1],
        };

        var stream = await service.InitAsync(initRequest);

        Assert.Equal(3, stream.OutputSchema.FieldsList.Count);
    }
}

file sealed class StubBufferingFunction : ITableBufferingFunction
{
    public string Name => "stub_buffering";

    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("a", Int32Type.Default, nullable: true),
            new Field("b", Int32Type.Default, nullable: true),
            new Field("c", Int32Type.Default, nullable: true),
        ],
        metadata: null);

    public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams) =>
        throw new NotSupportedException("Not exercised by these tests.");

    public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
        throw new NotSupportedException("Not exercised by these tests.");

    public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
        new NullProducer();

    private sealed class NullProducer : ITableFunctionProducer
    {
        public void Produce(OutputCollector output) => output.Finish();
    }
}

/// <summary>Covers the multi-branch milestone's <c>catalog_table_scan_branches_get</c> handler —
/// called for EVERY VGI table (not only multi-branch ones), per its doc comment.</summary>
public class CatalogTableScanBranchesGetTests
{
    private static VgiServiceImpl NewService(CatalogRegistry registry) => new(registry);

    [Fact]
    public async Task ScanFunctionBackedTable_SynthesizesExactlyOneUnconstrainedBranch()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable { Name = "numbers", SchemaName = "data", ScanFunction = new StubTableFunction("numbers_scan") });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], "data", "numbers", null, null, null);

        Assert.Single(result.Branches);
        Assert.Empty(result.RequiredExtensions);
        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal("numbers_scan", branch.FunctionName);
        Assert.Empty(branch.Arguments);
        Assert.Null(branch.BranchFilter);
        Assert.False(branch.Writable);
    }

    [Fact]
    public async Task BranchesDeclaredTable_ReportsEachBranchVerbatim_IncludingFilterAndWritable()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "multi",
            SchemaName = "data",
            Columns = new Schema([new Field("n", Int64Type.Default, nullable: true)], metadata: null),
            RequiredExtensions = ["iceberg"],
            Branches =
            [
                new ScanBranchSpec { FunctionName = "sequence", PositionalArguments = [50L], BranchFilter = "n < 50" },
                new ScanBranchSpec { FunctionName = "sequence", PositionalArguments = [50L], Writable = true },
            ],
        });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], "data", "multi", null, null, null);

        Assert.Equal(2, result.Branches.Count);
        Assert.Equal(["iceberg"], result.RequiredExtensions);
        var first = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal("sequence", first.FunctionName);
        Assert.Equal("n < 50", first.BranchFilter);
        Assert.False(first.Writable);
        Assert.NotEmpty(first.Arguments);
        var second = EmbeddedIpc.Decode<ScanBranch>(result.Branches[1]);
        Assert.Null(second.BranchFilter);
        Assert.True(second.Writable);
    }

    [Fact]
    public async Task EmptyBranchesList_IsReportedFaithfully_NotSynthesized()
    {
        // The C++ parser is the one that loud-fails on zero branches (multi_branch_empty_branches.test)
        // — this worker's job is only to report exactly what was declared, even an empty list.
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "empty",
            SchemaName = "data",
            Columns = new Schema([new Field("n", Int64Type.Default, nullable: true)], metadata: null),
            Branches = [],
        });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], "data", "empty", null, null, null);

        Assert.Empty(result.Branches);
    }

    [Fact]
    public async Task FormatBranch_EncodesLocationsAndNamedOptions()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "csv_table",
            SchemaName = "data",
            Columns = new Schema([new Field("n", Int64Type.Default, nullable: true)], metadata: null),
            Branches =
            [
                new ScanBranchSpec
                {
                    FormatName = "csv",
                    FormatLocations = ["/tmp/a.csv"],
                    FormatOptions = new Dictionary<string, object?> { ["header"] = true, ["delim"] = "|" },
                },
            ],
        });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], "data", "csv_table", null, null, null);

        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal("", branch.FunctionName);
        Assert.Equal("csv", branch.FormatName);
        Assert.Equal(["/tmp/a.csv"], branch.FormatLocations);
        Assert.NotNull(branch.FormatOptions);
        Assert.NotEmpty(branch.FormatOptions!);
    }

    [Fact]
    public async Task UnknownTable_Throws()
    {
        var service = NewService(new CatalogRegistry());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.CatalogTableScanBranchesGetAsync([], "data", "nope", null, null, null));
    }
}

/// <summary>Covers this milestone's additions: database-level comment/tags
/// (<see cref="CatalogRegistry.DatabaseComment"/>/<see cref="CatalogRegistry.DatabaseTags"/>),
/// per-column comment/default Arrow field metadata (<see cref="CatalogTable.ColumnComments"/>/
/// <see cref="CatalogTable.ColumnDefaults"/>), and the <see cref="CatalogTable.InlineScanFunction"/>
/// opt-out.</summary>
public class TableColumnMetadataAndDatabaseInfoTests
{
    private static VgiServiceImpl NewService(CatalogRegistry registry) => new(registry);

    [Fact]
    public async Task CatalogAttach_ReportsDatabaseCommentAndTags_WhenDeclared()
    {
        var registry = new CatalogRegistry
        {
            DatabaseComment = "Example VGI catalog for testing",
            DatabaseTags = new Dictionary<string, string> { ["source"] = "vgi-fixture-worker", ["version"] = "1" },
        };
        var service = NewService(registry);

        var result = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        Assert.Equal("Example VGI catalog for testing", result.Comment);
        Assert.Equal("vgi-fixture-worker", result.Tags["source"]);
        Assert.Equal("1", result.Tags["version"]);
    }

    [Fact]
    public async Task CatalogAttach_ReportsNoCommentOrTags_ByDefault()
    {
        var service = NewService(new CatalogRegistry());

        var result = await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" });

        Assert.Null(result.Comment);
        Assert.Empty(result.Tags);
    }

    [Fact]
    public async Task CatalogTableGet_AppliesColumnCommentsAndDefaults_AsBareArrowFieldMetadata()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "products",
            SchemaName = "data",
            Columns = new Schema(
                [
                    new Field("id", Int64Type.Default, nullable: false),
                    new Field("name", StringType.Default, nullable: true),
                ],
                metadata: null),
            ColumnComments = new Dictionary<string, string> { ["name"] = "Product display name" },
            ColumnDefaults = new Dictionary<string, string> { ["name"] = "'unknown'" },
        });
        var service = NewService(registry);

        var result = await service.CatalogTableGetAsync([], "data", "products", null, null, null);
        var table = EmbeddedIpc.Decode<TableInfo>(result.Items[0]);
        var columns = SchemaIpc.ReadSchemaOnly(table.Columns);

        var idField = columns.GetFieldByIndex(0);
        Assert.False(idField.HasMetadata);

        var nameField = columns.GetFieldByIndex(1);
        Assert.Equal("Product display name", nameField.Metadata!["comment"]);
        Assert.Equal("'unknown'", nameField.Metadata!["default"]);
    }

    [Fact]
    public async Task CatalogTableGet_InlineScanFunctionTrue_InlinesTheScanFunction_TheDefault()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "inlined",
            SchemaName = "data",
            ScanFunction = new StubTableFunction("inlined_scan"),
        });
        var service = NewService(registry);

        var result = await service.CatalogTableGetAsync([], "data", "inlined", null, null, null);
        var table = EmbeddedIpc.Decode<TableInfo>(result.Items[0]);

        Assert.NotNull(table.ScanFunction);
        var scanFunction = EmbeddedIpc.Decode<ScanFunctionResult>(table.ScanFunction!);
        Assert.Equal("inlined_scan", scanFunction.FunctionName);
    }

    [Fact]
    public async Task CatalogTableGet_InlineScanFunctionFalse_LeavesScanFunctionNull()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "not_inlined",
            SchemaName = "data",
            ScanFunction = new StubTableFunction("not_inlined_scan"),
            InlineScanFunction = false,
        });
        var service = NewService(registry);

        var result = await service.CatalogTableGetAsync([], "data", "not_inlined", null, null, null);
        var table = EmbeddedIpc.Decode<TableInfo>(result.Items[0]);

        Assert.Null(table.ScanFunction);

        // The C++ side falls back to catalog_table_scan_branches_get for a non-inlined table — that
        // RPC must still answer correctly (a single synthesized branch wrapping the same function).
        var branches = await service.CatalogTableScanBranchesGetAsync([], "data", "not_inlined", null, null, null);
        Assert.Single(branches.Branches);
        var branch = EmbeddedIpc.Decode<ScanBranch>(branches.Branches[0]);
        Assert.Equal("not_inlined_scan", branch.FunctionName);
    }
}
