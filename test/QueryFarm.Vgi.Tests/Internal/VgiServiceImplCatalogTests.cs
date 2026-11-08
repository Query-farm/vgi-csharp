using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
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

    private sealed class MonotonicScalar(IReadOnlyList<ArgumentMonotonicity> monotonicity) : ScalarFn
    {
        public override string Name => "monotonic_scalar";

        public override IReadOnlyList<ArgumentMonotonicity>? ArgumentMonotonicity => monotonicity;

        private void Compute([Param] Int64Array value, Int64Array.Builder result)
        {
            for (var row = 0; row < value.Length; row++)
            {
                result.Append(value.GetValue(row));
            }
        }
    }

    [Fact]
    public async Task CatalogFunctionMetadata_TransportsArgumentMonotonicity()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new MonotonicScalar([ArgumentMonotonicity.StrictlyIncreasing]));
        var service = new VgiServiceImpl(registry);

        var response = await service.CatalogSchemaContentsFunctionsAsync(
            [], ["main"], SchemaObjectType.ScalarFunction, null);
        var function = Assert.Single(response.Items.Select(EmbeddedIpc.Decode<FunctionInfo>));

        Assert.Equal(["STRICTLY_INCREASING"], function.ArgumentMonotonicity);
    }

    [Fact]
    public async Task CatalogFunctionMetadata_RejectsMisalignedArgumentMonotonicity()
    {
        var registry = new CatalogRegistry();
        registry.RegisterScalar(new MonotonicScalar([]));
        var service = new VgiServiceImpl(registry);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogSchemaContentsFunctionsAsync(
                [], ["main"], SchemaObjectType.ScalarFunction, null));

        Assert.Contains("expected 1 declaration slots", exception.Message, StringComparison.Ordinal);
    }

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
    public async Task CatalogFunctionMetadata_RejectsCapabilitiesWithoutAnEvaluator()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new StubTableFunction("unsupported_filter", additionalFilterFunctions:
        [
            new FilterFunctionCapability { Namespace = "acme.filters", Name = "overlaps", Version = 1 },
        ]));
        var service = new VgiServiceImpl(registry);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogSchemaContentsFunctionsAsync([], ["main"], SchemaObjectType.TableFunction, null));
        Assert.Contains("no registered evaluator", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CatalogFunctionMetadata_RejectsRuntimeAlgorithmsWithoutAnEvaluator()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new StubTableFunction("unsupported_runtime", runtimeFilterAlgorithms:
        [
            new RuntimeFilterAlgorithmCapability { Namespace = "acme.runtime", Name = "bloom", Version = 1 },
        ]));
        var service = new VgiServiceImpl(registry);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogSchemaContentsFunctionsAsync([], ["main"], SchemaObjectType.TableFunction, null));
        Assert.Contains("no registered evaluator", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CatalogFunctionMetadata_RejectsUnknownSemanticProfiles()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new StubTableFunction("unsupported_semantics",
            filterSemanticProfiles: ["example.unknown.v1"]));
        var service = new VgiServiceImpl(registry);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            service.CatalogSchemaContentsFunctionsAsync([], ["main"], SchemaObjectType.TableFunction, null));
        Assert.Contains("supports only", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CatalogFunctionMetadata_AllowsTheRegisteredSpatialExtensionIdentity()
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(new StubTableFunction("spatial_filter", additionalFilterFunctions:
        [
            new FilterFunctionCapability
            {
                Namespace = "duckdb.spatial",
                Name = "intersects_extent",
                Version = 1,
            },
        ]));
        var service = new VgiServiceImpl(registry);

        var response = await service.CatalogSchemaContentsFunctionsAsync(
            [], ["main"], SchemaObjectType.TableFunction, null);
        var function = Assert.Single(response.Items.Select(EmbeddedIpc.Decode<FunctionInfo>));
        var capability = Assert.Single(function.AdditionalFilterFunctions);
        Assert.Equal("duckdb.spatial", capability.Namespace);
        Assert.Equal("intersects_extent", capability.Name);
        Assert.Equal((ulong)1, capability.Version);
    }

    [Fact]
    public async Task TableFunctionPlan_MapsProtocol31DistributionMetadata()
    {
        var grouping = new KeyGrouping
        {
            Keys = [EmbeddedIpc.Encode(new TransformSpec { Id = "vgi.identity/1" })],
        };
        var ranges = new RangePartitioning
        {
            Keys = [EmbeddedIpc.Encode(new RangeKey
            {
                Transform = grouping.Keys[0],
                Direction = VgiOrderByDirection.Asc,
                NullOrder = VgiNullOrder.NullsFirst,
            })],
            SplitPoints = [4, 5, 6],
        };
        var plan = new PlanResult
        {
            Locations = ["worker-a", "worker-b"],
            KeyGrouping = grouping,
            RangePartitioning = ranges,
            Splits =
            [
                new ScanSplit
                {
                    Payload = [1],
                    KeyValue = [2],
                    RangeIndex = 1,
                    ColumnBounds = [3],
                    LocationIds = [0, 1],
                },
            ],
        };
        var registry = new CatalogRegistry();
        registry.RegisterTable(new StubTableFunction("distributed", planResult: plan));
        var service = new VgiServiceImpl(registry);
        var bind = new BindRequest
        {
            FunctionName = "distributed",
            FunctionType = FunctionType.Table,
            SchemaPath = ["main"],
        };

        var response = await service.TableFunctionPlanAsync(new TableFunctionPlanRequest
        {
            BindCall = EmbeddedIpc.Encode(bind),
        });

        Assert.Equal(["worker-a", "worker-b"], response.Locations);
        Assert.Equal("vgi.identity/1", EmbeddedIpc.Decode<TransformSpec>(
            Assert.Single(EmbeddedIpc.Decode<KeyGrouping>(response.KeyGrouping!).Keys)).Id);
        Assert.Equal([4, 5, 6], EmbeddedIpc.Decode<RangePartitioning>(response.RangePartitioning!).SplitPoints);
        var wireSplit = EmbeddedIpc.Decode<ScanSplitWire>(Assert.Single(response.Splits));
        Assert.Equal([2], wireSplit.KeyValue);
        Assert.Equal(1, wireSplit.RangeIndex);
        Assert.Equal([3], wireSplit.ColumnBounds);
        Assert.Equal([0L, 1L], wireSplit.LocationIds);
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

    [Fact]
    public async Task CatalogDiscoveryAndLookup_PreserveArbitrarilyNestedSchemaPaths()
    {
        var columns = new Schema([new Field("id", Int64Type.Default, nullable: true)], metadata: null);
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "events",
            SchemaPath = ["tenant.with.dot", "analytics", "daily"],
            Columns = columns,
        });
        var service = new VgiServiceImpl(registry);

        var schemas = await service.CatalogSchemasAsync([], null);
        var schema = Assert.Single(schemas.Items.Select(EmbeddedIpc.Decode<SchemaInfo>));
        Assert.Equal(["tenant.with.dot", "analytics", "daily"], schema.Path);

        var contents = await service.CatalogSchemaContentsTablesAsync(
            [], ["tenant.with.dot", "analytics", "daily"], null);
        var table = Assert.Single(contents.Items.Select(EmbeddedIpc.Decode<TableInfo>));
        Assert.Equal(["tenant.with.dot", "analytics", "daily"], table.SchemaPath);

        var lookup = await service.CatalogTableGetAsync(
            [], ["tenant.with.dot", "analytics", "daily"], "events", null, null, null);
        Assert.Single(lookup.Items);
        var wrongParent = await service.CatalogTableGetAsync(
            [], ["other", "analytics", "daily"], "events", null, null, null);
        Assert.Empty(wrongParent.Items);
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
            "catalog_schema_create" => service.CatalogSchemaCreateAsync(attach, ["s"], OnConflict.Error, null, null, null),
            "catalog_table_column_add" => service.CatalogTableColumnAddAsync(attach, ["main"], "t", ["c"], [], false, false, null),
            "catalog_table_column_drop" => service.CatalogTableColumnDropAsync(attach, ["main"], "t", ["c"], false, false, false, null),
            "catalog_table_drop" => service.CatalogTableDropAsync(attach, ["main"], "t", false, false, null),
            "catalog_view_create" => service.CatalogViewCreateAsync(attach, ["main"], "v", "SELECT 1", OnConflict.Error, null),
            _ => throw new InvalidOperationException(which),
        };

        var exception = await Assert.ThrowsAsync<CatalogReadOnlyException>(Call);
        Assert.Contains("catalog is read-only", exception.Message, StringComparison.Ordinal);
    }
}

file sealed class StubTableFunction(
    string name,
    string schemaName = "main",
    IReadOnlyList<FilterFunctionCapability>? additionalFilterFunctions = null,
    IReadOnlyList<RuntimeFilterAlgorithmCapability>? runtimeFilterAlgorithms = null,
    IReadOnlyList<string>? filterSemanticProfiles = null,
    PlanResult? planResult = null) : ITableFunction
{
    public string Name => name;

    public string SchemaName => schemaName;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public IReadOnlyList<FilterFunctionCapability> AdditionalFilterFunctions => additionalFilterFunctions ?? [];

    public IReadOnlyList<RuntimeFilterAlgorithmCapability> RuntimeFilterAlgorithms => runtimeFilterAlgorithms ?? [];

    public IReadOnlyList<string> FilterSemanticProfiles => filterSemanticProfiles ?? [];

    public bool SupportsSplits => planResult is not null;

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request) => planResult ?? PlanResult.Empty;

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

        var result = await service.CatalogTableScanBranchesGetAsync([], ["data"], "numbers", null, null, null);

        Assert.Single(result.Branches);
        Assert.Empty(result.RequiredExtensions);
        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal("numbers_scan", branch.FunctionName);
        Assert.Empty(branch.Arguments);
        Assert.Null(branch.BranchFilter);
        Assert.False(branch.Writable);
    }

    [Fact]
    public async Task ScanFunctionBackedTable_ReportsTheFunctionsOwnSchema_NotTheTablesSchema()
    {
        // A table's backing function is NOT necessarily registered in the table's own
        // schema — data.numbers is scanned by main.sequence. The synthesized branch reads the schema
        // off the ITableFunction INSTANCE the table holds, so this is authoritative, never a guess.
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "numbers",
            SchemaName = "data",
            ScanFunction = new StubTableFunction("sequence", "main"),
        });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], ["data"], "numbers", null, null, null);

        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal(["main"], branch.SchemaPath);
    }

    [Fact]
    public async Task DeclaredFunctionBranch_ResolvesSchemaFromTheRegistry_AndReportsNoneForANativeFunction()
    {
        // A ScanBranchSpec names its function by NAME alone, so the schema has to come from the
        // registry — which also answers "no schema at all" for a NATIVE DuckDB function this worker
        // never registered (read_parquet), the permanent case that keeps the field optional.
        var registry = new CatalogRegistry();
        registry.RegisterTable(new StubTableFunction("sequence", "main"));
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "hetero",
            SchemaName = "data",
            Columns = new Schema([new Field("n", Int64Type.Default, nullable: true)], metadata: null),
            Branches =
            [
                new ScanBranchSpec { FunctionName = "sequence", PositionalArguments = [50L] },
                new ScanBranchSpec { FunctionName = "read_parquet", PositionalArguments = ["/tmp/a.parquet"] },
            ],
        });
        var service = NewService(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], ["data"], "hetero", null, null, null);

        Assert.Equal(["main"], EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]).SchemaPath);
        Assert.Null(EmbeddedIpc.Decode<ScanBranch>(result.Branches[1]).SchemaPath);
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

        var result = await service.CatalogTableScanBranchesGetAsync([], ["data"], "multi", null, null, null);

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

        var result = await service.CatalogTableScanBranchesGetAsync([], ["data"], "empty", null, null, null);

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

        var result = await service.CatalogTableScanBranchesGetAsync([], ["data"], "csv_table", null, null, null);

        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal("", branch.FunctionName);
        Assert.Equal("csv", branch.FormatName);
        Assert.Equal(["/tmp/a.csv"], branch.FormatLocations);
        Assert.NotNull(branch.FormatOptions);
        Assert.NotEmpty(branch.FormatOptions!);
        // A format branch names no function, so it reports no function schema.
        Assert.Null(branch.SchemaPath);
    }

    [Fact]
    public async Task UnknownTable_Throws()
    {
        var service = NewService(new CatalogRegistry());

        await Assert.ThrowsAsync<VgiNotFoundException>(
            () => service.CatalogTableScanBranchesGetAsync([], ["data"], "nope", null, null, null));
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
                    new Field("id", Int64Type.Default, nullable: true),
                    new Field("name", StringType.Default, nullable: true),
                ],
                metadata: null),
            ColumnComments = new Dictionary<string, string> { ["name"] = "Product display name" },
            ColumnDefaults = new Dictionary<string, string> { ["name"] = "'unknown'" },
        });
        var service = NewService(registry);

        var result = await service.CatalogTableGetAsync([], ["data"], "products", null, null, null);
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

        var result = await service.CatalogTableGetAsync([], ["data"], "inlined", null, null, null);
        var table = EmbeddedIpc.Decode<TableInfo>(result.Items[0]);

        Assert.NotNull(table.ScanFunction);
        var scanFunction = EmbeddedIpc.Decode<ScanFunctionResult>(table.ScanFunction!);
        Assert.Equal("inlined_scan", scanFunction.FunctionName);
        // The inline result carries the function's own schema, which (as here) need
        // not be the containing table's.
        Assert.Equal(["main"], scanFunction.SchemaPath);
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

        var result = await service.CatalogTableGetAsync([], ["data"], "not_inlined", null, null, null);
        var table = EmbeddedIpc.Decode<TableInfo>(result.Items[0]);

        Assert.Null(table.ScanFunction);

        // The C++ side falls back to catalog_table_scan_branches_get for a non-inlined table — that
        // RPC must still answer correctly (a single synthesized branch wrapping the same function).
        var branches = await service.CatalogTableScanBranchesGetAsync([], ["data"], "not_inlined", null, null, null);
        Assert.Single(branches.Branches);
        var branch = EmbeddedIpc.Decode<ScanBranch>(branches.Branches[0]);
        Assert.Equal("not_inlined_scan", branch.FunctionName);
    }
}

/// <summary>Protocol 3.2 filtered scan planning: <see cref="CatalogTable.PlanScanBranches"/> dispatch,
/// <see cref="CatalogTable.ScanPlanning"/> validation/serialization, branch statistics, and
/// <see cref="ScanPlanningFilters.MayMatch"/> (vgi-python <c>tests/test_scan_planning.py</c>).</summary>
public class ScanPlanningTests
{
    private static readonly Schema DayColumns = new(
        [new Field("event_date", Date32Type.Default, nullable: true), new Field("n", Int64Type.Default, nullable: true)],
        metadata: null);

    /// <summary><c>WHERE event_date &gt;= DATE '2026-10-03' AND n &lt; 100</c>, as the client sends it.</summary>
    private static byte[] GoldenFilters => Protocol.CatalogContractsTests.Fixture("scan_planning_pushdown_filters");

    private static ScanPlanningFilters Filters() => new(GoldenFilters, () => DayColumns);

    private static Dictionary<string, ColumnStatisticsInput> Day(int day) => new()
    {
        ["event_date"] = new ColumnStatisticsInput { Min = new DateOnly(2026, 10, day), Max = new DateOnly(2026, 10, day), HasNull = false },
    };

    private static ScanBranchSpec DayBranch(int day) => new()
    {
        FunctionName = "read_parquet",
        PositionalArguments = [$"/tmp/{day}.parquet"],
        RowCount = 10,
        ColumnStatistics = Day(day),
    };

    [Fact]
    public void Filters_DecodeAgainstTheTablesDeclaredColumns_Lazily()
    {
        var calls = 0;
        var filters = new ScanPlanningFilters(GoldenFilters, () => { calls++; return DayColumns; });
        Assert.Equal(0, calls);
        var decoded = filters.Decode();
        Assert.NotNull(decoded);
        Assert.Equal("snapshot", decoded!.Kind);
        Assert.Equal(2, decoded.Predicates.Count);
        Assert.All(decoded.Predicates, p => Assert.Equal("advisory", p.Mode));
        Assert.Same(decoded, filters.Decode());
        Assert.Equal(1, calls);
        Assert.Equal(GoldenFilters, filters.Raw);
    }

    [Fact]
    public void MayMatch_PrunesOnlyWhatThePredicatesProvablyExclude()
    {
        var filters = Filters();
        Assert.False(filters.MayMatch(Day(1)));
        Assert.False(filters.MayMatch(Day(2)));
        Assert.True(filters.MayMatch(Day(3)));
        Assert.True(filters.MayMatch(Day(5)));
        // A range straddling the bound may match.
        Assert.True(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput>
        {
            ["event_date"] = new() { Min = new DateOnly(2026, 9, 1), Max = new DateOnly(2026, 10, 3) },
        }));
        // n < 100 against n in [100, 200] cannot match; [99, 200] can.
        Assert.False(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput> { ["n"] = new() { Min = 100L, Max = 200L } }));
        Assert.True(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput> { ["n"] = new() { Min = 99L, Max = 200L } }));
        // Only an upper bound known: n >= 100 is not proven.
        Assert.True(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput> { ["n"] = new() { Max = 50L } }));
        // An entirely-NULL column never satisfies a comparison.
        Assert.False(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput> { ["n"] = new() { HasNull = true, HasNotNull = false } }));
    }

    [Fact]
    public void MayMatch_KeepsWhatItCannotReasonAbout()
    {
        var filters = Filters();
        Assert.True(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput>()));
        Assert.True(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput> { ["other"] = new() { Min = 0L, Max = 0L } }));
        Assert.True(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput> { ["event_date"] = new() }));
        // A bound of an incomparable type (a long against a DATE literal) is not evidence.
        Assert.True(filters.MayMatch(new Dictionary<string, ColumnStatisticsInput> { ["event_date"] = new() { Min = 0L, Max = 1L } }));
        // An empty payload carries no predicates.
        Assert.True(new ScanPlanningFilters([], () => DayColumns).MayMatch(Day(1)));
    }

    [Fact]
    public async Task PlanningHook_ReceivesTheDecodedFilters_AndItsBranchesAreServed()
    {
        ScanPlanningRequest? seen = null;
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "events",
            SchemaName = "data",
            Columns = DayColumns,
            ScanPlanning = ScanPlanningModes.Filtered,
            RequiredExtensions = ["parquet"],
            PlanScanBranches = request =>
            {
                seen = request;
                return new ScanBranchesPlan
                {
                    Branches = [.. Enumerable.Range(1, 5).Where(d => request.Filters?.MayMatch(Day(d)) ?? true).Select(DayBranch)],
                };
            },
        });
        var service = new VgiServiceImpl(registry);

        var result = await service.CatalogTableScanBranchesGetAsync([], ["data"], "events", null, null, null, GoldenFilters);

        Assert.NotNull(seen);
        Assert.Equal("events", seen!.TableName);
        Assert.Equal(["data"], seen.SchemaPath);
        Assert.Equal(GoldenFilters, seen.Filters!.Raw);
        Assert.Equal(["parquet"], result.RequiredExtensions);
        Assert.Equal(3, result.Branches.Count);
        var branch = EmbeddedIpc.Decode<ScanBranch>(result.Branches[0]);
        Assert.Equal(10, branch.RowCount);
        var (column, stats) = Assert.Single(ColumnStatisticsCodec.Decode(branch.ColumnStatistics));
        Assert.Equal("event_date", column);
        Assert.Equal(new DateOnly(2026, 10, 3), stats.Min);
        Assert.Equal(new DateOnly(2026, 10, 3), stats.Max);
        Assert.False(stats.HasNull);
        Assert.True(stats.HasNotNull);

        // No pushdown_filters (bind-time planning): the hook sees null and keeps every day.
        var unfiltered = await service.CatalogTableScanBranchesGetAsync([], ["data"], "events", null, null, null);
        Assert.Null(seen.Filters);
        Assert.Equal(5, unfiltered.Branches.Count);
    }

    [Fact]
    public async Task PlanningHook_MayReturnZeroBranches_OrFallBackToTheDeclaredScan()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "none",
            SchemaName = "data",
            Columns = DayColumns,
            ScanPlanning = ScanPlanningModes.Filtered,
            PlanScanBranches = _ => new ScanBranchesPlan { RequiredExtensions = ["parquet"] },
        });
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "declared",
            SchemaName = "data",
            Columns = DayColumns,
            ScanPlanning = ScanPlanningModes.Filtered,
            RequiredExtensions = ["iceberg"],
            Branches = [DayBranch(1), DayBranch(2)],
            PlanScanBranches = _ => null,
        });
        var service = new VgiServiceImpl(registry);

        var none = await service.CatalogTableScanBranchesGetAsync([], ["data"], "none", null, null, null, GoldenFilters);
        Assert.Empty(none.Branches);
        Assert.Equal(["parquet"], none.RequiredExtensions);
        var roundTripped = EmbeddedIpc.Decode<ScanBranchesResult>(EmbeddedIpc.Encode(none));
        Assert.Empty(roundTripped.Branches);

        var declared = await service.CatalogTableScanBranchesGetAsync([], ["data"], "declared", null, null, null, GoldenFilters);
        Assert.Equal(2, declared.Branches.Count);
        Assert.Equal(["iceberg"], declared.RequiredExtensions);
    }

    [Fact]
    public async Task TableInfo_CarriesScanPlanning_DefaultingToBind()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(new CatalogTable { Name = "plain", SchemaName = "data", Columns = DayColumns, Branches = [] });
        registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "planned",
            SchemaName = "data",
            ScanPlanning = ScanPlanningModes.Filtered,
            ScanFunction = new StubTableFunction("planned_scan"),
            InlineScanFunction = false,
        });
        var service = new VgiServiceImpl(registry);

        var plain = EmbeddedIpc.Decode<TableInfo>((await service.CatalogTableGetAsync([], ["data"], "plain", null, null, null)).Items[0]);
        Assert.Equal("bind", plain.ScanPlanning);
        var planned = EmbeddedIpc.Decode<TableInfo>((await service.CatalogTableGetAsync([], ["data"], "planned", null, null, null)).Items[0]);
        Assert.Equal("filtered", planned.ScanPlanning);
        Assert.Null(planned.ScanFunction);
    }

    [Fact]
    public void Registration_RefusesUnknownModes_AndAFilteredTableThatInlinesItsScan()
    {
        var registry = new CatalogRegistry();
        var unknown = Assert.Throws<InvalidOperationException>(() => registry.RegisterCatalogTable(
            new CatalogTable { Name = "t", SchemaName = "data", Columns = DayColumns, ScanPlanning = "deferred" }));
        Assert.Contains("deferred", unknown.Message);
        var inlined = Assert.Throws<InvalidOperationException>(() => registry.RegisterCatalogTable(new CatalogTable
        {
            Name = "t",
            SchemaName = "data",
            ScanPlanning = ScanPlanningModes.Filtered,
            ScanFunction = new StubTableFunction("t_scan"),
        }));
        Assert.Contains("inlined ScanFunction", inlined.Message);
    }

    [Fact]
    public void BranchRowCount_MustBeNonNegative()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ScanBranchSpec { FunctionName = "f", RowCount = -1 });
        Assert.Equal(0, new ScanBranchSpec { FunctionName = "f", RowCount = 0 }.RowCount);
    }

    [Fact]
    public void ScanArguments_EncodeAVarcharList()
    {
        var bytes = ScanArgsCodec.Encode([new List<string> { "a.parquet", "b.parquet" }]);
        var batch = RecordBatchIpc.Read(bytes);
        var list = Assert.IsType<ListArray>(batch.Column("arg_0"));
        Assert.Equal(1, list.Length);
        var values = (StringArray)list.Values;
        Assert.Equal(["a.parquet", "b.parquet"], Enumerable.Range(0, values.Length).Select(i => values.GetString(i)));
    }
}
