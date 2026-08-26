using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Buffering;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Exercises M6's real-catalog-table/view additions to <see cref="CatalogRegistry"/> — the
/// same identity-fallback rules M1's function registries already had (see that class's own doc
/// comment), now extended to <see cref="CatalogTable"/>/<see cref="CatalogView"/>/schema metadata.</summary>
public class CatalogRegistryTests
{
    private static readonly Schema EmptySchema = new([], metadata: null);

    private sealed class StubTableFunction(string name, string schemaName) : ITableFunction
    {
        public string Name => name;

        public string SchemaName => schemaName;

        public Schema ArgumentsSchema => EmptySchema;

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
            throw new NotSupportedException("Not exercised by these tests.");
    }

    private static CatalogTable MakeTable(string name, string schemaName) => new()
    {
        Name = name,
        SchemaName = schemaName,
        ScanFunction = new StubTableFunction(name, schemaName),
    };

    private static CatalogView MakeView(string name, string schemaName) => new()
    {
        Name = name,
        SchemaName = schemaName,
        Definition = "SELECT 1",
    };

    [Fact]
    public void RegisterCatalogTable_SharingTheSameScanFunctionInstance_RegistersItOnlyOnce()
    {
        // Two catalog tables deliberately sharing the exact same ITableFunction INSTANCE (the
        // "rff_or reuses rff_simple_scan" pattern — see VgiServiceImpl.RequiredFiltersTables and
        // table/function_registration.test's hardcoded function-count inventory) must add only ONE
        // candidate to the function registry, not one per table — otherwise the reference count
        // this port is validated against drifts every time a table reuses another's scan function.
        var registry = new CatalogRegistry();
        var shared = new StubTableFunction("shared_scan", "data");
        registry.RegisterCatalogTable(new CatalogTable { Name = "t1", SchemaName = "data", ScanFunction = shared });
        registry.RegisterCatalogTable(new CatalogTable { Name = "t2", SchemaName = "data", ScanFunction = shared });

        Assert.Single(registry.TableFunctionsFor(""));

        // Two DISTINCT objects sharing a NAME (the overload-testing pattern) are untouched —
        // both remain independently registered.
        var registry2 = new CatalogRegistry();
        registry2.RegisterCatalogTable(MakeTable("t3", "data"));
        registry2.RegisterCatalogTable(new CatalogTable { Name = "t4", SchemaName = "data", ScanFunction = new StubTableFunction("t3", "data") });

        Assert.Equal(2, registry2.TableFunctionsFor("").Count);
    }

    [Fact]
    public void FindCatalogTable_ResolvesByExactSchemaAndName()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalogTable(MakeTable("t1", "main"));
        registry.RegisterCatalogTable(MakeTable("t1", "data"));

        Assert.Same(registry.FindCatalogTable("", "main", "t1"), registry.CatalogTablesFor("").Single(t => t.SchemaName == "main"));
        Assert.NotNull(registry.FindCatalogTable("", "data", "t1"));
        Assert.NotEqual(registry.FindCatalogTable("", "main", "t1")!.SchemaName, registry.FindCatalogTable("", "data", "t1")!.SchemaName);
        Assert.Null(registry.FindCatalogTable("", "nonexistent_schema", "t1"));
    }

    [Fact]
    public void RegisterCatalogTable_AlsoRegistersItsScanFunctionUnderTheSameIdentity()
    {
        var registry = new CatalogRegistry();
        var table = MakeTable("orders", "main");
        registry.RegisterCatalogTable(table);

        // The dual-registration Worker.RegisterCatalogTable's doc comment promises: a function-backed
        // table's scan function is independently resolvable via the normal table-FUNCTION path too
        // (schema.table_name()), because the C++ extension calls it by name once it decodes
        // TableInfo.ScanFunction — not through some special catalog-table-only RPC.
        Assert.Same(table.ScanFunction, registry.FindTable("", "main", "orders"));
    }

    [Fact]
    public void CatalogTable_IdentitySpecificRegistration_OverridesDefaultBucketOnCollision()
    {
        var registry = new CatalogRegistry();
        var defaultTable = MakeTable("shared", "main");
        var identityTable = MakeTable("shared", "main");
        registry.RegisterCatalogTable(defaultTable);
        registry.RegisterCatalogTable(identityTable, identity: "twin_a");

        Assert.Same(defaultTable, registry.FindCatalogTable("twin_b", "main", "shared"));
        Assert.Same(identityTable, registry.FindCatalogTable("twin_a", "main", "shared"));
        Assert.Same(identityTable, registry.CatalogTablesFor("twin_a").Single(t => t.Name == "shared"));
    }

    [Fact]
    public void FindView_FallsBackToDefaultIdentityWhenNoIdentitySpecificRegistrationExists()
    {
        var registry = new CatalogRegistry();
        var view = MakeView("active_users", "main");
        registry.RegisterView(view);

        Assert.Same(view, registry.FindView("some_attach_name", "main", "active_users"));
        Assert.Null(registry.FindView("some_attach_name", "main", "no_such_view"));
    }

    [Fact]
    public void SchemaNamesFor_IncludesSchemasImpliedByTablesViewsAndExplicitRegistration()
    {
        var registry = new CatalogRegistry { DefaultSchema = "main" };
        registry.RegisterCatalogTable(MakeTable("t1", "data"));
        registry.RegisterView(MakeView("v1", "reporting"));
        registry.RegisterSchema("empty_but_declared", comment: "no tables, still real");

        var names = registry.SchemaNamesFor("");

        Assert.Contains("data", names);
        Assert.Contains("reporting", names);
        Assert.Contains("empty_but_declared", names);
    }

    [Fact]
    public void SchemaMetadataFor_ReturnsExplicitCommentAndTags_ElseEmptyDefaults()
    {
        var registry = new CatalogRegistry();
        registry.RegisterSchema("documented", comment: "hello", tags: new Dictionary<string, string> { ["k"] = "v" });

        var (comment, tags) = registry.SchemaMetadataFor("", "documented");
        Assert.Equal("hello", comment);
        Assert.Equal("v", tags["k"]);

        var (undeclaredComment, undeclaredTags) = registry.SchemaMetadataFor("", "never_declared");
        Assert.Null(undeclaredComment);
        Assert.Empty(undeclaredTags);
    }

    [Fact]
    public void SchemaMetadataFor_IdentitySpecificRegistration_DoesNotLeakAcrossIdentities()
    {
        var registry = new CatalogRegistry();
        registry.RegisterSchema("main", comment: "twin_a's schema", identity: "twin_a");

        Assert.Equal("twin_a's schema", registry.SchemaMetadataFor("twin_a", "main").Comment);
        Assert.Null(registry.SchemaMetadataFor("twin_b", "main").Comment);
    }

    /// <summary>Regression for a real bug found via <c>accumulate/catalog.test</c>: an EXCLUSIVE
    /// identity (a wholly independent "MetaWorker" second catalog — see <see cref="CatalogRegistry.RegisterCatalog"/>'s
    /// doc comment) must not inherit a plain <see cref="CatalogRegistry.RegisterSchema"/> call made
    /// under the DEFAULT identity — <see cref="CatalogRegistry.SchemaNamesFor"/> used to include
    /// every default-bucket <c>RegisterSchema</c> unconditionally (unlike its sibling
    /// function/table/view collections, which already respected the exclusive-identity rule), so an
    /// exclusive second catalog would see the primary catalog's explicitly-declared schemas leak in
    /// alongside its own.</summary>
    [Fact]
    public void SchemaNamesFor_ExclusiveIdentity_DoesNotInheritDefaultBucketRegisterSchemaCalls()
    {
        var registry = new CatalogRegistry();
        registry.RegisterCatalog(new QueryFarm.Vgi.Protocol.CatalogInfo { Name = "accumulate" }, exclusive: true);
        registry.RegisterSchema("data", comment: "primary catalog's own schema");
        registry.RegisterSchema("main", comment: "primary catalog's own schema");
        registry.RegisterTableBuffering(new StubTableBufferingFunction("accumulate_fn", "main"), identity: "accumulate");

        var accumulateNames = registry.SchemaNamesFor("accumulate");

        Assert.Equal(["main"], accumulateNames);
    }

    private sealed class StubTableBufferingFunction(string name, string schemaName) : ITableBufferingFunction
    {
        public string Name => name;

        public string SchemaName => schemaName;

        public Schema ArgumentsSchema => EmptySchema;

        public Schema OutputSchema => EmptySchema;

        public byte[] Process(RecordBatch batch, TableBufferingProcessParams processParams) =>
            throw new NotSupportedException("Not exercised by these tests.");

        public IReadOnlyList<byte[]> Combine(IReadOnlyList<byte[]> stateIds, TableBufferingCombineParams combineParams) =>
            throw new NotSupportedException("Not exercised by these tests.");

        public ITableFunctionProducer CreateFinalizeProducer(byte[] finalizeStateId, TableBufferingFinalizeParams finalizeParams) =>
            throw new NotSupportedException("Not exercised by these tests.");
    }
}
