// Copyright 2025, 2026 Query Farm LLC - https://query.farm

using System.Text.Json;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

public class CatalogContractsTests
{
    [Theory]
    [InlineData("query_parameters", "named")]
    [InlineData("query_empty_parameters", "positional")]
    public void QueryParameterBatchesPreserveMappingMetadataAndEmptyRows(string name, string mode)
    {
        using var input = new MemoryStream(Fixture(name));
        using var reader = new ArrowStreamReader(input);
        using var original = reader.ReadNextRecordBatch();
        Assert.NotNull(original);
        using var encoded = new MemoryStream();
        using (var writer = new ArrowStreamWriter(encoded, original.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(original);
            writer.WriteEnd();
        }
        encoded.Position = 0;
        using var restoredReader = new ArrowStreamReader(encoded);
        using var restored = restoredReader.ReadNextRecordBatch();
        Assert.NotNull(restored);
        Assert.Equal(1, restored.Length);
        Assert.Equal(mode, restored.Schema.Metadata["vgi.query.parameters.v1"]);
        if (name == "query_empty_parameters") Assert.Empty(restored.Schema.FieldsList);
        else
        {
            Assert.True(restored.Column(0).IsNull(0));
            Assert.IsType<Int32Type>(restored.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal("example.optional", restored.Schema.GetFieldByIndex(0).Metadata["logical"]);
            var type = Assert.IsType<StructType>(restored.Schema.GetFieldByIndex(1).DataType);
            Assert.Equal("retained", type.Fields[0].Metadata["nested"]);
            Assert.False(type.Fields[0].IsNullable);
        }
        Assert.Null(reader.ReadNextRecordBatch());
        Assert.Null(restoredReader.ReadNextRecordBatch());
    }

    [Fact]
    public void QueryRecordsPreserveNestedContracts()
    {
        RoundTrip<ClientQueryCapabilities>("query_offer");
        RoundTrip<QueryCapabilities>("query_capabilities");
        RoundTrip<QueryEvaluationContext>("query_context");
        RoundTrip<QueryObjectReference>("query_object_reference");
        RoundTrip<CatalogQueryPrepareRequest>("query_prepare");
        RoundTrip<CatalogQueryPrepareResult>("query_accepted");
        RoundTrip<CatalogQueryPrepareResult>("query_declined");
        var request = RoundTrip<CatalogQueryPrepareRequest>("query_prepare");
        Assert.Equal("UTC", request.EvaluationContext.Timezone);
        Assert.Equal(["one.two", "nested"], request.ReferencedObjects[0].Object.SchemaPath);
        Assert.Equal(0L, request.CatalogVersion);
        Assert.Null(RoundTrip<CatalogQueryPrepareResult>("query_declined").PreparedQueryToken);
    }

    [Fact]
    public void DependenciesPreserveRawIdentitiesAndNestedPayloads()
    {
        var table = RoundTrip<CatalogObjectRef>("object_ref");
        Assert.Equal(["one.two", "nested"], table.SchemaPath);
        Assert.Null(table.OwnerTable);
        var trigger = RoundTrip<CatalogObjectRef>("trigger_ref");
        Assert.Equal(table.Name, trigger.OwnerTable);
        var edge = RoundTrip<ObjectDependency>("object_dependency");
        Assert.Equal("owned", edge.Kind);
        var graph = RoundTrip<DependencySnapshot>("dependency_snapshot");
        Assert.Equal(42L, graph.CatalogVersion);
        Assert.Equal(4, graph.Objects.Count);
        Assert.Equal(3, graph.Dependencies.Count);
        var empty = RoundTrip<DependencySnapshot>("empty_dependency_snapshot");
        Assert.Empty(empty.Objects);
        Assert.Empty(empty.Dependencies);
    }

    [Fact]
    public void MetadataPatchesPreserveCommentPresenceAndLiteralTags()
    {
        var patch = RoundTrip<CatalogMetadataChanges>("metadata_patch");
        Assert.True(patch.SetComment);
        Assert.Equal("", patch.Comment);
        Assert.Equal("", patch.SetTags["a.b"]);
        Assert.Equal("literal", patch.SetTags["__proto__"]);
        Assert.Equal("empty-key", patch.SetTags[""]);
        Assert.Equal(["old", "OLD"], patch.RemoveTags);
        var clear = RoundTrip<CatalogMetadataChanges>("metadata_clear_comment");
        Assert.True(clear.SetComment);
        Assert.Null(clear.Comment);
        Assert.Empty(clear.SetTags);
        var noop = RoundTrip<CatalogMetadataChanges>("metadata_noop");
        Assert.False(noop.SetComment);
        Assert.Null(noop.Comment);
        Assert.Empty(noop.SetTags);
        Assert.Empty(noop.RemoveTags);
    }

    [Fact]
    public void ViewSecuritySurvivesCodecRoundTrip()
    {
        Assert.Equal("regular", RoundTrip<ViewInfo>("view_info").Security);
        Assert.Equal("barrier", RoundTrip<ViewInfo>("view_info_barrier").Security);
    }

    [Fact]
    public void ViewAliasesPreserveRawIdentifiers()
    {
        Assert.Equal(["a.b", "with\"quote"], RoundTrip<ViewInfo>("view_info").Aliases);
        Assert.Empty(RoundTrip<ViewInfo>("view_info_implicit").Aliases);
    }

    [Fact]
    public void ColumnRequestsUseActualServiceParameterTypes()
    {
        var operations = new[] { "ColumnAdd", "ColumnDrop", "ColumnRename", "ColumnTypeChange",
            "ColumnDefaultSet", "ColumnDefaultDrop", "ColumnCommentSet", "NotNullSet", "NotNullDrop" };
        foreach (var operation in operations)
        {
            var method = new RpcMethodInfo(typeof(IVgiService).GetMethod($"CatalogTable{operation}Async")!);
            foreach (var variant in new[] { "literal", "nested" })
            {
                var name = method.WireName["catalog_table_".Length..] + "_" + variant;
                using var stream = new MemoryStream(Fixture(name));
                using var reader = new ArrowStreamReader(stream);
                using var batch = reader.ReadNextRecordBatch();
                Assert.NotNull(batch);
                var args = ValueCodec.ExtractRow(batch, method.ParameterTypes);
                using var restoredBatch = ValueCodec.BuildRow(method.ParamsSchema, args);
                var restored = ValueCodec.ExtractRow(restoredBatch, method.ParameterTypes);
                for (var i = 0; i < args.Length; i++) Assert.Equivalent(args[i], restored[i]);
                var pathIndex = method.ParamsSchema.FieldsList.ToList().FindIndex(field => field.Name == "column_path");
                Assert.Equal(variant == "literal" ? ["a.b"] : new[] { "a", "b.\"quoted" },
                    Assert.IsType<List<string>>(args[pathIndex]));
                Assert.Null(reader.ReadNextRecordBatch());
            }
        }
    }

    private static byte[] Fixture(string name)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            var path = Path.Combine(directory.FullName, "conformance", "catalog-contracts.json");
            if (File.Exists(path))
            {
                using var document = JsonDocument.Parse(File.ReadAllText(path));
                return Convert.FromBase64String(document.RootElement.GetProperty("records").GetProperty(name).GetString()!);
            }

            directory = directory.Parent;
        }

        throw new FileNotFoundException("Required catalog contract corpus is missing");
    }

    private static T RoundTrip<T>(string name) where T : class, new() =>
        EmbeddedIpc.Decode<T>(EmbeddedIpc.Encode(EmbeddedIpc.Decode<T>(Fixture(name))));

    [Fact]
    public void OptionsRetainOpaqueMetadataAndExplicitNullOverrides()
    {
        var changes = RoundTrip<OptionChanges>("changes");
        Assert.Equal(Fixture("values"), changes.SetValues);
        Assert.Equal(["reset.me"], changes.ResetNames);
        var reset = RoundTrip<OptionChanges>("reset_only");
        Assert.Null(reset.SetValues);
        Assert.Equal(["reset.me"], reset.ResetNames);
        var state = RoundTrip<OptionState>("state");
        Assert.Equal(Fixture("values"), state.Values);
        Assert.Equal(["optional"], state.ExplicitNames);
        var empty = RoundTrip<OptionState>("empty_state");
        Assert.Equal(Fixture("empty_values"), empty.Values);
        Assert.Empty(empty.ExplicitNames);
    }

    [Fact]
    public void RequiredPathsRetainComponentBoundariesAndGroups()
    {
        var paths = RoundTrip<RequiredFilterPaths>("paths");
        Assert.Equal("[[[\"a.b\"],[\"a\",\"b\"]],[[\"record\",\"literal.\\\"name\"]]]",
            JsonSerializer.Serialize(paths.Groups, new JsonSerializerOptions
            {
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            }));
        Assert.Empty(RoundTrip<RequiredFilterPaths>("empty_paths").Groups);
    }
    [Fact]
    public void CapabilityProfilesAndOptionDefinitionsRoundTrip()
    {
        var capabilities = RoundTrip<CatalogCapabilities>("capabilities");
        Assert.Equal(["vgi.schema.options.read.v1"], capabilities.Read);
        Assert.Equal(["vgi.schema.options.create.v1"], capabilities.Ddl);
        Assert.Empty(RoundTrip<CatalogCapabilities>("empty_capabilities").Read);
        var definitions = RoundTrip<OptionDefinitions>("definitions");
        Assert.Equal(Fixture("values"), definitions.Defaults);
        Assert.Equal(["optional", "record"], definitions.CreateNames);
        Assert.Equal(["optional"], definitions.AlterNames);
    }

    [Fact]
    public void LayoutsRetainOrdersAndClearVersusUnchanged()
    {
        var expression = RoundTrip<SqlExpression>("expression");
        Assert.Equal(["one.two", "nested"], expression.SchemaPath);
        var key = RoundTrip<SortKey>("sort_key");
        Assert.Equal(Fixture("expression"), key.Expression);
        Assert.Equal(SortDirection.Desc, key.Direction);
        Assert.Equal(NullOrder.First, key.NullOrder);
        var layout = RoundTrip<TableLayout>("layout");
        Assert.Equal(Fixture("expression"), Assert.Single(layout.PartitionBy));
        Assert.Equal(Fixture("sort_key"), Assert.Single(layout.SortedBy));
        Assert.Equal(LayoutCoverage.AllRows, layout.Coverage);
        Assert.Empty(RoundTrip<TableLayout>("empty_layout").SortedBy);
        var change = RoundTrip<TableLayoutChange>("layout_change");
        Assert.Equal(LayoutChangeMode.Rewrite, change.Mode);
        Assert.Empty(change.PartitionBy!);
        Assert.Equal(Fixture("sort_key"), Assert.Single(change.SortedBy!));
        var clear = RoundTrip<TableLayoutChange>("layout_clear");
        Assert.Equal(LayoutChangeMode.FutureWrites, clear.Mode);
        Assert.Null(clear.PartitionBy);
        Assert.Empty(clear.SortedBy!);
        var create = RoundTrip<TableCreateRequest>("table_create");
        Assert.Equal("kept", create.Comment);
        Assert.Equal(Fixture("values"), create.Options);
        Assert.Equal(Fixture("layout"), create.Layout);
        var old = RoundTrip<TableCreateRequest>("table_create_legacy");
        Assert.Null(old.Comment);
        Assert.Null(old.Tags);
        Assert.Null(old.Options);
        Assert.Null(old.Layout);
        var info = RoundTrip<TableInfo>("table_info");
        Assert.Equal(Fixture("state"), info.Options);
        Assert.Equal(Fixture("layout"), info.Layout);
    }
}
