// Copyright 2025, 2026 Query Farm LLC - https://query.farm

using System.Text.Json;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;
using Apache.Arrow.Ipc;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

public class CatalogContractsTests
{
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
