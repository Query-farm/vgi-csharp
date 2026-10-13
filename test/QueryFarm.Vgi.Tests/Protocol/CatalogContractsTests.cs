// Copyright 2025, 2026 Query Farm LLC - https://query.farm

using System.Text.Json;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

public class CatalogContractsTests
{
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
