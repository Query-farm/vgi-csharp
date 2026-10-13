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
}
