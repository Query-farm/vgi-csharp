using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Arguments;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Grades <c>duckdb.spatial/intersects_extent@1</c> against DuckDB core <c>a &amp;&amp; b</c>
/// (<c>Vectors/intersects_extent_cases.json</c>), through the public <see cref="FilterSet"/> path a
/// producer uses at runtime: the row's <c>geom</c> is the left argument, the literal the right.</summary>
public class IntersectsExtentConformanceTests
{
    private static readonly IReadOnlyDictionary<string, string> WkbMetadata =
        new Dictionary<string, string> { ["ARROW:extension:name"] = "geoarrow.wkb" };

    private static readonly FilterFunctionCapability Capability = new()
    {
        Namespace = "duckdb.spatial",
        Name = "intersects_extent",
        Version = 1,
    };

    private static readonly Schema OutputSchema =
        new([new Field("geom", BinaryType.Default, true, WkbMetadata)], null);

    [Theory]
    [InlineData("le", "le")]
    [InlineData("le", "be")]
    [InlineData("be", "le")]
    [InlineData("be", "be")]
    public void MatchesDuckDbCore(string leftEncoding, string rightEncoding)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Vectors", "intersects_extent_cases.json")));
        var geometries = document.RootElement.GetProperty("geometries").EnumerateArray().ToDictionary(
            g => g.GetProperty("name").GetString()!,
            g => (Le: Convert.FromHexString(g.GetProperty("wkb_le").GetString()!),
                Be: Convert.FromHexString(g.GetProperty("wkb_be").GetString()!)),
            StringComparer.Ordinal);
        byte[] Wkb(string name, string encoding) => encoding == "le" ? geometries[name].Le : geometries[name].Be;

        var filters = new Dictionary<string, FilterSet>(StringComparer.Ordinal);
        var mismatches = new List<string>();
        var cases = document.RootElement.GetProperty("cases").EnumerateArray().ToList();
        Assert.Equal(2116, cases.Count);
        foreach (var testCase in cases)
        {
            var left = testCase.GetProperty("left").GetString()!;
            var right = testCase.GetProperty("right").GetString()!;
            var expected = testCase.GetProperty("expected").GetBoolean();
            if (!filters.TryGetValue(right, out var filter))
            {
                filter = filters[right] = Filter(Wkb(right, rightEncoding), "required");
            }

            bool actual;
            try
            {
                actual = filter.Matches(new Dictionary<string, object?> { ["geom"] = Wkb(left, leftEncoding) });
            }
            catch (Exception exception)
            {
                mismatches.Add($"{left} && {right}: expected {expected}, threw {exception.GetType().Name}: {exception.Message}");
                continue;
            }

            if (actual != expected)
            {
                mismatches.Add($"{left} && {right}: expected {expected}, got {actual}");
            }
        }

        Assert.True(mismatches.Count == 0,
            $"{mismatches.Count} mismatches ({leftEncoding}/{rightEncoding}):\n{string.Join('\n', mismatches)}");
    }

    public static TheoryData<string, object?> UnevaluableValues => new()
    {
        { "truncated", Convert.FromHexString("0101000000000000000000F03F") },
        { "trailing data", Convert.FromHexString("0101000000000000000000F03F000000000000F03F00") },
        { "bad byte order", Convert.FromHexString("0201000000000000000000F03F000000000000F03F") },
        { "unsupported type", Convert.FromHexString("0108000000") },
        { "empty bytes", System.Array.Empty<byte>() },
        { "huge count", Convert.FromHexString("0102000000FFFFFFFF") },
        { "non-binary", "POINT(1 1)" },
    };

    [Theory]
    [MemberData(nameof(UnevaluableValues))]
    public void AdvisoryFilter_KeepsRowsItCannotEvaluate(string label, object? value)
    {
        var point = Convert.FromHexString("0101000000000000000000F03F000000000000F03F");
        Assert.True(Filter(point, "advisory").Matches(new Dictionary<string, object?> { ["geom"] = value }), label);
    }

    [Theory]
    [MemberData(nameof(UnevaluableValues))]
    public void RequiredFilter_FailsOnRowsItCannotEvaluate(string label, object? value)
    {
        var point = Convert.FromHexString("0101000000000000000000F03F000000000000F03F");
        var filter = Filter(point, "required");
        Assert.ThrowsAny<Exception>(() => filter.Matches(new Dictionary<string, object?> { ["geom"] = value }));
        _ = label;
    }

    [Theory]
    [InlineData("0101000020E6100000000000000000F03F000000000000F03F", "POINT(1 1)", true)]
    [InlineData("0101000020E6100000000000000000F03F000000000000F03F", "POINT(5 5)", false)]
    [InlineData("0020000001000010E63FF00000000000003FF0000000000000", "POINT(1 1)", true)]
    [InlineData("0101000080000000000000F03F000000000000F03F0000000000000000", "POINT(1 1)", true)]
    [InlineData("0101000040000000000000F03F000000000000F03F0000000000000000", "POINT(1 1)", true)]
    public void Ewkb_IsReadLikeDuckDbCore(string ewkbHex, string target, bool expected)
    {
        // Expected values from DuckDB 2.0.0: ST_GeomFromWKB(<ewkb>) && '<target>'::GEOMETRY.
        var literal = target == "POINT(1 1)"
            ? Convert.FromHexString("0101000000000000000000F03F000000000000F03F")
            : Convert.FromHexString("010100000000000000000014400000000000001440");
        var row = new Dictionary<string, object?> { ["geom"] = Convert.FromHexString(ewkbHex) };
        Assert.Equal(expected, Filter(literal, "required").Matches(row));
    }

    internal static FilterSet Filter(byte[] literal, string mode)
    {
        const string call = "{\"node\":\"call\",\"function\":{\"namespace\":\"duckdb.spatial\",\"name\":\"intersects_extent\",\"version\":1}," +
            "\"arguments\":[{\"node\":\"column_ref\",\"column_index\":0,\"column_name\":\"geom\"},{\"node\":\"literal\",\"value_ref\":0}]}";
        var json = "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"snapshot\",\"predicates\":[" +
            $"{{\"id\":\"spatial\",\"revision\":0,\"mode\":\"{mode}\",\"source\":\"query\",\"expression\":{call}}}]}}";
        var metadata = new Dictionary<string, string>
        {
            ["vgi_filter_encoding"] = "vgi.filters.v2",
            ["vgi_filter_version"] = "2",
            ["vgi_evaluation_context"] = "vgi.none.v1",
        };
        var schema = new Schema([
            new Field("filter_spec", StringType.Default, false),
            new Field("value_0", BinaryType.Default, true, WkbMetadata),
        ], metadata);
        var batch = new RecordBatch(schema, [
            new StringArray.Builder().Append(json).Build(),
            new BinaryArray.Builder().Append(literal).Build(),
        ], 1);
        return FilterSet.Decode(RecordBatchIpc.Write(batch), null, OutputSchema, [Capability])!;
    }
}
