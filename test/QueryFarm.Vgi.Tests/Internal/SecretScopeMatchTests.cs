using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Grades <see cref="SecretArgCodec.ScopeMatches"/> against DuckDB's own
/// <c>BaseSecret::MatchScore</c> (Haybarn 2.0.1 engine, duckdb#28684): an authority-only URI scope
/// has an implicit trailing <c>/</c>. <c>Vectors/scope_match_vectors.json</c> is the cross-SDK
/// vector file generated from the engine's C++ (copied verbatim from the vgi-python fix).</summary>
public class SecretScopeMatchTests
{
    [Theory]
    [InlineData("snmp://10.0.0.1", "snmp://10.0.0.1", true)]
    [InlineData("snmp://10.0.0.1", "snmp://10.0.0.1/x", true)]
    [InlineData("snmp://10.0.0.1", "snmp://10.0.0.15", false)]
    [InlineData("snmp://10.0.0.1", "snmp://10.0.0.1:161", false)]
    [InlineData("s3://bucket", "s3://bucket/key", true)]
    [InlineData("s3://bucket", "s3://bucket-evil/key", false)]
    [InlineData("s3://bucket", "s3://bucket?x=1", false)]
    [InlineData("dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture:shop", false)]
    [InlineData("dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture/shop", true)]
    [InlineData("s3://bucket/", "s3://bucket/key", true)]
    [InlineData("s3://bucket/pre", "s3://bucket/prefix/x", true)]
    [InlineData("s3://bucket/pre", "s3://bucket/other", false)]
    [InlineData("https://h?a", "https://h?ab", true)]
    [InlineData("https://h#f", "https://h#fx", true)]
    [InlineData("a://b\\c", "a://b\\cd", true)]
    [InlineData("snmp://", "snmp://anything", true)]
    [InlineData("snmp://10.0.0.", "snmp://10.0.0.15:161", false)]
    [InlineData("foo", "foobar", true)]
    [InlineData("c:\\data", "c:\\data2\\x", true)]
    [InlineData("x:/y://z", "x:/y://zz", true)]
    [InlineData("s3://bucket", "s3://buck", false)]
    [InlineData("s3://bucket", "s3://bucket", true)]
    [InlineData("vgi-conformance://host1", "vgi-conformance://host1/data", true)]
    [InlineData("vgi-conformance://host1", "vgi-conformance://host10/data", false)]
    [InlineData("vgi-conformance://host1", "vgi-conformance://host1:9000", false)]
    public void CuratedCases(string scope, string path, bool expected) =>
        Assert.Equal(expected, SecretArgCodec.ScopeMatches(scope, path));

    [Fact]
    public void EmptyPrefixIsNotAScope() => Assert.False(SecretArgCodec.ScopeMatches("", "s3://bucket/x"));

    [Fact]
    public void AgreesWithDuckDbOnEveryVector()
    {
        using var document = JsonDocument.Parse(File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Vectors", "scope_match_vectors.json")));
        var cases = document.RootElement.GetProperty("cases").EnumerateArray().ToList();
        Assert.True(cases.Count > 3000, $"expected the full vector file, got {cases.Count} cases");
        var mismatches = new List<string>();
        foreach (var c in cases)
        {
            var scope = c.GetProperty("scope").GetString()!;
            var path = c.GetProperty("path").GetString()!;
            var expected = c.GetProperty("match").GetBoolean();
            if (SecretArgCodec.ScopeMatches(scope, path) != expected)
            {
                mismatches.Add($"scope={JsonSerializer.Serialize(scope)} path={JsonSerializer.Serialize(path)} expected={expected}");
            }
        }

        Assert.True(mismatches.Count == 0, string.Join("\n", mismatches.Take(20)));
    }

    private static StructArray Secret(string scope, string apiKey)
    {
        var fields = new (string Name, string Value)[] { ("type", "snmp"), ("scope", scope), ("api_key", apiKey) };
        var structType = new StructType(fields.Select(f => new Field(f.Name, StringType.Default, nullable: true)).ToList());
        var children = fields.Select(f => (IArrowArray)new StringArray.Builder().Append(f.Value).Build()).ToList();
        return new StructArray(structType, 1, children, ArrowBuffer.Empty, nullCount: 0);
    }

    [Theory]
    [InlineData("snmp://10.0.0.1/sys", "host1")]
    [InlineData("snmp://10.0.0.15", "host15")]
    [InlineData("snmp://10.0.0.1:161", "default")]
    public void ForScopeOfType_StopsAtTheAuthorityBoundary(string path, string expectedKey)
    {
        var columns = new (string Name, StructArray Secret)[]
        {
            ("host1", Secret("snmp://10.0.0.1", "host1")),
            ("host15", Secret("snmp://10.0.0.15", "host15")),
            ("fallback", Secret("", "default")),
        };
        var schema = new Schema(columns.Select(c => new Field(c.Name, c.Secret.Data.DataType, nullable: true)).ToList(), metadata: null);
        var batch = new RecordBatch(schema, columns.Select(c => (IArrowArray)c.Secret).ToList(), 1);
        var decoded = SecretArgCodec.Decode(RecordBatchIpc.Write(batch));

        var found = SecretArgCodec.ForScopeOfType(decoded, path, "snmp");
        Assert.Equal(expectedKey, SecretArgCodec.FieldString(found, "api_key"));
    }
}
