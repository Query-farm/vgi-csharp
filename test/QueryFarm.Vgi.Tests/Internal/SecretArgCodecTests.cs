using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Exercises <see cref="SecretArgCodec"/> against the exact wire shape
/// <c>vgi_arrow_utils.cpp</c>'s <c>BuildSecretsBatch</c> produces: one row, one STRUCT column per
/// resolved secret, the column named by the secret's own DuckDB NAME (not its type) — see
/// <see cref="SecretArgCodec"/>'s doc comment.</summary>
public class SecretArgCodecTests
{
    private static StructArray BuildSecretStruct(params (string Name, IArrowArray Value)[] fields)
    {
        var structType = new StructType(fields.Select(f => new Field(f.Name, f.Value.Data.DataType, nullable: true)).ToList());
        return new StructArray(structType, 1, fields.Select(f => f.Value).ToList(), ArrowBuffer.Empty, nullCount: 0);
    }

    private static IArrowArray Str(string value) => new StringArray.Builder().Append(value).Build();

    private static byte[] BuildSecretsBytes(params (string ColumnName, StructArray Secret)[] columns)
    {
        var schema = new Schema(columns.Select(c => new Field(c.ColumnName, c.Secret.Data.DataType, nullable: true)).ToList(), metadata: null);
        var batch = new RecordBatch(schema, columns.Select(c => (IArrowArray)c.Secret).ToList(), 1);

        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            writer.WriteStart();
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }

    [Fact]
    public void Decode_NullOrEmptyBytes_ReturnsEmptyDictionary()
    {
        Assert.Empty(SecretArgCodec.Decode(null));
        Assert.Empty(SecretArgCodec.Decode([]));
    }

    [Fact]
    public void Decode_KeysByColumnName_NotByTypeField()
    {
        var secret = BuildSecretStruct(("type", Str("vgi_example")), ("secret_string", Str("hello")), ("scope", Str("")));
        var bytes = BuildSecretsBytes(("test_secret", secret));

        var decoded = SecretArgCodec.Decode(bytes);

        Assert.Single(decoded);
        Assert.True(decoded.ContainsKey("test_secret"));
        Assert.False(decoded.ContainsKey("vgi_example"));
    }

    [Fact]
    public void FindByType_MatchesTheTypeFieldNotTheColumnName()
    {
        var secret = BuildSecretStruct(("type", Str("vgi_example")), ("secret_string", Str("hello")));
        var bytes = BuildSecretsBytes(("test_secret", secret));
        var decoded = SecretArgCodec.Decode(bytes);

        var found = SecretArgCodec.FindByType(decoded, "vgi_example");
        Assert.NotNull(found);
        Assert.Equal("hello", SecretArgCodec.FieldString(found, "secret_string"));

        Assert.Null(SecretArgCodec.FindByType(decoded, "some_other_type"));
    }

    [Fact]
    public void ForScopeOfType_PicksLongestPrefixMatch()
    {
        var a = BuildSecretStruct(("type", Str("vgi_example")), ("scope", Str("s3://bucket-a/")), ("api_key", Str("ka")));
        var b = BuildSecretStruct(("type", Str("vgi_example")), ("scope", Str("s3://bucket-a/sub/")), ("api_key", Str("kb")));
        var bytes = BuildSecretsBytes(("scoped_a", a), ("scoped_b", b));
        var decoded = SecretArgCodec.Decode(bytes);

        // Both scopes are prefixes of this path; the longer (more specific) one wins.
        var match = SecretArgCodec.ForScopeOfType(decoded, "s3://bucket-a/sub/file.parquet", "vgi_example");
        Assert.Equal("kb", SecretArgCodec.FieldString(match, "api_key"));

        // Only the shorter scope is a prefix here.
        var match2 = SecretArgCodec.ForScopeOfType(decoded, "s3://bucket-a/other.parquet", "vgi_example");
        Assert.Equal("ka", SecretArgCodec.FieldString(match2, "api_key"));

        // No scope is a prefix of this path at all.
        Assert.Null(SecretArgCodec.ForScopeOfType(decoded, "s3://no-such-bucket/x", "vgi_example"));
    }

    [Fact]
    public void ForScopeOfType_UnscopedSecretIsAFallback_NotAPrefixMatch()
    {
        var scoped = BuildSecretStruct(("type", Str("vgi_example")), ("scope", Str("s3://bucket-a/")), ("api_key", Str("ka")));
        var unscoped = BuildSecretStruct(("type", Str("vgi_example")), ("scope", Str("")), ("api_key", Str("default")));
        var bytes = BuildSecretsBytes(("scoped_a", scoped), ("fallback", unscoped));
        var decoded = SecretArgCodec.Decode(bytes);

        // A path matching the scoped secret prefers it over the unscoped fallback.
        Assert.Equal("ka", SecretArgCodec.FieldString(SecretArgCodec.ForScopeOfType(decoded, "s3://bucket-a/x", "vgi_example"), "api_key"));

        // A path matching NEITHER scope falls back to the unscoped secret.
        Assert.Equal("default", SecretArgCodec.FieldString(SecretArgCodec.ForScopeOfType(decoded, "s3://elsewhere/x", "vgi_example"), "api_key"));
    }

    [Fact]
    public void AllOfType_ReturnsEveryMatchingSecret()
    {
        var a = BuildSecretStruct(("type", Str("vgi_example")), ("scope", Str("s3://bucket-a/")));
        var b = BuildSecretStruct(("type", Str("vgi_example")), ("scope", Str("s3://bucket-b/")));
        var other = BuildSecretStruct(("type", Str("other_type")), ("scope", Str("")));
        var bytes = BuildSecretsBytes(("scoped_a", a), ("scoped_b", b), ("unrelated", other));
        var decoded = SecretArgCodec.Decode(bytes);

        Assert.Equal(2, SecretArgCodec.AllOfType(decoded, "vgi_example").Count);
        Assert.Single(SecretArgCodec.AllOfType(decoded, "other_type"));
    }
}
