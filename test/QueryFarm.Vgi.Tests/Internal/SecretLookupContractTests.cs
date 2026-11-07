using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Arguments;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>
/// The worker side of the secret-lookup contract (vgi <c>docs/secret_lookup_fixtures.md</c>): the
/// secrets batch is keyed by each secret's DuckDB NAME (<c>__default_&lt;type&gt;</c> for an unnamed
/// one), so by-type selection must read the <c>type</c> field; by-name selection must check the
/// type; scope selection is longest prefix, then unscoped, then none; and every static
/// requirement must reach <see cref="FunctionInfo.RequiredSecrets"/> or fail loudly.
/// </summary>
public class SecretLookupContractTests
{
    private static IArrowArray Str(string value) => new StringArray.Builder().Append(value).Build();

    private static StructArray Secret(params (string Name, string Value)[] fields)
    {
        var structType = new StructType(fields.Select(f => new Field(f.Name, StringType.Default, nullable: true)).ToList());
        return new StructArray(structType, 1, fields.Select(f => Str(f.Value)).ToList(), ArrowBuffer.Empty, nullCount: 0);
    }

    /// <summary>A delivered secret as the extension serializes it: type, provider, name, the custom
    /// fields, scope.</summary>
    private static (string, StructArray) Delivered(string name, string type, string scope, params (string, string)[] custom) =>
        (name, Secret([("type", type), ("provider", "config"), ("name", name), .. custom, ("scope", scope)]));

    private static byte[] Batch(params (string Column, StructArray Secret)[] columns)
    {
        var schema = new Schema(columns.Select(c => new Field(c.Column, c.Secret.Data.DataType, nullable: true)).ToList(), metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema, columns.Select(c => (IArrowArray)c.Secret).ToList(), 1));
    }

    private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> Decode(params (string, StructArray)[] columns) =>
        SecretArgCodec.Decode(Batch(columns));

    private static string? Key(IReadOnlyDictionary<string, IArrowArray>? secret) => SecretArgCodec.FieldString(secret, "api_key");

    // ---- by type -----------------------------------------------------------------------------

    [Fact]
    public void ByType_FindsAnUnnamedDefaultSecret()
    {
        var resolved = Decode(Delivered("__default_vgi_example", "vgi_example", "", ("api_key", "unnamed-key")));
        Assert.Equal("unnamed-key", Key(SecretArgCodec.FindByType(resolved, "vgi_example")));
        Assert.Equal("unnamed-key", SecretValues.FromResolved(Batch(Delivered("__default_vgi_example", "vgi_example", "", ("api_key", "unnamed-key")))).FindByType("vgi_example")?.GetString("api_key"));
    }

    [Fact]
    public void ByType_FindsASecretNamedUnlikeItsType()
    {
        var resolved = Decode(Delivered("conformance_creds", "vgi_example", "", ("api_key", "named-key")));
        Assert.Equal("named-key", Key(SecretArgCodec.FindByType(resolved, "vgi_example")));
        Assert.Null(SecretArgCodec.FindByType(resolved, "s3"));
    }

    [Fact]
    public void ByType_PrefersAnUnscopedSecretOverAScopedOneOfTheSameType()
    {
        var resolved = Decode(
            Delivered("scoped", "vgi_example", "vgi-conformance://a/", ("api_key", "scoped-key")),
            Delivered("plain", "vgi_example", "", ("api_key", "plain-key")));
        Assert.Equal("plain-key", Key(SecretArgCodec.FindByType(resolved, "vgi_example")));
    }

    [Fact]
    public void ByType_FallsBackToTheFirstOfTheTypeWhenAllAreScoped()
    {
        var resolved = Decode(Delivered("scoped", "vgi_example", "vgi-conformance://a/", ("api_key", "scoped-key")));
        Assert.Equal("scoped-key", Key(SecretArgCodec.FindByType(resolved, "vgi_example")));
    }

    [Fact]
    public void ByType_FallsBackToAColumnNamedAfterTheTypeOnlyWhenNoSecretHasTheType()
    {
        // A client that keys the batch by type and sends bare key/value structs (no type field).
        var resolved = Decode(("vgi_example", Secret(("api_key", "bare-key"))));
        Assert.Equal("bare-key", Key(SecretArgCodec.FindByType(resolved, "vgi_example")));

        // A secret of another type that happens to be NAMED like the requested type does not shadow
        // a real secret of that type.
        var shadow = Decode(
            Delivered("vgi_example", "s3", "", ("api_key", "s3-key")),
            Delivered("__default_vgi_example", "vgi_example", "", ("api_key", "real-key")));
        Assert.Equal("real-key", Key(SecretArgCodec.FindByType(shadow, "vgi_example")));
    }

    // ---- by name -----------------------------------------------------------------------------

    [Fact]
    public void ByName_SelectsEachSameTypeSecretByItsName()
    {
        var resolved = Decode(
            Delivered("conformance_one", "vgi_example", "", ("api_key", "key-one")),
            Delivered("conformance_two", "vgi_example", "", ("api_key", "key-two")));
        Assert.Equal("key-one", Key(SecretArgCodec.Lookup(resolved, "vgi_example", name: "conformance_one")));
        Assert.Equal("key-two", Key(SecretArgCodec.Lookup(resolved, "vgi_example", name: "conformance_two")));
        Assert.Null(SecretArgCodec.Lookup(resolved, "vgi_example", name: "conformance_missing"));
    }

    [Fact]
    public void ByName_RejectsASecretOfAnotherType()
    {
        var resolved = Decode(Delivered("conformance_s3", "s3", "", ("key_id", "AKIA")));
        Assert.Null(SecretArgCodec.Lookup(resolved, "vgi_example", name: "conformance_s3"));
        Assert.Null(SecretValues.FromResolved(Batch(Delivered("conformance_s3", "s3", "", ("key_id", "AKIA")))).Get("vgi_example", name: "conformance_s3"));
        Assert.NotNull(SecretArgCodec.Lookup(resolved, "s3", name: "conformance_s3"));
    }

    // ---- by scope ----------------------------------------------------------------------------

    private static IReadOnlyDictionary<string, IReadOnlyDictionary<string, IArrowArray>> Nested(bool withAny = true, bool withAb = true)
    {
        var columns = new List<(string, StructArray)> { Delivered("conformance_a", "vgi_example", "vgi-conformance://a/", ("api_key", "key-a")) };
        if (withAb)
        {
            columns.Add(Delivered("conformance_ab", "vgi_example", "vgi-conformance://a/b/", ("api_key", "key-ab")));
        }

        if (withAny)
        {
            columns.Add(Delivered("conformance_any", "vgi_example", "", ("api_key", "key-any")));
        }

        columns.Add(Delivered("other", "s3", "vgi-conformance://", ("api_key", "s3-key")));
        return Decode([.. columns]);
    }

    [Theory]
    [InlineData("vgi-conformance://a/b/file.parquet", "key-ab")]
    [InlineData("vgi-conformance://a/b/c/d", "key-ab")]
    [InlineData("vgi-conformance://a/other/file.parquet", "key-a")]
    [InlineData("vgi-conformance://a/x", "key-a")]
    [InlineData("vgi-conformance://z/file.parquet", "key-any")]
    public void ForScopeOfType_LongestPrefixThenUnscopedFallback(string path, string expected)
    {
        Assert.Equal(expected, Key(SecretArgCodec.ForScopeOfType(Nested(), path, "vgi_example")));
        Assert.Equal(expected, Key(SecretArgCodec.Lookup(Nested(), "vgi_example", scope: path)));
    }

    [Fact]
    public void ForScopeOfType_NoMatchAndNoUnscopedSecretIsNone()
    {
        Assert.Null(SecretArgCodec.ForScopeOfType(Nested(withAny: false), "vgi-conformance://z/file.parquet", "vgi_example"));
    }

    [Fact]
    public void ForScopeOfType_FallsToTheOuterScopeWhenTheInnerIsGone()
    {
        Assert.Equal("key-a", Key(SecretArgCodec.ForScopeOfType(Nested(withAny: false, withAb: false), "vgi-conformance://a/b/file.parquet", "vgi_example")));
    }

    [Fact]
    public void ForScopeOfType_ReadsEveryNewlineSeparatedPrefix()
    {
        var resolved = Decode(Delivered("multi", "vgi_example", "vgi-conformance://x/\nvgi-conformance://y/", ("api_key", "multi-key")));
        Assert.Equal("multi-key", Key(SecretArgCodec.ForScopeOfType(resolved, "vgi-conformance://y/1", "vgi_example")));
        Assert.Null(SecretArgCodec.ForScopeOfType(resolved, "vgi-conformance://z/1", "vgi_example"));
    }

    [Fact]
    public void AllOfType_MatchesTheTypeFieldAcrossNames()
    {
        Assert.Equal(3, SecretArgCodec.AllOfType(Nested(), "vgi_example").Count);
        Assert.Single(SecretArgCodec.AllOfType(Nested(), "s3"));
    }

    // ---- bind-time accessor --------------------------------------------------------------------

    [Fact]
    public void Accessor_FirstAttempt_AnswersAStaticallyDeliveredSecretByTypeWithoutARoundTrip()
    {
        var accessor = new SecretsAccessor(Batch(Delivered("__default_vgi_example", "vgi_example", "", ("api_key", "k"))), isRetry: false);
        Assert.Equal("k", Key(accessor.Get("vgi_example")));
        Assert.False(accessor.NeedsResolution);
    }

    [Fact]
    public void Accessor_FirstAttempt_AScopedLookupAlwaysAsksTheClient()
    {
        var accessor = new SecretsAccessor(Batch(Delivered("conformance_a", "vgi_example", "vgi-conformance://a/", ("api_key", "k"))), isRetry: false);
        Assert.Null(accessor.Get("vgi_example", scope: "vgi-conformance://a/b/"));
        Assert.Equal("vgi-conformance://a/b/", Assert.Single(accessor.PendingLookups).Scope);
    }

    [Fact]
    public void Accessor_FirstAttempt_AnUndeliveredNameIsRequested()
    {
        var accessor = new SecretsAccessor(null, isRetry: false);
        Assert.Null(accessor.Get("vgi_example", name: "conformance_one"));
        var lookup = Assert.Single(accessor.PendingLookups);
        Assert.Equal(("vgi_example", "conformance_one"), (lookup.SecretType, lookup.SecretName));
    }

    [Fact]
    public void Accessor_Retry_ByNameChecksTheType()
    {
        var bytes = Batch(Delivered("conformance_s3", "s3", "", ("key_id", "AKIA")));
        Assert.Null(new SecretsAccessor(bytes, isRetry: true).Get("vgi_example", name: "conformance_s3"));
    }

    [Fact]
    public void Require_AbsentAfterResolution_ThrowsNamingTheTypeAndNoValue()
    {
        // A scoped-only secret does not satisfy an unscoped by-type requirement once the client
        // has resolved it to nothing — but even if one were delivered, no value may leak.
        var values = SecretValues.FromResolved(null);
        var ex = Assert.Throws<VgiInvalidArgumentException>(() => values.Require("vgi_example"));
        Assert.Contains("vgi_example", ex.Message);

        var retry = new SecretsAccessor(Batch(Delivered("other", "s3", "", ("secret", "sk-do-not-leak"))), isRetry: true);
        var ex2 = Assert.Throws<VgiInvalidArgumentException>(() => retry.Require("vgi_example"));
        Assert.Contains("vgi_example", ex2.Message);
        Assert.DoesNotContain("sk-do-not-leak", ex2.Message);
    }

    [Fact]
    public void Require_FirstAttempt_RequestsTheSecretInsteadOfFailing()
    {
        var accessor = new SecretsAccessor(null, isRetry: false);
        Assert.Null(accessor.Require("vgi_example"));
        Assert.True(accessor.NeedsResolution);
    }

    [Fact]
    public void Require_Present_ReturnsIt()
    {
        var values = SecretValues.FromResolved(Batch(Delivered("conformance_creds", "vgi_example", "", ("api_key", "named-key"))));
        var secret = values.Require("vgi_example");
        Assert.Equal("conformance_creds", secret?.Name);
        Assert.Equal("named-key", secret?.GetString("api_key"));
    }

    // ---- declarations ------------------------------------------------------------------------

    [Fact]
    public void Declarations_KeepEveryEntryInOrder_AndNormaliseEmptyScopeAndName()
    {
        var declared = RequiredSecretDeclarations.Validate("f",
        [
            new RequiredSecret { SecretType = "vgi_example" },
            new RequiredSecret { SecretType = "vgi_example", Scope = "vgi-conformance://a/" },
            new RequiredSecret { SecretType = "vgi_example", Scope = "vgi-conformance://a/b/" },
            new RequiredSecret { SecretType = "s3", Scope = "", SecretName = "" },
        ]);
        Assert.Equal(4, declared.Count);
        Assert.Equal([null, "vgi-conformance://a/", "vgi-conformance://a/b/", null], declared.Select(d => d.Scope));
        Assert.Null(declared[3].SecretName);
    }

    [Fact]
    public void Declarations_RejectMalformedEntriesInsteadOfDroppingThem()
    {
        Assert.Contains("SecretType", Assert.Throws<InvalidOperationException>(
            () => RequiredSecretDeclarations.Validate("f", [new RequiredSecret { SecretType = "" }])).Message);
        Assert.Contains("RequiredSecrets[1]", Assert.Throws<InvalidOperationException>(
            () => RequiredSecretDeclarations.Validate("f", [new RequiredSecret { SecretType = "s3" }, null!])).Message);
        Assert.Contains("'f'", Assert.Throws<InvalidOperationException>(
            () => RequiredSecretDeclarations.Validate("f", null)).Message);
    }

    private sealed class DeclaringTable(IReadOnlyList<RequiredSecret> secrets) : ITableFunction
    {
        public string Name => "declaring";

        public IReadOnlyList<RequiredSecret> RequiredSecrets => secrets;

        public Schema ArgumentsSchema { get; } = new([], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("x", Int64Type.Default, nullable: true)], metadata: null);

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) => throw new NotSupportedException();
    }

    private static async Task<FunctionInfo> ListAsync(ITableFunction function)
    {
        var registry = new CatalogRegistry();
        registry.RegisterTable(function);
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData;
        var response = await service.CatalogSchemaContentsFunctionsAsync(attach, ["main"], SchemaObjectType.TableFunction, null);
        return Assert.Single(response.Items.Select(EmbeddedIpc.Decode<FunctionInfo>), f => f.Name == function.Name);
    }

    [Fact]
    public async Task Discovery_AdvertisesEveryStaticRequirement()
    {
        var info = await ListAsync(new DeclaringTable(
        [
            new RequiredSecret { SecretType = "vgi_example" },
            new RequiredSecret { SecretType = "s3", Scope = "s3://vgi-conformance/" },
        ]));
        Assert.Equal(
            [("vgi_example", (string?)null), ("s3", "s3://vgi-conformance/")],
            info.RequiredSecrets!.Select(r => (r.SecretType, r.Scope)));
    }

    [Fact]
    public async Task Discovery_FailsLoudlyOnAMalformedRequirement()
    {
        await Assert.ThrowsAnyAsync<Exception>(() => ListAsync(new DeclaringTable([new RequiredSecret { SecretType = " " }])));
    }
}
