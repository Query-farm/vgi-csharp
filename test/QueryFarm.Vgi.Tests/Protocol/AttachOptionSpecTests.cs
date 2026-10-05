using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>
/// The <see cref="AttachOptionSpec"/> wire shape: the four shared spec columns plus the appended
/// nullable <c>required</c> and <c>secret</c> booleans, which readers look up by name so a spec from
/// an older peer (no such column) reads as <see langword="false"/>.
/// </summary>
public class AttachOptionSpecTests
{
    private static AttachOptionSpec Spec(bool required, bool secret, bool withDefault = false) =>
        AttachOptionSpecBuilder.Build(
            "api_key",
            "API key",
            StringType.Default,
            withDefault ? new StringArray.Builder().Append("default").Build() : null,
            required: required,
            secret: secret);

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void RoundTrips_RequiredAndSecret(bool required, bool secret)
    {
        var decoded = EmbeddedIpc.Decode<AttachOptionSpec>(EmbeddedIpc.Encode(Spec(required, secret)));

        Assert.Equal("api_key", decoded.Name);
        Assert.Equal("API key", decoded.Description);
        Assert.Null(decoded.DefaultValue);
        Assert.Equal(required, decoded.Required);
        Assert.Equal(secret, decoded.Secret);
    }

    [Fact]
    public void Secret_DefaultsToFalse()
    {
        Assert.False(new AttachOptionSpec().Secret);
        Assert.False(AttachOptionSpecBuilder.Build("region", "Region", StringType.Default, null).Secret);
    }

    [Fact]
    public void Secret_WithDefault_IsAllowedAndRoundTrips()
    {
        var decoded = EmbeddedIpc.Decode<AttachOptionSpec>(EmbeddedIpc.Encode(Spec(required: false, secret: true, withDefault: true)));

        Assert.True(decoded.Secret);
        Assert.False(decoded.Required);
        Assert.NotNull(decoded.DefaultValue);
    }

    [Fact]
    public void Wire_SecretIsANullableBoolean_AppendedAfterRequired()
    {
        using var batch = RecordBatchIpc.Read(EmbeddedIpc.Encode(Spec(required: true, secret: true)));
        var names = batch.Schema.FieldsList.Select(f => f.Name).ToList();

        Assert.Equal(["name", "description", "type", "default_value", "required", "secret"], names);
        foreach (var flag in new[] { "required", "secret" })
        {
            var field = batch.Schema.GetFieldByName(flag);
            Assert.IsType<BooleanType>(field.DataType);
            Assert.True(field.IsNullable);
            Assert.True(((BooleanArray)batch.Column(flag)).GetValue(0));
        }
    }

    [Fact]
    public void SpecWithoutSecretColumn_ReadsAsNotSecret()
    {
        // An older peer's spec: the four shared columns plus `required`, no `secret`.
        var decoded = EmbeddedIpc.Decode<AttachOptionSpec>(WithoutColumns(Spec(required: true, secret: true), "secret"));

        Assert.Equal("api_key", decoded.Name);
        Assert.True(decoded.Required);
        Assert.False(decoded.Secret);
    }

    [Fact]
    public void SpecWithoutRequiredOrSecretColumns_ReadsBothAsFalse()
    {
        var decoded = EmbeddedIpc.Decode<AttachOptionSpec>(WithoutColumns(Spec(required: true, secret: true), "required", "secret"));

        Assert.Equal("api_key", decoded.Name);
        Assert.False(decoded.Required);
        Assert.False(decoded.Secret);
    }

    [Fact]
    public void NullSecret_ReadsAsFalse()
    {
        using var source = RecordBatchIpc.Read(EmbeddedIpc.Encode(Spec(required: true, secret: true)));
        var index = source.Schema.GetFieldIndex("secret");
        var arrays = source.Arrays.ToList();
        arrays[index] = new BooleanArray.Builder().AppendNull().Build();

        var decoded = EmbeddedIpc.Decode<AttachOptionSpec>(RecordBatchIpc.Write(new RecordBatch(source.Schema, arrays, 1)));

        Assert.True(decoded.Required);
        Assert.False(decoded.Secret);
    }

    [Fact]
    public void UnknownTrailingColumn_IsIgnored()
    {
        // A newer peer may append further columns after `secret`.
        using var source = RecordBatchIpc.Read(EmbeddedIpc.Encode(Spec(required: false, secret: true)));
        var fields = source.Schema.FieldsList.Append(new Field("future_flag", BooleanType.Default, nullable: true)).ToList();
        var arrays = source.Arrays.Append(new BooleanArray.Builder().Append(true).Build()).ToList();

        var decoded = EmbeddedIpc.Decode<AttachOptionSpec>(RecordBatchIpc.Write(new RecordBatch(new Schema(fields, null), arrays, 1)));

        Assert.False(decoded.Required);
        Assert.True(decoded.Secret);
    }

    private static byte[] WithoutColumns(AttachOptionSpec spec, params string[] drop)
    {
        using var source = RecordBatchIpc.Read(EmbeddedIpc.Encode(spec));
        var keep = Enumerable.Range(0, source.ColumnCount)
            .Where(i => !drop.Contains(source.Schema.GetFieldByIndex(i).Name))
            .ToList();
        var schema = new Schema(keep.Select(source.Schema.GetFieldByIndex), metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema, keep.Select(source.Column), 1));
    }
}
