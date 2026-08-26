using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Exercises <see cref="ScanArgsCodec"/> — the flat <c>arg_&lt;N&gt;</c>/bare-name wire
/// shape a <see cref="Protocol.ScanBranch"/>'s <c>Arguments</c>/<c>FormatOptions</c> use, distinct
/// from <see cref="TableArgCodec"/>'s <c>args:struct&lt;...&gt;</c>-wrapped bind-call shape.</summary>
public class ScanArgsCodecTests
{
    [Fact]
    public void Encode_NoArguments_ReturnsEmptyBytes()
    {
        Assert.Empty(ScanArgsCodec.Encode([]));
        Assert.Empty(ScanArgsCodec.Encode([], new Dictionary<string, object?>()));
    }

    [Fact]
    public void Encode_PositionalArguments_RoundTripsAsFlatArgColumns()
    {
        var bytes = ScanArgsCodec.Encode([50L, "hello"]);

        var batch = RecordBatchIpc.Read(bytes);

        Assert.Equal(1, batch.Length);
        Assert.Equal(2, batch.Schema.FieldsList.Count);
        Assert.Equal("arg_0", batch.Schema.GetFieldByIndex(0).Name);
        Assert.Equal("arg_1", batch.Schema.GetFieldByIndex(1).Name);
        Assert.Equal(50L, ScalarArgCodec.ReadScalar(batch.Column(0)));
        Assert.Equal("hello", ScalarArgCodec.ReadScalar(batch.Column(1)));
    }

    [Fact]
    public void Encode_NamedArguments_UseBareFieldNames_NoPrefix()
    {
        var bytes = ScanArgsCodec.Encode([], new Dictionary<string, object?> { ["delim"] = "|", ["header"] = true });

        var batch = RecordBatchIpc.Read(bytes);

        Assert.Equal(["delim", "header"], batch.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal("|", ScalarArgCodec.ReadScalar(batch.Column(0)));
        Assert.Equal(true, ScalarArgCodec.ReadScalar(batch.Column(1)));
    }

    [Fact]
    public void Encode_MixedPositionalAndNamed_PositionalColumnsComeFirst()
    {
        var bytes = ScanArgsCodec.Encode([30L], new Dictionary<string, object?> { ["splits"] = 6L });

        var batch = RecordBatchIpc.Read(bytes);

        Assert.Equal(["arg_0", "splits"], batch.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(30L, ScalarArgCodec.ReadScalar(batch.Column(0)));
        Assert.Equal(6L, ScalarArgCodec.ReadScalar(batch.Column(1)));
    }
}
