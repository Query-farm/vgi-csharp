using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Protocol;

/// <summary>
/// M0's smoke test: proves <see cref="EmbeddedIpc"/> (itself built directly from
/// <c>QueryFarm.VgiRpc.Reflection.SchemaDerivation</c>/<c>ValueCodec</c>'s public API) round-trips
/// a hand-written VGI protocol type as embedded IPC — the mechanism every packed request/response
/// and every catalog-discovery item relies on.
/// </summary>
public class EmbeddedIpcTests
{
    [Fact]
    public void RoundTrips_BindRequest_WithNestedStructsAndOptionalFields()
    {
        var original = new BindRequest
        {
            FunctionName = "upper_case",
            Arguments = [1, 2, 3],
            FunctionType = FunctionType.Scalar,
            InputSchema = [4, 5],
            Settings = null,
            Secrets = null,
            AttachOpaqueData = [9, 9],
            TransactionOpaqueData = null,
            ResolvedSecretsProvided = true,
            AtUnit = null,
            AtValue = null,
            CopyFrom = new CopyFromContext { Format = "csv", FilePath = "/tmp/x.csv", ExpectedSchema = [1] },
            CopyTo = null,
            SchemaName = "main",
        };

        var bytes = EmbeddedIpc.Encode(original);
        var decoded = EmbeddedIpc.Decode<BindRequest>(bytes);

        Assert.Equal(original.FunctionName, decoded.FunctionName);
        Assert.Equal(original.Arguments, decoded.Arguments);
        Assert.Equal(original.FunctionType, decoded.FunctionType);
        Assert.Equal(original.InputSchema, decoded.InputSchema);
        Assert.Null(decoded.Settings);
        Assert.Equal(original.AttachOpaqueData, decoded.AttachOpaqueData);
        Assert.Equal(original.ResolvedSecretsProvided, decoded.ResolvedSecretsProvided);
        Assert.NotNull(decoded.CopyFrom);
        Assert.Equal("csv", decoded.CopyFrom!.Format);
        Assert.Equal("/tmp/x.csv", decoded.CopyFrom.FilePath);
        Assert.Null(decoded.CopyTo);
        Assert.Equal("main", decoded.SchemaName);
    }

    [Fact]
    public void RoundTrips_FunctionInfo_WithEnumsListsAndMaps()
    {
        var original = new FunctionInfo
        {
            Comment = "a comment",
            Tags = new Dictionary<string, string> { ["k"] = "v" },
            Name = "upper_case",
            SchemaName = "main",
            FunctionType = FunctionType.Scalar,
            Arguments = [1, 2],
            OutputSchema = [3, 4],
            Description = "uppercases a string",
            Examples = [new FunctionExample { Sql = "SELECT upper_case('a')", Description = "ex", ExpectedOutput = "A" }],
            Categories = ["string"],
            RequiredSettings = [],
            RequiredSecrets = [new RequiredSecret { SecretType = "s3", Scope = null, SecretName = null }],
        };

        var bytes = EmbeddedIpc.Encode(original);
        var decoded = EmbeddedIpc.Decode<FunctionInfo>(bytes);

        Assert.Equal(original.Name, decoded.Name);
        Assert.Equal(original.FunctionType, decoded.FunctionType);
        Assert.Equal("v", decoded.Tags["k"]);
        Assert.Single(decoded.Examples);
        Assert.Equal("ex", decoded.Examples[0].Description);
        Assert.Equal("A", decoded.Examples[0].ExpectedOutput);
        Assert.Single(decoded.RequiredSecrets);
        Assert.Equal("s3", decoded.RequiredSecrets[0].SecretType);
        Assert.Equal(VgiPartitionKind.NotPartitioned, decoded.PartitionKind);
        Assert.Equal(AggregateOrderDependent.NotOrderDependent, decoded.OrderDependent);
    }
}
