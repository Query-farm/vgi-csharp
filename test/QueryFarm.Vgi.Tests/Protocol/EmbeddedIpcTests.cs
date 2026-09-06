using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using QueryFarm.VgiRpc.Reflection;
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

    [Fact]
    public void RoundTrips_ScanFunctionResult_SchemaName_WhenSet()
    {
        // Protocol 1.5.0's addition — see ScanFunctionResult.SchemaName's doc comment.
        var original = new ScanFunctionResult
        {
            FunctionName = "rowid_sequence",
            Arguments = [],
            RequiredExtensions = [],
            SchemaName = "main",
        };

        var decoded = EmbeddedIpc.Decode<ScanFunctionResult>(EmbeddedIpc.Encode(original));

        Assert.Equal("rowid_sequence", decoded.FunctionName);
        Assert.Equal("main", decoded.SchemaName);
    }

    [Fact]
    public void RoundTrips_ScanFunctionResult_SchemaName_NullWhenUnset()
    {
        // A native DuckDB function this worker never registered has no VGI-side schema to report —
        // the permanent case that keeps the field optional rather than mandatory.
        var original = new ScanFunctionResult { FunctionName = "read_parquet" };

        var decoded = EmbeddedIpc.Decode<ScanFunctionResult>(EmbeddedIpc.Encode(original));

        Assert.Equal("read_parquet", decoded.FunctionName);
        Assert.Null(decoded.SchemaName);
    }

    [Fact]
    public void RoundTrips_ScanBranch_SchemaName_WhenSet()
    {
        var original = new ScanBranch
        {
            FunctionName = "rowid_sequence",
            Arguments = [],
            SchemaName = "main",
        };

        var decoded = EmbeddedIpc.Decode<ScanBranch>(EmbeddedIpc.Encode(original));

        Assert.Equal("rowid_sequence", decoded.FunctionName);
        Assert.Equal("main", decoded.SchemaName);
    }

    [Fact]
    public void RoundTrips_ScanBranch_SchemaName_NullForANonFunctionBranch()
    {
        // A catalog-table branch names no function at all, so it reports no function schema — its
        // SourceSchema is a different field entirely (the SOURCE TABLE's schema).
        var original = new ScanBranch
        {
            FunctionName = "",
            SourceCatalog = "lakehouse",
            SourceSchema = "bronze",
            SourceTable = "orders",
        };

        var decoded = EmbeddedIpc.Decode<ScanBranch>(EmbeddedIpc.Encode(original));

        Assert.Equal("bronze", decoded.SourceSchema);
        Assert.Null(decoded.SchemaName);
    }

    [Fact]
    public void ScanFunctionResultAndScanBranch_DeriveSchemaName_AsATrailingNullableStringField()
    {
        // Both wire schemas append schema_name LAST and nullable, matching the reference
        // ScanFunctionResultSchema()/ScanBranchSchema() — a pre-1.5.0 peer simply omits the column.
        foreach (var clrType in new[] { typeof(ScanFunctionResult), typeof(ScanBranch) })
        {
            var schema = SchemaDerivation.InnerSchemaFor(clrType);
            var field = schema.GetFieldByIndex(schema.FieldsList.Count - 1);

            Assert.Equal("schema_name", field.Name);
            Assert.Equal(Apache.Arrow.Types.StringType.Default.TypeId, field.DataType.TypeId);
            Assert.True(field.IsNullable);
        }
    }
}
