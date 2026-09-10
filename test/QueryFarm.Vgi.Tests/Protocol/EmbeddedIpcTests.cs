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
            SchemaPath = ["warehouse", "main"],
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
        Assert.Equal(["warehouse", "main"], decoded.SchemaPath);
    }

    [Fact]
    public void RoundTrips_FunctionInfo_WithEnumsListsAndMaps()
    {
        var original = new FunctionInfo
        {
            Comment = "a comment",
            Tags = new Dictionary<string, string> { ["k"] = "v" },
            Name = "upper_case",
            SchemaPath = ["warehouse", "main"],
            FunctionType = FunctionType.Scalar,
            Arguments = [1, 2],
            OutputSchema = [3, 4],
            Description = "uppercases a string",
            Examples = [new FunctionExample { Sql = "SELECT upper_case('a')", Description = "ex", ExpectedOutput = "A" }],
            Categories = ["string"],
            FilterSemanticProfiles = ["vgi.duckdb.standard.v1"],
            AdditionalFilterFunctions = [new FilterFunctionCapability { Namespace = "acme.filters", Name = "overlaps", Version = 1 }],
            RuntimeFilterAlgorithms = [new RuntimeFilterAlgorithmCapability { Namespace = "acme.runtime", Name = "bloom", Version = 2 }],
            FilterEvaluationContexts = [new EvaluationContextCapability { Profile = "vgi.duckdb.session.v1", ProviderFingerprint = "duckdb-1.5" }],
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
        Assert.Equal(["vgi.duckdb.standard.v1"], decoded.FilterSemanticProfiles);
        Assert.Equal("overlaps", Assert.Single(decoded.AdditionalFilterFunctions).Name);
        Assert.Equal((ulong)2, Assert.Single(decoded.RuntimeFilterAlgorithms).Version);
        Assert.Equal("duckdb-1.5", Assert.Single(decoded.FilterEvaluationContexts).ProviderFingerprint);

        var schema = SchemaDerivation.InnerSchemaFor(typeof(FunctionInfo));
        Assert.Equal(39, schema.FieldsList.Count);
        Assert.Equal([
            "filter_semantic_profiles", "additional_filter_functions", "runtime_filter_algorithms", "filter_evaluation_contexts",
        ], schema.FieldsList.Skip(16).Take(4).Select(field => field.Name));
    }

    [Fact]
    public void RoundTrips_ScanFunctionResult_SchemaPath_WhenSet()
    {
        var original = new ScanFunctionResult
        {
            FunctionName = "rowid_sequence",
            Arguments = [],
            RequiredExtensions = [],
            SchemaPath = ["warehouse", "main"],
        };

        var decoded = EmbeddedIpc.Decode<ScanFunctionResult>(EmbeddedIpc.Encode(original));

        Assert.Equal("rowid_sequence", decoded.FunctionName);
        Assert.Equal(["warehouse", "main"], decoded.SchemaPath);
    }

    [Fact]
    public void RoundTrips_ScanFunctionResult_SchemaPath_NullWhenUnset()
    {
        // A native DuckDB function this worker never registered has no VGI-side schema to report —
        // the permanent case that keeps the field optional rather than mandatory.
        var original = new ScanFunctionResult { FunctionName = "read_parquet" };

        var decoded = EmbeddedIpc.Decode<ScanFunctionResult>(EmbeddedIpc.Encode(original));

        Assert.Equal("read_parquet", decoded.FunctionName);
        Assert.Null(decoded.SchemaPath);
    }

    [Fact]
    public void RoundTrips_ScanBranch_SchemaPath_WhenSet()
    {
        var original = new ScanBranch
        {
            FunctionName = "rowid_sequence",
            Arguments = [],
            SchemaPath = ["warehouse", "main"],
        };

        var decoded = EmbeddedIpc.Decode<ScanBranch>(EmbeddedIpc.Encode(original));

        Assert.Equal("rowid_sequence", decoded.FunctionName);
        Assert.Equal(["warehouse", "main"], decoded.SchemaPath);
    }

    [Fact]
    public void RoundTrips_ScanBranch_SchemaPath_NullForANonFunctionBranch()
    {
        // A catalog-table branch names no function at all, so it reports no function schema — its
        // SourceSchemaPath is a different field entirely (the SOURCE TABLE's schema).
        var original = new ScanBranch
        {
            FunctionName = "",
            SourceCatalog = "lakehouse",
            SourceSchemaPath = ["warehouse", "bronze"],
            SourceTable = "orders",
        };

        var decoded = EmbeddedIpc.Decode<ScanBranch>(EmbeddedIpc.Encode(original));

        Assert.Equal(["warehouse", "bronze"], decoded.SourceSchemaPath);
        Assert.Null(decoded.SchemaPath);
    }

    [Fact]
    public void ScanFunctionResultAndScanBranch_DeriveSchemaPath_AsATrailingNullableStringListField()
    {
        // Both wire schemas append schema_path LAST and nullable, matching the reference
        // ScanFunctionResultSchema()/ScanBranchSchema().
        foreach (var clrType in new[] { typeof(ScanFunctionResult), typeof(ScanBranch) })
        {
            var schema = SchemaDerivation.InnerSchemaFor(clrType);
            var field = schema.GetFieldByIndex(schema.FieldsList.Count - 1);

            Assert.Equal("schema_path", field.Name);
            var listType = Assert.IsType<Apache.Arrow.Types.ListType>(field.DataType);
            Assert.Equal(Apache.Arrow.Types.StringType.Default.TypeId, listType.ValueDataType.TypeId);
            Assert.True(field.IsNullable);
        }
    }

    [Fact]
    public void RoundTrips_V2NamedRecords_WithNestedSchemaPaths()
    {
        var capabilities = new ClientCapabilities
        {
            Engine = "duckdb",
            NativeFormats = ["arrow_stream"],
            Catalogs = ["memory", "vgi"],
            CanStream = true,
            FilterEncodings = ["vgi.filters.v2"],
        };
        var decodedCapabilities = EmbeddedIpc.Decode<ClientCapabilities>(EmbeddedIpc.Encode(capabilities));
        Assert.Equal(capabilities.Engine, decodedCapabilities.Engine);
        Assert.Equal(capabilities.NativeFormats, decodedCapabilities.NativeFormats);
        Assert.Equal(capabilities.Catalogs, decodedCapabilities.Catalogs);
        Assert.True(decodedCapabilities.CanStream);
        Assert.Equal(capabilities.FilterEncodings, decodedCapabilities.FilterEncodings);

        var foreignKey = new ForeignKeyInfo
        {
            FkColumns = ["customer_id"],
            PkColumns = ["id"],
            ReferencedSchemaPath = ["warehouse", "silver"],
            ReferencedTable = "customers",
        };
        var decodedForeignKey = EmbeddedIpc.Decode<ForeignKeyInfo>(EmbeddedIpc.Encode(foreignKey));
        Assert.Equal(["warehouse", "silver"], decodedForeignKey.ReferencedSchemaPath);
    }
}
