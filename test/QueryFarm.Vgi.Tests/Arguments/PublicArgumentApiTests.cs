using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Arguments;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;
using Xunit;
using CatalogRegistry = QueryFarm.Vgi.Internal.CatalogRegistry;
using EmbeddedIpc = QueryFarm.Vgi.Internal.EmbeddedIpc;
using RecordBatchIpc = QueryFarm.Vgi.Internal.RecordBatchIpc;
using VgiServiceImpl = QueryFarm.Vgi.Internal.VgiServiceImpl;

namespace QueryFarm.Vgi.Tests.Arguments;

/// <summary>The public argument API (namespace <c>QueryFarm.Vgi.Arguments</c>) — everything an
/// author needs without importing <c>QueryFarm.Vgi.Internal</c>.</summary>
public class PublicArgumentApiTests
{
    internal static byte[] ArgsBytes(params (string Name, IArrowArray Value)[] fields)
    {
        var structType = new StructType(fields.Select(f => new Field(f.Name, f.Value.Data.DataType, nullable: true)).ToList());
        var validity = new ArrowBuffer.BitmapBuilder().Append(true).Build();
        var structArray = new StructArray(structType, 1, fields.Select(f => f.Value).ToList(), validity);
        var schema = new Schema([new Field("args", structType, nullable: false)], metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema, [structArray], 1));
    }

    internal static IArrowArray Long(long? v) => new Int64Array.Builder().Append(v).Build();

    internal static IArrowArray Str(string? v) => v is null ? new StringArray.Builder().AppendNull().Build() : new StringArray.Builder().Append(v).Build();

    internal static byte[] SecretsBytes(params (string Column, (string Name, string Value)[] Fields)[] secrets)
    {
        var arrays = secrets.Select(s =>
        {
            var type = new StructType(s.Fields.Select(f => new Field(f.Name, StringType.Default, true)).ToList());
            return (IArrowArray)new StructArray(type, 1, s.Fields.Select(f => Str(f.Value)).ToList(), ArrowBuffer.Empty, nullCount: 0);
        }).ToList();
        var schema = new Schema(secrets.Select((s, i) => new Field(s.Column, arrays[i].Data.DataType, true)).ToList(), metadata: null);
        return RecordBatchIpc.Write(new RecordBatch(schema, arrays, 1));
    }

    [Fact]
    public void TableArguments_TypedAccessors()
    {
        var args = TableArguments.Decode(ArgsBytes(("positional_0", Long(5)), ("positional_1", Long(null)), ("named_label", Str("x"))));

        Assert.Equal(5L, args.Get<long>(0));
        Assert.Equal(5, args.Get<int>(0));
        Assert.Null(args.Get<long?>(1));
        Assert.Equal("x", args.Get<string>("label"));
        Assert.Equal(7L, args.GetOrDefault("missing", 7L));
        Assert.Equal(9L, args.GetOrDefault(1, 9L));
        Assert.True(args.HasNamed("label"));
        var error = Assert.Throws<VgiInvalidArgumentException>(() => args.Get<long>(1));
        Assert.Contains("positional argument 1", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TableArguments_LegacyInternalTypeStillConvertsBothWays()
    {
#pragma warning disable CS0618
        global::QueryFarm.Vgi.Internal.TableArguments legacy = global::QueryFarm.Vgi.Internal.TableArgCodec.Decode(ArgsBytes(("positional_0", Long(3))));
        TableArguments modern = legacy;
        global::QueryFarm.Vgi.Internal.TableArguments back = modern;
#pragma warning restore CS0618
        Assert.Equal(3L, modern.Int64(0));
        Assert.Equal(3L, back.Int64(0));

        // Code written against 0.23 — a helper typed with the legacy name — still accepts the property.
        var bind = new TableBindParams { FunctionName = "f", Arguments = legacy, Secrets = new global::QueryFarm.Vgi.Internal.SecretsAccessor(null, true) };
        Assert.Equal(3L, LegacyHelper(bind.Arguments));
    }

    private static long LegacyHelper(global::QueryFarm.Vgi.Internal.TableArguments args) => args.Int64(0);

    [Fact]
    public void ConstantArguments_ByPositionAndByDeclaredName()
    {
        var schema = new Schema(
        [
            new Field("value", DoubleType.Default, true),
            new Field("precision", Int64Type.Default, true, new Dictionary<string, string> { ["vgi_const"] = "true" }),
            new Field("prefix", StringType.Default, true, new Dictionary<string, string> { ["vgi_const"] = "true" }),
        ], metadata: null);
        var p = new ScalarBindParams
        {
            FunctionName = "format_number",
            Arguments = ArgsBytes(("positional_0", Long(2)), ("positional_1", Str("$"))),
            DeclaredArgumentsSchema = schema,
        };

        Assert.Equal(2L, p.Constants.Get<long>(0));
        Assert.Equal(2L, p.Constants.Get<long>("precision"));
        Assert.Equal("$", p.Constants.Get<string>("prefix"));
        Assert.Equal(["precision", "prefix"], p.Constants.Names);
        Assert.Equal(2, p.Constants.Count);
        Assert.Null(p.Constants.Get("nope"));

        // Without a schema (hand-built params), positional access still works.
        Assert.Equal("$", ConstantArguments.Decode(p.Arguments).Get<string>(1));
        Assert.Equal(0, new ScalarBindParams { FunctionName = "f" }.Constants.Count);
    }

    [Fact]
    public void SecretValues_ReadOnlyViewFindsByTypeAndScope()
    {
        var bytes = SecretsBytes(
            ("bucket_a", [("type", "s3"), ("key_id", "AAA"), ("scope", "s3://a/")]),
            ("bucket_b", [("type", "s3"), ("key_id", "BBB"), ("scope", "s3://b/")]));
        var init = new TableInitParams
        {
            FunctionName = "f",
            Arguments = TableArguments.Empty,
            OutputSchema = new Schema([], null),
            Secrets = bytes,
        };

        var secrets = init.SecretValues;
        Assert.Equal(2, secrets.Count);
        Assert.Equal("BBB", secrets.ForScope("s3://b/key.parquet", "s3")!.GetString("key_id"));
        var byName = secrets.Get("s3", name: "bucket_a")!;
        Assert.Equal("bucket_a", byName.Name);
        Assert.Equal("s3", byName.Type);
        Assert.Equal("AAA", byName.GetRequiredString("key_id"));
        Assert.Equal(2, secrets.AllOfType("s3").Count);
        Assert.Null(secrets.FindByType("gcs"));
        Assert.Throws<VgiInvalidArgumentException>(() => byName.GetRequiredString("missing"));
        Assert.Empty(SecretValues.Empty.Resolved);
    }

    [Fact]
    public void SecretValues_AtBind_GetDrivesTheTwoPhaseLookup()
    {
        var accessor = new global::QueryFarm.Vgi.Internal.SecretsAccessor(null, isRetry: false);
        var bind = new TableBindParams { FunctionName = "f", Arguments = TableArguments.Empty, Secrets = accessor };

        Assert.Null(bind.SecretValues.Get("s3", scope: "s3://a/"));
        Assert.True(accessor.NeedsResolution);
        Assert.Equal("s3://a/", Assert.Single(accessor.PendingLookups).Scope);

        var retry = new TableBindParams
        {
            FunctionName = "f",
            Arguments = TableArguments.Empty,
            Secrets = new global::QueryFarm.Vgi.Internal.SecretsAccessor(SecretsBytes(("bucket_a", [("type", "s3"), ("key_id", "AAA"), ("scope", "s3://a/")])), isRetry: true),
        };
        Assert.Equal("AAA", retry.SecretValues.Get("s3", scope: "s3://a/x")!.GetString("key_id"));
    }

    [Fact]
    public void Filters_DecodedLazilyAndEvaluated()
    {
        var spec = "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"snapshot\",\"predicates\":[" +
                   "{\"id\":\"p\",\"revision\":0,\"mode\":\"required\",\"source\":\"query\",\"expression\":" +
                   "{\"node\":\"comparison\",\"op\":\"gt\",\"left\":{\"node\":\"column_ref\",\"column_index\":0,\"column_name\":\"n\"}," +
                   "\"right\":{\"node\":\"literal\",\"value_ref\":0}}}]}";
        var metadata = new Dictionary<string, string>
        {
            ["vgi_filter_encoding"] = "vgi.filters.v2",
            ["vgi_filter_version"] = "2",
            ["vgi_evaluation_context"] = "vgi.none.v1",
        };
        var filterSchema = new Schema(
            [new Field("filter_spec", StringType.Default, false), new Field("value_0", Int64Type.Default, true)], metadata);
        var bytes = RecordBatchIpc.Write(new RecordBatch(filterSchema, [Str(spec), Long(10)], 1));
        var outputSchema = new Schema([new Field("n", Int64Type.Default, true)], null);

        var init = new TableInitParams
        {
            FunctionName = "f",
            Arguments = TableArguments.Empty,
            OutputSchema = outputSchema,
            PushdownFilters = bytes,
        };

        var filters = init.Filters!;
        Assert.Same(filters, init.Filters);
        Assert.Equal("snapshot", filters.Kind);
        Assert.Equal("required", Assert.Single(filters.Predicates).Mode);
        Assert.Equal(10L, filters.ValueRef(0));
        var batch = new RecordBatch(outputSchema, [new Int64Array.Builder().Append(5).Append(11).Build()], 2);
        Assert.False(filters.Matches(batch, 0));
        Assert.True(filters.Matches(batch, 1));
        Assert.True(filters.Matches(new Dictionary<string, object?> { ["n"] = 20L }));

        Assert.Null(new TableInitParams { FunctionName = "f", Arguments = TableArguments.Empty, OutputSchema = outputSchema }.Filters);
    }
}

/// <summary>[ConstParam] / TableArgFields constraints: advertised AND enforced at bind.</summary>
public class ArgumentConstraintEnforcementTests
{
    private sealed class ChoiceFn : ScalarFn
    {
        public override string Name => "choice_fn";

        private static void Compute(
            [Param] StringArray value,
            [ConstParam(Name = "mode", Choices = new object[] { "upper", "lower" })] string mode,
            [ConstParam(Name = "code", Pattern = "^[A-Z]{3}$")] string code,
            [ConstParam(Name = "digits", Ge = 0, Lt = 10)] long digits,
            StringArray.Builder result)
        {
            for (var i = 0; i < value.Length; i++)
            {
                result.Append(mode);
            }
        }
    }

    private sealed class ConstrainedTable : ITableFunction
    {
        public string Name => "constrained_table";

        public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.PositionalWithRange("count", Int64Type.Default, ge: 0),
            TableArgFields.Constrained(TableArgFields.Named("format", StringType.Default), choices: ["csv", "json"]),
            TableArgFields.Constrained(TableArgFields.Named("prefix", StringType.Default), pattern: "^p_"),
            TableArgFields.Constrained(TableArgFields.Named("ratio", DoubleType.Default), gt: 0, le: 1),
        ], metadata: null);

        public Schema OutputSchema { get; } = new([new Field("n", Int64Type.Default, true)], null);

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Done();

        private sealed class Done : ITableFunctionProducer
        {
            public void Produce(OutputCollector output) => output.Finish();
        }
    }

    private static async Task<(VgiServiceImpl Service, byte[] Attach)> ServiceAsync(Action<CatalogRegistry> register)
    {
        var registry = new CatalogRegistry();
        register(registry);
        var service = new VgiServiceImpl(registry);
        var attach = (await service.CatalogAttachAsync(new CatalogAttachRequest { Name = "example" })).AttachOpaqueData ?? [];
        return (service, attach);
    }

    private static BindRequest ScalarBind(byte[] attach, string mode, string code, long digits) => new()
    {
        FunctionName = "choice_fn",
        FunctionType = FunctionType.Scalar,
        Arguments = PublicArgumentApiTests.ArgsBytes(
            ("positional_0", PublicArgumentApiTests.Str(mode)),
            ("positional_1", PublicArgumentApiTests.Str(code)),
            ("positional_2", PublicArgumentApiTests.Long(digits))),
        InputSchema = global::QueryFarm.Vgi.Internal.SchemaIpc.WriteSchemaOnly(new Schema([new Field("col_0", StringType.Default, true)], null)),
        AttachOpaqueData = attach,
    };

    [Fact]
    public void ConstParam_AdvertisesChoicesPatternAndRange()
    {
        var fields = new ChoiceFn().ArgumentsSchema.FieldsList;
        Assert.Equal("[\"upper\", \"lower\"]", fields[1].Metadata!["vgi_choices"]);
        Assert.Equal("^[A-Z]{3}$", fields[2].Metadata!["vgi_pattern"]);
        Assert.Equal("[0, 10)", fields[3].Metadata!["vgi_range"]);
    }

    [Fact]
    public async Task Scalar_ConstConstraintsEnforcedAtBind()
    {
        var (service, attach) = await ServiceAsync(r => r.RegisterScalar(new ChoiceFn()));

        await service.BindAsync(ScalarBind(attach, "upper", "ABC", 3));

        var choice = await Assert.ThrowsAsync<VgiInvalidArgumentException>(() => service.BindAsync(ScalarBind(attach, "sideways", "ABC", 3)));
        Assert.Contains("'mode'", choice.Message, StringComparison.Ordinal);
        Assert.Contains("[\"upper\", \"lower\"]", choice.Message, StringComparison.Ordinal);

        var pattern = await Assert.ThrowsAsync<VgiInvalidArgumentException>(() => service.BindAsync(ScalarBind(attach, "upper", "abcd", 3)));
        Assert.Contains("'code'", pattern.Message, StringComparison.Ordinal);

        var range = await Assert.ThrowsAsync<VgiInvalidArgumentException>(() => service.BindAsync(ScalarBind(attach, "upper", "ABC", 10)));
        Assert.Contains("'digits'", range.Message, StringComparison.Ordinal);
        Assert.Contains("< 10", range.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Pattern_IsAnUnanchoredSearch()
    {
        var (service, attach) = await ServiceAsync(r => r.RegisterTable(new ConstrainedTable()));
        BindRequest Bind(string prefix) => new()
        {
            FunctionName = "constrained_table",
            FunctionType = FunctionType.Table,
            Arguments = PublicArgumentApiTests.ArgsBytes(("positional_0", PublicArgumentApiTests.Long(1)), ("named_prefix", PublicArgumentApiTests.Str(prefix))),
            AttachOpaqueData = attach,
        };

        await service.BindAsync(Bind("p_anything"));
        var error = await Assert.ThrowsAsync<VgiInvalidArgumentException>(() => service.BindAsync(Bind("x_p_")));
        Assert.Contains("named argument 'prefix'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Table_PositionalAndNamedConstraintsEnforcedAtBind()
    {
        var (service, attach) = await ServiceAsync(r => r.RegisterTable(new ConstrainedTable()));
        BindRequest Bind(params (string, IArrowArray)[] args) => new()
        {
            FunctionName = "constrained_table",
            FunctionType = FunctionType.Table,
            Arguments = PublicArgumentApiTests.ArgsBytes(args),
            AttachOpaqueData = attach,
        };

        await service.BindAsync(Bind(("positional_0", PublicArgumentApiTests.Long(3)), ("named_format", PublicArgumentApiTests.Str("csv"))));
        // SQL NULL skips value constraints.
        await service.BindAsync(Bind(("positional_0", PublicArgumentApiTests.Long(null))));

        var negative = await Assert.ThrowsAsync<VgiInvalidArgumentException>(() => service.BindAsync(Bind(("positional_0", PublicArgumentApiTests.Long(-1)))));
        Assert.Contains("'count'", negative.Message, StringComparison.Ordinal);
        Assert.Contains(">= 0", negative.Message, StringComparison.Ordinal);

        var format = await Assert.ThrowsAsync<VgiInvalidArgumentException>(() =>
            service.BindAsync(Bind(("positional_0", PublicArgumentApiTests.Long(1)), ("named_format", PublicArgumentApiTests.Str("xml")))));
        Assert.Contains("named argument 'format'", format.Message, StringComparison.Ordinal);

        var ratio = await Assert.ThrowsAsync<VgiInvalidArgumentException>(() => service.BindAsync(Bind(
            ("positional_0", PublicArgumentApiTests.Long(1)), ("named_ratio", new DoubleArray.Builder().Append(0).Build()))));
        Assert.Contains("'ratio'", ratio.Message, StringComparison.Ordinal);
        Assert.Contains("> 0", ratio.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Constrained_RejectsConflictingBoundsAndBadRegex()
    {
        Assert.Throws<ArgumentException>(() => TableArgFields.Constrained(TableArgFields.Positional("x", Int64Type.Default), ge: 0, gt: 0));
        Assert.Throws<InvalidOperationException>(() => TableArgFields.Constrained(TableArgFields.Positional("x", StringType.Default), pattern: "("));
    }
}
