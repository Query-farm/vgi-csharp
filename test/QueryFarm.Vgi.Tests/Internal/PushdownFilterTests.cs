using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Protocol;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

public class PushdownFilterTests
{
    private static readonly IReadOnlyDictionary<string, string> V2Metadata = new Dictionary<string, string>
    {
        ["vgi_filter_encoding"] = "vgi.filters.v2",
        ["vgi_filter_version"] = "2",
        ["vgi_evaluation_context"] = "vgi.none.v1",
    };

    [Fact]
    public void Decode_NullOrEmptyBytes_ReturnsNull()
    {
        Assert.Null(Decode(null));
        Assert.Null(Decode([]));
    }

    [Theory]
    [InlineData("eq", 5L, true)]
    [InlineData("ne", 5L, false)]
    [InlineData("gt", 6L, true)]
    [InlineData("ge", 5L, true)]
    [InlineData("lt", 4L, true)]
    [InlineData("le", 5L, true)]
    public void Snapshot_Comparison_UsesSqlSemantics(string op, long candidate, bool expected)
    {
        var expression = $"{{\"node\":\"comparison\",\"op\":\"{op}\",\"left\":{Column("n", 0)},\"right\":{Literal(0)}}}";
        var decoded = Decode(Filter(Snapshot(Predicate("p", "required", expression)),
            (new Field("value_0", Int64Type.Default, true), Longs(5))));

        Assert.Equal(expected, PushdownFilterEvaluator.Matches(decoded, Row(("n", candidate))));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, Row(("n", null))));
    }

    [Fact]
    public void Snapshot_Comparison_BindsDictionaryColumnToLogicalValueType()
    {
        var dictionaryType = new DictionaryType(Int8Type.Default, StringType.Default, ordered: false);
        var schema = new Schema([new Field("s", dictionaryType, true)], null);
        var expression = $"{{\"node\":\"comparison\",\"op\":\"eq\",\"left\":{Column("s", 0)},\"right\":{Literal(0)}}}";
        var strings = new StringArray.Builder().Append("green").Build();

        var decoded = Decode(Filter(Snapshot(Predicate("dictionary", "required", expression)),
            (new Field("value_0", StringType.Default, true), strings)), outputSchema: schema);

        Assert.NotNull(decoded);
    }

    [Fact]
    public void Snapshot_ArbitrarilyNestedFieldRef_IsEvaluated()
    {
        var nested = $"{{\"node\":\"field_ref\",\"expression\":{{\"node\":\"field_ref\",\"expression\":{Column("outer", 0)},\"field_index\":0,\"field_name\":\"middle\"}},\"field_index\":0,\"field_name\":\"leaf\"}}";
        var expression = $"{{\"node\":\"comparison\",\"op\":\"eq\",\"left\":{nested},\"right\":{Literal(0)}}}";
        var nestedSchema = new Schema([new Field("outer", new StructType([
            new Field("middle", new StructType([new Field("leaf", Int64Type.Default, true)]), true),
        ]), true)], null);
        var decoded = Decode(Filter(Snapshot(Predicate("nested", "required", expression)),
            (new Field("value_0", Int64Type.Default, true), Longs(7))), outputSchema: nestedSchema);
        var row = Row(("outer", Row(("middle", Row(("leaf", 7L))))));

        Assert.True(PushdownFilterEvaluator.Matches(decoded, row));
    }

    [Fact]
    public void Snapshot_InlineIn_HandlesMatchNullAndEmptySet()
    {
        var listBuilder = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)listBuilder.ValueBuilder;
        listBuilder.Append();
        values.Append(1).AppendNull().Append(3);
        var expression = $"{{\"node\":\"in\",\"expression\":{Column("n", 0)},\"set\":{{\"kind\":\"literal\",\"value_ref\":0}},\"negated\":false}}";
        var decoded = Decode(Filter(Snapshot(Predicate("in", "required", expression)),
            (new Field("value_0", new ListType(Int64Type.Default), true), listBuilder.Build())));

        Assert.True(PushdownFilterEvaluator.Matches(decoded, Row(("n", 3L))));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, Row(("n", 2L))));

        var emptyBuilder = new ListArray.Builder(Int64Type.Default);
        emptyBuilder.Append();
        var empty = Decode(Filter(Snapshot(Predicate("empty", "required", expression)),
            (new Field("value_0", new ListType(Int64Type.Default), true), emptyBuilder.Build())));
        Assert.False(PushdownFilterEvaluator.Matches(empty, Row(("n", 1L))));
    }

    [Fact]
    public void Snapshot_ExternalIn_UsesAuthoritativeBatchAndColumnIndexes()
    {
        var expression = $"{{\"node\":\"in\",\"expression\":{Column("n", 0)},\"set\":{{\"kind\":\"external\",\"batch_index\":1,\"column_index\":1,\"column_name\":\"keys\"}},\"negated\":false}}";
        var unusedBatch = Batch(new Schema([new Field("unused", Int64Type.Default, true)], null), Longs(100));
        var keyBatch = Batch(new Schema([
            new Field("ignored", Int64Type.Default, true),
            new Field("keys", Int64Type.Default, true),
        ], null), Longs(99, 99), Longs(1, 3));
        var decoded = Decode(Filter(Snapshot(Predicate("external", "required", expression))),
            [Write(unusedBatch), Write(keyBatch)]);

        Assert.True(PushdownFilterEvaluator.Matches(decoded, Row(("n", 3L))));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, Row(("n", 99L))));
    }

    [Fact]
    public void Decode_RejectsV1OrMalformedV2InsteadOfFailingOpen()
    {
        var v1 = Filter(Snapshot(Predicate("p", "required", $"{{\"node\":\"is_null\",\"expression\":{Column("n", 0)},\"negated\":false}}")),
            metadata: new Dictionary<string, string> { ["vgi_filter_version"] = "1" });
        Assert.Throws<InvalidDataException>(() => Decode(v1));

        var unknownNode = Snapshot(Predicate("p", "required", "{\"node\":\"future_node\"}"));
        Assert.Throws<InvalidDataException>(() => Decode(Filter(unknownNode)));
    }

    [Fact]
    public void Decode_RejectsColumnAndNestedFieldIndexNameMismatches()
    {
        var wrongColumn = $"{{\"node\":\"is_null\",\"expression\":{Column("wrong", 0)},\"negated\":false}}";
        Assert.Throws<InvalidDataException>(() => Decode(Filter(Snapshot(Predicate("column", "required", wrongColumn)))));

        var nestedSchema = new Schema([new Field("outer", new StructType([
            new Field("actual", Int64Type.Default, true),
        ]), true)], null);
        var wrongField = $"{{\"node\":\"is_null\",\"expression\":{{\"node\":\"field_ref\",\"expression\":{Column("outer", 0)},\"field_index\":0,\"field_name\":\"wrong\"}},\"negated\":false}}";
        Assert.Throws<InvalidDataException>(() => Decode(
            Filter(Snapshot(Predicate("field", "required", wrongField))), outputSchema: nestedSchema));
    }

    [Fact]
    public void Decode_RejectsNonBooleanRootsContextDependentCastsAndUnknownExtensions()
    {
        var literalRoot = Filter(Snapshot(Predicate("root", "required", Literal(0))),
            (new Field("value_0", Int64Type.Default, true), Longs(1)));
        Assert.Throws<InvalidDataException>(() => Decode(literalRoot));

        var strings = new StringArray.Builder().Append("1").Build();
        var nullString = new StringArray.Builder().AppendNull().Build();
        var cast = $"{{\"node\":\"comparison\",\"op\":\"eq\",\"left\":{{\"node\":\"cast\",\"expression\":{Column("s", 0)},\"type_ref\":0}},\"right\":{Literal(1)}}}";
        var stringSchema = new Schema([new Field("s", StringType.Default, true)], null);
        var contextual = Filter(Snapshot(Predicate("cast", "required", cast)),
            (new Field("type_0", StringType.Default, true), nullString),
            (new Field("value_1", StringType.Default, true), strings));
        Assert.Throws<InvalidDataException>(() => Decode(contextual, outputSchema: stringSchema));

        var unknownExtension = new Field("n", Int64Type.Default, true,
            new Dictionary<string, string> { ["ARROW:extension:name"] = "example.unknown" });
        var extensionSchema = new Schema([unknownExtension], null);
        var isNull = $"{{\"node\":\"is_null\",\"expression\":{Column("n", 0)},\"negated\":false}}";
        Assert.Throws<InvalidDataException>(() => Decode(
            Filter(Snapshot(Predicate("extension", "required", isNull))), outputSchema: extensionSchema));
    }

    [Fact]
    public void Decode_EnforcesIdAndEncodedPayloadLimits()
    {
        var expression = $"{{\"node\":\"is_null\",\"expression\":{Column("n", 0)},\"negated\":false}}";
        Assert.Throws<InvalidDataException>(() => Decode(Filter(Snapshot(Predicate(new string('x', 129), "required", expression)))));
        Assert.Throws<InvalidDataException>(() => Decode(new byte[(16 << 20) + 1]));
    }

    [Fact]
    public void Decode_EnforcesTheGlobalExpressionNodeLimit()
    {
        var children = string.Join(',', Enumerable.Repeat(Literal(0), 10_000));
        var expression = $"{{\"node\":\"and\",\"children\":[{children}]}}";
        var booleans = new BooleanArray.Builder().Append(true).Build();
        var filter = Filter(Snapshot(Predicate("nodes", "required", expression)),
            (new Field("value_0", BooleanType.Default, true), booleans));

        var error = Assert.Throws<InvalidDataException>(() => Decode(filter));
        Assert.Contains("expression-node limit", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_ValidatesStructuredExtensionIdentityAndAdvertisement()
    {
        var call = $"{{\"node\":\"call\",\"function\":{{\"namespace\":\"duckdb.spatial\",\"name\":\"intersects_extent\",\"version\":1}},\"arguments\":[{Column("geom", 0)},{Literal(0)}]}}";
        var wkbMetadata = new Dictionary<string, string> { ["ARROW:extension:name"] = "geoarrow.wkb" };
        var field = new Field("geom", BinaryType.Default, true, wkbMetadata);
        var literalField = new Field("value_0", BinaryType.Default, true, wkbMetadata);
        var bytes = new BinaryArray.Builder().Append([1, 2, 3]).Build();
        var filter = Filter(Snapshot(Predicate("spatial", "advisory", call)), (literalField, bytes));
        Assert.Throws<InvalidDataException>(() => Decode(filter, outputSchema: new Schema([field], null)));

        var capability = new FilterFunctionCapability
        {
            Namespace = "duckdb.spatial",
            Name = "intersects_extent",
            Version = 1,
        };
        Assert.NotNull(Decode(filter, outputSchema: new Schema([field], null), extensionFunctions: [capability]));

        var zeroVersion = call.Replace("\"version\":1", "\"version\":0", StringComparison.Ordinal);
        Assert.Throws<InvalidDataException>(() => Decode(
            Filter(Snapshot(Predicate("spatial", "advisory", zeroVersion)), (literalField, bytes)),
            outputSchema: new Schema([field], null), extensionFunctions: [capability]));
    }

    [Fact]
    public void SpatialExtensionCapability_UsesTheEmbeddedDuckDbEvaluator()
    {
        var call = $"{{\"node\":\"call\",\"function\":{{\"namespace\":\"duckdb.spatial\",\"name\":\"intersects_extent\",\"version\":1}},\"arguments\":[{Column("geom", 0)},{Literal(0)}]}}";
        var metadata = new Dictionary<string, string> { ["ARROW:extension:name"] = "geoarrow.wkb" };
        var field = new Field("geom", BinaryType.Default, true, metadata);
        var point = WkbPoint(1, 2);
        var values = new BinaryArray.Builder().Append(point).Build();
        var capability = new FilterFunctionCapability
        {
            Namespace = "duckdb.spatial",
            Name = "intersects_extent",
            Version = 1,
        };
        var decoded = Decode(Filter(Snapshot(Predicate("spatial", "required", call)),
            (new Field("value_0", BinaryType.Default, true, metadata), values)),
            outputSchema: new Schema([field], null), extensionFunctions: [capability]);

        Assert.True(PushdownFilterEvaluator.Matches(decoded, Row(("geom", point))));
    }

    [Fact]
    public void Decode_DeltaAcceptsAdvisoryUpsertAndRemove()
    {
        var expression = $"{{\"node\":\"is_null\",\"expression\":{Column("n", 0)},\"negated\":false}}";
        var delta = $"{{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"delta\",\"updates\":[{{\"operation\":\"upsert\",\"id\":\"top_n:0\",\"revision\":1,\"mode\":\"advisory\",\"source\":\"top_n\",\"expression\":{expression}}}]}}";
        var decoded = Decode(Filter(delta));

        Assert.Equal("delta", decoded!.Kind);
        Assert.Single(decoded.Updates);
        Assert.False(PushdownFilterEvaluator.Matches(decoded, Row(("n", 1L))));

        var remove = "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"delta\",\"updates\":[{\"operation\":\"remove\",\"id\":\"top_n:0\",\"revision\":2}]}";
        var removed = Decode(Filter(remove));
        Assert.Empty(removed!.Predicates);
    }

    [Fact]
    public void FilterState_AppliesNewRevisionsAtomicallyAndIgnoresStaleUpdates()
    {
        var expression = $"{{\"node\":\"comparison\",\"op\":\"ge\",\"left\":{Column("n", 0)},\"right\":{Literal(0)}}}";
        var snapshot = Decode(Filter(Snapshot(),
            (new Field("value_0", Int64Type.Default, true), Longs(0))))!;
        var state = new PushdownFilterState(snapshot);
        var upsert = $"{{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"delta\",\"updates\":[{{\"operation\":\"upsert\",\"id\":\"top_n:0\",\"revision\":2,\"mode\":\"advisory\",\"source\":\"top_n\",\"expression\":{expression}}}]}}";
        state.ApplyDelta(Decode(Filter(upsert,
            (new Field("value_0", Int64Type.Default, true), Longs(5))))!);
        Assert.False(state.Matches(Row(("n", 4L))));
        Assert.True(state.Matches(Row(("n", 5L))));

        var staleRemove = "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"delta\",\"updates\":[{\"operation\":\"remove\",\"id\":\"top_n:0\",\"revision\":1}]}";
        state.ApplyDelta(Decode(Filter(staleRemove))!);
        Assert.False(state.Matches(Row(("n", 4L))));

        var remove = staleRemove.Replace("\"revision\":1", "\"revision\":3", StringComparison.Ordinal);
        state.ApplyDelta(Decode(Filter(remove))!);
        Assert.True(state.Matches(Row(("n", 4L))));
    }

    [Fact]
    public void FilterState_PreservesFullUInt64RevisionOrdering()
    {
        var snapshot = new PushdownFilterState(Decode(Filter(Snapshot()))!);
        const string high = "9223372036854775808";
        var expression = $"{{\"node\":\"is_null\",\"expression\":{Column("n", 0)},\"negated\":false}}";
        var upsert = $"{{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"delta\",\"updates\":[{{\"operation\":\"upsert\",\"id\":\"top_n:wide\",\"revision\":{high},\"mode\":\"advisory\",\"source\":\"top_n\",\"expression\":{expression}}}]}}";
        snapshot.ApplyDelta(Decode(Filter(upsert))!);
        Assert.False(snapshot.Matches(Row(("n", 1L))));

        var stale = "{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"delta\",\"updates\":[{\"operation\":\"remove\",\"id\":\"top_n:wide\",\"revision\":9223372036854775807}]}";
        snapshot.ApplyDelta(Decode(Filter(stale))!);
        Assert.False(snapshot.Matches(Row(("n", 1L))));
    }

    [Fact]
    public void FilterState_RejectsAMultiUpdateDeltaAtomically()
    {
        var required = $"{{\"node\":\"is_null\",\"expression\":{Column("n", 0)},\"negated\":true}}";
        var state = new PushdownFilterState(Decode(Filter(Snapshot(Predicate("required", "required", required))))!);
        var advisory = $"{{\"node\":\"is_null\",\"expression\":{Column("n", 0)},\"negated\":false}}";
        var delta = $"{{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"delta\",\"updates\":[" +
            $"{{\"operation\":\"upsert\",\"id\":\"dynamic\",\"revision\":1,\"mode\":\"advisory\",\"source\":\"top_n\",\"expression\":{advisory}}}," +
            "{\"operation\":\"remove\",\"id\":\"required\",\"revision\":1}]}";

        Assert.Throws<InvalidDataException>(() => state.ApplyDelta(Decode(Filter(delta))!));
        Assert.True(state.Matches(Row(("n", 1L))));
    }

    private static string Snapshot(params string[] predicates) =>
        $"{{\"encoding\":\"vgi.filters.v2\",\"semantics\":\"vgi.duckdb.standard.v1\",\"kind\":\"snapshot\",\"predicates\":[{string.Join(',', predicates)}]}}";

    private static DecodedFilters? Decode(byte[]? bytes, IReadOnlyList<byte[]>? joinKeys = null, Schema? outputSchema = null,
        IReadOnlyList<FilterFunctionCapability>? extensionFunctions = null) =>
        PushdownFilterCodec.Decode(bytes, joinKeys,
            outputSchema ?? new Schema([new Field("n", Int64Type.Default, true)], null), extensionFunctions);

    private static string Predicate(string id, string mode, string expression) =>
        $"{{\"id\":\"{id}\",\"revision\":0,\"mode\":\"{mode}\",\"source\":\"query\",\"expression\":{expression}}}";

    private static string Column(string name, int index) =>
        $"{{\"node\":\"column_ref\",\"column_index\":{index},\"column_name\":\"{name}\"}}";

    private static string Literal(int index) => $"{{\"node\":\"literal\",\"value_ref\":{index}}}";

    private static IReadOnlyDictionary<string, object?> Row(params (string Name, object? Value)[] values) =>
        values.ToDictionary(value => value.Name, value => value.Value, StringComparer.Ordinal);

    private static Int64Array Longs(params long?[] values)
    {
        var builder = new Int64Array.Builder();
        foreach (var value in values)
        {
            builder.Append(value);
        }

        return builder.Build();
    }

    private static byte[] WkbPoint(double x, double y)
    {
        var bytes = new byte[21];
        bytes[0] = 1;
        BitConverter.TryWriteBytes(bytes.AsSpan(1, 4), 1u);
        BitConverter.TryWriteBytes(bytes.AsSpan(5, 8), x);
        BitConverter.TryWriteBytes(bytes.AsSpan(13, 8), y);
        return bytes;
    }

    private static byte[] Filter(string json,
        params (Field Field, IArrowArray Array)[] payload) => Filter(json, V2Metadata, payload);

    private static byte[] Filter(string json, IReadOnlyDictionary<string, string> metadata,
        params (Field Field, IArrowArray Array)[] payload)
    {
        var spec = new StringArray.Builder().Append(json).Build();
        var fields = new List<Field> { new("filter_spec", StringType.Default, false) };
        fields.AddRange(payload.Select(item => item.Field));
        return Write(Batch(new Schema(fields, metadata), [spec, .. payload.Select(item => item.Array)]));
    }

    private static RecordBatch Batch(Schema schema, params IArrowArray[] arrays) =>
        new(schema, arrays, arrays.Length == 0 ? 0 : arrays[0].Length);

    private static byte[] Write(RecordBatch batch)
    {
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
        }

        return stream.ToArray();
    }
}
