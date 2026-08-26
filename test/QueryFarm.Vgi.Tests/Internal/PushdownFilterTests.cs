using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using Xunit;

namespace QueryFarm.Vgi.Tests.Internal;

/// <summary>Exercises <see cref="PushdownFilterCodec"/>/<see cref="PushdownFilterEvaluator"/> — the
/// filter-tree JSON decode + evaluation machinery M3's pushdown-introspection fixtures
/// (<c>filter_echo</c>, <c>order_echo</c>, <c>value_prune</c>, ...) share. The exact JSON shape was
/// reverse-engineered against the real C++ extension (see <c>filter_echo.test</c>'s pass), not from
/// written spec — these tests pin that discovered contract so a future change notices a drift.</summary>
public class PushdownFilterTests
{
    private static byte[] BuildFilterBytes(string specJson, params (string Name, long Value)[] constValues)
    {
        var fields = new List<Field> { new("filter_spec", StringType.Default, nullable: false) };
        var arrays = new List<IArrowArray>();

        var specBuilder = new StringArray.Builder();
        specBuilder.Append(specJson);
        arrays.Add(specBuilder.Build());

        foreach (var (name, value) in constValues)
        {
            fields.Add(new Field(name, Int64Type.Default, nullable: true));
            var builder = new Int64Array.Builder();
            builder.Append(value);
            arrays.Add(builder.Build());
        }

        return WriteBatch(new Schema(fields, metadata: null), arrays);
    }

    private static byte[] BuildJoinKeysBytes(string columnName, params long[] values)
    {
        var schema = new Schema([new Field(columnName, Int64Type.Default, nullable: true)], metadata: null);
        var builder = new Int64Array.Builder();
        foreach (var v in values)
        {
            builder.Append(v);
        }

        return WriteBatch(schema, [builder.Build()]);
    }

    private static byte[] WriteBatch(Schema schema, IReadOnlyList<IArrowArray> arrays)
    {
        var batch = new RecordBatch(schema, arrays, arrays.Count == 0 ? 0 : arrays[0].Length);
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
    public void Decode_NullOrEmptyBytes_ReturnsNull()
    {
        Assert.Null(PushdownFilterCodec.Decode(null));
        Assert.Null(PushdownFilterCodec.Decode([]));
    }

    [Fact]
    public void Evaluator_ConstantEquality_MatchesOnlyEqualValue()
    {
        const string json = "[{\"type\":\"constant\",\"column_name\":\"n\",\"column_index\":0,\"op\":\"eq\",\"value_ref\":0}]";
        var bytes = BuildFilterBytes(json, ("_val_0", 5));
        var decoded = PushdownFilterCodec.Decode(bytes);

        Assert.True(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 5L }));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 6L }));
    }

    [Theory]
    [InlineData("ne", 5L, false)]
    [InlineData("ne", 6L, true)]
    [InlineData("gt", 5L, false)]
    [InlineData("gt", 6L, true)]
    [InlineData("ge", 5L, true)]
    [InlineData("lt", 4L, true)]
    [InlineData("lt", 5L, false)]
    [InlineData("le", 5L, true)]
    public void Evaluator_EveryComparisonOperator(string op, long candidate, bool expectedMatch)
    {
        var json = "[{\"type\":\"constant\",\"column_name\":\"n\",\"column_index\":0,\"op\":\"" + op + "\",\"value_ref\":0}]";
        var bytes = BuildFilterBytes(json, ("_val_0", 5));
        var decoded = PushdownFilterCodec.Decode(bytes);

        Assert.Equal(expectedMatch, PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = candidate }));
    }

    [Fact]
    public void Evaluator_AndNode_RequiresEveryChild()
    {
        const string json =
            "[{\"type\":\"and\",\"children\":[" +
            "{\"type\":\"constant\",\"column_name\":\"n\",\"op\":\"ge\",\"value_ref\":0}," +
            "{\"type\":\"constant\",\"column_name\":\"n\",\"op\":\"lt\",\"value_ref\":1}" +
            "]}]";
        var bytes = BuildFilterBytes(json, ("_val_0", 3), ("_val_1", 7));
        var decoded = PushdownFilterCodec.Decode(bytes);

        Assert.True(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 5L }));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 2L }));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 7L }));
    }

    [Fact]
    public void Evaluator_OrNode_MatchesAnyChild()
    {
        const string json =
            "[{\"type\":\"or\",\"children\":[" +
            "{\"type\":\"constant\",\"column_name\":\"n\",\"op\":\"eq\",\"value_ref\":0}," +
            "{\"type\":\"constant\",\"column_name\":\"n\",\"op\":\"eq\",\"value_ref\":1}" +
            "]}]";
        var bytes = BuildFilterBytes(json, ("_val_0", 1), ("_val_1", 9));
        var decoded = PushdownFilterCodec.Decode(bytes);

        Assert.True(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 1L }));
        Assert.True(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 9L }));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 5L }));
    }

    [Fact]
    public void Evaluator_TopLevelArray_IsAnImplicitConjunction()
    {
        // Two independent top-level filter entries (different columns) — DuckDB's TableFilterSet
        // is inherently a conjunction of independent per-column filters; no explicit "and" wrapper
        // needed at the root.
        const string json =
            "[" +
            "{\"type\":\"constant\",\"column_name\":\"n\",\"op\":\"ge\",\"value_ref\":0}," +
            "{\"type\":\"constant\",\"column_name\":\"s\",\"op\":\"eq\",\"value_ref\":1}" +
            "]";
        var bytes = BuildFilterBytes(json, ("_val_0", 3), ("_val_1", 42));
        var decoded = PushdownFilterCodec.Decode(bytes);

        var matchingRow = new Dictionary<string, object?> { ["n"] = 5L, ["s"] = 42L };
        var nonMatchingRow = new Dictionary<string, object?> { ["n"] = 5L, ["s"] = 41L };
        Assert.True(PushdownFilterEvaluator.Matches(decoded, matchingRow));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, nonMatchingRow));
    }

    [Fact]
    public void Evaluator_IsNullAndIsNotNull()
    {
        var isNullBytes = BuildFilterBytes("[{\"type\":\"is_null\",\"column_name\":\"n\"}]");
        var isNotNullBytes = BuildFilterBytes("[{\"type\":\"is_not_null\",\"column_name\":\"n\"}]");

        var isNullDecoded = PushdownFilterCodec.Decode(isNullBytes);
        var isNotNullDecoded = PushdownFilterCodec.Decode(isNotNullBytes);

        Assert.True(PushdownFilterEvaluator.Matches(isNullDecoded, new Dictionary<string, object?> { ["n"] = null }));
        Assert.False(PushdownFilterEvaluator.Matches(isNullDecoded, new Dictionary<string, object?> { ["n"] = 1L }));
        Assert.True(PushdownFilterEvaluator.Matches(isNotNullDecoded, new Dictionary<string, object?> { ["n"] = 1L }));
        Assert.False(PushdownFilterEvaluator.Matches(isNotNullDecoded, new Dictionary<string, object?> { ["n"] = null }));
    }

    [Fact]
    public void JoinKeyValues_ResolvesFromSiblingInitRequestJoinKeysBatch()
    {
        // The wire shape DuckDB actually uses for a literal/semi-join IN-list, discovered
        // empirically: the filter_spec node is type "join_keys" (NOT a bare "in"/"in_list" node),
        // and the candidate values live in a SEPARATE InitRequest.JoinKeys sibling batch, matched
        // by the node's "keys_column" name.
        const string specJson = "[{\"type\":\"join_keys\",\"column_name\":\"n\",\"column_index\":0,\"keys_column\":\"n\"}]";
        var specBytes = BuildFilterBytes(specJson);
        var joinKeysBytes = BuildJoinKeysBytes("n", 1, 3, 7);

        var decoded = PushdownFilterCodec.Decode(specBytes, [joinKeysBytes]);

        Assert.Equal([1L, 3L, 7L], decoded!.JoinKeyValues("n"));
        Assert.True(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 3L }));
        Assert.False(PushdownFilterEvaluator.Matches(decoded, new Dictionary<string, object?> { ["n"] = 4L }));
    }
}
