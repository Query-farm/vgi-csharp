using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>dynamic_filter_echo(count [, batch_size])</c> — like <see cref="FilterEchoFunction"/>, but the
/// <c>pushed_filters</c> column reports ONLY the per-tick DYNAMIC filter (Top-N's
/// <c>OptionalFilter(DynamicFilter)</c>, delivered as <c>vgi_pushdown_filters</c> tick metadata —
/// see <see cref="Splits.SplitDynamicFilterFunction"/>'s doc comment for the wire mechanism),
/// rendered as <c>"(none)"</c> until the Top-N heap fills and a bound first arrives, then
/// <c>"ConstantFilter(&lt;col&gt; &lt;op&gt; &lt;value&gt;)"</c> — Python-repr-shaped by deliberate
/// choice (see <see cref="DynamicFilterFormatter"/>'s doc comment for why that's a free choice
/// here, not a wire requirement) — thereafter, tightening on every subsequent tick.
///
/// Emits data in DESCENDING order (<c>count-1, count-2, ..., 0</c>): backs
/// <c>table/dynamic_filter.test</c>'s "Dynamic filter echo" section, whose own comment explains why
/// — with ascending data, LIMIT's Top-N heap would fill (and the dynamic filter would already be
/// tight) on the very first batch, defeating the point of proving the filter TIGHTENS over many
/// ticks.
///
/// Row selection only ever applies the STATIC (init-time) pushdown filters — never the dynamic
/// tick component — for the same "purely optional, ignoring it can only under-optimize" reason
/// documented on <see cref="SequenceFunction"/>.
/// </summary>
public sealed class DynamicFilterEchoFunction : ITableFunction
{
    public string Name => "dynamic_filter_echo";

    public string Description => "Echoes the per-tick dynamic filter (Top-N boundary) pushed down alongside any static filter";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("batch_size", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("pushed_filters", StringType.Default, nullable: true),
        ],
        metadata: null);

    public bool? FilterPushdown => true;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var batchSize = initParams.Arguments.Int64Named("batch_size", 2048);
        var staticDecoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new Producer(count, Math.Max(1, batchSize), staticDecoded, initParams.OutputSchema);
    }

    /// <summary>See <see cref="Splits.SplitDynamicFilterFunction"/>'s doc comment on the same
    /// constant — the tick-metadata key DuckDB's dynamic-filter machinery attaches a fresh,
    /// base64-encoded pushdown-filter blob under.</summary>
    private const string DynamicFilterMetadataKey = "vgi_pushdown_filters";

    private sealed class Producer(long count, long batchSize, DecodedFilters? staticDecoded, Schema outputSchema)
        : ITableFunctionProducer
    {
        private long _next;
        private readonly Dictionary<string, object?> _row = new(1);

        public void Produce(OutputCollector output)
        {
            var dynamicText = "(none)";
            if (output.InputMetadata is { } meta
                && meta.TryGetValue(DynamicFilterMetadataKey, out var base64) && !string.IsNullOrEmpty(base64))
            {
                var dynamicDecoded = PushdownFilterCodec.Decode(Convert.FromBase64String(base64));
                dynamicText = DynamicFilterFormatter.Format(dynamicDecoded);
            }

            var ns = new List<long>();

            // Loop across candidate chunks rather than emitting one possibly-empty batch per tick
            // (mirrors FilterEchoFunction) — stop once this call has SOME rows, or the range is
            // exhausted. Data is emitted in DESCENDING order — see class doc comment.
            while (ns.Count == 0 && _next < count)
            {
                var candidateRows = (int)Math.Min(batchSize, count - _next);
                var start = _next;
                _next += candidateRows;

                for (var i = 0; i < candidateRows; i++)
                {
                    var n = count - 1 - (start + i);
                    _row["n"] = n;
                    if (PushdownFilterEvaluator.Matches(staticDecoded, _row))
                    {
                        ns.Add(n);
                    }
                }
            }

            if (ns.Count == 0)
            {
                output.Finish();
                return;
            }

            var nBuilder = new Int64Array.Builder();
            var pBuilder = new StringArray.Builder();
            foreach (var n in ns)
            {
                nBuilder.Append(n);
                pBuilder.Append(dynamicText);
            }

            output.Emit(new RecordBatch(outputSchema, [nBuilder.Build(), pBuilder.Build()], ns.Count));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}

/// <summary>
/// Renders a decoded per-tick DYNAMIC filter (always a single top-level <c>"constant"</c> node —
/// Top-N's <c>DynamicFilter</c> serializes through the exact same <c>VgiSerializeFilters</c> path
/// as any other static <c>ConstantFilter</c>, per
/// <c>~/Development/vgi/src/vgi_table_function_impl.cpp</c>'s <c>UpdateDynamicFilterState</c> — the
/// wire JSON's <c>type</c> field is the ordinary lowercase <c>"constant"</c>, NOT literally
/// <c>"ConstantFilter"</c>) as <c>"ConstantFilter(&lt;col&gt; &lt;op&gt; &lt;value&gt;)"</c> — the
/// SAME shape DuckDB's own reference (Python) worker's <c>ConstantFilter.__repr__</c> happens to
/// produce, e.g. <c>"ConstantFilter(n &gt;= 5)"</c> (see <c>vgi-python</c>'s
/// <c>tests/test_filter_pushdown.py</c>). This fixture is free to choose its own rendering (there is
/// no wire-level requirement here, unlike <c>SplitFilterBoundsFormatter</c>'s cross-SDK
/// <c>col&gt;=min</c> convention — see that type's doc comment) and deliberately mirrors the
/// reference worker's choice since <c>dynamic_filter.test</c>'s assertion
/// (<c>pushed_filters LIKE '%ConstantFilter(n &lt;%'</c>) reads as a substring check anyone's own
/// debug-style rendering can satisfy.
/// </summary>
internal static class DynamicFilterFormatter
{
    public static string Format(DecodedFilters? filters)
    {
        if (filters is null || filters.Root.ValueKind != JsonValueKind.Array || filters.Root.GetArrayLength() == 0)
        {
            return "(none)";
        }

        var node = filters.Root.EnumerateArray().First();
        var type = node.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;
        if (type != "constant")
        {
            return "(none)";
        }

        var column = node.TryGetProperty("column_name", out var name) ? name.GetString() ?? "?" : "?";
        var op = node.TryGetProperty("op", out var opProp) ? opProp.GetString() : "eq";
        var opSymbol = op switch
        {
            "eq" => "=",
            "ne" => "!=",
            "gt" => ">",
            "ge" => ">=",
            "lt" => "<",
            "le" => "<=",
            _ => op ?? "=",
        };

        var value = node.TryGetProperty("value_ref", out var vr) ? filters.ValueRef(vr.GetInt32()) : null;
        var valueText = Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL";
        return $"ConstantFilter({column} {opSymbol} {valueText})";
    }
}
