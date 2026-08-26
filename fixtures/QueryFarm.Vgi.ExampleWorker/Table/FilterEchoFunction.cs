using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>filter_echo(count [, batch_size])</c> — emits <c>count</c> rows with a <c>pushed_filters</c>
/// column echoing a SQL-ish rendering of whatever DuckDB pushed down via
/// <c>InitRequest.PushdownFilters</c>. Backs <c>filter_echo.test</c> (and the shared formatter
/// backs <c>dynamic_filter.test</c>/<c>value_prune.test</c>). Doesn't itself filter rows — DuckDB
/// re-checks every pushed filter unless a function declares <see cref="ITableFunction.FiltersExactlyApplied"/>,
/// so emitting the full unfiltered set and letting DuckDB do the filtering keeps the result set
/// correct while still exercising the pushdown wire round-trip.
/// </summary>
public sealed class FilterEchoFunction : ITableFunction
{
    public string Name => "filter_echo";

    public string Description => "Echoes pushed-down filter predicates in output";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("batch_size", Int64Type.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("s", StringType.Default, nullable: true),
            new Field("pushed_filters", StringType.Default, nullable: true),
        ],
        metadata: null);

    public bool? FilterPushdown => true;

    public bool? ProjectionPushdown => true;

    /// <summary>Honest row-count estimate — see <see cref="Splits.SplitDynamicFilterFunction.Cardinality"/>'s
    /// doc comment for why: a table function that reports "unknown" here is planned as the
    /// hash-join BUILD side instead of the probe side, and <c>join_keys_pushdown.test</c>'s
    /// filter-echo join assertion specifically needs this scan on the PROBE side so the join-key
    /// pushdown actually reaches it.</summary>
    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(0);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var batchSize = initParams.Arguments.Int64Named("batch_size", 2048);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var filterText = PushdownFilterFormatter.Format(decoded);
        return new Producer(count, Math.Max(1, batchSize), filterText, decoded, initParams.ProjectedSchema, initParams.ProjectionIds);
    }

    private sealed class Producer(
        long count, long batchSize, string filterText, DecodedFilters? decoded, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
        : ITableFunctionProducer
    {
        private long _next;

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var ss = new List<string>();
            var row = new Dictionary<string, object?>();

            // Loop across candidate chunks (rather than emitting one possibly-empty batch per
            // tick) so a highly-selective filter doesn't need one round trip per `batchSize`
            // candidates skipped — stop once this call has SOME rows to emit, or count is exhausted.
            while (ns.Count == 0 && _next < count)
            {
                var candidateRows = (int)Math.Min(batchSize, count - _next);
                var start = _next;
                _next += candidateRows;

                for (var i = 0; i < candidateRows; i++)
                {
                    var n = start + i;
                    var s = $"row_{n}";
                    row["n"] = n;
                    row["s"] = s;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        ns.Add(n);
                        ss.Add(s);
                    }
                }
            }

            if (ns.Count == 0)
            {
                output.Finish();
                return;
            }

            var rows = ns.Count;

            IArrowArray BuildColumn(int fullIndex)
            {
                switch (fullIndex)
                {
                    case 0:
                        var nBuilder = new Int64Array.Builder();
                        foreach (var n in ns)
                        {
                            nBuilder.Append(n);
                        }

                        return nBuilder.Build();
                    case 1:
                        var sBuilder = new StringArray.Builder();
                        foreach (var s in ss)
                        {
                            sBuilder.Append(s);
                        }

                        return sBuilder.Build();
                    default:
                        var pBuilder = new StringArray.Builder();
                        for (var i = 0; i < rows; i++)
                        {
                            pBuilder.Append(filterText);
                        }

                        return pBuilder.Build();
                }
            }

            var indices = projectionIds ?? [0, 1, 2];
            var columns = indices.Select(id => BuildColumn((int)id)).ToList();
            output.Emit(new RecordBatch(projectedSchema, columns, rows));

            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}

/// <summary>Renders a <see cref="DecodedFilters"/> tree into a SQL-ish string matching the
/// C++ extension's own filter-to-string rendering (as observed via <c>filter_echo.test</c>): a
/// conjunction/disjunction node always parenthesizes itself; the top-level array of per-column
/// filters is simply joined with <c>" AND "</c> (DuckDB's <c>TableFilterSet</c> is inherently a
/// conjunction of independent per-column filters, so no extra wrapping applies there); an empty
/// filter set renders as <c>"(none)"</c>.</summary>
public static class PushdownFilterFormatter
{
    public static string Format(DecodedFilters? filters)
    {
        if (filters is null)
        {
            return "(none)";
        }

        if (filters.Root.ValueKind != JsonValueKind.Array || filters.Root.GetArrayLength() == 0)
        {
            return "(none)";
        }

        var parts = filters.Root.EnumerateArray().Select(node => FormatNode(node, filters));
        return string.Join(" AND ", parts);
    }

    private static string FormatNode(JsonElement node, DecodedFilters filters)
    {
        var type = node.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;
        switch (type)
        {
            case "constant":
                return FormatConstant(node, filters);
            case "is_null":
                return $"{ColumnName(node)} IS NULL";
            case "is_not_null":
                return $"{ColumnName(node)} IS NOT NULL";
            case "and":
                return "(" + string.Join(" AND ", Children(node, filters)) + ")";
            case "or":
                return "(" + string.Join(" OR ", Children(node, filters)) + ")";
            case "in":
            case "in_list":
                return FormatIn(node, filters);
            case "join_keys":
                return FormatJoinKeys(node, filters);
            default:
                return node.GetRawText();
        }
    }

    private static IEnumerable<string> Children(JsonElement node, DecodedFilters filters) =>
        node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array
            ? children.EnumerateArray().Select(c => FormatNode(c, filters))
            : [];

    private static string FormatConstant(JsonElement node, DecodedFilters filters)
    {
        var op = node.TryGetProperty("op", out var opProp) ? opProp.GetString() : "eq";
        var opText = op switch
        {
            "eq" => "=",
            "ne" => "!=",
            "gt" => ">",
            "ge" => ">=",
            "lt" => "<",
            "le" => "<=",
            _ => op ?? "=",
        };

        var value = FormatValue(ResolveValue(node, filters));
        return $"{ColumnName(node)} {opText} {value}";
    }

    private static string FormatIn(JsonElement node, DecodedFilters filters)
    {
        IEnumerable<object?> values;
        if (node.TryGetProperty("value_refs", out var refs) && refs.ValueKind == JsonValueKind.Array)
        {
            values = refs.EnumerateArray().Select(r => filters.ValueRef(r.GetInt32()));
        }
        else if (node.TryGetProperty("values", out var vals) && vals.ValueKind == JsonValueKind.Array)
        {
            values = vals.EnumerateArray().Select(r => filters.ValueRef(r.GetInt32()));
        }
        else
        {
            values = [];
        }

        return $"{ColumnName(node)} IN ({string.Join(", ", values.Select(FormatValue))})";
    }

    /// <summary>Every DuckDB <c>InFilter</c> — whether from a literal SQL <c>IN (...)</c> or a
    /// join-derived build-side key set — serializes as this SAME <c>"join_keys"</c> JSON type (see
    /// <c>~/Development/vgi/src/vgi_table_function_impl.cpp</c>'s <c>SerializeFilterInto</c>,
    /// <c>TableFilterType::IN_FILTER</c> case), so a literal-vs-join-derived distinction isn't
    /// available to key off here. Lists the values for a small candidate set (as
    /// <c>filter_echo.test</c>'s literal <c>IN (1, 3, 7)</c>/<c>IN (0, 10, ..., 90)</c> cases — up
    /// to 10 values — expect verbatim) and falls back to a <c>"(N values)"</c> count summary once
    /// the set is large enough that listing it would be unreadable (as
    /// <c>join_keys_pushdown.test</c>'s 200-key build side expects) — this fixture's own choice of
    /// cutoff, not a wire-level threshold.</summary>
    private const int MaxListedJoinKeyValues = 20;

    private static string FormatJoinKeys(JsonElement node, DecodedFilters filters)
    {
        var keysColumn = node.TryGetProperty("keys_column", out var kc) ? kc.GetString() ?? "" : "";
        var values = filters.JoinKeyValues(keysColumn);
        var rendered = values.Count > MaxListedJoinKeyValues
            ? $"{values.Count} values"
            : string.Join(", ", values.Select(FormatValue));
        return $"{ColumnName(node)} IN ({rendered})";
    }

    private static object? ResolveValue(JsonElement node, DecodedFilters filters) =>
        node.TryGetProperty("value_ref", out var vr) ? filters.ValueRef(vr.GetInt32()) : null;

    private static string ColumnName(JsonElement node) =>
        node.TryGetProperty("column_name", out var name) ? name.GetString() ?? "?" : "?";

    private static string FormatValue(object? value) => value switch
    {
        null => "NULL",
        string s => $"'{s}'",
        bool b => b ? "true" : "false",
        double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
        float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL",
    };
}
