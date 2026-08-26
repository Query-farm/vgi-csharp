using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Splits;

/// <summary>
/// <c>split_dynamic_filter(n, splits)</c> — a range-partitioned split scan (like
/// <see cref="SplitRangeFunction"/>) that additionally decodes and APPLIES whatever pushdown
/// filters/join keys reach EACH split's own <c>init</c> (not <c>plan()</c> — see
/// <c>dynamic_filters.test</c>'s own doc comment on why this is the per-re-init hazard, distinct
/// from <see cref="SplitEchoFiltersFunction"/>'s plan-time-visibility claim), echoing the
/// canonical cross-SDK column-bounds rendering (<see cref="SplitFilterBoundsFormatter"/>) as a
/// second output column.
///
/// Declares <see cref="Cardinality"/> honestly so the join-key-pushdown scenario in that test
/// plans this scan onto the correct side of the hash join — see that override's doc comment.
///
/// <para><b>Root cause of the join-key-pushdown gap (found, fixed)</b> — a real worker-side bug,
/// NOT a C++-extension gap as an earlier investigation concluded. That investigation never
/// actually ran the join test against the canonical vgi-python reference worker; it does, and
/// passes 18/18 — proof the C++ delivery mechanism works fine when a worker is implemented
/// correctly. Comparing <c>EXPLAIN</c> output side by side was the key: vgi-python's fixture feeds
/// the hash join DIRECTLY from its scan node (<c>Projections: n</c> shown ON the
/// <c>SPLIT_DYNAMIC_FILTER</c> node itself); this fixture, lacking a
/// <see cref="ProjectionPushdown"/> declaration, forced DuckDB to insert an extra
/// <c>PROJECTION</c> operator ABOVE the scan to narrow its 2-column output down to the join's
/// single needed column. <c>join_filter_pushdown_optimizer.cpp</c>'s
/// <c>GetPushdownFilterTargets</c> traverses through a <c>LOGICAL_PROJECTION</c> node fine in
/// principle, but — confirmed via <c>VGI_STDERR_LOG=1</c> — that extra operator was enough to
/// make <c>GenerateJoinFilters</c> never even ATTEMPT to serialize a filter for this function at
/// all: zero <c>table_function.filters_serialized</c>/<c>join_keys_serialized</c> events fired for
/// the JOIN queries against this worker, while the identical query against vgi-python fired both.
/// Declaring <see cref="ProjectionPushdown"/> (and actually narrowing the emitted columns to
/// <see cref="TableInitParams.ProjectionIds"/> — the data-narrowing half matters just as much as
/// the flag, per <c>TableInOut/EchoFunction.cs</c>'s doc comment on that exact gotcha) removes the
/// extra operator and the scan feeds the join directly, matching vgi-python's plan shape exactly —
/// fixes the gap.</para>
///
/// <para><b>Statistics, tested and ruled out</b> (a user hint pointed at "statistics" as a
/// possible cause): declaring <see cref="Statistics"/> below — mirroring
/// <see cref="Table.SequenceFunction"/>'s column-statistics declaration — was tried FIRST and
/// empirically made no difference on its own; the real fix was the projection-pushdown one above.
/// Kept the statistics declaration anyway (accurate, harmless) since it's still a legitimate
/// improvement matching the established pattern elsewhere in this fixture set.</para>
/// </summary>
public sealed class SplitDynamicFilterFunction : ITableFunction
{
    public string Name => "split_dynamic_filter";

    public string Description => "Split scan that applies pushdown filters/join keys reaching each split's own init";

    public bool SupportsSplits => true;

    public bool? FilterPushdown => true;

    /// <summary>See this class's doc comment — without this, DuckDB has to insert an extra
    /// <c>PROJECTION</c> operator above the scan whenever a query needs fewer than both output
    /// columns, and that extra operator is what was silently defeating join-key-filter generation
    /// entirely (not merely filter DELIVERY) for <c>dynamic_filters.test</c>'s JOIN assertions.</summary>
    public bool? ProjectionPushdown => true;

    public int? MaxWorkers => 8;

    public Schema ArgumentsSchema { get; } = new(
        [TableArgFields.Named("n", Int64Type.Default), TableArgFields.Named("splits", Int64Type.Default)],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("pushed_filters", StringType.Default, nullable: true),
        ],
        metadata: null);

    /// <summary>Honest row-count estimate. A table function that reports "unknown" here is
    /// planned as the hash-join BUILD side instead of the probe side, and the dynamic join-key
    /// filter DuckDB derives from the build side is then never pushed into it — right answers,
    /// silently no pushdown. Reporting the true <c>n</c> (always larger than the tiny build-side
    /// table the test joins against) keeps this scan on the probe side.</summary>
    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64Named("n", 0);

    /// <summary>Column statistics for <c>n</c> — see this class's doc comment ("Statistics, tested
    /// and ruled out"). Kept as an accurate, harmless declaration matching
    /// <see cref="Table.SequenceFunction"/>'s established pattern.</summary>
    public IReadOnlyDictionary<string, Catalog.ColumnStatisticsInput>? Statistics(TableBindParams bindParams)
    {
        var n = bindParams.Arguments.Int64Named("n", 0);
        if (n <= 0)
        {
            return null;
        }

        return new Dictionary<string, Catalog.ColumnStatisticsInput>
        {
            ["n"] = new()
            {
                Min = 0L,
                Max = n - 1,
                HasNull = false,
                HasNotNull = true,
                DistinctCount = n,
            },
        };
    }

    public PlanResult Plan(TableBindParams bindParams, PlanRequest request)
    {
        var n = bindParams.Arguments.Int64Named("n", 0);
        var splits = bindParams.Arguments.Int64Named("splits", 1);
        var ranges = SplitRanges.Even(n, splits);
        var scanSplits = ranges.Select((r, i) => ScanSplit.Of(SplitPayloadCodec.Encode(i, r.Start, r.End))).ToList();
        return PlanResult.Of(scanSplits);
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var payloads = SplitOnlyGuard.RequireSingle(initParams, Name);
        var (_, start, end) = SplitPayloadCodec.Decode(payloads[0]);
        var staticDecoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        return new Producer(start, end, staticDecoded, initParams.ProjectedSchema, initParams.ProjectionIds);
    }

    /// <summary>The tick metadata key DuckDB's dynamic-filter machinery attaches a fresh,
    /// base64-encoded pushdown-filter blob under on every producer tick once a join-derived
    /// min/max range becomes available — see <c>UpdateDynamicFilterState</c>/
    /// <c>vgi_function_connection.cpp</c>'s tick-metadata write. Distinct from
    /// <see cref="TableInitParams.PushdownFilters"/>/<see cref="TableInitParams.JoinKeys"/>, which
    /// are fixed snapshots from whenever THIS split's init happened — a join's build side may not
    /// have finished yet at that point, so the genuinely dynamic half of pushdown only ever
    /// arrives this way.</summary>
    private const string DynamicFilterMetadataKey = "vgi_pushdown_filters";

    private sealed class Producer(
        long start, long end, DecodedFilters? staticDecoded, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
        : ITableFunctionProducer
    {
        private const int CandidateBatchSize = 500;
        private long _next = start;

        public void Produce(OutputCollector output)
        {
            var decoded = staticDecoded;
            DecodedFilters? dynamicDecoded = null;
            if (output.InputMetadata is { } meta
                && meta.TryGetValue(DynamicFilterMetadataKey, out var base64) && !string.IsNullOrEmpty(base64))
            {
                // The dynamic-filter tick carries ONLY the dynamic (join-derived) component — the
                // static half was already delivered at init and isn't re-sent — so row selection
                // below still uses just `staticDecoded`; the reported STRING is the union of both.
                dynamicDecoded = PushdownFilterCodec.Decode(Convert.FromBase64String(base64));
            }

            var filterText = SplitFilterBoundsFormatter.Format(staticDecoded, dynamicDecoded);

            var matched = new List<long>();
            var row = new Dictionary<string, object?>();

            // Loop across candidate chunks rather than emitting one possibly-empty batch per tick
            // (mirrors FilterEchoFunction) — stop once this call has SOME rows, or the range is
            // exhausted.
            while (matched.Count == 0 && _next < end)
            {
                var candidateRows = (int)Math.Min(CandidateBatchSize, end - _next);
                var chunkStart = _next;
                _next += candidateRows;

                for (var i = 0; i < candidateRows; i++)
                {
                    var n = chunkStart + i;
                    row["n"] = n;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
                    {
                        matched.Add(n);
                    }
                }
            }

            if (matched.Count == 0)
            {
                output.Finish();
                return;
            }

            var rows = matched.Count;

            IArrowArray BuildColumn(int fullIndex)
            {
                if (fullIndex == 0)
                {
                    var nBuilder = new Int64Array.Builder();
                    foreach (var n in matched)
                    {
                        nBuilder.Append(n);
                    }

                    return nBuilder.Build();
                }

                var pBuilder = new StringArray.Builder();
                for (var i = 0; i < rows; i++)
                {
                    pBuilder.Append(filterText);
                }

                return pBuilder.Build();
            }

            var indices = projectionIds ?? [0, 1];
            var columns = indices.Select(id => BuildColumn((int)id)).ToList();
            output.Emit(new RecordBatch(projectedSchema, columns, rows));

            if (_next >= end)
            {
                output.Finish();
            }
        }
    }
}

/// <summary>
/// Renders a <see cref="DecodedFilters"/> tree as the canonical cross-SDK column-bounds form
/// <c>dynamic_filters.test</c> pins: per referenced column, the tightest INTEGER
/// <c>[min, max]</c> implied by every comparison/IN/join-key node on it (deliberately NOT any
/// one language's own debug/repr output — every SDK's worker has to be able to reproduce this
/// same string). <c>col&gt;=min</c> and/or <c>col&lt;=max</c>, omitting whichever bound is
/// unset, columns and fragments both in sorted order, comma-joined. <c>n &lt; 30</c> normalizes
/// to the inclusive <c>n&lt;=29</c> — the only shape every SDK can produce (some client-side
/// column-bounds representations are integer-coerced and carry no exclusive/inclusive flag at
/// all). An empty/absent filter set renders as <c>"(none)"</c>.
/// </summary>
internal static class SplitFilterBoundsFormatter
{
    /// <summary>Formats one or more independently-decoded filter sets (e.g. the static init-time
    /// set plus a dynamic per-tick set — see <see cref="SplitDynamicFilterFunction"/>) as one
    /// combined bounds string. Each set's own leaf nodes resolve their constant/IN/join-key
    /// values against THAT set's own value columns, so the sets are walked independently rather
    /// than concatenated into one JSON tree.</summary>
    public static string Format(params DecodedFilters?[] filterSets)
    {
        var bounds = new SortedDictionary<string, (long? Min, long? Max)>(StringComparer.Ordinal);
        var sawAny = false;

        foreach (var filters in filterSets)
        {
            if (filters is null || filters.Root.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            foreach (var node in filters.Root.EnumerateArray())
            {
                sawAny |= Collect(node, filters, bounds);
            }
        }

        if (!sawAny)
        {
            return "(none)";
        }

        var parts = new List<string>();
        foreach (var (column, (min, max)) in bounds)
        {
            if (min.HasValue)
            {
                parts.Add($"{column}>={min.Value}");
            }

            if (max.HasValue)
            {
                parts.Add($"{column}<={max.Value}");
            }
        }

        return parts.Count == 0 ? "(none)" : string.Join(",", parts);
    }

    private static bool Collect(JsonElement node, DecodedFilters filters, SortedDictionary<string, (long? Min, long? Max)> bounds)
    {
        var type = node.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;
        switch (type)
        {
            case "constant":
                var column = ColumnName(node);
                var op = node.TryGetProperty("op", out var opProp) ? opProp.GetString() : "eq";
                var value = ToLong(node.TryGetProperty("value_ref", out var vr) ? filters.ValueRef(vr.GetInt32()) : null);
                if (value is null)
                {
                    return false;
                }

                switch (op)
                {
                    case "ge": MergeMin(bounds, column, value.Value); break;
                    case "gt": MergeMin(bounds, column, value.Value + 1); break;
                    case "le": MergeMax(bounds, column, value.Value); break;
                    case "lt": MergeMax(bounds, column, value.Value - 1); break;
                    case "eq": MergeMin(bounds, column, value.Value); MergeMax(bounds, column, value.Value); break;
                    default: return false;
                }

                return true;

            case "and":
            case "or":
                var any = false;
                if (node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array)
                {
                    foreach (var child in children.EnumerateArray())
                    {
                        any |= Collect(child, filters, bounds);
                    }
                }

                return any;

            case "in":
            case "in_list":
                var inColumn = ColumnName(node);
                var refs = node.TryGetProperty("value_refs", out var vrs) ? vrs
                    : node.TryGetProperty("values", out var vs) ? vs : default;
                var inValues = refs.ValueKind == JsonValueKind.Array
                    ? refs.EnumerateArray().Select(r => ToLong(filters.ValueRef(r.GetInt32()))).Where(v => v.HasValue).Select(v => v!.Value).ToList()
                    : [];
                if (inValues.Count == 0)
                {
                    return false;
                }

                MergeMin(bounds, inColumn, inValues.Min());
                MergeMax(bounds, inColumn, inValues.Max());
                return true;

            case "join_keys":
                var jkColumn = ColumnName(node);
                var keysColumn = node.TryGetProperty("keys_column", out var kc) ? kc.GetString() ?? "" : "";
                var jkValues = filters.JoinKeyValues(keysColumn).Select(ToLong).Where(v => v.HasValue).Select(v => v!.Value).ToList();
                if (jkValues.Count == 0)
                {
                    return false;
                }

                MergeMin(bounds, jkColumn, jkValues.Min());
                MergeMax(bounds, jkColumn, jkValues.Max());
                return true;

            default:
                return false;
        }
    }

    private static void MergeMin(SortedDictionary<string, (long? Min, long? Max)> bounds, string column, long value)
    {
        var (min, max) = bounds.TryGetValue(column, out var existing) ? existing : (null, null);
        bounds[column] = (min.HasValue ? Math.Max(min.Value, value) : value, max);
    }

    private static void MergeMax(SortedDictionary<string, (long? Min, long? Max)> bounds, string column, long value)
    {
        var (min, max) = bounds.TryGetValue(column, out var existing) ? existing : (null, null);
        bounds[column] = (min, max.HasValue ? Math.Min(max.Value, value) : value);
    }

    private static string ColumnName(JsonElement node) =>
        node.TryGetProperty("column_name", out var name) ? name.GetString() ?? "?" : "?";

    private static long? ToLong(object? value) => value switch
    {
        null => null,
        long l => l,
        int i => i,
        short s => s,
        byte b => b,
        double d => (long)d,
        float f => (long)f,
        bool boolean => boolean ? 1 : 0,
        _ => long.TryParse(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture), out var parsed) ? parsed : null,
    };
}
