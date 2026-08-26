using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>value_prune(count)</c> — end-to-end coverage for the "resolve the discrete candidate-value
/// set for column <c>n</c>" pattern (vgi-python's <c>PushdownFilters.get_column_values()</c>):
/// echoes the resolved set (or <c>"(scan)"</c> when the pushed-down filter tree for <c>n</c> isn't
/// reducible to a discrete set) in the <c>resolved</c> column, and also genuinely filters (real
/// <see cref="ITableFunction.FilterPushdown"/>) so row results stay correct regardless. Backs
/// <c>value_prune.test</c>.
///
/// <para><b>Root cause of the AND-descent gap (found, fixed)</b> — a real worker-side bug, NOT a
/// C++-extension gap as an earlier investigation concluded (that investigation never actually ran
/// this test against the canonical vgi-python reference worker; it does, and passes 26/26). Same
/// class of bug as <see cref="Splits.SplitDynamicFilterFunction"/>'s (identical fix): this
/// function has TWO output columns (<c>n</c>, <c>resolved</c>) and, lacking a
/// <see cref="ProjectionPushdown"/> declaration, forced DuckDB to insert an extra
/// <c>PROJECTION</c> operator above the scan to narrow down to just <c>resolved</c> for this
/// query's <c>SELECT DISTINCT resolved ... WHERE n IN (subquery)</c> shape — every other case in
/// the file happens to need BOTH columns (or is a `SELECT *`), so it never hit this. That extra
/// operator was enough to make <c>join_filter_pushdown_optimizer.cpp</c>'s
/// <c>GenerateJoinFilters</c> never attempt to serialize a filter for this function at all for
/// this one query shape (confirmed via <c>VGI_STDERR_LOG=1</c>: zero
/// <c>table_function.filters_serialized</c> events, vs. python's non-zero). Declaring
/// <see cref="ProjectionPushdown"/> (and actually narrowing the emitted columns to
/// <see cref="TableInitParams.ProjectionIds"/> — the data-narrowing half matters just as much as
/// the flag, per <c>TableInOut/EchoFunction.cs</c>'s doc comment on that exact gotcha) removes the
/// extra operator and fixes the gap.
///
/// <b>Statistics, tested and ruled out</b> (a follow-up hypothesis, tried first): declaring
/// <see cref="Statistics"/> below — mirroring <see cref="SequenceFunction"/>'s column-statistics
/// declaration — made no difference on its own; the real fix was the projection-pushdown one
/// above. Kept the declaration anyway (accurate, harmless).</para>
/// </summary>
public sealed class ValuePruneFunction : ITableFunction
{
    public string Name => "value_prune";

    public string Description => "Resolves the discrete value set for n via pushdown filter descent";

    public bool? FilterPushdown => true;

    /// <summary>See this class's doc comment — without this, DuckDB has to insert an extra
    /// <c>PROJECTION</c> operator above the scan whenever a query needs fewer than both output
    /// columns, and that extra operator was silently defeating join-key-filter generation
    /// entirely (not merely filter delivery) for the AND-descent case.</summary>
    public bool? ProjectionPushdown => true;

    /// <summary>Honest row-count estimate — see <see cref="Splits.SplitDynamicFilterFunction.Cardinality"/>'s
    /// doc comment for why this matters: a table function that reports "unknown" here is planned
    /// as the hash-join BUILD side instead of the probe side.</summary>
    public long? Cardinality(TableBindParams bindParams) => bindParams.Arguments.Int64(0);

    /// <summary>Column statistics for <c>n</c> — see this class's doc comment ("Statistics, tested
    /// and ruled out"). Kept as an accurate, harmless declaration matching
    /// <see cref="SequenceFunction"/>'s established pattern.</summary>
    public IReadOnlyDictionary<string, Catalog.ColumnStatisticsInput>? Statistics(TableBindParams bindParams)
    {
        var count = bindParams.Arguments.Int64(0);
        if (count <= 0)
        {
            return null;
        }

        return new Dictionary<string, Catalog.ColumnStatisticsInput>
        {
            ["n"] = new()
            {
                Min = 0L,
                Max = count - 1,
                HasNull = false,
                HasNotNull = true,
                DistinctCount = count,
            },
        };
    }

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Positional("count", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("resolved", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var resolvedText = FormatResolved(ResolveColumnValues(decoded, "n"));
        return new Producer(count, decoded, resolvedText, initParams.ProjectedSchema, initParams.ProjectionIds);
    }

    /// <summary>Attempts to reduce the pushdown-filter tree to a discrete candidate-value set for
    /// <paramref name="column"/>: a top-level conjunction descends into the first child that itself
    /// resolves (AND-descent); a disjunction resolves only when EVERY branch resolves, as the union
    /// of their sets (OR-union) — a single non-enumerable branch (a bare range, a different column)
    /// makes the whole node non-enumerable, since dropping that branch's rows would be wrong.</summary>
    internal static IReadOnlyList<object?>? ResolveColumnValues(DecodedFilters? filters, string column)
    {
        if (filters is null || filters.Root.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        foreach (var node in filters.Root.EnumerateArray())
        {
            var resolved = ResolveNode(node, column, filters);
            if (resolved is not null)
            {
                return resolved;
            }
        }

        return null;
    }

    private static IReadOnlyList<object?>? ResolveNode(JsonElement node, string column, DecodedFilters filters)
    {
        var type = node.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;
        var nodeColumn = node.TryGetProperty("column_name", out var cn) ? cn.GetString() : null;

        switch (type)
        {
            case "constant" when nodeColumn == column:
                var op = node.TryGetProperty("op", out var opProp) ? opProp.GetString() : null;
                if (op != "eq")
                {
                    return null;
                }

                return node.TryGetProperty("value_ref", out var vr) ? [filters.ValueRef(vr.GetInt32())] : null;
            case "in" or "in_list" when nodeColumn == column:
                var refs = node.TryGetProperty("value_refs", out var r1) && r1.ValueKind == JsonValueKind.Array
                    ? r1.EnumerateArray()
                    : node.TryGetProperty("values", out var r2) && r2.ValueKind == JsonValueKind.Array
                        ? r2.EnumerateArray()
                        : [];
                return refs.Select(r => filters.ValueRef(r.GetInt32())).ToList();
            case "join_keys" when nodeColumn == column:
                var keysColumn = node.TryGetProperty("keys_column", out var kc) ? kc.GetString() ?? "" : "";
                return filters.JoinKeyValues(keysColumn);
            case "and":
                foreach (var child in Children(node))
                {
                    var resolved = ResolveNode(child, column, filters);
                    if (resolved is not null)
                    {
                        return resolved;
                    }
                }

                return null;
            case "or":
                var union = new List<object?>();
                foreach (var child in Children(node))
                {
                    var resolved = ResolveNode(child, column, filters);
                    if (resolved is null)
                    {
                        return null;
                    }

                    union.AddRange(resolved);
                }

                return union;
            default:
                return null;
        }
    }

    private static IEnumerable<JsonElement> Children(JsonElement node) =>
        node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array
            ? children.EnumerateArray()
            : [];

    private static string FormatResolved(IReadOnlyList<object?>? values)
    {
        if (values is null)
        {
            return "(scan)";
        }

        var sorted = values.Select(v => Convert.ToInt64(v)).Distinct().OrderBy(v => v);
        return string.Join(",", sorted);
    }

    private sealed class Producer(
        long count, DecodedFilters? decoded, string resolvedText, Schema projectedSchema, IReadOnlyList<long>? projectionIds)
        : ITableFunctionProducer
    {
        private const int BatchSize = 2048;
        private long _next;

        public void Produce(OutputCollector output)
        {
            var ns = new List<long>();
            var row = new Dictionary<string, object?>();
            while (ns.Count == 0 && _next < count)
            {
                var candidateRows = (int)Math.Min(BatchSize, count - _next);
                var start = _next;
                _next += candidateRows;
                for (var i = 0; i < candidateRows; i++)
                {
                    var n = start + i;
                    row["n"] = n;
                    if (PushdownFilterEvaluator.Matches(decoded, row))
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

            var rows = ns.Count;

            IArrowArray BuildColumn(int fullIndex)
            {
                if (fullIndex == 0)
                {
                    var nBuilder = new Int64Array.Builder();
                    foreach (var n in ns)
                    {
                        nBuilder.Append(n);
                    }

                    return nBuilder.Build();
                }

                var resolvedBuilder = new StringArray.Builder();
                for (var i = 0; i < rows; i++)
                {
                    resolvedBuilder.Append(resolvedText);
                }

                return resolvedBuilder.Build();
            }

            var indices = projectionIds ?? [0, 1];
            var columns = indices.Select(id => BuildColumn((int)id)).ToList();
            output.Emit(new RecordBatch(projectedSchema, columns, rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }
    }
}
