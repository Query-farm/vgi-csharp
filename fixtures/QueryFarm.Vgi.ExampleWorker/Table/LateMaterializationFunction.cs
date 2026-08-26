using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Backs the three <c>data.late_mat*</c> catalog tables (<c>table/late_materialization.test</c>):
/// a fixed <paramref name="rowCount"/>-row <c>(row_id, ord, payload, pushed)</c> table whose
/// <c>row_id</c> is a rowid virtual column and whose <c>ord</c> column is a SCRAMBLED permutation
/// of <c>0..rowCount-1</c> (a multiplicative-hash permutation, coprime with <paramref name="rowCount"/>)
/// so an <c>ORDER BY ord LIMIT k</c> Top-N scatters its survivor rowids across the table instead of
/// picking a contiguous prefix — the whole point of the late-materialization SEMI-join rewrite this
/// fixture proves out. <c>pushed</c> echoes whatever <c>row_id</c> pushdown filter THIS scan
/// actually received (see <see cref="BuildWitness"/>) — the wide (second) scan the rewrite issues
/// receives a genuine, ordinary bind-time <c>row_id IN (...)</c>/range filter on its OWN
/// independent bind, so this reads through the same <see cref="PushdownFilterCodec"/> path already
/// proven by <c>value_prune</c>'s bare-IN-list case (nothing per-tick/dynamic is needed here).
/// </summary>
public sealed class LateMaterializationFunction : ITableFunction
{
    private const int BatchSize = 2048;

    public required string Name { get; init; }

    public required int RowCount { get; init; }

    /// <summary>index -&gt; row_id (identity by default; <c>late_mat_dup</c> maps two indices per
    /// rowid to deliberately violate the worker-contract's UNIQUE-rowid requirement).</summary>
    public Func<int, long> RowIdFor { get; init; } = i => i;

    /// <summary>Fraction-of-rows-null stride for <c>ord</c> (0 = never null); <c>late_mat_nulls</c>
    /// sets this so some rows sort first/last depending on NULLS FIRST/LAST.</summary>
    public int NullOrdStride { get; init; }

    public string Description => "Late-materialization rowid fixture";

    public bool? ProjectionPushdown => true;

    public bool? FilterPushdown => true;

    public bool? LateMaterialization => true;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field(
                "row_id", Int64Type.Default, nullable: true,
                new Dictionary<string, string> { [VgiRowIdMetadata.Key] = VgiRowIdMetadata.Value }),
            new Field("ord", Int64Type.Default, nullable: true),
            new Field("payload", StringType.Default, nullable: true),
            new Field("pushed", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var decoded = PushdownFilterCodec.Decode(initParams.PushdownFilters, initParams.JoinKeys);
        var witness = BuildWitness(decoded);
        var indices = initParams.ProjectionIds
            ?? Enumerable.Range(0, initParams.OutputSchema.FieldsList.Count).Select(i => (long)i).ToList();

        // Multiplicative-hash permutation of 0..RowCount-1: the smallest odd multiplier that is
        // coprime with RowCount and large enough to scatter neighbouring indices apart.
        var multiplier = CoprimeMultiplier(RowCount);

        return new Producer(RowCount, RowIdFor, multiplier, NullOrdStride, witness, indices, initParams.ProjectedSchema, decoded);
    }

    private static long CoprimeMultiplier(int rowCount)
    {
        for (var candidate = 37L; candidate < rowCount; candidate += 2)
        {
            if (Gcd(candidate, rowCount) == 1)
            {
                return candidate;
            }
        }

        return 1;
    }

    private static long Gcd(long a, long b)
    {
        while (b != 0)
        {
            (a, b) = (b, a % b);
        }

        return a;
    }

    /// <summary>Formats whatever <c>row_id</c> pushdown filter this scan received as
    /// <c>"rid:in=&lt;N&gt;;rng=&lt;lo&gt;..&lt;hi&gt;"</c> (or <c>"rid:in=0;rng=none"</c> when
    /// nothing was pushed) — <c>in</c> is the discrete IN-list/join-keys candidate count (via the
    /// same AND-descent/OR-union resolution <see cref="ValuePruneFunction.ResolveColumnValues"/>
    /// already implements), <c>rng</c> is any min/max range bound also present (independently, so
    /// an IN-list combined with a range — the small-k case — reports BOTH).</summary>
    private static string BuildWitness(DecodedFilters? decoded)
    {
        var discrete = ValuePruneFunction.ResolveColumnValues(decoded, "row_id");
        var (lo, hi) = ResolveRange(decoded, "row_id");
        var rng = lo is not null && hi is not null ? $"{lo}..{hi}" : "none";
        return $"rid:in={discrete?.Count ?? 0};rng={rng}";
    }

    private static (long? Lo, long? Hi) ResolveRange(DecodedFilters? decoded, string column)
    {
        if (decoded is null || decoded.Root.ValueKind != JsonValueKind.Array)
        {
            return (null, null);
        }

        long? lo = null;
        long? hi = null;
        foreach (var node in decoded.Root.EnumerateArray())
        {
            CollectRange(node, decoded, column, ref lo, ref hi);
        }

        return (lo, hi);
    }

    private static void CollectRange(JsonElement node, DecodedFilters decoded, string column, ref long? lo, ref long? hi)
    {
        var type = node.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;
        var nodeColumn = node.TryGetProperty("column_name", out var cn) ? cn.GetString() : null;

        if (type == "and")
        {
            foreach (var child in Children(node))
            {
                CollectRange(child, decoded, column, ref lo, ref hi);
            }

            return;
        }

        if (type == "constant" && nodeColumn == column && node.TryGetProperty("op", out var opProp))
        {
            var op = opProp.GetString();
            if (op is "ge" or "gt" && node.TryGetProperty("value_ref", out var geRef))
            {
                var v = Convert.ToInt64(decoded.ValueRef(geRef.GetInt32()));
                lo = lo is null ? v : Math.Max(lo.Value, v);
            }
            else if (op is "le" or "lt" && node.TryGetProperty("value_ref", out var leRef))
            {
                var v = Convert.ToInt64(decoded.ValueRef(leRef.GetInt32()));
                hi = hi is null ? v : Math.Min(hi.Value, v);
            }
        }
    }

    private static IEnumerable<JsonElement> Children(JsonElement node) =>
        node.TryGetProperty("children", out var children) && children.ValueKind == JsonValueKind.Array
            ? children.EnumerateArray()
            : [];

    private sealed class Producer(
        int rowCount, Func<int, long> rowIdFor, long multiplier, int nullOrdStride, string witness,
        IReadOnlyList<long> projectionIds, Schema projectedSchema, DecodedFilters? decoded)
        : ITableFunctionProducer
    {
        private int _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= rowCount)
            {
                output.Finish();
                return;
            }

            var count = Math.Min(BatchSize, rowCount - _next);
            var start = _next;
            _next += count;

            var rowIdBuilder = projectionIds.Contains(0) ? new Int64Array.Builder() : null;
            var ordBuilder = projectionIds.Contains(1) ? new Int64Array.Builder() : null;
            var payloadBuilder = projectionIds.Contains(2) ? new StringArray.Builder() : null;
            var pushedBuilder = projectionIds.Contains(3) ? new StringArray.Builder() : null;
            var emitted = 0;

            for (var i = start; i < start + count; i++)
            {
                var rowId = rowIdFor(i);
                var row = new Dictionary<string, object?> { ["row_id"] = rowId };
                if (!PushdownFilterEvaluator.Matches(decoded, row))
                {
                    continue;
                }

                emitted++;
                rowIdBuilder?.Append(rowId);
                var ord = (i * multiplier) % rowCount;
                if (nullOrdStride > 0 && i % nullOrdStride == 0)
                {
                    ordBuilder?.AppendNull();
                }
                else
                {
                    ordBuilder?.Append(ord);
                }

                payloadBuilder?.Append($"payload_{rowId}");
                pushedBuilder?.Append(witness);
            }

            if (emitted > 0)
            {
                var columns = projectionIds.Select(index => index switch
                {
                    0 => (IArrowArray)rowIdBuilder!.Build(),
                    1 => ordBuilder!.Build(),
                    2 => payloadBuilder!.Build(),
                    _ => pushedBuilder!.Build(),
                }).ToList();
                output.Emit(new RecordBatch(projectedSchema, columns, emitted));
            }

            if (_next >= rowCount)
            {
                output.Finish();
            }
        }
    }
}
