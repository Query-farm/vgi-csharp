using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary><c>vgi_window_median(value DOUBLE) -> DOUBLE</c> — true statistical median (average of
/// the two middle values for an even-sized frame, the single middle value for odd), primarily
/// exercised windowed (<c>aggregate/window.test</c>'s <c>OVER (... ROWS BETWEEN 2 PRECEDING AND
/// 2 FOLLOWING)</c>) — DIFFERENT from <see cref="PercentileFunction"/>'s "nearest rank, round up"
/// formula, which <c>const_param.test</c> pins to a different definition. Neither
/// <see cref="Protocol.FunctionInfo.SupportsWindow"/> nor the specialized <c>aggregate_window</c>
/// RPC surface is used — DuckDB's own generic window-segment-tree execution drives the same
/// update/combine/finalize path a plain <c>GROUP BY</c> would.</summary>
public sealed class WindowMedianFunction : IAggregateFunction
{
    public string Name => "vgi_window_median";

    public string Description => "True median of a DOUBLE column (windowed)";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = new([new Field("value", DoubleType.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var values = (DoubleArray)inputColumns.Column(0);
        var byGroup = new Dictionary<long, List<double>>();
        for (var i = 0; i < groupIds.Length; i++)
        {
            if (values.IsNull(i))
            {
                continue;
            }

            if (!byGroup.TryGetValue(groupIds[i], out var list))
            {
                byGroup[groupIds[i]] = list = [];
            }

            list.Add(values.GetValue(i)!.Value);
        }

        foreach (var (gid, newValues) in byGroup)
        {
            var existing = states.TryGetValue(gid, out var bytes) ? ReplayStateCodec.ReadDoubles(bytes) : [];
            existing.AddRange(newValues);
            states[gid] = ReplayStateCodec.WriteDoubles(existing);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams)
    {
        var merged = target is null ? [] : ReplayStateCodec.ReadDoubles(target);
        merged.AddRange(ReplayStateCodec.ReadDoubles(source));
        return ReplayStateCodec.WriteDoubles(merged);
    }

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var builder = new DoubleArray.Builder();
        foreach (var state in states)
        {
            var values = ReplayStateCodec.ReadDoubles(state);
            if (values.Count == 0)
            {
                builder.AppendNull();
                continue;
            }

            values.Sort();
            var mid = values.Count / 2;
            var median = values.Count % 2 == 1 ? values[mid] : (values[mid - 1] + values[mid]) / 2.0;
            builder.Append(median);
        }

        return builder.Build();
    }
}
