using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary>
/// <c>vgi_percentile(value DOUBLE, percentile DOUBLE) -> DOUBLE</c> — a <c>ConstParam</c> fixture
/// (<c>aggregate/const_param.test</c>). <c>percentile</c> carries
/// <c>{VgiWireMetadata.ConstKey: VgiWireMetadata.ConstTrueValue}</c> metadata so the C++ side
/// constant-folds it and erases it before <see cref="Update"/> ever sees a column for it — the
/// value only ever reaches this fixture via <see cref="AggregateBindParams.Arguments"/> /
/// <see cref="AggregateCallParams.Arguments"/>.
///
/// State replays every raw value seen (an order-independent percentile needs the full sorted set,
/// not a running fold) — see <see cref="ReplayStateCodec"/>. The "nearest rank, round up" index
/// formula (<c>ceil(p * (n - 1))</c>, clamped) is confirmed empirically against
/// <c>const_param.test</c>'s expected outputs (equivalent to NumPy's <c>interpolation='higher'</c>).
/// </summary>
public sealed class PercentileFunction : IAggregateFunction
{
    public string Name => "vgi_percentile";

    public string Description => "Computes a percentile over a DOUBLE column (ConstParam)";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = new(
        [
            new Field("value", DoubleType.Default, nullable: true),
            new Field("percentile", DoubleType.Default, nullable: true, ConstMetadata),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    private static readonly Dictionary<string, string> ConstMetadata = new() { [VgiWireMetadata.ConstKey] = VgiWireMetadata.ConstTrueValue };

    public void Bind(AggregateBindParams bindParams) => ValidatePercentile(bindParams.Arguments);

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
        var percentile = ReadPercentile(callParams.Arguments);
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
            var index = (int)Math.Ceiling(percentile * (values.Count - 1));
            index = Math.Clamp(index, 0, values.Count - 1);
            builder.Append(values[index]);
        }

        return builder.Build();
    }

    private static void ValidatePercentile(TableArguments arguments)
    {
        var value = arguments.Positional(0);
        if (value is null)
        {
            throw new InvalidOperationException("vgi_percentile: percentile must not be NULL");
        }

        var p = Convert.ToDouble(value);
        if (double.IsNaN(p) || double.IsInfinity(p))
        {
            throw new InvalidOperationException("vgi_percentile: percentile must be a finite number");
        }

        if (p is < 0.0 or > 1.0)
        {
            throw new InvalidOperationException("vgi_percentile: percentile must be in [0, 1]");
        }
    }

    private static double ReadPercentile(TableArguments arguments) => Convert.ToDouble(arguments.Positional(0));
}
