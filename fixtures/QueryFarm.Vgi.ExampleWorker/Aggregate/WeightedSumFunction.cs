using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary><c>vgi_weighted_sum(value DOUBLE, weight DOUBLE) -> DOUBLE</c> — a two-Param aggregate:
/// <c>SUM(value * weight)</c>. Also registered as <c>secret_typed_sum</c> is a SEPARATE, simpler
/// fixture — see <see cref="SecretTypedSumFunction"/> — this one exists purely to exercise a
/// multi-column <c>Update</c> batch.</summary>
public sealed class WeightedSumFunction : IAggregateFunction
{
    public string Name => "vgi_weighted_sum";

    public string Description => "Sums value*weight across two Param columns";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = new(
        [new Field("value", DoubleType.Default, nullable: true), new Field("weight", DoubleType.Default, nullable: true)],
        metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var values = (DoubleArray)inputColumns.Column(0);
        var weights = (DoubleArray)inputColumns.Column(1);
        for (var i = 0; i < groupIds.Length; i++)
        {
            if (values.IsNull(i) || weights.IsNull(i))
            {
                continue;
            }

            var gid = groupIds[i];
            var current = states.TryGetValue(gid, out var bytes) ? BitConverter.ToDouble(bytes) : 0.0;
            current += values.GetValue(i)!.Value * weights.GetValue(i)!.Value;
            states[gid] = BitConverter.GetBytes(current);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams) =>
        BitConverter.GetBytes(BitConverter.ToDouble(source) + (target is null ? 0.0 : BitConverter.ToDouble(target)));

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var builder = new DoubleArray.Builder();
        foreach (var state in states)
        {
            if (state is null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(BitConverter.ToDouble(state));
            }
        }

        return builder.Build();
    }
}
