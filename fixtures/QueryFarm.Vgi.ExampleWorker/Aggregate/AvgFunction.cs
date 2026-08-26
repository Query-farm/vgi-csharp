using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary><c>vgi_avg(value BIGINT) -> DOUBLE</c> — two-field state (running sum + count), packed
/// as 16 bytes (two little-endian <c>long</c>s).</summary>
public sealed class AvgFunction : IAggregateFunction
{
    public string Name => "vgi_avg";

    public string Description => "Averages a BIGINT column";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var values = (Int64Array)inputColumns.Column(0);
        for (var i = 0; i < groupIds.Length; i++)
        {
            var gid = groupIds[i];
            var (sum, count) = states.TryGetValue(gid, out var bytes) ? Decode(bytes) : (0L, 0L);
            if (!values.IsNull(i))
            {
                sum += values.GetValue(i)!.Value;
                count += 1;
            }

            states[gid] = Encode(sum, count);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams)
    {
        var (s1, c1) = Decode(source);
        var (s2, c2) = target is null ? (0L, 0L) : Decode(target);
        return Encode(s1 + s2, c1 + c2);
    }

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var builder = new DoubleArray.Builder();
        foreach (var state in states)
        {
            if (state is null)
            {
                builder.AppendNull();
                continue;
            }

            var (sum, count) = Decode(state);
            if (count == 0)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append((double)sum / count);
            }
        }

        return builder.Build();
    }

    private static (long Sum, long Count) Decode(byte[] bytes) => (BitConverter.ToInt64(bytes, 0), BitConverter.ToInt64(bytes, 8));

    private static byte[] Encode(long sum, long count)
    {
        var buffer = new byte[16];
        BitConverter.GetBytes(sum).CopyTo(buffer, 0);
        BitConverter.GetBytes(count).CopyTo(buffer, 8);
        return buffer;
    }
}
