using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary>
/// <c>vgi_count() -> BIGINT</c> — a NULLARY aggregate (zero declared columns): every row still
/// invokes <see cref="Update"/> once (there's no argument value that could be NULL to skip), so
/// this is <c>COUNT(*)</c>, not <c>COUNT(col)</c>. A group id with no accumulated state at
/// <see cref="Finalize"/> (an empty input table) reports 0, not SQL NULL — the one place this
/// fixture's "absent state" semantics differ from <see cref="SumFunction"/>'s.
/// </summary>
public sealed class CountFunction : IAggregateFunction
{
    public string Name => "vgi_count";

    public string Description => "Counts input rows (COUNT(*) semantics)";

    public IReadOnlyList<string> Categories => ["aggregation"];

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", Int64Type.Default, nullable: false)], metadata: null);

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        foreach (var gid in groupIds)
        {
            var current = states.TryGetValue(gid, out var bytes) ? BitConverter.ToInt64(bytes) : 0L;
            states[gid] = BitConverter.GetBytes(current + 1);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams)
    {
        var s = BitConverter.ToInt64(source);
        var t = target is null ? 0L : BitConverter.ToInt64(target);
        return BitConverter.GetBytes(s + t);
    }

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var builder = new Int64Array.Builder();
        foreach (var state in states)
        {
            builder.Append(state is null ? 0L : BitConverter.ToInt64(state));
        }

        return builder.Build();
    }
}
