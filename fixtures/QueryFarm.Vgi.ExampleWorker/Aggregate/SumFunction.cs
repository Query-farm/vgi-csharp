using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary>
/// <c>f(value BIGINT) -> BIGINT</c> — plain SQL <c>SUM</c>. The M5 anchor fixture
/// (<c>aggregate/basic.test</c>/<c>grouped.test</c>): a single 8-byte little-endian <c>long</c> per
/// group as accumulator state.
///
/// Registered under several names sharing IDENTICAL semantics —
/// <see cref="Protocol.FunctionInfo.SupportsWindow"/>/<c>StreamingPartitioned</c> are left at their
/// default <see langword="false"/>, so DuckDB never routes an <c>OVER(...)</c> call through the
/// specialized <c>aggregate_window</c>/<c>aggregate_streaming_*</c> RPC surface (this port defers
/// those — see the M5 report) for <c>vgi_window_sum</c>/<c>vgi_window_sum_batch</c>/
/// <c>vgi_streaming_sum</c>; DuckDB's OWN generic window-segment-tree execution drives the SAME
/// update/combine/finalize path a plain <c>GROUP BY</c> uses instead, which is correct for any
/// frame shape — just not as fast as a purpose-built streaming/window implementation would be.
/// </summary>
public sealed class SumFunction(string name) : IAggregateFunction
{
    public string Name => name;

    public string Description => "Sums a BIGINT column";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", Int64Type.Default, nullable: true)], metadata: null);

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var values = (Int64Array)inputColumns.Column(0);
        for (var i = 0; i < groupIds.Length; i++)
        {
            // A NULL value contributes nothing — and, critically, must NOT create a state entry
            // for a group that has never had a non-NULL value: SUM() over zero non-NULL rows is
            // SQL NULL, not 0. Presence in `states`, not a value comparison, is what the runner
            // persists — so a brand-new group with only NULL rows this call must stay absent.
            if (values.IsNull(i))
            {
                continue;
            }

            var gid = groupIds[i];
            var current = states.TryGetValue(gid, out var bytes) ? BitConverter.ToInt64(bytes) : 0L;
            current += values.GetValue(i)!.Value;
            states[gid] = BitConverter.GetBytes(current);
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
            if (state is null)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(BitConverter.ToInt64(state));
            }
        }

        return builder.Build();
    }
}
