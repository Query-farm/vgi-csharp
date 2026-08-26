using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary><c>f(value VARCHAR) -> VARCHAR</c> — comma-joins every non-NULL value seen, in the
/// order <see cref="Update"/> sees them. Registered as both <c>vgi_listagg</c> (plain
/// <c>GROUP BY</c>, always run under <c>SET threads=1</c> in <c>listagg.test</c> so ordering is
/// deterministic without needing to declare <see cref="Protocol.AggregateOrderDependent"/>) and
/// <c>vgi_window_listagg</c> (windowed — DuckDB's default segment-tree combine can reorder
/// concatenation for a wide/parallel frame; the sliding-frame case <c>window.test</c> actually
/// exercises stays small/sequential enough in practice to read out in source order).</summary>
public sealed class ListaggFunction(string name) : IAggregateFunction
{
    public string Name => name;

    public string Description => "Comma-joins a VARCHAR column";

    public IReadOnlyList<string> Categories => ["aggregation", "string"];

    public Schema ArgumentsSchema { get; } = new([new Field("value", StringType.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", StringType.Default, nullable: true)], metadata: null);

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var values = (StringArray)inputColumns.Column(0);
        var byGroup = new Dictionary<long, List<string>>();
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

            list.Add(values.GetString(i));
        }

        foreach (var (gid, newValues) in byGroup)
        {
            var existing = states.TryGetValue(gid, out var bytes) ? ReplayStateCodec.ReadStrings(bytes) : [];
            existing.AddRange(newValues);
            states[gid] = ReplayStateCodec.WriteStrings(existing);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams)
    {
        var merged = target is null ? [] : ReplayStateCodec.ReadStrings(target);
        merged.AddRange(ReplayStateCodec.ReadStrings(source));
        return ReplayStateCodec.WriteStrings(merged);
    }

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var builder = new StringArray.Builder();
        foreach (var state in states)
        {
            var values = ReplayStateCodec.ReadStrings(state);
            if (values.Count == 0)
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(string.Join(",", values));
            }
        }

        return builder.Build();
    }
}
