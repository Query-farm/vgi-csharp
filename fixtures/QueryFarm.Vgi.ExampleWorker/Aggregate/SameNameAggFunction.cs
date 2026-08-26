using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary>
/// Schema-disambiguation probe (<c>aggregate/same_name_schemas.test</c>) — <c>test_same_name_agg</c>
/// is registered TWICE, once per schema (<c>main</c>/<c>data</c>), each tagging its
/// <c>SUM(n)</c> result with its OWN schema name so a mis-routed RPC (bind resolved to one
/// implementation, update/finalize resolved to the other by bare name) reads as a wrong tag rather
/// than merely a wrong number.
/// </summary>
public sealed class SameNameAggFunction(string schemaName, string description) : IAggregateFunction
{
    public string Name => "test_same_name_agg";

    public string SchemaName => schemaName;

    public string Description => description;

    public Schema ArgumentsSchema { get; } = new([new Field("n", Int64Type.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", StringType.Default, nullable: true)], metadata: null);

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var values = (Int64Array)inputColumns.Column(0);
        for (var i = 0; i < groupIds.Length; i++)
        {
            var gid = groupIds[i];
            var current = states.TryGetValue(gid, out var bytes) ? BitConverter.ToInt64(bytes) : 0L;
            if (!values.IsNull(i))
            {
                current += values.GetValue(i)!.Value;
            }

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
        var builder = new StringArray.Builder();
        foreach (var state in states)
        {
            var sum = state is null ? 0L : BitConverter.ToInt64(state);
            builder.Append($"{schemaName}:{sum}");
        }

        return builder.Build();
    }
}
