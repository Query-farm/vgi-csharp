using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;

namespace QueryFarm.Vgi.DocsExamples;

public sealed class SumFunction : IAggregateFunction
{
    public string Name => "sum_int64";

    public string Description => "Sum BIGINT values";

    public Schema ArgumentsSchema { get; } = new(
        [new Field("value", Int64Type.Default, nullable: true)],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("result", Int64Type.Default, nullable: true)],
        metadata: null);

    public void Update(
        RecordBatch inputColumns,
        long[] groupIds,
        Dictionary<long, byte[]> states,
        AggregateCallParams callParams)
    {
        var values = (Int64Array)inputColumns.Column(0);
        for (var row = 0; row < groupIds.Length; row++)
        {
            if (values.IsNull(row))
            {
                continue;
            }

            var groupId = groupIds[row];
            var current = states.TryGetValue(groupId, out var state) ? BitConverter.ToInt64(state) : 0;
            states[groupId] = BitConverter.GetBytes(current + values.GetValue(row)!.Value);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams) =>
        BitConverter.GetBytes(BitConverter.ToInt64(source) + (target is null ? 0 : BitConverter.ToInt64(target)));

    public IArrowArray Finalize(
        long[] groupIds,
        byte[]?[] states,
        Schema outputSchema,
        AggregateCallParams callParams)
    {
        var result = new Int64Array.Builder();
        foreach (var state in states)
        {
            if (state is null)
            {
                result.AppendNull();
            }
            else
            {
                result.Append(BitConverter.ToInt64(state));
            }
        }

        return result.Build();
    }
}
