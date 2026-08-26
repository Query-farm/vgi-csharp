using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary><c>vgi_generic_sum(value ANY) -> ANY</c> — the aggregate analog of the scalar suite's
/// dynamic-return-type fixtures: the output type tracks whatever numeric type
/// <see cref="AggregateBindParams.InputSchema"/> resolves to (BIGINT in → BIGINT out, DOUBLE in →
/// DOUBLE out, etc.), resolved once at bind time via <see cref="ResolveOutputSchema"/> and echoed
/// back on every later <c>aggregate_finalize</c> call's <c>output_schema</c>. The accumulator
/// itself is always a <c>double</c> (adequate for every value this suite's fixtures sum), converted
/// to the resolved output type only at <see cref="Finalize"/>.</summary>
public sealed class GenericSumFunction : IAggregateFunction
{
    public string Name => "vgi_generic_sum";

    public string Description => "Sums a column, output type matches input type";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = AnyScalarSchema.SingleArg("value");

    public Schema OutputSchema { get; } = AnyScalarSchema.SingleResult();

    public Schema ResolveOutputSchema(AggregateBindParams bindParams)
    {
        var fieldType = bindParams.InputSchema?.FieldsList.FirstOrDefault()?.DataType ?? DoubleType.Default;
        return new Schema([new Field("result", fieldType, nullable: true)], metadata: null);
    }

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var column = inputColumns.Column(0);
        for (var i = 0; i < groupIds.Length; i++)
        {
            if (NumericArrayMath.ReadAsDouble(column, i) is not { } v)
            {
                continue;
            }

            var gid = groupIds[i];
            var current = states.TryGetValue(gid, out var bytes) ? BitConverter.ToDouble(bytes) : 0.0;
            states[gid] = BitConverter.GetBytes(current + v);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams) =>
        BitConverter.GetBytes(BitConverter.ToDouble(source) + (target is null ? 0.0 : BitConverter.ToDouble(target)));

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var type = outputSchema.GetFieldByIndex(0).DataType;
        var values = states.Select(s => s is null ? (object?)null : BitConverter.ToDouble(s)).ToList();
        return AnyArrayBuilder.Build(type, values);
    }
}
