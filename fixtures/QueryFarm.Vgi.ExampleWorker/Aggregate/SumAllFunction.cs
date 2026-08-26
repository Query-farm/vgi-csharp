using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary><c>vgi_sum_all(...columns) -> DOUBLE</c> — varargs aggregate: sums every value across
/// every declared column AND every row. The output type is a fixed <c>DOUBLE</c> regardless of the
/// (dynamic, ANY-typed) input column types — unlike <see cref="GenericSumFunction"/>, whose output
/// type tracks the input's.</summary>
public sealed class SumAllFunction : IAggregateFunction
{
    public string Name => "vgi_sum_all";

    public string Description => "Sums every value across every varargs column";

    public IReadOnlyList<string> Categories => ["aggregation", "numeric"];

    public Schema ArgumentsSchema { get; } = AnyScalarSchema.Varargs("columns");

    public Schema OutputSchema { get; } = new([new Field("result", DoubleType.Default, nullable: true)], metadata: null);

    public void Bind(AggregateBindParams bindParams)
    {
        if (bindParams.InputSchema is null || bindParams.InputSchema.FieldsList.Count == 0)
        {
            throw new InvalidOperationException("vgi_sum_all requires at least 1 value");
        }
    }

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        for (var i = 0; i < groupIds.Length; i++)
        {
            double rowContribution = 0;
            var touched = false;
            for (var c = 0; c < inputColumns.ColumnCount; c++)
            {
                var column = inputColumns.Column(c);
                // NumericArrayMath.ReadAsDouble doesn't cover Decimal128Array by design (a decimal
                // needs SqlDecimal-range arithmetic for a WRITE path) — this is a read-only sum
                // contribution, so a plain double conversion is adequate. Same inline pattern
                // SumAllColumnsFunction uses for the same reason.
                double? v = column is Decimal128Array decimalColumn
                    ? decimalColumn.IsNull(i) ? null : (double)decimalColumn.GetValue(i)!.Value
                    : NumericArrayMath.ReadAsDouble(column, i);
                if (v is not null)
                {
                    rowContribution += v.Value;
                    touched = true;
                }
            }

            if (!touched)
            {
                continue;
            }

            var gid = groupIds[i];
            var current = states.TryGetValue(gid, out var bytes) ? BitConverter.ToDouble(bytes) : 0.0;
            states[gid] = BitConverter.GetBytes(current + rowContribution);
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
