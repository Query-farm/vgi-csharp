using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Types;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>sum_values(values...: ANY) -&gt; ANY</c> — sums a variable (&gt;=1) number of numeric/decimal
/// columns. DuckDB coerces every vararg call site to one common SQL type before invoking the
/// worker, so the FIRST column's type already reflects the common type; output type is
/// <see cref="TypeRules.PromoteForAddition"/> of that first column's type (matching vgi-python's
/// <c>on_bind</c>).
/// </summary>
public sealed class SumValuesFunction : IScalarFunction
{
    public string Name => "sum_values";

    public string Description => "Sum multiple numeric values";

    public Schema ArgumentsSchema { get; } = AnyScalarSchema.Varargs("values");

    public Schema OutputSchema { get; } = AnyScalarSchema.SingleResult();

    public void Bind(ScalarBindParams bindParams)
    {
        var fields = bindParams.InputSchema?.FieldsList;
        if (fields is { Count: 0 })
        {
            throw new InvalidOperationException("sum_values requires at least 1 value");
        }

        if (fields is not null)
        {
            foreach (var field in fields)
            {
                AnyScalarSchema.RequireAddable(Name, field.DataType);
            }
        }
    }

    public Schema ResolveOutputSchema(Schema? inputSchema)
    {
        var fields = inputSchema?.FieldsList;
        if (fields is not { Count: > 0 })
        {
            return OutputSchema;
        }

        var outputType = TypeRules.PromoteForAddition(fields[0].DataType);
        return new Schema([new Field("result", outputType, nullable: true)], metadata: null);
    }

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var length = processParams.Input.Length;
        if (processParams.Input.ColumnCount == 0)
        {
            throw new InvalidOperationException("sum_values requires at least 1 value");
        }

        var columns = Enumerable.Range(0, processParams.Input.ColumnCount).Select(processParams.Input.Column).ToList();
        var outputType = processParams.OutputSchema.GetFieldByIndex(0).DataType;
        var result = NumericArrayMath.Sum(columns, outputType, length);
        return new RecordBatch(processParams.OutputSchema, [result], length);
    }
}
