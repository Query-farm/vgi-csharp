using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Types;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>double(value: ANY) -&gt; ANY</c> — doubles a numeric/decimal value; the output type is
/// resolved at bind time via <see cref="TypeRules.PromoteForAddition"/> for overflow headroom
/// (int width doubles capped at 64 bits, float promotes to float64, decimal gains one digit of
/// precision capped at 38). Implements <see cref="IScalarFunction"/> directly (not
/// <see cref="ScalarFn"/>) since its output type is dynamic — see <c>ComputePlan</c>'s doc comment.
/// </summary>
public sealed class DoubleFunction : IScalarFunction
{
    public string Name => "double";

    public string Description => "Doubles numeric values";

    public Schema ArgumentsSchema { get; } = AnyScalarSchema.SingleArg("value");

    public Schema OutputSchema { get; } = AnyScalarSchema.SingleResult();

    public void Bind(ScalarBindParams bindParams)
    {
        var field = bindParams.InputSchema?.FieldsList.FirstOrDefault();
        if (field is not null)
        {
            AnyScalarSchema.RequireAddable(Name, field.DataType);
        }
    }

    public Schema ResolveOutputSchema(Schema? inputSchema)
    {
        var field = inputSchema?.FieldsList.FirstOrDefault();
        if (field is null)
        {
            return OutputSchema;
        }

        var outputType = TypeRules.PromoteForAddition(field.DataType);
        return new Schema([new Field("result", outputType, nullable: true)], metadata: null);
    }

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var length = processParams.Input.Length;
        var outputType = processParams.OutputSchema.GetFieldByIndex(0).DataType;
        var result = NumericArrayMath.Double(processParams.Input.Column(0), outputType, length);
        return new RecordBatch(processParams.OutputSchema, [result], length);
    }
}
