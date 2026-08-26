using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;
using QueryFarm.Vgi.Types;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>add_values(col1: ANY, col2: ANY) -&gt; ANY</c> — sums two numeric/decimal columns; the
/// output type is the <see cref="TypeRules.CommonTypeForAddition(Apache.Arrow.Types.IArrowType,Apache.Arrow.Types.IArrowType)"/>
/// of the two inputs. See <see cref="DoubleFunction"/>'s doc comment for why this implements
/// <see cref="IScalarFunction"/> directly.
/// </summary>
public sealed class AddValuesFunction : IScalarFunction
{
    public string Name => "add_values";

    public string Description => "Adds two numeric values";

    public Schema ArgumentsSchema { get; } = new([AnyScalarSchema.AnyField("col1"), AnyScalarSchema.AnyField("col2")], metadata: null);

    public Schema OutputSchema { get; } = AnyScalarSchema.SingleResult();

    public void Bind(ScalarBindParams bindParams)
    {
        var fields = bindParams.InputSchema?.FieldsList;
        if (fields is { Count: >= 2 })
        {
            AnyScalarSchema.RequireAddable(Name, fields[0].DataType);
            AnyScalarSchema.RequireAddable(Name, fields[1].DataType);
        }
    }

    public Schema ResolveOutputSchema(Schema? inputSchema)
    {
        var fields = inputSchema?.FieldsList;
        if (fields is not { Count: >= 2 })
        {
            return OutputSchema;
        }

        var outputType = TypeRules.CommonTypeForAddition(fields[0].DataType, fields[1].DataType);
        return new Schema([new Field("result", outputType, nullable: true)], metadata: null);
    }

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var length = processParams.Input.Length;
        var outputType = processParams.OutputSchema.GetFieldByIndex(0).DataType;
        var result = NumericArrayMath.Add(processParams.Input.Column(0), processParams.Input.Column(1), outputType, length);
        return new RecordBatch(processParams.OutputSchema, [result], length);
    }
}
