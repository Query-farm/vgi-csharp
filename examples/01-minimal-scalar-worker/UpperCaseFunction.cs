using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.Examples.MinimalScalarWorker;

/// <summary>
/// <c>upper_case(value: VARCHAR) -> VARCHAR</c> — the function
/// <c>test/sql/integration/scalar/upper_case.test</c> exercises. Implements the raw
/// <see cref="IScalarFunction"/> contract directly to show what it involves; the README's
/// "Your first worker" writes the same function with the attribute-driven <see cref="ScalarFn"/>
/// base class, which is the shorter route for most functions.
/// </summary>
public sealed class UpperCaseFunction : IScalarFunction
{
    public string Name => "upper_case";

    public Schema ArgumentsSchema { get; } = new(
        [new Field("value", StringType.Default, nullable: true)],
        metadata: null);

    public Schema OutputSchema { get; } = new(
        [new Field("result", StringType.Default, nullable: true)],
        metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var input = processParams.Input;
        var values = (StringArray)input.Column(0);
        var builder = new StringArray.Builder();
        for (var i = 0; i < values.Length; i++)
        {
            if (values.IsNull(i))
            {
                builder.AppendNull();
                continue;
            }

            builder.Append(values.GetString(i).ToUpperInvariant());
        }

        return new RecordBatch(OutputSchema, [builder.Build()], values.Length);
    }
}
