using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.Examples.MinimalScalarWorker;

/// <summary>
/// <c>upper_case(value: VARCHAR) -> VARCHAR</c> — the function
/// <c>test/sql/integration/scalar/upper_case.test</c> exercises. Implements
/// <see cref="IScalarFunction"/> directly rather than via <see cref="ScalarFn"/>: M1's stated
/// fallback, since <see cref="ScalarFn"/>'s attribute-driven reflection dispatch is still a plain
/// pass-through pending M2.
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
