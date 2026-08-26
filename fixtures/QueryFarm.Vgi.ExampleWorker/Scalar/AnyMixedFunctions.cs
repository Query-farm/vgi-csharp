using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>any_mixed</c> — two overloads sharing one name and the same (2) argument COUNT: the FIRST
/// parameter is <c>ANY</c>-typed (matches unconditionally — see <see cref="Internal.OverloadResolver"/>'s
/// <c>vgi_type=any</c> handling) in both overloads, so only the SECOND parameter's type (int64 vs.
/// string) disambiguates. Implements <see cref="IScalarFunction"/> directly — <see cref="ScalarFn"/>'s
/// reflection dispatch doesn't support <c>[Param(Any = true)]</c>. Backs
/// <c>overload/scalar_overload.test</c>.
/// </summary>
public sealed class AnyMixedIntFunction : IScalarFunction
{
    public string Name => "any_mixed";

    public string Description => "Any+int dispatch";

    public Schema ArgumentsSchema { get; } = new([AnyScalarSchema.AnyField("a"), new Field("b", Int64Type.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", StringType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var b = (Int64Array)processParams.Input.Column(1);
        var builder = new StringArray.Builder();
        for (var i = 0; i < b.Length; i++)
        {
            builder.Append(b.IsNull(i) ? null : $"any+int: {b.GetValue(i)}");
        }

        return new RecordBatch(processParams.OutputSchema, [builder.Build()], b.Length);
    }
}

public sealed class AnyMixedStrFunction : IScalarFunction
{
    public string Name => "any_mixed";

    public string Description => "Any+str dispatch";

    public Schema ArgumentsSchema { get; } = new([AnyScalarSchema.AnyField("a"), new Field("b", StringType.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", StringType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var b = (StringArray)processParams.Input.Column(1);
        var builder = new StringArray.Builder();
        for (var i = 0; i < b.Length; i++)
        {
            builder.Append(b.IsNull(i) ? null : $"any+str: {b.GetString(i)}");
        }

        return new RecordBatch(processParams.OutputSchema, [builder.Build()], b.Length);
    }
}
