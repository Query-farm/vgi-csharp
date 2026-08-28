using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.BadEnumWorker;

/// <summary>
/// <c>double(value: BIGINT) -&gt; BIGINT</c> — the ONE function <c>bad_enum.test</c> needs
/// (<c>SELECT badenum.double(1)</c>). Deliberately the simplest possible <c>double</c> (fixed
/// BIGINT, not ExampleWorker's ANY-typed promotion-aware version) — this fixture exists to corrupt
/// its catalog-metadata <c>null_handling</c> field (see <see cref="BadEnumFunctionInfoEncoder"/>),
/// not to exercise scalar type promotion, and the test's query never reaches <see cref="Process"/>
/// at all (the client rejects the corrupted metadata during BIND, before ever calling the worker).
/// </summary>
internal sealed class SimpleDoubleFunction : IScalarFunction
{
    public string Name => "double";

    public Schema ArgumentsSchema { get; } = new([new Field("value", Int64Type.Default, nullable: true)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", Int64Type.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var input = (Int64Array)processParams.Input.Column(0);
        var builder = new Int64Array.Builder();
        for (var i = 0; i < input.Length; i++)
        {
            if (input.IsNull(i))
            {
                builder.AppendNull();
            }
            else
            {
                builder.Append(input.GetValue(i)!.Value * 2);
            }
        }

        return new RecordBatch(processParams.OutputSchema, [builder.Build()], input.Length);
    }
}
