using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>Verifies that bind receives the complete resolved VGI 2 function signature.</summary>
public sealed class ArgumentNamesProbeFunction : ScalarFn
{
    public override string Name => "argument_names_probe";

    public override string Description => "Checks VGI 2.0 bind-time argument names";

    public override RecordBatch ParameterDefaultValues { get; } = new(
        new Schema([new Field("scale", Int64Type.Default, nullable: false)], metadata: null),
        [new Int64Array.Builder().Append(2).Build()],
        1);

    public override void Bind(ScalarBindParams bindParams)
    {
        string[] expected = ["left", "right", "scale"];
        if (bindParams.ArgumentNames is null || !bindParams.ArgumentNames.SequenceEqual(expected))
        {
            throw new InvalidOperationException(
                $"argument_names_probe expected [{string.Join(", ", expected)}], got " +
                (bindParams.ArgumentNames is null
                    ? "null"
                    : $"[{string.Join(", ", bindParams.ArgumentNames.Select(value => value ?? "null"))}]"));
        }
    }

    private void Compute(
        [Param] Int64Array left,
        [Param] Int64Array right,
        [ConstParam] long scale,
        Int64Array.Builder result)
    {
        for (var i = 0; i < left.Length; i++)
        {
            if (left.IsNull(i) || right.IsNull(i))
            {
                result.AppendNull();
            }
            else
            {
                result.Append(checked((left.GetValue(i)!.Value + right.GetValue(i)!.Value) * scale));
            }
        }
    }
}
