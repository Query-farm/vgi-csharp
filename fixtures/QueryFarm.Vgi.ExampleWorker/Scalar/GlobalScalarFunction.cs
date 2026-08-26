using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// Global-function probe (test/sql/integration/global_functions/*.test): tags its own output with
/// its own name, proving a call through the globally-published name (<c>vgi_example_global_scalar</c>)
/// reached THIS function rather than some other candidate.
/// </summary>
public sealed class GlobalScalarFunction : ScalarFn
{
    public override string Name => "global_scalar";

    public override string Description => "Global-function probe (scalar)";

    private void Compute([Param] Int64Array v, StringArray.Builder result)
    {
        for (var i = 0; i < v.Length; i++)
        {
            if (v.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append($"global_scalar:{v.GetValue(i)}");
        }
    }
}
