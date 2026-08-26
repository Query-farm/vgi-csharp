using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>null_handling(value: BIGINT) -&gt; BIGINT</c> — returns <c>value</c> when non-null, else
/// <c>-5000</c>. Advertises <see cref="Protocol.FunctionNullHandling.Special"/> so DuckDB actually
/// delivers NULL rows to the worker instead of short-circuiting them to a NULL result client-side.
/// </summary>
public sealed class NullHandlingFunction : ScalarFn
{
    private const long NullSentinel = -5000;

    public override string Name => "null_handling";

    public override string Description => "Returns value or -5000 if null";

    public override FunctionNullHandling? NullHandling => Protocol.FunctionNullHandling.Special;

    private void Compute([Param] Int64Array value, Int64Array.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            result.Append(value.IsNull(i) ? NullSentinel : value.GetValue(i)!.Value);
        }
    }
}
