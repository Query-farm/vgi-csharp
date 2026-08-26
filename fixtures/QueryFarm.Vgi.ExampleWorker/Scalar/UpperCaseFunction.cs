using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>upper_case(value: VARCHAR) -&gt; VARCHAR</c>. Uses <see cref="System.Globalization.CultureInfo.InvariantCulture"/>
/// uppercasing (not the current-thread culture) so this can never trip a locale-dependent
/// uppercasing quirk (e.g. Turkish dotless-i) regardless of the host environment's locale.
/// </summary>
public sealed class UpperCaseFunction : ScalarFn
{
    public override string Name => "upper_case";

    public override string Description => "Converts string values to uppercase";

    private void Compute([Param] StringArray value, StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetString(i).ToUpperInvariant());
        }
    }
}
