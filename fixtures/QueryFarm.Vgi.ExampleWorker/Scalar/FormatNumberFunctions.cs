using System.Globalization;
using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>format_number</c> — three overloads sharing one name, disambiguated by <see cref="ConstParamAttribute"/>
/// COUNT (0/1/2), pinning <see cref="Internal.OverloadResolver"/>'s arity-based matching. Backs
/// <c>overload/scalar_overload.test</c>.
/// </summary>
public sealed class FormatNumberDefaultFunction : ScalarFn
{
    public override string Name => "format_number";

    public override string Description => "Format number with default precision (0 decimals)";

    private void Compute([Param] DoubleArray value, StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetValue(i)!.Value.ToString("F0", CultureInfo.InvariantCulture));
        }
    }
}

public sealed class FormatNumberPrecisionFunction : ScalarFn
{
    public override string Name => "format_number";

    public override string Description => "Format number with specified precision";

    private void Compute(
        [ConstParam(Name = "precision", Doc = "Number of decimal places", Ge = 0, Le = 10)] long precision,
        [Param] DoubleArray value,
        StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetValue(i)!.Value.ToString("F" + precision, CultureInfo.InvariantCulture));
        }
    }
}

public sealed class FormatNumberFullFunction : ScalarFn
{
    public override string Name => "format_number";

    public override string Description => "Format number with precision and prefix";

    private void Compute(
        [ConstParam(Name = "precision", Doc = "Number of decimal places", Ge = 0, Le = 10)] long precision,
        [ConstParam(Name = "prefix", Doc = "Prefix string")] string prefix,
        [Param] DoubleArray value,
        StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(prefix + value.GetValue(i)!.Value.ToString("F" + precision, CultureInfo.InvariantCulture));
        }
    }
}
