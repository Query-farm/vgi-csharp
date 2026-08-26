using System.Globalization;
using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>smart_format</c> — two overloads sharing one name and the same (2) argument COUNT,
/// distinguished by the FIRST (<see cref="ConstParamAttribute"/>) parameter's TYPE (int width vs.
/// string prefix) rather than count — pins <see cref="Internal.OverloadResolver"/>'s per-field type
/// matching on a const-parameter position. Backs <c>overload/scalar_overload.test</c>.
/// </summary>
public sealed class SmartFormatWidthFunction : ScalarFn
{
    public override string Name => "smart_format";

    public override string Description => "Right-align value in field of given width";

    private void Compute(
        [ConstParam(Name = "width", Doc = "Field width")] int width,
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

            result.Append(PyFloatRepr(value.GetValue(i)!.Value).PadLeft(width));
        }
    }

    internal static string PyFloatRepr(double v)
    {
        var s = v.ToString(CultureInfo.InvariantCulture);
        return s.Contains('.') || s.Contains('E') || s.Contains('e') || s is "NaN" or "Infinity" or "-Infinity"
            ? s
            : s + ".0";
    }
}

public sealed class SmartFormatPrefixFunction : ScalarFn
{
    public override string Name => "smart_format";

    public override string Description => "Prepend prefix to formatted value";

    private void Compute(
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

            result.Append(prefix + SmartFormatWidthFunction.PyFloatRepr(value.GetValue(i)!.Value));
        }
    }
}
