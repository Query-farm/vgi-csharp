using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>multiply_by_setting(value)</c> — multiplies each row by the <c>multiplier</c> DuckDB
/// session setting (an INT64-typed global setting; see <c>ExampleWorker.Settings</c> in
/// <c>Program.cs</c>). Backs <c>settings/multiply_by_setting.test</c>.
/// </summary>
public sealed class MultiplyBySettingFunction : ScalarFn
{
    public override string Name => "multiply_by_setting";

    public override string Description => "Multiply the input value by a setting value";

    private void Compute(
        [Param] Int64Array value,
        [Setting(Key = "multiplier")] long multiplier,
        Int64Array.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetValue(i)!.Value * multiplier);
        }
    }
}

/// <summary>
/// <c>scale_by_setting(value)</c> — scales each row by the DOUBLE-typed <c>scale_factor</c> DuckDB
/// session setting. Companion to <see cref="MultiplyBySettingFunction"/>, reading a floating-point
/// setting rather than an integer one. Backs <c>settings/settings_types.test</c>.
/// </summary>
public sealed class ScaleBySettingFunction : ScalarFn
{
    public override string Name => "scale_by_setting";

    public override string Description => "Scale the input value by the float setting `scale_factor`";

    private void Compute(
        [Param] DoubleArray value,
        [Setting(Key = "scale_factor")] double scaleFactor,
        DoubleArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetValue(i)!.Value * scaleFactor);
        }
    }
}
