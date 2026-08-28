using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.DocsExamples;

public sealed class UpperCaseFunction : ScalarFn
{
    public override string Name => "upper_case";

    public override string Description => "Convert strings to upper case";

    private void Compute([Param(Doc = "Text to convert")] StringArray value, StringArray.Builder result)
    {
        for (var row = 0; row < value.Length; row++)
        {
            if (value.IsNull(row))
            {
                result.AppendNull();
            }
            else
            {
                result.Append(value.GetString(row).ToUpperInvariant());
            }
        }
    }
}
