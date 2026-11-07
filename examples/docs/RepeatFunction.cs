using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.DocsExamples;

public sealed class RepeatFunction : ScalarFn
{
    public override string Name => "repeat_text";

    public override string Description => "Repeat each string a fixed number of times";

    public override IReadOnlyDictionary<string, string> Tags =>
        new Dictionary<string, string> { ["domain"] = "text" };

    public override IReadOnlyList<FunctionExample> Examples =>
    [
        new() { Sql = "SELECT demo.repeat_text('ab', 3)", Description = "Repeat a string three times", ExpectedOutput = "ababab" },
    ];

    // Static is fine: Compute needs no instance state. The [ConstParam] range is advertised
    // through vgi_function_arguments() and enforced when the call is bound.
    private static void Compute(
        [Param(Doc = "Text to repeat")] StringArray value,
        [ConstParam(Doc = "Number of repetitions", Ge = 0, Le = 100)] long times,
        StringArray.Builder result)
    {
        for (var row = 0; row < value.Length; row++)
        {
            if (value.IsNull(row))
            {
                result.AppendNull();
            }
            else
            {
                result.Append(string.Concat(Enumerable.Repeat(value.GetString(row), (int)times)));
            }
        }
    }
}
