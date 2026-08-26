using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>conditional_message(repeat_count: BIGINT [const], message: VARCHAR [const], condition: BOOLEAN)
/// -&gt; VARCHAR</c> — emits <c>message</c> repeated <c>repeat_count</c> times when
/// <c>condition</c> is true (and non-null), else an empty string.
/// </summary>
public sealed class ConditionalMessageFunction : ScalarFn
{
    public override string Name => "conditional_message";

    public override string Description => "Returns repeated message when condition is true";

    private void Compute(
        [ConstParam] long repeatCount,
        [ConstParam] string? message,
        [Param] BooleanArray condition,
        StringArray.Builder result)
    {
        var repeated = repeatCount <= 0 || message is null
            ? ""
            : string.Concat(Enumerable.Repeat(message, (int)repeatCount));

        for (var i = 0; i < condition.Length; i++)
        {
            var isTrue = !condition.IsNull(i) && condition.GetValue(i)!.Value;
            result.Append(isTrue ? repeated : "");
        }
    }
}
