using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>whoami(x: BIGINT) -&gt; VARCHAR</c> — an auth-context probe. Over the subprocess/stdio
/// transport this worker serves, auth is always anonymous (no bearer/JWT identity to surface),
/// so this always answers <c>"anonymous"</c> regardless of the input value.
/// </summary>
public sealed class WhoAmIFunction : ScalarFn
{
    public override string Name => "whoami";

    public override string Description => "Return the caller's auth identity (anonymous over subprocess transport)";

    private void Compute([Param] Int64Array x, StringArray.Builder result)
    {
        for (var i = 0; i < x.Length; i++)
        {
            result.Append("anonymous");
        }
    }
}
