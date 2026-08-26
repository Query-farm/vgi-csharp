using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>random_int(min_val: BIGINT, max_val: BIGINT) -&gt; BIGINT</c> — an inclusive random integer
/// in <c>[min_val, max_val]</c>. Per-row <see cref="ParamAttribute"/> columns, NOT
/// <see cref="ConstParamAttribute"/>s — <c>dedup.test</c> calls this with COLUMN (non-constant)
/// bounds (<c>SELECT example.random_int(lo, hi) FROM rin</c>), which a <c>[ConstParam]</c>
/// declaration would reject at bind time ("must be a constant value"). VOLATILE (non-deterministic;
/// also proves to <c>dedup.test</c> that a VOLATILE function is never input-deduped). No fixed
/// seed — tests only assert range membership, never exact values or cross-run determinism.
/// </summary>
public sealed class RandomIntFunction : ScalarFn
{
    public override string Name => "random_int";

    public override string Description => "Generate random integers (demonstrates VOLATILE stability)";

    public override FunctionStability? Stability => FunctionStability.Volatile;

    private void Compute([Param] Int64Array minVal, [Param] Int64Array maxVal, Int64Array.Builder result)
    {
        for (var i = 0; i < minVal.Length; i++)
        {
            if (minVal.IsNull(i) || maxVal.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(NextInclusive(minVal.GetValue(i)!.Value, maxVal.GetValue(i)!.Value));
        }
    }

    /// <summary>Random.Shared.NextInt64(lo, hi) is EXCLUSIVE of hi — the naive "+1 to make it
    /// inclusive" trick overflows when hi is long.MaxValue, so that one case is left exclusive of
    /// the top (still within [min_val, max_val], which is all the test suite checks).</summary>
    private static long NextInclusive(long min, long max)
    {
        if (max <= min)
        {
            return min;
        }

        return max == long.MaxValue ? Random.Shared.NextInt64(min, max) : Random.Shared.NextInt64(min, max + 1);
    }
}
