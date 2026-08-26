using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// A small "ladder" of scalar fixtures backing <c>scalar/function_registration.test</c>'s pinned
/// roster — mirrors vgi-python's <c>_test_fixtures/scalar/bench_ladder.py</c>/<c>random_demo.py</c>
/// naming and shapes exactly (name/description/parameter/stability), since that test validates
/// every VGI worker implementation against one shared specification.
/// </summary>
public sealed class PassthruFunction : ScalarFn
{
    public override string Name => "passthru";

    public override string Description => "Returns the input string unchanged (zero-compute wire probe)";

    private void Compute([Param] StringArray value, StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetString(i));
        }
    }
}

public sealed class CollatzStepsFunction : ScalarFn
{
    public override string Name => "collatz_steps";

    public override string Description => "Number of Collatz (3n+1) steps for each integer to reach 1";

    private void Compute([Param] Int64Array value, Int64Array.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            var n = value.GetValue(i)!.Value;
            long steps = 0;
            while (n > 1 && steps < 10_000)
            {
                n = n % 2 == 0 ? n / 2 : (3 * n) + 1;
                steps++;
            }

            result.Append(steps);
        }
    }
}

public sealed class Sha256HexFunction : ScalarFn
{
    public override string Name => "sha256_hex";

    public override string Description => "Lowercase hex SHA-256 digest of the UTF-8 string";

    private void Compute([Param] StringArray value, StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value.GetString(i)));
            result.Append(Convert.ToHexString(hash).ToLowerInvariant());
        }
    }
}

public sealed class HashRoundsFunction : ScalarFn
{
    public override string Name => "hash_rounds";

    public override string Description => "Apply SHA-256 rounds times (key-stretching); rounds is a const compute knob";

    private void Compute([Param] StringArray value, [ConstParam] long rounds, StringArray.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            var bytes = Encoding.UTF8.GetBytes(value.GetString(i));
            for (var r = 0; r < rounds; r++)
            {
                bytes = SHA256.HashData(bytes);
            }

            result.Append(Convert.ToHexString(bytes).ToLowerInvariant());
        }
    }
}

/// <summary>VOLATILE, zero-param — demonstrates a scalar function with no column/const input at
/// all, driven purely by <see cref="OutputLengthAttribute"/>.</summary>
public sealed class BernoulliFunction : ScalarFn
{
    public override string Name => "bernoulli";

    public override string Description => "Generate random booleans (demonstrates VOLATILE stability)";

    public override FunctionStability? Stability => FunctionStability.Volatile;

    private void Compute([OutputLength] int rows, BooleanArray.Builder result)
    {
        for (var i = 0; i < rows; i++)
        {
            result.Append(Random.Shared.Next(2) == 0);
        }
    }
}

public sealed class MultiplyFunction : ScalarFn
{
    public override string Name => "multiply";

    public override string Description => "Multiplies a value by a constant factor";

    private void Compute(
        [Param(Doc = "Integer value to multiply")] Int64Array value,
        [ConstParam(Doc = "Multiplication factor")] long factor,
        Int64Array.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetValue(i)!.Value * factor);
        }
    }
}

/// <summary>CONSISTENT_WITHIN_QUERY — the wire enum's third stability variant (alongside
/// CONSISTENT/VOLATILE) needs at least one fixture emitting it. Adds a fixed per-query-stable
/// offset (the reference implementations do the same) rather than anything actually query-random,
/// so SQL expectations stay stable across runs.</summary>
public sealed class QuerySeedFunction : ScalarFn
{
    private const long QueryOffset = 1000;

    public override string Name => "query_seed";

    public override string Description => "Add a per-query-stable seed to each value (CONSISTENT_WITHIN_QUERY)";

    public override FunctionStability? Stability => FunctionStability.ConsistentWithinQuery;

    private void Compute([Param] Int64Array value, Int64Array.Builder result)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (value.IsNull(i))
            {
                result.AppendNull();
                continue;
            }

            result.Append(value.GetValue(i)!.Value + QueryOffset);
        }
    }
}

/// <summary>CONSISTENT despite the name — deterministic from <c>seed</c>, so calling twice with the
/// same seed/length reproduces the same bytes.</summary>
public sealed class RandomBytesFunction : ScalarFn
{
    public override string Name => "random_bytes";

    public override string Description => "Generate pseudo-random binary blobs from seed and length";

    private void Compute([ConstParam] long seed, [ConstParam] long byteLength, [OutputLength] int rows, BinaryArray.Builder result)
    {
        var length = (int)Math.Max(0, byteLength);
        for (var i = 0; i < rows; i++)
        {
            var rng = new Random(unchecked((int)(seed + i)));
            var buffer = new byte[length];
            rng.NextBytes(buffer);
            result.Append(buffer);
        }
    }
}
