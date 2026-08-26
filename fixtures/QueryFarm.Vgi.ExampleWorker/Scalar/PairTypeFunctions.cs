using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>pair_type</c> — three overloads sharing one name and the same (2) argument COUNT,
/// distinguished by BOTH columns' types together (int+int / str+str / int+str) — pins
/// <see cref="Internal.OverloadResolver"/> matching more than one field position at once. Backs
/// <c>overload/scalar_overload.test</c>.
/// </summary>
public sealed class PairTypeIntIntFunction : ScalarFn
{
    public override string Name => "pair_type";

    public override string Description => "Return type pair name for int+int";

    private void Compute([Param] Int64Array a, [Param] Int64Array b, StringArray.Builder result)
    {
        for (var i = 0; i < a.Length; i++)
        {
            result.Append(a.IsNull(i) || b.IsNull(i) ? null : "int+int");
        }
    }
}

public sealed class PairTypeStrStrFunction : ScalarFn
{
    public override string Name => "pair_type";

    public override string Description => "Return type pair name for str+str";

    private void Compute([Param] StringArray a, [Param] StringArray b, StringArray.Builder result)
    {
        for (var i = 0; i < a.Length; i++)
        {
            result.Append(a.IsNull(i) || b.IsNull(i) ? null : "str+str");
        }
    }
}

public sealed class PairTypeIntStrFunction : ScalarFn
{
    public override string Name => "pair_type";

    public override string Description => "Return type pair name for int+str";

    private void Compute([Param] Int64Array a, [Param] StringArray b, StringArray.Builder result)
    {
        for (var i = 0; i < a.Length; i++)
        {
            result.Append(a.IsNull(i) || b.IsNull(i) ? null : "int+str");
        }
    }
}
