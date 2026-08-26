using Apache.Arrow;
using QueryFarm.Vgi.Attributes;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>type_info</c> — five overloads sharing one name and the same (1) argument COUNT, distinguished
/// purely by the argument's COLUMN TYPE — pins <see cref="Internal.OverloadResolver"/>'s per-field
/// type matching (arity alone can't disambiguate these). Backs <c>overload/scalar_overload.test</c>.
/// </summary>
public sealed class TypeInfoInt32Function : ScalarFn
{
    public override string Name => "type_info";

    public override string Description => "Return type name for int32 input";

    private void Compute([Param] Int32Array v, StringArray.Builder result)
    {
        for (var i = 0; i < v.Length; i++)
        {
            result.Append(v.IsNull(i) ? null : "int32");
        }
    }
}

public sealed class TypeInfoInt64Function : ScalarFn
{
    public override string Name => "type_info";

    public override string Description => "Return type name for int64 input";

    private void Compute([Param] Int64Array v, StringArray.Builder result)
    {
        for (var i = 0; i < v.Length; i++)
        {
            result.Append(v.IsNull(i) ? null : "int64");
        }
    }
}

public sealed class TypeInfoUInt32Function : ScalarFn
{
    public override string Name => "type_info";

    public override string Description => "Return type name for uint32 input";

    private void Compute([Param] UInt32Array v, StringArray.Builder result)
    {
        for (var i = 0; i < v.Length; i++)
        {
            result.Append(v.IsNull(i) ? null : "uint32");
        }
    }
}

public sealed class TypeInfoUInt64Function : ScalarFn
{
    public override string Name => "type_info";

    public override string Description => "Return type name for uint64 input";

    private void Compute([Param] UInt64Array v, StringArray.Builder result)
    {
        for (var i = 0; i < v.Length; i++)
        {
            result.Append(v.IsNull(i) ? null : "uint64");
        }
    }
}

public sealed class TypeInfoStringFunction : ScalarFn
{
    public override string Name => "type_info";

    public override string Description => "Return type name for string input";

    private void Compute([Param] StringArray v, StringArray.Builder result)
    {
        for (var i = 0; i < v.Length; i++)
        {
            result.Append(v.IsNull(i) ? null : "varchar");
        }
    }
}
