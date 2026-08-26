using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>concat_values</c> — two VARARGS overloads sharing one name, disambiguated by the varargs
/// column TYPE (int64 vs. string) rather than count — every call-site column, however many, must
/// share that one type. Implements <see cref="IScalarFunction"/> directly (fixed-type varargs, not
/// <see cref="ScalarFn"/>'s reflection dispatch). Backs <c>overload/scalar_varargs_overload.test</c>.
/// </summary>
public sealed class ConcatValuesIntFunction : IScalarFunction
{
    public string Name => "concat_values";

    public string Description => "Sum integer varargs and return as string";

    public Schema ArgumentsSchema { get; } = new([VarargsField("values", Int64Type.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", StringType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var input = processParams.Input;
        var rows = input.Length;
        var sums = new long?[rows];
        for (var c = 0; c < input.ColumnCount; c++)
        {
            var column = (Int64Array)input.Column(c);
            for (var i = 0; i < rows; i++)
            {
                if (column.IsNull(i))
                {
                    sums[i] = null;
                }
                else if (sums[i] is not null || c == 0)
                {
                    sums[i] = (sums[i] ?? 0) + column.GetValue(i)!.Value;
                }
            }
        }

        var builder = new StringArray.Builder();
        foreach (var s in sums)
        {
            builder.Append(s?.ToString());
        }

        return new RecordBatch(processParams.OutputSchema, [builder.Build()], rows);
    }

    internal static Field VarargsField(string name, IArrowType elementType) => new(
        name, elementType, nullable: true,
        new Dictionary<string, string> { [VgiWireMetadata.VarargsKey] = VgiWireMetadata.VarargsTrueValue });
}

public sealed class ConcatValuesStrFunction : IScalarFunction
{
    public string Name => "concat_values";

    public string Description => "Concatenate string varargs";

    public Schema ArgumentsSchema { get; } = new([ConcatValuesIntFunction.VarargsField("values", StringType.Default)], metadata: null);

    public Schema OutputSchema { get; } = new([new Field("result", StringType.Default, nullable: true)], metadata: null);

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var input = processParams.Input;
        var rows = input.Length;
        var parts = new string?[rows];
        for (var c = 0; c < input.ColumnCount; c++)
        {
            var column = (StringArray)input.Column(c);
            for (var i = 0; i < rows; i++)
            {
                if (parts[i] is null && c > 0)
                {
                    continue; // already went null on an earlier column
                }

                parts[i] = column.IsNull(i) ? null : (parts[i] ?? "") + column.GetString(i);
            }
        }

        var builder = new StringArray.Builder();
        foreach (var p in parts)
        {
            builder.Append(p);
        }

        return new RecordBatch(processParams.OutputSchema, [builder.Build()], rows);
    }
}
