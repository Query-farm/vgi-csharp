using Apache.Arrow;
using Apache.Arrow.Types;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Encodes a <see cref="Protocol.ScanBranch"/>/<see cref="Protocol.ScanFunctionResult"/>'s call
/// arguments (or a format branch's reader options) as the FLAT wire shape
/// <c>DecodeScanArguments</c> (<c>vgi_catalog_api.cpp</c>) expects: a single-row record batch whose
/// columns are literally named <c>arg_&lt;N&gt;</c> for each positional argument and the bare
/// option/parameter name for each named one — NOT the <c>args:struct&lt;...&gt;</c>-wrapped shape
/// <see cref="TableArgCodec"/> decodes for an ordinary bind call. An empty/zero-length result (both
/// lists empty) means "no arguments" per <see cref="Protocol.ScanFunctionResult"/>'s doc comment —
/// <c>DecodeScanArguments</c> returns immediately on an empty byte array rather than parsing a
/// degenerate zero-column batch.
/// </summary>
public static class ScanArgsCodec
{
    public static byte[] Encode(IReadOnlyList<object?> positional, IReadOnlyDictionary<string, object?>? named = null)
    {
        named ??= new Dictionary<string, object?>();
        if (positional.Count == 0 && named.Count == 0)
        {
            return [];
        }

        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();

        for (var i = 0; i < positional.Count; i++)
        {
            AppendColumn($"arg_{i}", positional[i], fields, arrays);
        }

        foreach (var (name, value) in named)
        {
            AppendColumn(name, value, fields, arrays);
        }

        var schema = new Schema(fields, null);
        var batch = new RecordBatch(schema, arrays, 1);
        return RecordBatchIpc.Write(batch);
    }

    private static void AppendColumn(string name, object? value, List<Field> fields, List<IArrowArray> arrays)
    {
        if (value is IReadOnlyList<string?> strings)
        {
            // A VARCHAR[] argument — e.g. read_parquet(['a.parquet', 'b.parquet']).
            var builder = new ListArray.Builder(StringType.Default);
            var values = (StringArray.Builder)builder.ValueBuilder;
            builder.Append();
            foreach (var item in strings)
            {
                if (item is null)
                {
                    values.AppendNull();
                }
                else
                {
                    values.Append(item);
                }
            }

            var list = builder.Build();
            fields.Add(new Field(name, list.Data.DataType, nullable: true));
            arrays.Add(list);
            return;
        }

        var type = InferType(value);
        fields.Add(new Field(name, type, nullable: true));
        arrays.Add(AnyArrayBuilder.Build(type, [value]));
    }

    private static IArrowType InferType(object? value) => value switch
    {
        null => StringType.Default,
        bool => BooleanType.Default,
        sbyte or byte or short or ushort or int or uint or long or ulong => Int64Type.Default,
        float or double => DoubleType.Default,
        string => StringType.Default,
        byte[] => BinaryType.Default,
        _ => throw new NotSupportedException($"ScanArgsCodec: unsupported argument value type '{value.GetType()}'."),
    };
}
