using Apache.Arrow;

namespace QueryFarm.Vgi.Internal;

/// <summary>
/// Builds a new Arrow array holding only the given row indices of a source array — the "gather"/
/// "take" operation Apache.Arrow.C# doesn't expose as a public compute kernel in this vendored
/// fork. Covers the primitive scalar types VGI's own fixtures actually need to filter/reorder a
/// generic-schema batch by (e.g. <c>filter_by_setting</c>); a nested (struct/list) source array
/// throws rather than silently mishandling it.
/// </summary>
public static class RowSelector
{
    public static IArrowArray Select(IArrowArray source, IReadOnlyList<int> indices) => source switch
    {
        Int8Array a => Select(a, indices, new Int8Array.Builder()),
        Int16Array a => Select(a, indices, new Int16Array.Builder()),
        Int32Array a => Select(a, indices, new Int32Array.Builder()),
        Int64Array a => Select(a, indices, new Int64Array.Builder()),
        UInt8Array a => Select(a, indices, new UInt8Array.Builder()),
        UInt16Array a => Select(a, indices, new UInt16Array.Builder()),
        UInt32Array a => Select(a, indices, new UInt32Array.Builder()),
        UInt64Array a => Select(a, indices, new UInt64Array.Builder()),
        FloatArray a => Select(a, indices, new FloatArray.Builder()),
        DoubleArray a => Select(a, indices, new DoubleArray.Builder()),
        BooleanArray a => Select(a, indices),
        StringArray a => Select(a, indices),
        BinaryArray a => Select(a, indices),
        _ => throw new NotSupportedException($"RowSelector.Select: unsupported array type '{source.GetType()}'."),
    };

    private static IArrowArray Select(Int8Array source, IReadOnlyList<int> indices, Int8Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(Int16Array source, IReadOnlyList<int> indices, Int16Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(Int32Array source, IReadOnlyList<int> indices, Int32Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(Int64Array source, IReadOnlyList<int> indices, Int64Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(UInt8Array source, IReadOnlyList<int> indices, UInt8Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(UInt16Array source, IReadOnlyList<int> indices, UInt16Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(UInt32Array source, IReadOnlyList<int> indices, UInt32Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(UInt64Array source, IReadOnlyList<int> indices, UInt64Array.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(FloatArray source, IReadOnlyList<int> indices, FloatArray.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(DoubleArray source, IReadOnlyList<int> indices, DoubleArray.Builder builder)
    {
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(BooleanArray source, IReadOnlyList<int> indices)
    {
        var builder = new BooleanArray.Builder();
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetValue(i)!.Value); }
        return builder.Build();
    }

    private static IArrowArray Select(StringArray source, IReadOnlyList<int> indices)
    {
        var builder = new StringArray.Builder();
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetString(i)); }
        return builder.Build();
    }

    private static IArrowArray Select(BinaryArray source, IReadOnlyList<int> indices)
    {
        var builder = new BinaryArray.Builder();
        foreach (var i in indices) { if (source.IsNull(i)) builder.AppendNull(); else builder.Append(source.GetBytes(i)); }
        return builder.Build();
    }
}
