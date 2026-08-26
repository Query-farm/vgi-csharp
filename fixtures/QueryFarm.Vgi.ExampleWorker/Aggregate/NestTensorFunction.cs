using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Aggregate;
using QueryFarm.Vgi.Internal;

namespace QueryFarm.Vgi.ExampleWorker.Aggregate;

/// <summary>
/// <c>nest_tensor(value ANY, axes ANY) -> STRUCT(tensor: nested-list, axes: STRUCT(axis: list&lt;coord&gt;, ...))</c>
/// — builds an N-dimensional dense tensor from (value, coordinate) rows. Companion to the scalar
/// <c>unnest_tensor</c> (<see cref="ExampleWorker.Scalar.UnnestTensorFunction"/>), whose exact
/// output shape this must produce for the round-trip tests in <c>scalar/unnest_tensor.test</c> and
/// <c>table_in_out/unnest_tensor_rows.test</c> to pass.
///
/// State replays every (value, coords) row seen — a dense tensor's shape (the per-axis coordinate
/// UNIVERSE) isn't known until every row has been seen, so nothing can be folded into a running
/// scalar the way <see cref="SumFunction"/> does. Duplicate-coordinate detection is two-tier,
/// matching <c>nest_tensor.test</c>'s two distinct regression cases: <see cref="Update"/> catches a
/// duplicate WITHIN its own incoming batch immediately (cheap, no state read needed);
/// <see cref="Finalize"/> catches one that only becomes visible once state from multiple
/// batches/workers has been combined.
/// </summary>
public sealed class NestTensorFunction : IAggregateFunction
{
    public string Name => "nest_tensor";

    public string Description => "Builds an N-dimensional dense tensor from (value, coords) rows";

    public IReadOnlyList<string> Categories => ["tensor"];

    public Schema ArgumentsSchema { get; } = new([AnyScalarSchema.AnyField("value"), AnyScalarSchema.AnyField("axes")], metadata: null);

    public Schema OutputSchema { get; } = AnyScalarSchema.SingleResult();

    public void Bind(AggregateBindParams bindParams)
    {
        if (bindParams.InputSchema is { FieldsList.Count: >= 2 } schema)
        {
            Describe(schema.FieldsList[1].DataType);
        }
    }

    public Schema ResolveOutputSchema(AggregateBindParams bindParams)
    {
        if (bindParams.InputSchema is not { FieldsList.Count: >= 2 } schema)
        {
            return OutputSchema;
        }

        var cellType = schema.FieldsList[0].DataType;
        var axesStruct = Describe(schema.FieldsList[1].DataType);

        var tensorType = cellType;
        foreach (var _ in axesStruct.Fields)
        {
            tensorType = new ListType(new Field("item", tensorType, nullable: true));
        }

        var axesFields = axesStruct.Fields
            .Select(f => new Field(f.Name, new ListType(new Field("item", f.DataType, nullable: false)), nullable: true))
            .ToArray();
        var resultType = new StructType(
            [new Field("tensor", tensorType, nullable: true), new Field("axes", new StructType(axesFields), nullable: false)]);

        return new Schema([new Field("result", resultType, nullable: true)], metadata: null);
    }

    /// <summary>Validates the <c>axes</c> argument is a struct with no floating-point coordinate
    /// field, returning it (typed) for the caller to reuse.</summary>
    private static StructType Describe(IArrowType axesType)
    {
        if (axesType is not StructType s)
        {
            throw new InvalidOperationException("nest_tensor: 'axes' argument must be a struct of coordinate columns");
        }

        foreach (var f in s.Fields)
        {
            if (f.DataType is FloatType or DoubleType)
            {
                throw new InvalidOperationException(
                    $"nest_tensor: axis '{f.Name}' coordinate type must not be floating-point");
            }
        }

        return s;
    }

    public void Update(RecordBatch inputColumns, long[] groupIds, Dictionary<long, byte[]> states, AggregateCallParams callParams)
    {
        var valueColumn = inputColumns.Column(0);
        var axesColumn = (StructArray)inputColumns.Column(1);
        var axesType = (StructType)axesColumn.Data.DataType;
        var axisCount = axesType.Fields.Count;

        var newRowsByGroup = new Dictionary<long, List<(object? Value, object?[] Coords)>>();
        var seenThisCallByGroup = new Dictionary<long, HashSet<string>>();

        for (var i = 0; i < groupIds.Length; i++)
        {
            if (axesColumn.IsNull(i))
            {
                continue;
            }

            var coords = new object?[axisCount];
            for (var a = 0; a < axisCount; a++)
            {
                coords[a] = ScalarArgCodec.ReadScalar(axesColumn.Fields[a], i);
                if (coords[a] is null)
                {
                    throw new InvalidOperationException(
                        $"NestTensorError: nest_tensor: null coord value for axis '{axesType.Fields[a].Name}'");
                }
            }

            var gid = groupIds[i];
            var key = CoordKey(coords);
            if (!seenThisCallByGroup.TryGetValue(gid, out var seen))
            {
                seenThisCallByGroup[gid] = seen = [];
            }

            if (!seen.Add(key))
            {
                throw new InvalidOperationException($"NestTensorError: nest_tensor: duplicate coordinate ({key})");
            }

            var value = ScalarArgCodec.ReadScalar(valueColumn, i);
            if (!newRowsByGroup.TryGetValue(gid, out var list))
            {
                newRowsByGroup[gid] = list = [];
            }

            list.Add((value, coords));
        }

        foreach (var (gid, rows) in newRowsByGroup)
        {
            var existing = states.TryGetValue(gid, out var bytes) ? ReadRows(bytes) : [];
            existing.AddRange(rows);
            states[gid] = WriteRows(existing);
        }
    }

    public byte[] Combine(byte[] source, byte[]? target, AggregateCallParams callParams)
    {
        var merged = target is null ? [] : ReadRows(target);
        merged.AddRange(ReadRows(source));
        return WriteRows(merged);
    }

    public IArrowArray Finalize(long[] groupIds, byte[]?[] states, Schema outputSchema, AggregateCallParams callParams)
    {
        var resultType = (StructType)outputSchema.GetFieldByIndex(0).DataType;
        var tensorType = resultType.Fields[0].DataType;
        var axesStructType = (StructType)resultType.Fields[1].DataType;
        var axisCount = axesStructType.Fields.Count;

        var count = groupIds.Length;
        var tensorPerGroup = new List<object?>(count);
        var axisListsPerGroup = new List<object?>[axisCount];
        for (var a = 0; a < axisCount; a++)
        {
            axisListsPerGroup[a] = new List<object?>(count);
        }

        for (var g = 0; g < count; g++)
        {
            var rows = ReadRows(states[g]);

            var seen = new HashSet<string>();
            foreach (var row in rows)
            {
                var key = CoordKey(row.Coords);
                if (!seen.Add(key))
                {
                    throw new InvalidOperationException($"NestTensorError: nest_tensor: duplicate coordinate ({key})");
                }
            }

            if (rows.Count == 0)
            {
                tensorPerGroup.Add(null);
                for (var a = 0; a < axisCount; a++)
                {
                    axisListsPerGroup[a].Add(null);
                }

                continue;
            }

            var axisValues = new List<object>[axisCount];
            var indexMaps = new Dictionary<object, int>[axisCount];
            for (var a = 0; a < axisCount; a++)
            {
                var distinct = rows.Select(r => r.Coords[a]!).Distinct().ToList();
                distinct.Sort(CompareBoxed);
                axisValues[a] = distinct;
                var map = new Dictionary<object, int>();
                for (var k = 0; k < distinct.Count; k++)
                {
                    map[distinct[k]] = k;
                }

                indexMaps[a] = map;
            }

            var shape = axisValues.Select(v => v.Count).ToArray();
            var grid = BuildEmptyGrid(shape, 0);
            foreach (var row in rows)
            {
                var idx = new int[axisCount];
                for (var a = 0; a < axisCount; a++)
                {
                    idx[a] = indexMaps[a][row.Coords[a]!];
                }

                SetGridValue(grid, idx, 0, row.Value);
            }

            tensorPerGroup.Add(grid);
            for (var a = 0; a < axisCount; a++)
            {
                axisListsPerGroup[a].Add(axisValues[a].Cast<object?>().ToList());
            }
        }

        var tensorArray = BuildNested(tensorType, tensorPerGroup);
        var axisArrays = new IArrowArray[axisCount];
        for (var a = 0; a < axisCount; a++)
        {
            axisArrays[a] = BuildNested(axesStructType.Fields[a].DataType, axisListsPerGroup[a]);
        }

        var axesStructArray = new StructArray(axesStructType, count, axisArrays, AllValidBuffer(count));
        return new StructArray(resultType, count, [tensorArray, axesStructArray], AllValidBuffer(count));
    }

    /// <summary>Recursively builds a (possibly multi-level) list array from CLR
    /// <c>List&lt;object?&gt;</c> values matching <paramref name="targetType"/>'s nesting — a
    /// non-list <paramref name="targetType"/> is the recursion's leaf case, built via
    /// <see cref="AnyArrayBuilder"/>. A <see langword="null"/> item at any level produces a null
    /// list/leaf at that position (sparse cell, or "group had no rows").</summary>
    private static IArrowArray BuildNested(IArrowType targetType, List<object?> items)
    {
        if (targetType is not ListType listType)
        {
            return AnyArrayBuilder.Build(targetType, items);
        }

        var offsets = new ArrowBuffer.Builder<int>();
        var validity = new ArrowBuffer.BitmapBuilder();
        offsets.Append(0);
        var flatChildren = new List<object?>();
        var running = 0;
        var nullCount = 0;
        foreach (var item in items)
        {
            if (item is null)
            {
                validity.Append(false);
                offsets.Append(running);
                nullCount++;
                continue;
            }

            var sub = (List<object?>)item;
            flatChildren.AddRange(sub);
            running += sub.Count;
            validity.Append(true);
            offsets.Append(running);
        }

        var childArray = BuildNested(listType.ValueDataType, flatChildren);
        var data = new ArrayData(
            listType, items.Count, nullCount, 0, [validity.Build(), offsets.Build()], [childArray.Data]);
        return new ListArray(data);
    }

    private static List<object?> BuildEmptyGrid(int[] shape, int level)
    {
        var n = shape[level];
        var list = new List<object?>(n);
        if (level == shape.Length - 1)
        {
            for (var i = 0; i < n; i++)
            {
                list.Add(null);
            }
        }
        else
        {
            for (var i = 0; i < n; i++)
            {
                list.Add(BuildEmptyGrid(shape, level + 1));
            }
        }

        return list;
    }

    private static void SetGridValue(List<object?> grid, int[] idx, int level, object? value)
    {
        if (level == idx.Length - 1)
        {
            grid[idx[level]] = value;
        }
        else
        {
            SetGridValue((List<object?>)grid[idx[level]]!, idx, level + 1, value);
        }
    }

    private static int CompareBoxed(object? a, object? b) => (a, b) switch
    {
        (long x, long y) => x.CompareTo(y),
        (double x, double y) => x.CompareTo(y),
        (string x, string y) => string.CompareOrdinal(x, y),
        (bool x, bool y) => x.CompareTo(y),
        _ => Comparer<object>.Default.Compare(a, b),
    };

    private static string CoordKey(object?[] coords) => string.Join("|", coords.Select(c => c?.ToString() ?? "\0null"));

    private static byte[] WriteRows(List<(object? Value, object?[] Coords)> rows)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, System.Text.Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(rows.Count);
            foreach (var (value, coords) in rows)
            {
                WriteBoxed(writer, value);
                writer.Write(coords.Length);
                foreach (var c in coords)
                {
                    WriteBoxed(writer, c);
                }
            }
        }

        return stream.ToArray();
    }

    private static List<(object? Value, object?[] Coords)> ReadRows(byte[]? bytes)
    {
        var result = new List<(object?, object?[])>();
        if (bytes is null || bytes.Length == 0)
        {
            return result;
        }

        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, System.Text.Encoding.UTF8, leaveOpen: true);
        var count = reader.ReadInt32();
        for (var i = 0; i < count; i++)
        {
            var value = ReadBoxed(reader);
            var coordCount = reader.ReadInt32();
            var coords = new object?[coordCount];
            for (var c = 0; c < coordCount; c++)
            {
                coords[c] = ReadBoxed(reader);
            }

            result.Add((value, coords));
        }

        return result;
    }

    private static void WriteBoxed(BinaryWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.Write((byte)0);
                break;
            case bool b:
                writer.Write((byte)1);
                writer.Write(b);
                break;
            case string s:
                writer.Write((byte)2);
                writer.Write(s);
                break;
            case float f:
                writer.Write((byte)3);
                writer.Write((double)f);
                break;
            case double d:
                writer.Write((byte)3);
                writer.Write(d);
                break;
            default:
                writer.Write((byte)4);
                writer.Write(Convert.ToInt64(value));
                break;
        }
    }

    private static object? ReadBoxed(BinaryReader reader)
    {
        var tag = reader.ReadByte();
        return tag switch
        {
            0 => null,
            1 => reader.ReadBoolean(),
            2 => reader.ReadString(),
            3 => reader.ReadDouble(),
            4 => reader.ReadInt64(),
            _ => throw new InvalidOperationException("nest_tensor: corrupt state"),
        };
    }

    private static ArrowBuffer AllValidBuffer(int length)
    {
        var builder = new ArrowBuffer.BitmapBuilder();
        for (var i = 0; i < length; i++)
        {
            builder.Append(true);
        }

        return builder.Build();
    }
}
