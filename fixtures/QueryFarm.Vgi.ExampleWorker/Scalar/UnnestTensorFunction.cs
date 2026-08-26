using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Scalar;

namespace QueryFarm.Vgi.ExampleWorker.Scalar;

/// <summary>
/// <c>unnest_tensor(t: STRUCT(tensor: nested-list, axes: STRUCT(axis_1: list&lt;coord_1&gt;, ...)))
/// -&gt; LIST&lt;STRUCT(value: cell, axes: STRUCT(axis_1: coord_1, ...))&gt;</c> — flattens an
/// N-dimensional tensor struct (as produced by the companion AGGREGATE <c>nest_tensor</c>, not yet
/// implemented in this port — see below) into one row per cell, emitting the FULL Cartesian
/// product of the axes' coordinate lists (including a null <c>value</c> for a sparse/unfilled
/// cell). The input's exact shape (tensor nesting depth, cell type, per-axis coordinate type) is
/// arbitrary — resolved dynamically from the caller's actual input schema, like <c>double</c>/
/// <c>add_values</c>.
///
/// <para>KNOWN GAP: this only implements the scalar function itself. The 3D-round-trip and lateral
/// subtests in <c>unnest_tensor.test</c> compose this with <c>nest_tensor</c> (an aggregate
/// function) to build a tensor from a table first — aggregate function support is a later
/// milestone (M5) in this port's roadmap, so those specific cases fail with "unknown function"
/// regardless of this scalar implementation's own correctness.</para>
/// </summary>
public sealed class UnnestTensorFunction : IScalarFunction
{
    public string Name => "unnest_tensor";

    public string Description => "Unnest an N-dimensional tensor struct into (value, axes) cells";

    public Schema ArgumentsSchema { get; } = AnyScalarSchema.SingleArg("t");

    public Schema OutputSchema { get; } = AnyScalarSchema.SingleResult();

    public void Bind(ScalarBindParams bindParams)
    {
        var field = bindParams.InputSchema?.FieldsList.FirstOrDefault();
        if (field is not null)
        {
            Describe(field.DataType);
        }
    }

    public Schema ResolveOutputSchema(Schema? inputSchema)
    {
        var field = inputSchema?.FieldsList.FirstOrDefault();
        if (field is null)
        {
            return OutputSchema;
        }

        var desc = Describe(field.DataType);
        var axesFields = desc.Axes.Select(a => new Field(a.Name, a.CoordType, nullable: true)).ToArray();
        var axesType = new StructType(axesFields);
        var elementType = new StructType([new Field("value", desc.CellType, nullable: true), new Field("axes", axesType, nullable: false)]);
        var listType = new ListType(new Field("item", elementType, nullable: false));
        return new Schema([new Field("result", listType, nullable: true)], metadata: null);
    }

    public RecordBatch Process(ScalarProcessParams processParams)
    {
        var input = processParams.Input.Column(0);
        var length = processParams.Input.Length;
        var listType = (ListType)processParams.OutputSchema.GetFieldByIndex(0).DataType;
        var elementType = (StructType)listType.ValueField.DataType;
        var cellType = elementType.Fields[0].DataType;
        var axesType = (StructType)elementType.Fields[1].DataType;

        var cellValues = new List<object?>();
        var axisValues = axesType.Fields.Select(_ => new List<object?>()).ToList();
        var offsets = new ArrowBuffer.Builder<int>();
        var listValidity = new ArrowBuffer.BitmapBuilder();
        offsets.Append(0);
        var runningOffset = 0;
        var nullRows = 0;

        for (var row = 0; row < length; row++)
        {
            if (input is not StructArray s || s.IsNull(row))
            {
                offsets.Append(runningOffset);
                listValidity.Append(false);
                nullRows++;
                continue;
            }

            var structType = (StructType)s.Data.DataType;
            var tensorIdx = IndexOfField(structType, "tensor");
            var axesIdx = IndexOfField(structType, "axes");
            var tensorColumn = s.Fields[tensorIdx];
            var axesColumn = (StructArray)s.Fields[axesIdx];
            var axesColumnType = (StructType)axesColumn.Data.DataType;

            var shape = new int[axesType.Fields.Count];
            var coordsPerAxis = new List<object?>[axesType.Fields.Count];
            for (var a = 0; a < axesType.Fields.Count; a++)
            {
                var srcIdx = IndexOfField(axesColumnType, axesType.Fields[a].Name);
                var listArr = (ListArray)axesColumn.Fields[srcIdx];
                var coords = new List<object?>();
                if (row < listArr.Length && !listArr.IsNull(row))
                {
                    var values = listArr.GetSlicedValues(row);
                    for (var k = 0; k < values.Length; k++)
                    {
                        coords.Add(ScalarArgCodec.ReadScalar(values, k));
                    }
                }

                coordsPerAxis[a] = coords;
                shape[a] = coords.Count;
            }

            var totalCells = shape.Length == 0 ? 0 : shape.Aggregate(1, (acc, s2) => acc * s2);
            var idx = new int[shape.Length];
            for (var cell = 0; cell < totalCells; cell++)
            {
                var remainder = cell;
                for (var a = shape.Length - 1; a >= 0; a--)
                {
                    idx[a] = shape[a] == 0 ? 0 : remainder % shape[a];
                    remainder /= Math.Max(shape[a], 1);
                }

                cellValues.Add(WalkTensor(tensorColumn, row, idx));
                for (var a = 0; a < shape.Length; a++)
                {
                    axisValues[a].Add(coordsPerAxis[a][idx[a]]);
                }
            }

            runningOffset += totalCells;
            offsets.Append(runningOffset);
            listValidity.Append(true);
        }

        var cellArray = AnyArrayBuilder.Build(cellType, cellValues);
        var axisArrays = new IArrowArray[axesType.Fields.Count];
        for (var a = 0; a < axesType.Fields.Count; a++)
        {
            axisArrays[a] = AnyArrayBuilder.Build(axesType.Fields[a].DataType, axisValues[a]);
        }

        var elementCount = cellValues.Count;
        var axesStructArray = new StructArray(axesType, elementCount, axisArrays, AllValidBuffer(elementCount));
        var elementStructArray = new StructArray(
            elementType, elementCount, [cellArray, axesStructArray], AllValidBuffer(elementCount));

        var listData = new ArrayData(
            listType, length, nullRows, 0, [listValidity.Build(), offsets.Build()], [elementStructArray.Data]);
        var resultList = new ListArray(listData);

        return new RecordBatch(processParams.OutputSchema, [resultList], length);
    }

    private static ArrowBuffer AllValidBuffer(int length)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        for (var i = 0; i < length; i++)
        {
            b.Append(true);
        }

        return b.Build();
    }

    /// <summary>Navigates from a tensor column's row down through <paramref name="idx"/>'s
    /// nested-list levels to the leaf scalar value — <c>null</c> at any missing/short/null level
    /// (a sparse cell).</summary>
    private static object? WalkTensor(IArrowArray tensorColumn, int row, int[] idx)
    {
        if (tensorColumn is not ListArray rowList || row >= rowList.Length || rowList.IsNull(row))
        {
            return null;
        }

        IArrowArray current = rowList.GetSlicedValues(row);
        for (var level = 0; level < idx.Length; level++)
        {
            var i = idx[level];
            if (level == idx.Length - 1)
            {
                return i < current.Length && !current.IsNull(i) ? ScalarArgCodec.ReadScalar(current, i) : null;
            }

            if (current is not ListArray inner || i >= inner.Length || inner.IsNull(i))
            {
                return null;
            }

            current = inner.GetSlicedValues(i);
        }

        return null;
    }

    private static int IndexOfField(StructType type, string name)
    {
        for (var i = 0; i < type.Fields.Count; i++)
        {
            if (type.Fields[i].Name == name)
            {
                return i;
            }
        }

        throw new InvalidOperationException($"unnest_tensor: expected field '{name}'.");
    }

    private sealed record AxisDesc(string Name, IArrowType CoordType);

    private sealed record TensorDesc(IArrowType CellType, IReadOnlyList<AxisDesc> Axes);

    private static TensorDesc Describe(IArrowType type)
    {
        if (type is not StructType s)
        {
            throw new InvalidOperationException("unnest_tensor: argument must be a struct");
        }

        Field? tensorField = null;
        Field? axesField = null;
        foreach (var f in s.Fields)
        {
            if (f.Name == "tensor")
            {
                tensorField = f;
            }
            else if (f.Name == "axes")
            {
                axesField = f;
            }
        }

        if (tensorField is null || axesField is null)
        {
            throw new InvalidOperationException("unnest_tensor: argument struct must have both 'tensor' and 'axes' fields");
        }

        if (axesField.DataType is not StructType axesStruct)
        {
            throw new InvalidOperationException("unnest_tensor: 'axes' must be a struct of coordinate lists");
        }

        var axes = new List<AxisDesc>();
        foreach (var af in axesStruct.Fields)
        {
            if (af.DataType is not ListType lt)
            {
                throw new InvalidOperationException($"unnest_tensor: axis '{af.Name}' must be a list of coordinates");
            }

            axes.Add(new AxisDesc(af.Name, lt.ValueDataType));
        }

        var depth = 0;
        var cellType = tensorField.DataType;
        while (cellType is ListType inner)
        {
            cellType = inner.ValueDataType;
            depth++;
        }

        if (depth != axes.Count)
        {
            throw new InvalidOperationException(
                $"unnest_tensor: tensor nesting depth ({depth}) must match the number of axes ({axes.Count})");
        }

        return new TensorDesc(cellType, axes);
    }
}
