using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.TableInOut;

/// <summary>
/// <c>unnest_tensor_rows(data TABLE) -&gt; TABLE(value ANY, axes STRUCT(axis_1 coord_1, ...))</c> —
/// the table-in-out sibling of the scalar
/// <see cref="ExampleWorker.Scalar.UnnestTensorFunction"/>: same Cartesian-product cell walk, but
/// streamed as one OUTPUT ROW per cell (two top-level columns) rather than one row containing a
/// LIST of cell structs — the shape a LATERAL join needs (<c>table_in_out/unnest_tensor_rows.test</c>).
/// No finalize phase — a function advertising one is REJECTED for correlated LATERAL use (see
/// <see cref="ITableInOutFunction.HasFinalize"/>'s doc comment), and this needs none: each input row
/// (one tensor struct, typically one per LATERAL invocation) is fully resolved into its output rows
/// within a single <see cref="Processor.Process"/> call.
/// </summary>
public sealed class UnnestTensorRowsFunction : ITableInOutFunction
{
    public string Name => "unnest_tensor_rows";

    public string Description => "Invert nest_tensor, streaming one row per cell (LATERAL-friendly)";

    public IReadOnlyList<string> Categories => ["tensor"];

    public Schema ArgumentsSchema { get; } = new([TableArgFields.Table("data")], metadata: null);

    public Schema OutputSchema { get; } = new([], metadata: null);

    public Schema ResolveOutputSchema(TableInOutBindParams bindParams)
    {
        var field = bindParams.InputSchema.FieldsList.FirstOrDefault();
        if (field is null)
        {
            return OutputSchema;
        }

        var desc = Describe(field.DataType);
        var axesFields = desc.Axes.Select(a => new Field(a.Name, a.CoordType, nullable: true)).ToArray();
        return new Schema(
            [
                new Field("value", desc.CellType, nullable: true),
                new Field("axes", new StructType(axesFields), nullable: false),
            ],
            metadata: null);
    }

    public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor(initParams.OutputSchema);

    private sealed class Processor(Schema outputSchema) : ITableInOutProcessor
    {
        public void Process(RecordBatch input, OutputCollector output)
        {
            var valueType = outputSchema.GetFieldByIndex(0).DataType;
            var axesType = (StructType)outputSchema.GetFieldByIndex(1).DataType;
            var axisCount = axesType.Fields.Count;

            var values = new List<object?>();
            var axisValues = axesType.Fields.Select(_ => new List<object?>()).ToList();

            if (input.Column(0) is StructArray tensorColumn)
            {
                for (var row = 0; row < input.Length; row++)
                {
                    if (tensorColumn.IsNull(row))
                    {
                        continue;
                    }

                    var structType = (StructType)tensorColumn.Data.DataType;
                    var tensorIdx = IndexOfField(structType, "tensor");
                    var axesIdx = IndexOfField(structType, "axes");
                    var tensorColumnData = tensorColumn.Fields[tensorIdx];
                    var axesColumn = (StructArray)tensorColumn.Fields[axesIdx];
                    var axesColumnType = (StructType)axesColumn.Data.DataType;

                    var shape = new int[axisCount];
                    var coordsPerAxis = new List<object?>[axisCount];
                    for (var a = 0; a < axisCount; a++)
                    {
                        var srcIdx = IndexOfField(axesColumnType, axesType.Fields[a].Name);
                        var listArr = (ListArray)axesColumn.Fields[srcIdx];
                        var coords = new List<object?>();
                        if (row < listArr.Length && !listArr.IsNull(row))
                        {
                            var sliced = listArr.GetSlicedValues(row);
                            for (var k = 0; k < sliced.Length; k++)
                            {
                                coords.Add(ScalarArgCodec.ReadScalar(sliced, k));
                            }
                        }

                        coordsPerAxis[a] = coords;
                        shape[a] = coords.Count;
                    }

                    var totalCells = shape.Length == 0 ? 0 : shape.Aggregate(1, (acc, s) => acc * s);
                    if (totalCells == 0)
                    {
                        continue;
                    }

                    var idx = new int[shape.Length];
                    for (var cell = 0; cell < totalCells; cell++)
                    {
                        var remainder = cell;
                        for (var a = shape.Length - 1; a >= 0; a--)
                        {
                            idx[a] = shape[a] == 0 ? 0 : remainder % shape[a];
                            remainder /= Math.Max(shape[a], 1);
                        }

                        values.Add(WalkTensor(tensorColumnData, row, idx));
                        for (var a = 0; a < shape.Length; a++)
                        {
                            axisValues[a].Add(coordsPerAxis[a][idx[a]]);
                        }
                    }
                }
            }

            var count = values.Count;
            var valueArray = AnyArrayBuilder.Build(valueType, values);
            var axisArrays = new IArrowArray[axisCount];
            for (var a = 0; a < axisCount; a++)
            {
                axisArrays[a] = AnyArrayBuilder.Build(axesType.Fields[a].DataType, axisValues[a]);
            }

            var axesArray = new StructArray(axesType, count, axisArrays, AllValidBuffer(count));
            output.Emit(new RecordBatch(outputSchema, [valueArray, axesArray], count));
        }
    }

    /// <summary>Navigates from a tensor column's row down through <paramref name="idx"/>'s
    /// nested-list levels to the leaf scalar value — <see langword="null"/> at any missing/short/null
    /// level (a sparse cell). Mirrors <see cref="ExampleWorker.Scalar.UnnestTensorFunction"/>'s own
    /// private helper of the same name.</summary>
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

        throw new InvalidOperationException($"unnest_tensor_rows: expected field '{name}'.");
    }

    private sealed record AxisDesc(string Name, IArrowType CoordType);

    private sealed record TensorDesc(IArrowType CellType, IReadOnlyList<AxisDesc> Axes);

    private static TensorDesc Describe(IArrowType type)
    {
        if (type is not StructType s)
        {
            throw new InvalidOperationException("unnest_tensor_rows: input column must be a tensor struct");
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
            throw new InvalidOperationException("unnest_tensor_rows: input struct must have both 'tensor' and 'axes' fields");
        }

        if (axesField.DataType is not StructType axesStruct)
        {
            throw new InvalidOperationException("unnest_tensor_rows: 'axes' must be a struct of coordinate lists");
        }

        var axes = new List<AxisDesc>();
        foreach (var af in axesStruct.Fields)
        {
            if (af.DataType is not ListType lt)
            {
                throw new InvalidOperationException($"unnest_tensor_rows: axis '{af.Name}' must be a list of coordinates");
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
                $"unnest_tensor_rows: tensor nesting depth ({depth}) must match the number of axes ({axes.Count})");
        }

        return new TensorDesc(cellType, axes);
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
}
