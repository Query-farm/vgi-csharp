using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>constant_columns(count, *values)</c> — an ANY-typed varargs generator: each vararg value
/// (of ANY Arrow type, including arbitrarily nested list/struct/map) becomes an output column
/// (<c>col_0</c>, <c>col_1</c>, ...) broadcast to <c>count</c> rows. The dynamic output schema is
/// resolved from <see cref="TableBindParams.InputSchema"/> (the concrete per-call argument types
/// DuckDB resolved for the ANY-typed varargs — mirrors the scalar ANY-typed fixtures'
/// <see cref="Scalar.IScalarFunction.ResolveOutputSchema"/> pattern). Backs
/// <c>constant_columns.test</c>/<c>constant_columns_types.test</c>.
/// </summary>
public sealed class ConstantColumnsFunction : ITableFunction
{
    public string Name => "constant_columns";

    public string Description => "Generates rows with constant values from varargs";

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.AnyVarargs("values"),
        ],
        metadata: null);

    /// <summary>Placeholder — the function is entirely dynamic-output; see
    /// <see cref="ResolveOutputSchema"/>.</summary>
    public Schema OutputSchema { get; } = new([], metadata: null);

    /// <summary>Derives the output schema straight from the ACTUAL decoded argument values
    /// (<see cref="TableBindParams.Arguments"/>) rather than <see cref="TableBindParams.InputSchema"/>:
    /// each vararg's own Arrow array already carries its concrete (possibly deeply nested) resolved
    /// type — <c>InputSchema</c> empirically only reflects the function's DECLARED argument shape
    /// (count + one <c>vgi_type=any/vgi_varargs=true</c> sentinel field), not one field per actual
    /// vararg passed at the call site.</summary>
    public Schema ResolveOutputSchema(TableBindParams bindParams)
    {
        var varargCount = bindParams.Arguments.PositionalCount - 1;
        if (varargCount <= 0)
        {
            return OutputSchema;
        }

        var fields = new List<Field>(varargCount);
        for (var i = 0; i < varargCount; i++)
        {
            var array = bindParams.Arguments.PositionalArray(i + 1)
                ?? throw new InvalidOperationException($"constant_columns: missing value for vararg {i}.");

            // Preserve the wire struct field's own metadata (e.g. an ARROW:extension:name
            // annotation DuckDB attaches to an exotic constant like HUGEINT/UUID under
            // arrow_lossless_conversion) so the value round-trips as its ORIGINAL type instead of
            // its raw physical storage (a HUGEINT constant is physically fixed_size_binary(16) —
            // without the extension annotation it comes back as a BLOB of raw bytes).
            var metadata = bindParams.Arguments.PositionalMetadata(i + 1);
            fields.Add(new Field($"col_{i}", array.Data.DataType, nullable: true, metadata));
        }

        return new Schema(fields, metadata: null);
    }

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        var count = initParams.Arguments.Int64(0);
        var columnCount = initParams.OutputSchema.FieldsList.Count;
        var values = new IArrowArray[columnCount];
        for (var i = 0; i < columnCount; i++)
        {
            values[i] = initParams.Arguments.PositionalArray(i + 1)
                ?? throw new InvalidOperationException($"constant_columns: missing value for vararg {i}.");
        }

        return new Producer(count, values, initParams.OutputSchema);
    }

    private sealed class Producer(long count, IReadOnlyList<IArrowArray> values, Schema outputSchema) : ITableFunctionProducer
    {
        private const int BatchSize = 1000;
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= count)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, count - _next);
            _next += rows;

            var columns = values.Select(v => Broadcast(v, rows)).ToList();
            output.Emit(new RecordBatch(outputSchema, columns, rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }

        /// <summary>Repeats a single-row Arrow value <paramref name="rows"/> times — generic over
        /// ANY Arrow type (including nested list/struct/map) via
        /// <see cref="ArrowArrayConcatenator.Concatenate"/> rather than a type-switched builder,
        /// since a vararg's resolved type isn't known until bind time and may be arbitrarily
        /// nested.</summary>
        private static IArrowArray Broadcast(IArrowArray oneRow, int rows)
        {
            // DictionaryArray (an ENUM constant, e.g. 'happy'::my_enum) needs special handling:
            // the vendored ArrayDataConcatenator has no IArrowTypeVisitor<DictionaryType> — since
            // DictionaryType IS-A FixedWidthType, concatenating it directly dispatches to the
            // plain FixedWidthType visitor, which builds a NEW ArrayData from only the index
            // buffer and drops the dictionary VALUES array reference entirely (ArrayData's
            // Dictionary field is left null), so the resulting batch decodes as empty. Work around
            // it by concatenating only the (plain, non-dictionary) INDEX array — which the
            // existing FixedWidthType visitor handles correctly — and re-wrapping the result with
            // the ORIGINAL single dictionary values array, which a broadcast never needs to change.
            if (oneRow is DictionaryArray dict)
            {
                var indices = BroadcastPlain(dict.Indices, rows);
                return new DictionaryArray((DictionaryType)dict.Data.DataType, indices, dict.Dictionary);
            }

            return BroadcastPlain(oneRow, rows);
        }

        private static IArrowArray BroadcastPlain(IArrowArray oneRow, int rows)
        {
            var repeated = new IArrowArray[rows];
            System.Array.Fill(repeated, oneRow);
            return ArrowArrayConcatenator.Concatenate(repeated);
        }
    }
}
