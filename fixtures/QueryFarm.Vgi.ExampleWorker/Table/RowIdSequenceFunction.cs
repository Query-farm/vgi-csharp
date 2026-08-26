using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// <c>rowid_sequence(count [, layout, row_id_type])</c> — a chunked <c>(name, value)</c> generator
/// (<c>item_&lt;i&gt;</c>/<c>val_&lt;i&gt;</c>) whose OWN virtual <c>row_id</c> column (exposed to
/// SQL as DuckDB's special <c>rowid</c> pseudocolumn — see <see cref="VgiRowIdMetadata"/>) can be
/// placed at any of the three column positions and given any of three shapes. Backs
/// <c>table/rowid.test</c>: the <c>data.rowid_first/middle/last/string/struct</c> catalog tables
/// each reuse this SAME function instance with fixed <see cref="Catalog.CatalogTable.ScanArguments"/>/
/// <see cref="Catalog.CatalogTable.ScanNamedArguments"/> (mirroring <c>large_sequence</c>'s
/// positional-args pattern), so this class alone answers every variant.
/// </summary>
public sealed class RowIdSequenceFunction : ITableFunction
{
    private static readonly string[] LayoutChoices = ["first", "middle", "last"];
    private static readonly string[] RowIdTypeChoices = ["int64", "string", "struct"];

    public string Name => "rowid_sequence";

    public string Description => "Sequence with a repositionable, retypeable row_id virtual column";

    // Required: row_id is a virtual column (DuckDB rejects one without projection pushdown).
    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new(
        [
            TableArgFields.Positional("count", Int64Type.Default),
            TableArgFields.Named("layout", StringType.Default),
            TableArgFields.Named("row_id_type", StringType.Default),
        ],
        metadata: null);

    public Schema OutputSchema { get; } = BuildSchema("first", "int64");

    public void Bind(TableBindParams bindParams) => Validate(bindParams.Arguments);

    public Schema ResolveOutputSchema(TableBindParams bindParams) => BuildSchema(
        bindParams.Arguments.StringNamed("layout", "first"),
        bindParams.Arguments.StringNamed("row_id_type", "int64"));

    public ITableFunctionProducer CreateProducer(TableInitParams initParams)
    {
        Validate(initParams.Arguments);
        var count = initParams.Arguments.Int64(0);
        var layout = initParams.Arguments.StringNamed("layout", "first");
        var rowIdType = initParams.Arguments.StringNamed("row_id_type", "int64");
        var indices = initParams.ProjectionIds
            ?? Enumerable.Range(0, initParams.OutputSchema.FieldsList.Count).Select(i => (long)i).ToList();
        return new Producer(count, layout, rowIdType, indices, initParams.ProjectedSchema);
    }

    /// <summary>Builds the 3-field <c>(row_id, name, value)</c> schema (in whichever order
    /// <paramref name="layout"/> selects) — a PUBLIC helper so <c>Program.cs</c> can precompute each
    /// <c>data.rowid_*</c> catalog table's explicit <see cref="Catalog.CatalogTable.Columns"/>
    /// (the declarative table-listing path resolves columns from the STATIC
    /// <see cref="OutputSchema"/>, not a per-args bind, so each variant needs its own explicit
    /// schema — see this function's class doc comment).</summary>
    public static Schema BuildSchema(string layout, string rowIdType)
    {
        IArrowType rowIdArrowType = rowIdType switch
        {
            "string" => StringType.Default,
            "struct" => new StructType(
            [
                new Field("a", Int64Type.Default, nullable: true),
                new Field("b", StringType.Default, nullable: true),
            ]),
            _ => Int64Type.Default,
        };
        var rowIdField = new Field(
            "row_id", rowIdArrowType, nullable: true,
            new Dictionary<string, string> { [VgiRowIdMetadata.Key] = VgiRowIdMetadata.Value });
        var nameField = new Field("name", StringType.Default, nullable: true);
        var valueField = new Field("value", StringType.Default, nullable: true);

        return layout switch
        {
            "middle" => new Schema([nameField, rowIdField, valueField], metadata: null),
            "last" => new Schema([nameField, valueField, rowIdField], metadata: null),
            _ => new Schema([rowIdField, nameField, valueField], metadata: null), // "first" (default)
        };
    }

    private static void Validate(TableArguments args)
    {
        RequireChoice(args, "layout", LayoutChoices, "first");
        RequireChoice(args, "row_id_type", RowIdTypeChoices, "int64");
    }

    private static void RequireChoice(TableArguments args, string name, IReadOnlyList<string> choices, string defaultValue)
    {
        var value = args.StringNamed(name, defaultValue);
        if (!choices.Contains(value, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                $"Argument '{name}' with value '{value}' must be one of the allowed choices: {string.Join(", ", choices)}");
        }
    }

    private sealed class Producer(
        long count, string layout, string rowIdType, IReadOnlyList<long> projectionIds, Schema projectedSchema)
        : ITableFunctionProducer
    {
        private const int BatchSize = 2048;
        private long _next;

        public void Produce(OutputCollector output)
        {
            if (_next >= count)
            {
                output.Finish();
                return;
            }

            var rows = (int)Math.Min(BatchSize, count - _next);
            var start = _next;
            _next += rows;

            var columns = projectionIds.Select(index => BuildColumn(FieldNameAt((int)index), start, rows)).ToList();
            output.Emit(new RecordBatch(projectedSchema, columns, rows));
            if (_next >= count)
            {
                output.Finish();
            }
        }

        private string FieldNameAt(int layoutIndex) => layout switch
        {
            "middle" => layoutIndex switch { 0 => "name", 1 => "row_id", _ => "value" },
            "last" => layoutIndex switch { 0 => "name", 1 => "value", _ => "row_id" },
            _ => layoutIndex switch { 0 => "row_id", 1 => "name", _ => "value" },
        };

        private IArrowArray BuildColumn(string fieldName, long start, int rows) => fieldName switch
        {
            "name" => BuildStringColumn(i => $"item_{start + i}", rows),
            "value" => BuildStringColumn(i => $"val_{start + i}", rows),
            _ => BuildRowIdColumn(start, rows),
        };

        private static StringArray BuildStringColumn(Func<long, string> format, int rows)
        {
            var builder = new StringArray.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(format(i));
            }

            return builder.Build();
        }

        private IArrowArray BuildRowIdColumn(long start, int rows) => rowIdType switch
        {
            "string" => BuildStringColumn(i => $"rid_{start + i}", rows),
            "struct" => BuildStructRowIdColumn(start, rows),
            _ => BuildInt64RowIdColumn(start, rows),
        };

        private static Int64Array BuildInt64RowIdColumn(long start, int rows)
        {
            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(start + i);
            }

            return builder.Build();
        }

        private static StructArray BuildStructRowIdColumn(long start, int rows)
        {
            var aBuilder = new Int64Array.Builder();
            var bBuilder = new StringArray.Builder();
            for (var i = 0; i < rows; i++)
            {
                aBuilder.Append(start + i);
                bBuilder.Append($"s_{start + i}");
            }

            var structType = new StructType(
            [
                new Field("a", Int64Type.Default, nullable: true),
                new Field("b", StringType.Default, nullable: true),
            ]);
            return new StructArray(structType, rows, [aBuilder.Build(), bBuilder.Build()], ArrowBuffer.Empty, nullCount: 0);
        }
    }
}
