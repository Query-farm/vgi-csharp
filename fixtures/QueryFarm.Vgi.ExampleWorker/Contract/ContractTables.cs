using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Catalog;
using QueryFarm.Vgi.Protocol;
using QueryFarm.Vgi.Table;
using QueryFarm.Vgi.TableInOut;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Contract;

/// <summary>
/// The cross-SDK <c>example.contract</c> schema (vgi <c>visibility/example_table_only.test</c>,
/// <c>write_contract/example_insert_finalize.test</c>, <c>write_contract/example_decimal_width.test</c>).
/// Every scan is table_only (no <see cref="CatalogTable.ExposeScan"/>, never also
/// <c>RegisterTable</c>'d); the schema carries no comment. Nothing is stored: the insert functions
/// validate, count and discard.
/// </summary>
public static class ContractTables
{
    public const string SchemaName = "contract";

    public static IEnumerable<CatalogTable> All =>
    [
        new CatalogTable { Name = "good", SchemaName = SchemaName, ScanFunction = new GoodScanFunction(), RowIdColumn = "rowid" },
        new CatalogTable { Name = "ranged", SchemaName = SchemaName, ScanFunction = new RangedScanFunction() },
        new CatalogTable
        {
            Name = "notes",
            SchemaName = SchemaName,
            ScanFunction = new NotesScanFunction(),
            Constraints = ConstraintRecord.For([SchemaName], "notes").NotNull("id", id: "notes_id_not_null").Build(),
            WriteResultModes = new Dictionary<string, string> { [WriteOperation.Insert] = WriteResultMode.Count },
            InsertFunction = new NotesInsertFunction(),
        },
        new CatalogTable
        {
            Name = "amounts",
            SchemaName = SchemaName,
            ScanFunction = new AmountsScanFunction(),
            WriteResultModes = new Dictionary<string, string> { [WriteOperation.Insert] = WriteResultMode.Rows },
            InsertFunction = new AmountsInsertFunction(),
        },
    ];

    internal static readonly Schema NotesColumns = new(
        [new Field("id", Int64Type.Default, nullable: false), new Field("note", StringType.Default, nullable: true)],
        metadata: null);

    internal static readonly Schema AmountsColumns = new(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("d32", new Decimal32Type(7, 2), nullable: true),
            new Field("d64", new Decimal64Type(15, 3), nullable: true),
            new Field("d128", new Decimal128Type(9, 2), nullable: true),
        ],
        metadata: null);

    internal static Schema WriteArguments { get; } = new(
        [TableArgFields.Table("data"), TableArgFields.Named("write_options", BinaryType.Default)], metadata: null);

    internal static RecordBatch EmptyCountBatch() =>
        new(WriteResults.CountSchema, [new Int64Array.Builder().Build()], 0);

    /// <summary>A zero-argument scan emitting <paramref name="build"/>'s rows once, narrowed to the
    /// projected columns when the scan advertises projection pushdown.</summary>
    public abstract class FixedScan : ITableFunction
    {
        public abstract string Name { get; }

        public string SchemaName => ContractTables.SchemaName;

        public Schema ArgumentsSchema { get; } = new([], metadata: null);

        public abstract Schema OutputSchema { get; }

        public FunctionStability? Stability => FunctionStability.Volatile;

        public virtual bool? ProjectionPushdown => null;

        protected abstract RecordBatch? Rows();

        public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(this, initParams);

        private sealed class Producer(FixedScan scan, TableInitParams initParams) : ITableFunctionProducer
        {
            private bool _emitted;

            public void Produce(OutputCollector output)
            {
                if (!_emitted && scan.Rows() is { } full)
                {
                    _emitted = true;
                    var projected = initParams.ProjectedSchema;
                    output.Emit(new RecordBatch(
                        projected,
                        projected.FieldsList.Select(f => full.Column(full.Schema.GetFieldIndex(f.Name))),
                        full.Length));
                    return;
                }

                output.Finish();
            }
        }
    }

    /// <summary><c>contract.good</c>: (id, label) plus a row-id column equal to id.</summary>
    public sealed class GoodScanFunction : FixedScan
    {
        public override string Name => "contract_good_scan";

        public override bool? ProjectionPushdown => true;

        public override Schema OutputSchema { get; } = new(
            [
                new Field("id", Int64Type.Default, nullable: true),
                new Field("label", StringType.Default, nullable: true),
                WriteResults.RowIdField("rowid"),
            ],
            metadata: null);

        protected override RecordBatch Rows() => new(
            OutputSchema,
            [
                new Int64Array.Builder().AppendRange([1, 2, 3]).Build(),
                new StringArray.Builder().AppendRange(["a", "b", "c"]).Build(),
                new Int64Array.Builder().AppendRange([1, 2, 3]).Build(),
            ],
            3);
    }

    /// <summary><c>contract.ranged</c>: a table_only scan named exactly <c>range</c>, so the
    /// worker's function binds rather than DuckDB's own <c>range()</c>.</summary>
    public sealed class RangedScanFunction : FixedScan
    {
        public override string Name => "range";

        public override Schema OutputSchema { get; } = new([new Field("who", StringType.Default, nullable: true)], metadata: null);

        protected override RecordBatch Rows() =>
            new(OutputSchema, [new StringArray.Builder().Append("vgi example worker").Build()], 1);
    }

    /// <summary><c>contract.notes</c>'s scan: always empty (inserts are discarded).</summary>
    public sealed class NotesScanFunction : FixedScan
    {
        public override string Name => "contract_notes_scan";

        public override Schema OutputSchema => NotesColumns;

        protected override RecordBatch? Rows() => null;
    }

    /// <summary><c>contract.amounts</c>'s scan: three fixed rows at the declared decimal widths.</summary>
    public sealed class AmountsScanFunction : FixedScan
    {
        public override string Name => "contract_amounts_scan";

        public override Schema OutputSchema => AmountsColumns;

        protected override RecordBatch Rows() => new(
            OutputSchema,
            [
                new Int64Array.Builder().AppendRange([1, 2, 3]).Build(),
                new Decimal32Array.Builder(new Decimal32Type(7, 2)).Append(12345.67m).Append(-99999.99m).AppendNull().Build(),
                new Decimal64Array.Builder(new Decimal64Type(15, 3))
                    .Append(-123456789012.345m).Append(999999999999.999m).Append(0.001m).Build(),
                new Decimal128Array.Builder(new Decimal128Type(9, 2)).Append(1234567.89m).Append(-9999999.99m).AppendNull().Build(),
            ],
            3);
    }

    /// <summary>
    /// <c>contract_notes_insert</c>: count-only, with a finalize stage that reports the call's total.
    /// Each input batch must arrive with every field nullable and no NULL id (the worker enforces the
    /// table's NOT NULL); it is counted and answered with an empty count batch. The running total is
    /// a processor field: the SDK keeps the INPUT-phase processor for its FINALIZE on every
    /// transport (one <c>VgiServiceImpl</c> per server over HTTP).
    /// </summary>
    public sealed class NotesInsertFunction : ITableInOutFunction
    {
        public string Name => "contract_notes_insert";

        public string SchemaName => ContractTables.SchemaName;

        public Schema ArgumentsSchema => WriteArguments;

        public Schema OutputSchema => WriteResults.CountSchema;

        public FunctionStability? Stability => FunctionStability.Volatile;

        public bool HasFinalize => true;

        public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) => new Processor();

        private sealed class Processor : ITableInOutProcessor
        {
            private long _count;
            private bool _finalized;

            public void Process(RecordBatch input, OutputCollector output)
            {
                foreach (var field in input.Schema.FieldsList)
                {
                    if (!field.IsNullable)
                    {
                        throw new InvalidOperationException($"contract: notes_insert input field {field.Name} is non-nullable");
                    }
                }

                if (input.Column(input.Schema.GetFieldIndex("id")).NullCount > 0)
                {
                    throw new InvalidOperationException("contract: notes.id required");
                }

                _count += input.Length;
                output.Emit(EmptyCountBatch());
            }

            public void Finalize(OutputCollector output)
            {
                if (_finalized)
                {
                    output.Finish();
                    return;
                }

                _finalized = true;
                output.Emit(WriteResults.CountBatch(_count));
                _count = 0;
            }
        }
    }

    /// <summary>
    /// <c>contract_amounts_insert</c>: checks each column arrives at exactly its declared Arrow type
    /// (decimal bit width included), then answers with the count, or with the received rows at the
    /// declared types for RETURNING.
    /// </summary>
    public sealed class AmountsInsertFunction : ITableInOutFunction
    {
        public string Name => "contract_amounts_insert";

        public string SchemaName => ContractTables.SchemaName;

        public Schema ArgumentsSchema => WriteArguments;

        public Schema OutputSchema => AmountsColumns;

        public FunctionStability? Stability => FunctionStability.Volatile;

        public Schema ResolveOutputSchema(TableInOutBindParams bindParams) =>
            WriteResults.Schema(WriteRequest.From(bindParams.Arguments).ResultMode, AmountsColumns);

        public ITableInOutProcessor CreateProcessor(TableInOutInitParams initParams) =>
            new Processor(WriteRequest.From(initParams.Arguments).ResultMode);

        private sealed class Processor(string mode) : ITableInOutProcessor
        {
            public void Process(RecordBatch input, OutputCollector output)
            {
                foreach (var field in input.Schema.FieldsList)
                {
                    var declared = AmountsColumns.GetFieldByName(field.Name)?.DataType;
                    if (declared is null || Describe(declared) != Describe(field.DataType))
                    {
                        throw new InvalidOperationException(
                            $"contract: amounts.{field.Name} arrived as {Describe(field.DataType)}, declared {(declared is null ? "nothing" : Describe(declared))}");
                    }
                }

                if (mode == WriteResultMode.Count)
                {
                    output.Emit(WriteResults.CountBatch(input.Length));
                    return;
                }

                output.Emit(new RecordBatch(
                    AmountsColumns,
                    AmountsColumns.FieldsList.Select(f => input.Column(input.Schema.GetFieldIndex(f.Name))),
                    input.Length));
            }
        }

        private static string Describe(IArrowType type) => type switch
        {
            Decimal32Type d => $"decimal32({d.Precision}, {d.Scale})",
            Decimal64Type d => $"decimal64({d.Precision}, {d.Scale})",
            Decimal128Type d => $"decimal128({d.Precision}, {d.Scale})",
            Decimal256Type d => $"decimal256({d.Precision}, {d.Scale})",
            _ => type.Name,
        };
    }
}
