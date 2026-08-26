using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.Table;

/// <summary>
/// Backs <c>data.generated_sequence</c> (<c>table/generated_columns.test</c>). DuckDB requests
/// projected columns by index into the table's FULL declared column list (matching
/// <see cref="Catalog.CatalogTable.Columns"/>, 3 fields here) even for its two GENERATED ones
/// (<c>doubled</c>/<c>label</c>) — this scan's own <see cref="OutputSchema"/> therefore mirrors
/// that same 3-field shape (rather than only the one truly physical column, <c>n</c>) and computes
/// matching values for all three; DuckDB discards whatever it receives for a generated column's
/// slot and recomputes it from the declared expression instead, so correctness here isn't
/// load-bearing — but computing the real values keeps this scan meaningful standalone (it's also
/// independently callable as <c>example.generated_sequence_scan()</c>).
///
/// <para><b>Confirmed NOT reusable as the shared <c>sequence</c> function</b> (unlike
/// <c>numbers</c>/<c>volatile_numbers</c> — see <c>DataSchemaTables.BuildNumbers</c>'s doc comment):
/// tried swapping this table's <c>ScanFunction</c> to the shared <c>sequenceFunction</c> (with
/// <c>InlineScanFunction=false</c> and <c>ScanArguments=[10]</c>, mirroring vgi-python's
/// <c>Table(name="generated_sequence", columns=..., generated_columns=...)</c> — no <c>function=</c>
/// field) — this worker's own C++ extension DOES fetch by index into the FULL 3-column width even
/// over the legacy per-bind <c>table_scan_function_get</c> path, not just the inline fast path: the
/// live error was <c>"VGI function 'sequence' emitted a batch with 1 column(s) but the scan needs
/// column index 1"</c>, i.e. DuckDB genuinely requests physical column index 1 (<c>doubled</c>) from
/// the underlying scan, which <c>sequence</c>'s own 1-column <c>n</c>-only output schema can't
/// satisfy. So despite vgi-python's fixture reusing <c>sequence</c> for this table (confirmed in
/// <c>vgi/_test_fixtures/worker.py</c>), replicating that exactly here regresses
/// <c>table/generated_columns.test</c> — kept as a dedicated function instead. This IS one of
/// <c>table/function_registration.test</c>'s 4-function gap (166→162 roadmap item (2) in
/// <c>Program.cs</c>), just not achievable without also changing how DuckDB is asked to fetch
/// generated columns (a C++-side or python-worker-generation-mismatch question, not something this
/// worker's wire data can route around).</para></summary>
public sealed class GeneratedSequenceScanFunction : ITableFunction
{
    public string Name => "generated_sequence_scan";

    public string SchemaName => "main";

    // Required: DuckDB otherwise expects every emitted batch to be the FULL declared column
    // width (TableInfo.Columns' count) — see the doc comment above.
    public bool? ProjectionPushdown => true;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("n", Int64Type.Default, nullable: true),
            new Field("doubled", Int64Type.Default, nullable: true),
            new Field("label", StringType.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        new Producer(initParams.ProjectionIds ?? [0, 1, 2], initParams.ProjectedSchema);

    private sealed class Producer(IReadOnlyList<long> projectionIds, Schema projectedSchema) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var columns = new List<IArrowArray>();
                foreach (var columnIndex in projectionIds)
                {
                    columns.Add(BuildColumn(columnIndex));
                }

                output.Emit(new RecordBatch(projectedSchema, columns, 10));
            }

            output.Finish();
        }

        private static IArrowArray BuildColumn(long columnIndex)
        {
            if (columnIndex == 2) // label
            {
                var sb = new StringArray.Builder();
                for (var i = 0; i < 10; i++)
                {
                    sb.Append($"item_{i}");
                }

                return sb.Build();
            }

            // n (0) and doubled (1) are both int64.
            var builder = new Int64Array.Builder();
            for (var i = 0; i < 10; i++)
            {
                builder.Append(columnIndex == 1 ? i * 2 : i);
            }

            return builder.Build();
        }
    }
}
