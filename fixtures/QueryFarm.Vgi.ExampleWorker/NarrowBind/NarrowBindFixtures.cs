using Apache.Arrow;
using Apache.Arrow.Types;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.ExampleWorker.NarrowBind;

/// <summary>
/// <c>narrow_scan()</c> — the scan function backing the <c>"narrow_bind"</c> catalog's
/// <c>mismatch</c> table (see <c>narrow_bind_mismatch.test</c>). The catalog table declares
/// <c>Columns = {id, val}</c> (2 columns) but this function's OWN bind-time output schema is only
/// <c>{id}</c> (1 column) — a deliberate worker-side inconsistency, simulating the real-world bug
/// this regression test protects against: the C++ extension's <c>GetScanFunctionImpl</c> bind path
/// must detect the mismatch and fail closed with a clear <c>BinderException</c> BEFORE ever
/// reaching scan-time Arrow conversion (which used to walk off the end of the worker's narrower
/// batch and segfault the client). <see cref="CreateProducer"/> is never expected to run — bind
/// should always fail first — so it just throws if somehow reached.
/// </summary>
public sealed class NarrowScanFunction : ITableFunction
{
    public string Name => "narrow_scan";

    public string Description => "Deliberately narrow scan function — binds to {id} only, for narrow_bind_mismatch.test";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    /// <summary>Only ONE column — narrower than the <c>mismatch</c> catalog table's declared
    /// <c>{id, val}</c>, by design.</summary>
    public Schema OutputSchema { get; } = new([new Field("id", Int64Type.Default, nullable: true)], metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) =>
        throw new InvalidOperationException(
            "narrow_scan's producer should never run — the C++ extension must fail the bind before init.");
}

/// <summary>
/// <c>wide_scan()</c> — the scan function backing the <c>"narrow_bind"</c> catalog's
/// <c>consistent</c> table: the positive control for <c>narrow_bind_mismatch.test</c>. Both the
/// catalog's declared <c>Columns</c> and this function's own output schema agree on
/// <c>{id, val}</c>, so every query shape must keep working with no regression. Emits exactly
/// three rows: (0, 10), (1, 20), (2, 30).
/// </summary>
public sealed class WideScanFunction : ITableFunction
{
    public string Name => "wide_scan";

    public string Description => "Consistent scan function — binds to {id, val}, matching the catalog table's declared columns";

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema { get; } = new(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("val", Int64Type.Default, nullable: true),
        ],
        metadata: null);

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams.OutputSchema);

    private sealed class Producer(Schema outputSchema) : ITableFunctionProducer
    {
        private bool _done;

        public void Produce(OutputCollector output)
        {
            if (_done)
            {
                output.Finish();
                return;
            }

            _done = true;
            var idBuilder = new Int64Array.Builder();
            var valBuilder = new Int64Array.Builder();
            foreach (var (id, val) in new[] { (0L, 10L), (1L, 20L), (2L, 30L) })
            {
                idBuilder.Append(id);
                valBuilder.Append(val);
            }

            output.Emit(new RecordBatch(outputSchema, [idBuilder.Build(), valBuilder.Build()], 3));
            output.Finish();
        }
    }
}
