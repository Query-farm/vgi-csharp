using Apache.Arrow;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.SimpleWritableWorker;

/// <summary>Zero-argument read path for a writable table — emits every currently-stored row
/// (FULL schema, including the hidden <see cref="WritableTableFixture.RowIdColumn"/> DuckDB needs
/// for UPDATE/DELETE targeting) as one batch per call. Independently registered under the table's
/// own name (see <see cref="Catalog.CatalogTable.ScanFunction"/>'s doc comment) so
/// <c>schema.table_name</c> resolves it via <see cref="Protocol.TableInfo.ScanFunction"/>.
///
/// MUST advertise <see cref="ProjectionPushdown"/>: the hidden row-id is a DuckDB "virtual column"
/// (present in the table's schema but excluded from <c>SELECT *</c>) — DuckDB refuses to plan a scan
/// that might need to materialize a virtual column from a function that doesn't support projection
/// pushdown ("Virtual columns require projection pushdown"), which UPDATE/DELETE's own row-locating
/// scan always does (it specifically projects the row-id column alongside the WHERE-clause's
/// referenced columns).</summary>
public sealed class RowStoreScanFunction(string name, Schema fullSchema, RowStore store) : ITableFunction
{
    public string Name => name;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema => fullSchema;

    public bool? ProjectionPushdown => true;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(initParams, store);

    private sealed class Producer(TableInitParams initParams, RowStore store) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                var rows = store.ScanAll(initParams.AttachOpaqueData).Select(batch => RowCodec.ReadRow(batch, 0)).ToList();
                output.Emit(RowCodec.BuildBatch(initParams.ProjectedSchema, rows));
            }

            output.Finish();
        }
    }
}
