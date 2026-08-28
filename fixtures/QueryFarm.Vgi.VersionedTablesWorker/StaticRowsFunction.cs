using Apache.Arrow;
using QueryFarm.Vgi.Internal;
using QueryFarm.Vgi.Table;
using QueryFarm.VgiRpc.Streaming;

namespace QueryFarm.Vgi.VersionedTablesWorker;

/// <summary>
/// A zero-argument table function that emits one fixed, pre-built <see cref="RecordBatch"/> and
/// finishes — the read path a real catalog table backs itself with (M6's "function-backed table"
/// pattern, see <c>docs/catalog-interface.md</c>): <c>Worker.RegisterCatalogTable</c> both registers
/// the <see cref="QueryFarm.Vgi.Catalog.CatalogTable"/> AND (via <see cref="CatalogTable.ScanFunction"/>)
/// this same zero-arg function, independently callable as <c>schema.name()</c> too.
/// </summary>
public sealed class StaticRowsFunction(string name, string schemaName, RecordBatch data, long? cardinality = null) : ITableFunction
{
    private readonly byte[] _serializedData = RecordBatchIpc.Write(data);

    public string Name => name;

    public string SchemaName => schemaName;

    public Schema ArgumentsSchema { get; } = new([], metadata: null);

    public Schema OutputSchema => data.Schema;

    /// <summary>Best-effort cardinality reported via the per-bind <c>table_function_cardinality</c>
    /// RPC — <see langword="null"/> (the default) unless the caller opts in.</summary>
    public long? Cardinality(TableBindParams bindParams) => cardinality;

    public ITableFunctionProducer CreateProducer(TableInitParams initParams) => new Producer(_serializedData);

    private sealed class Producer(byte[] serializedData) : ITableFunctionProducer
    {
        private bool _emitted;

        public void Produce(OutputCollector output)
        {
            if (!_emitted)
            {
                _emitted = true;
                // OutputCollector.Emit transfers ownership to vgi-rpc. Materialize a fresh batch
                // for every scan so disposing one response cannot invalidate this reusable table.
                output.Emit(RecordBatchIpc.Read(serializedData));
            }

            output.Finish();
        }
    }
}
